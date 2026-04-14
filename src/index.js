const axios = require("axios");
const http = require("http");

let tokenReminder;
try { tokenReminder = require("./tokenReminder"); }
catch { tokenReminder = require("../tokenReminder"); }
const { startTokenReminder, registerTestTokenCommand, handleTestTokenSlash } = tokenReminder;

const { Client, GatewayIntentBits } = require("discord.js");
const { DateTime } = require("luxon");
const { mustEnv } = require("./utils");
const { getClients } = require("./google");
const { createMediaHandler, prefetchFile, clearBufferedFiles } = require("./media-server");

const {
  parseFolderIdFromUrl, getFolderName, deriveSkuFromFolderName,
  listMediaFiles, listChildFolders, driveDirectDownloadUrl
} = require("./drive");

const { parseVnDatetime, appendJob, fetchAllJobs, updateRow, nowVn } = require("./queue");

const {
  igCreateMediaContainerWithRetry, igCreateCarouselContainer,
  igPublishWithRetry, igGetPermalink,
  waitUntilFinished, waitAllUntilFinished, isRateLimitError
} = require("./ig");

const DISCORD_TOKEN = mustEnv("DISCORD_TOKEN");
const QUEUE_SHEET_ID = mustEnv("SHEET_ID_QUEUE");
const IG_SORTER_WEB_APP_URL = process.env.IG_SORTER_WEB_APP_URL || "";

const DELAY_BETWEEN_JOBS_MS = 5000;
const COOLDOWN_EVERY_N_JOBS = 5;
const COOLDOWN_MS = 30000;
const DELAY_BETWEEN_CHILDREN_MS = 500;
const MAX_AUTO_RETRIES = 3;
const RETRY_BACKOFF_MS = 10 * 1000;

function optEnv(name) {
  const v = process.env[name];
  return v && String(v).trim() ? String(v).trim() : null;
}

// ===== Shop config =====
const SHOP = {
  MAUME: {
    name: "Màu mè",
    igUserId: mustEnv("IG_USER_ID_MAUME"),
    pageToken: mustEnv("FB_PAGE_TOKEN_MAUME"),
    sheetId: mustEnv("SHEET_ID_MAUME"),
    sheetTab: null, sheetRange: "E:L",
    captionColIndexInRange: 0, codeColIndexInRange: 7, khoStatusCol: "B"
  },
  BURGER: {
    name: "Burger",
    igUserId: mustEnv("IG_USER_ID_BURGER"),
    pageToken: mustEnv("FB_PAGE_TOKEN_BURGER"),
    sheetId: mustEnv("SHEET_ID_BURGER"),
    sheetTab: null, sheetRange: "F:I",
    captionColIndexInRange: 3, codeColIndexInRange: 0, khoStatusCol: "D"
  }
};

const testIgUserId = optEnv("IG_USER_ID_TEST");
const testPageToken = optEnv("FB_PAGE_TOKEN_TEST");
const testSheetId = optEnv("SHEET_ID_TEST");
if (testIgUserId && testPageToken && testSheetId) {
  SHOP.TEST = {
    name: "Test", igUserId: testIgUserId, pageToken: testPageToken,
    sheetId: testSheetId, sheetTab: null, sheetRange: "E:L",
    captionColIndexInRange: 0, codeColIndexInRange: 7, khoStatusCol: "B"
  };
  console.log("[CONFIG] TEST shop enabled");
} else {
  console.log("[CONFIG] TEST shop not configured");
}

// ===== SKU lookup =====
const TAB_CACHE_TTL_MS = 10 * 60 * 1000;
const tabCache = new Map();

function canonSku(s) {
  return String(s ?? "").trim().normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[đĐ]/g, m => m === "đ" ? "d" : "D")
    .replace(/[^A-Za-z0-9]/g, "").toUpperCase();
}

function tabIsLikelyInventory(title) {
  const up = String(title || "").trim().toUpperCase();
  const deny = new Set(["QUEUE","DASHBOARD","CONFIG","README","LOG","SETTING","SETTINGS"]);
  if (deny.has(up)) return false;
  if (String(title).startsWith("_")) return false;
  return true;
}

function sortTabsNewestFirst(titles) {
  return [...titles].sort((a, b) => b.localeCompare(a, "en", { numeric: true, sensitivity: "base" }));
}

async function getTabTitles(sheets, spreadsheetId) {
  const cached = tabCache.get(spreadsheetId);
  if (cached && Date.now() - cached.ts < TAB_CACHE_TTL_MS) return cached.titles;
  const meta = await sheets.spreadsheets.get({ spreadsheetId, fields: "sheets(properties(title))" });
  const titles = (meta.data.sheets || []).map(s => s.properties?.title).filter(Boolean);
  tabCache.set(spreadsheetId, { ts: Date.now(), titles });
  return titles;
}

async function findCaptionBySku({ sheets, shopKey, sku }) {
  const cfg = SHOP[shopKey];
  if (!cfg) throw new Error(`Unknown shop: ${shopKey}`);
  const targetCanon = canonSku(sku);
  const allTitles = await getTabTitles(sheets, cfg.sheetId);
  const titles = cfg.sheetTab ? [cfg.sheetTab] : sortTabsNewestFirst(allTitles.filter(tabIsLikelyInventory));
  const ranges = titles.map(t => `${t}!${cfg.sheetRange}`);

  const CHUNK = 80;
  for (let i = 0; i < ranges.length; i += CHUNK) {
    const chunkRanges = ranges.slice(i, i + CHUNK);
    const chunkTitles = titles.slice(i, i + CHUNK);
    const resp = await sheets.spreadsheets.values.batchGet({
      spreadsheetId: cfg.sheetId, ranges: chunkRanges, majorDimension: "ROWS"
    });
    for (let vrIdx = 0; vrIdx < (resp.data.valueRanges || []).length; vrIdx++) {
      const rows = resp.data.valueRanges[vrIdx].values || [];
      const tabName = chunkTitles[vrIdx];
      for (let rowIdx = 0; rowIdx < rows.length; rowIdx++) {
        const row = rows[rowIdx];
        if (canonSku((row[cfg.codeColIndexInRange] ?? "").toString()) === targetCanon) {
          return { caption: (row[cfg.captionColIndexInRange] ?? "").toString(), tabName, rowNum: rowIdx + 1 };
        }
      }
    }
  }
  return null;
}

// ===== Kho update =====
const khoValidationCache = new Map();
const KHO_VALIDATION_CACHE_TTL_MS = 30 * 60 * 1000;
const POSTED_KEYWORDS = ["đã đăng","da dang","đăng rồi","dang roi","done","posted","đã up","da up"];

async function readCellDropdownValues({ sheets, spreadsheetId, tabName, col, rowNum }) {
  const cacheKey = `${spreadsheetId}:${tabName}:${col}`;
  const cached = khoValidationCache.get(cacheKey);
  if (cached && Date.now() - cached.ts < KHO_VALIDATION_CACHE_TTL_MS) return cached.values;
  const resp = await sheets.spreadsheets.get({
    spreadsheetId, ranges: [`${tabName}!${col}${rowNum}`],
    fields: "sheets.data.rowData.values.dataValidation"
  });
  const validation = resp.data.sheets?.[0]?.data?.[0]?.rowData?.[0]?.values?.[0]?.dataValidation;
  if (!validation || validation.condition?.type !== "ONE_OF_LIST") {
    khoValidationCache.set(cacheKey, { ts: Date.now(), values: null });
    return null;
  }
  const values = (validation.condition.values || []).map(v => v.userEnteredValue).filter(Boolean);
  khoValidationCache.set(cacheKey, { ts: Date.now(), values });
  return values;
}

function pickPostedValue(dropdownValues) {
  if (!dropdownValues?.length) return null;
  function norm(s) { return String(s).trim().toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g,"").replace(/[đĐ]/g,"d"); }
  for (const kw of POSTED_KEYWORDS) {
    for (const val of dropdownValues) { if (norm(val).includes(norm(kw))) return { value: val, matched: kw }; }
  }
  return null;
}

async function updateKhoPostStatus({ sheets, shopKey, tabName, rowNum }) {
  const cfg = SHOP[shopKey];
  const col = cfg.khoStatusCol;
  const dropdownValues = await readCellDropdownValues({ sheets, spreadsheetId: cfg.sheetId, tabName, col, rowNum });
  let valueToWrite;
  if (dropdownValues?.length) {
    const pick = pickPostedValue(dropdownValues);
    if (pick) valueToWrite = pick.value;
    else throw new Error(`Không tìm thấy "đã đăng" trong dropdown [${dropdownValues.join(", ")}]`);
  } else {
    valueToWrite = "Đã đăng";
  }
  await sheets.spreadsheets.values.update({
    spreadsheetId: cfg.sheetId, range: `${tabName}!${col}${rowNum}`,
    valueInputOption: "RAW", requestBody: { values: [[valueToWrite]] }
  });
  console.log(`[KHO] ${cfg.name}: ${tabName}!${col}${rowNum} = "${valueToWrite}"`);
  return valueToWrite;
}

function isVideoName(name) { return /\.mp4$/i.test(name || ""); }

function prioritizeVideosFirst(mediaFiles) {
  const videos = [], images = [];
  for (const f of mediaFiles) { if (isVideoName(f.name)) videos.push(f); else images.push(f); }
  return videos.length ? [...videos, ...images] : mediaFiles;
}

// ===== Prefetch =====
async function prefetchAllMedia(drive, mediaFiles) {
  console.log(`[PREFETCH] Downloading ${mediaFiles.length} files...`);
  const CONCURRENCY = 3;
  for (let i = 0; i < mediaFiles.length; i += CONCURRENCY) {
    const batch = mediaFiles.slice(i, i + CONCURRENCY);
    const results = await Promise.allSettled(batch.map(f => prefetchFile(drive, f.id)));
    for (let j = 0; j < results.length; j++) {
      if (results[j].status === "rejected") {
        throw new Error(`Prefetch failed for ${batch[j].name}: ${results[j].reason?.message}`);
      }
    }
  }
  console.log(`[PREFETCH] All ${mediaFiles.length} files ready`);
}

// ===== Publish =====
async function publishJob({ shopKey, caption, mediaFiles, drive }) {
  const cfg = SHOP[shopKey];
  const ordered = prioritizeVideosFirst(mediaFiles);

  // Prefetch tất cả vào RAM
  await prefetchAllMedia(drive, ordered);

  // Collect file IDs để xoá buffer sau
  const fileIds = ordered.map(f => f.id);

  try {
    if (ordered.length === 1) {
      const f = ordered[0];
      const isVideo = isVideoName(f.name);
      const mediaUrl = driveDirectDownloadUrl(f.id);

      const creationId = await igCreateMediaContainerWithRetry({
        igUserId: cfg.igUserId, pageToken: cfg.pageToken,
        imageUrl: isVideo ? null : mediaUrl, videoUrl: isVideo ? mediaUrl : null,
        caption, isCarouselItem: false
      });
      await waitUntilFinished({ creationId, pageToken: cfg.pageToken, isVideo });
      const mediaId = await igPublishWithRetry({ igUserId: cfg.igUserId, pageToken: cfg.pageToken, creationId });
      const permalink = await igGetPermalink({ mediaId, pageToken: cfg.pageToken });
      return { mediaId, permalink };
    }

    // Carousel
    console.log(`[PUBLISH] Creating ${ordered.length} child containers...`);
    const children = [];
    for (let idx = 0; idx < ordered.length; idx++) {
      const f = ordered[idx];
      const isVideo = isVideoName(f.name);
      const mediaUrl = driveDirectDownloadUrl(f.id);
      const childCreationId = await igCreateMediaContainerWithRetry({
        igUserId: cfg.igUserId, pageToken: cfg.pageToken,
        imageUrl: isVideo ? null : mediaUrl, videoUrl: isVideo ? mediaUrl : null,
        caption: null, isCarouselItem: true
      });
      children.push({ creationId: childCreationId, isVideo });
      if (idx < ordered.length - 1) await new Promise(r => setTimeout(r, DELAY_BETWEEN_CHILDREN_MS));
    }

    console.log(`[PUBLISH] Batch polling ${children.length} containers...`);
    await waitAllUntilFinished({ items: children, pageToken: cfg.pageToken });

    const parentCreationId = await igCreateCarouselContainer({
      igUserId: cfg.igUserId, pageToken: cfg.pageToken,
      childrenIds: children.map(c => c.creationId), caption
    });
    await waitUntilFinished({ creationId: parentCreationId, pageToken: cfg.pageToken, isVideo: false });

    const mediaId = await igPublishWithRetry({ igUserId: cfg.igUserId, pageToken: cfg.pageToken, creationId: parentCreationId });
    const permalink = await igGetPermalink({ mediaId, pageToken: cfg.pageToken });
    return { mediaId, permalink };
  } finally {
    // ===== v7: Xoá buffer ngay sau khi xong (dù thành công hay thất bại) =====
    clearBufferedFiles(fileIds);
  }
}

// ===== Rate limit warning =====
const SAFE_JOBS_PER_HOUR = 12;

function countJobsInHourWindow(items, shopKey, targetDt) {
  const hourStart = targetDt.startOf("hour");
  const hourEnd = hourStart.plus({ hours: 1 });
  return items.filter(j => {
    if (j.shop !== shopKey) return false;
    if (!["PENDING", "RUNNING"].includes(j.status) && !j.status.startsWith("RETRYING")) return false;
    const jDt = DateTime.fromISO(j.scheduled_time, { zone: "Asia/Ho_Chi_Minh" });
    return jDt.isValid && jDt >= hourStart && jDt < hourEnd;
  }).length;
}

function findNextAvailableSlot(items, shopKey, startDt) {
  for (let offset = 1; offset <= 6; offset++) {
    const slotStart = startDt.startOf("hour").plus({ hours: offset });
    const count = countJobsInHourWindow(items, shopKey, slotStart);
    if (count < SAFE_JOBS_PER_HOUR) {
      return { hour: slotStart, count };
    }
  }
  return null;
}

function buildRateLimitWarning(items, shopKey, targetDt) {
  const count = countJobsInHourWindow(items, shopKey, targetDt);
  if (count < SAFE_JOBS_PER_HOUR) return null;

  const hourStart = targetDt.startOf("hour");
  const hourEnd = hourStart.plus({ hours: 1 });
  let msg = `⚠️ **Cảnh báo rate limit**: Khung ${hourStart.toFormat("HH:mm")}–${hourEnd.toFormat("HH:mm")} đã có **${count} bài** cho ${SHOP[shopKey].name} (giới hạn an toàn: ${SAFE_JOBS_PER_HOUR} bài/giờ)\n💡 Dùng \`/ig_cancel\` để thu hồi lịch nếu cần`;

  const slot = findNextAvailableSlot(items, shopKey, targetDt);
  if (slot) {
    const slotEnd = slot.hour.plus({ hours: 1 });
    msg += `\n→ Gợi ý: khung **${slot.hour.toFormat("HH:mm")}–${slotEnd.toFormat("HH:mm")}** còn trống (${slot.count} bài)`;
  }
  return msg;
}

// ===== Dedup =====
async function cancelSiblingFailedJobs({ sheets, allItems, currentJob }) {
  const siblings = allItems.filter(j =>
    j.rowNum !== currentJob.rowNum && j.shop === currentJob.shop &&
    canonSku(j.sku) === canonSku(currentJob.sku) &&
    (j.status === "FAILED" || j.status.startsWith("RETRYING"))
  );
  for (const sib of siblings) {
    await updateRow(sheets, {
      queueSheetId: QUEUE_SHEET_ID, rowNum: sib.rowNum,
      patch: { status: "CANCELLED_DUP", last_error: `Dup — succeeded row ${currentJob.rowNum}` }
    }).catch(() => {});
  }
  return siblings.length;
}

let tickRunning = false;

async function tick({ client, sheets, drive }) {
  if (tickRunning) { console.log("[TICK] Skipped"); return; }
  tickRunning = true;

  try {
    const { items } = await fetchAllJobs(sheets, { queueSheetId: QUEUE_SHEET_ID });
    const now = nowVn();

    const successKeys = new Set();
    for (const j of items) { if (j.status === "SUCCESS") successKeys.add(`${j.shop}::${canonSku(j.sku)}`); }

    const due = [], retryable = [], retrySeenKeys = new Set();

    for (const j of items) {
      if (!SHOP[j.shop]) continue;

      if (j.status === "PENDING") {
        const dt = DateTime.fromISO(j.scheduled_time, { zone: "Asia/Ho_Chi_Minh" });
        if (dt.isValid && dt <= now) {
          const key = `${j.shop}::${canonSku(j.sku)}`;
          if (successKeys.has(key)) {
            await updateRow(sheets, { queueSheetId: QUEUE_SHEET_ID, rowNum: j.rowNum,
              patch: { status: "CANCELLED_DUP", last_error: "Already published" } }).catch(() => {});
            continue;
          }
          due.push(j);
        }
      }

      if (j.status === "FAILED" && j.attempts < MAX_AUTO_RETRIES) {
        const key = `${j.shop}::${canonSku(j.sku)}`;
        if (successKeys.has(key)) {
          await updateRow(sheets, { queueSheetId: QUEUE_SHEET_ID, rowNum: j.rowNum,
            patch: { status: "CANCELLED_DUP", last_error: "Already published" } }).catch(() => {});
          continue;
        }
        if (retrySeenKeys.has(key)) continue;
        const scheduledDt = DateTime.fromISO(j.scheduled_time, { zone: "Asia/Ho_Chi_Minh" });
        if (!scheduledDt.isValid) continue;
        if (now >= scheduledDt.plus({ milliseconds: j.attempts * RETRY_BACKOFF_MS })) {
          retryable.push(j);
          retrySeenKeys.add(key);
        }
      }
    }

    const allJobs = [...due, ...retryable];
    if (retryable.length) console.log(`[TICK] ${due.length} due + ${retryable.length} retryable`);

    let consecutiveJobCount = 0;

    for (let jobIdx = 0; jobIdx < allJobs.length; jobIdx++) {
      const job = allJobs[jobIdx];
      const isRetry = job.status === "FAILED";
      const channel = await client.channels.fetch(job.channel_id).catch(() => null);

      try {
        await updateRow(sheets, { queueSheetId: QUEUE_SHEET_ID, rowNum: job.rowNum,
          patch: { status: isRetry ? `RETRYING (${job.attempts+1}/${MAX_AUTO_RETRIES})` : "RUNNING", attempts: job.attempts+1, last_error: "" }
        });

        if (isRetry && channel) {
          await channel.send(`🔄 Auto-retry ${job.attempts+1}/${MAX_AUTO_RETRIES} (${SHOP[job.shop]?.name}) | SKU: **${job.sku}**`);
        }

        const folderName = await getFolderName(drive, job.folder_id);
        const sku = job.sku || deriveSkuFromFolderName(folderName);
        const skuResult = await findCaptionBySku({ sheets, shopKey: job.shop, sku });
        if (!skuResult?.caption) throw new Error(`Không tìm caption SKU=${sku} shop ${job.shop}`);
        const { caption, tabName: khoTab, rowNum: khoRow } = skuResult;
        const mediaFiles = await listMediaFiles(drive, job.folder_id);

        const { mediaId, permalink } = await publishJob({ shopKey: job.shop, caption, mediaFiles, drive });

        await updateRow(sheets, { queueSheetId: QUEUE_SHEET_ID, rowNum: job.rowNum,
          patch: { status: "SUCCESS", attempts: job.attempts+1, ig_media_id: mediaId, ig_permalink: permalink, published_at: nowVn().toISO() }
        });

        const cancelled = await cancelSiblingFailedJobs({ sheets, allItems: items, currentJob: job });
        if (cancelled) console.log(`[DEDUP] Cancelled ${cancelled} dups for SKU=${sku}`);
        successKeys.add(`${job.shop}::${canonSku(sku)}`);

        try {
          const sv = await updateKhoPostStatus({ sheets, shopKey: job.shop, tabName: khoTab, rowNum: khoRow });
          if (channel) {
            const note = isRetry ? " (auto-retry OK)" : "";
            await channel.send(`✅ Thành công${note} (${SHOP[job.shop].name}) | SKU: **${sku}** | ${permalink}\n📋 Kho → **${sv}**`);
          }
        } catch (khoErr) {
          if (channel) await channel.send(`✅ IG OK (${SHOP[job.shop].name}) | ${permalink}\n⚠️ Lỗi kho: ${khoErr.message}`);
        }

        consecutiveJobCount++;

      } catch (e) {
        const msg = (e.response?.data && JSON.stringify(e.response.data)) ? JSON.stringify(e.response.data) : (e.message || String(e));
        const isRL = isRateLimitError(e);
        let newStatus = isRL ? "PENDING" : (job.attempts+1 < MAX_AUTO_RETRIES ? "FAILED" : "GIVE_UP");

        await updateRow(sheets, { queueSheetId: QUEUE_SHEET_ID, rowNum: job.rowNum,
          patch: { status: newStatus, attempts: job.attempts+1, last_error: msg.slice(0,5000) }
        });

        if (channel) {
          if (isRL) await channel.send(`⏳ Rate-limit (${SHOP[job.shop].name}) | SKU: **${job.sku}**\n\`\`\`${msg.slice(0,1800)}\`\`\``);
          else if (newStatus === "FAILED") await channel.send(`⚠️ Lỗi ${job.attempts+1}/${MAX_AUTO_RETRIES}, retry ${RETRY_BACKOFF_MS/1000}s (${SHOP[job.shop].name}) | SKU: **${job.sku}**\n\`\`\`${msg.slice(0,1200)}\`\`\``);
          else await channel.send(`❌ Thất bại ${MAX_AUTO_RETRIES} lần (${SHOP[job.shop].name}) | SKU: **${job.sku}**\n\`\`\`${msg.slice(0,1500)}\`\`\`\n⚠️ **Nhờ đăng tay + cập nhật kho!**`);
        }

        if (isRL) { console.log("[TICK] Rate-limited — stopping"); break; }
      }

      if (jobIdx < allJobs.length - 1) {
        if (consecutiveJobCount > 0 && consecutiveJobCount % COOLDOWN_EVERY_N_JOBS === 0) {
          console.log(`[TICK] Cooldown ${COOLDOWN_MS/1000}s`);
          await new Promise(r => setTimeout(r, COOLDOWN_MS));
        } else {
          await new Promise(r => setTimeout(r, DELAY_BETWEEN_JOBS_MS));
        }
      }
    }
  } finally { tickRunning = false; }
}

async function handleIgCancel(interaction, { sheets }) {
  try {
    await interaction.deferReply();
  } catch { return; }

  try {
    const shopKey = interaction.options.getString("shop", true);
    const sku = interaction.options.getString("sku", true);
    if (!SHOP[shopKey]) throw new Error(`Shop "${shopKey}" chưa cấu hình.`);

    const { items } = await fetchAllJobs(sheets, { queueSheetId: QUEUE_SHEET_ID });
    const targetCanon = canonSku(sku);
    // Also include DRAFT rows so a user who regrets a /ig_folder_schedule batch can
    // clean up without having to open the sorter Web App.
    const pending = items.filter(j =>
      j.shop === shopKey && (j.status === "PENDING" || j.status === "DRAFT") && canonSku(j.sku) === targetCanon
    );

    if (!pending.length) {
      await interaction.editReply(`❌ Không tìm thấy lịch PENDING/DRAFT cho SKU=**${sku}** shop **${SHOP[shopKey].name}**`);
      return;
    }

    for (const job of pending) {
      await updateRow(sheets, {
        queueSheetId: QUEUE_SHEET_ID, rowNum: job.rowNum,
        patch: { status: "CANCELLED", last_error: `Thu hồi bởi ${interaction.user.tag}` }
      });
    }

    const times = pending.map(j => {
      const jDt = DateTime.fromISO(j.scheduled_time, { zone: "Asia/Ho_Chi_Minh" });
      return jDt.isValid ? jDt.toFormat("yyyy-MM-dd HH:mm") : j.scheduled_time;
    });
    await interaction.editReply(`✅ Đã thu hồi **${pending.length}** lịch cho SKU=**${sku}** (${SHOP[shopKey].name})\n- Giờ: ${times.join(", ")}`);
    console.log(`[CMD] /ig_cancel SKU=${sku} shop=${shopKey} → cancelled ${pending.length} jobs by ${interaction.user.tag}`);
  } catch (e) {
    try { await interaction.editReply(`❌ ${e.message}`); } catch {}
  }
}

// ===== /batch-confirm HTTP handler (called by Apps Script sorter) =====
// Reads every row with the given batch_id, groups by channel_id, and sends a summary
// back to each Discord channel that originally created the batch. Auth uses the same
// MEDIA_PROXY_TOKEN the media-server already trusts so no extra env var is required.
async function handleBatchConfirm(req, res, url, { client, sheets }) {
  try {
    if (req.method !== "POST") {
      res.statusCode = 405;
      return res.end("method not allowed");
    }
    const token = process.env.MEDIA_PROXY_TOKEN;
    if (!token || url.searchParams.get("token") !== token) {
      res.statusCode = 403;
      return res.end("forbidden");
    }
    const batchId = url.searchParams.get("batch") || "";
    if (!batchId) {
      res.statusCode = 400;
      return res.end("missing batch");
    }

    const { items } = await fetchAllJobs(sheets, { queueSheetId: QUEUE_SHEET_ID });
    const rows = items.filter(j => j.batch_id === batchId);
    if (!rows.length) {
      res.statusCode = 404;
      return res.end("batch not found");
    }

    // ===== Auto-demote old SUCCESS rows so re-posts via /ig_folder_schedule bypass dedup =====
    // When the user schedules a SKU again to replace a bad Instagram post, the tick loop would
    // normally mark the new row CANCELLED_DUP because an older SUCCESS row exists. Flipping that
    // old row to REPOSTED removes it from the dedup set while preserving its audit trail
    // (ig_media_id / ig_permalink / published_at are passed through, not wiped).
    //
    // Race guard: if tick ran between the sorter flipping DRAFT→PENDING and this handler,
    // some batch rows may already be CANCELLED_DUP("Already published"). We revive those back
    // to PENDING so they participate in the demote pass and get re-scheduled.
    const liveBatchRows = rows.filter(j =>
      j.status === "PENDING" ||
      (j.status === "CANCELLED_DUP" && j.last_error === "Already published")
    );
    const batchSkuKeys = new Set(liveBatchRows.map(r => `${r.shop}::${canonSku(r.sku)}`));
    const batchRowNums = new Set(rows.map(r => r.rowNum));

    const toDemote = items.filter(j =>
      j.status === "SUCCESS" &&
      !batchRowNums.has(j.rowNum) &&
      batchSkuKeys.has(`${j.shop}::${canonSku(j.sku)}`)
    );

    let demotedCount = 0;
    for (const old of toDemote) {
      try {
        await updateRow(sheets, { queueSheetId: QUEUE_SHEET_ID, rowNum: old.rowNum, patch: {
          status: "REPOSTED",
          attempts: old.attempts,
          last_error: `Superseded by batch ${batchId}`,
          ig_media_id: old.ig_media_id,
          ig_permalink: old.ig_permalink,
          published_at: old.published_at
        }});
        old.status = "REPOSTED";
        demotedCount++;
      } catch (e) {
        console.error(`[REPOST] demote failed row=${old.rowNum}: ${e.message}`);
      }
    }

    const toRevive = rows.filter(j => j.status === "CANCELLED_DUP" && j.last_error === "Already published");
    let revivedCount = 0;
    for (const row of toRevive) {
      try {
        await updateRow(sheets, { queueSheetId: QUEUE_SHEET_ID, rowNum: row.rowNum, patch: {
          status: "PENDING",
          attempts: row.attempts,
          last_error: "",
          ig_media_id: row.ig_media_id,
          ig_permalink: row.ig_permalink,
          published_at: row.published_at
        }});
        row.status = "PENDING";
        row.last_error = "";
        revivedCount++;
      } catch (e) {
        console.error(`[REPOST] revive failed row=${row.rowNum}: ${e.message}`);
      }
    }

    if (demotedCount || revivedCount) {
      console.log(`[REPOST] batch=${batchId} demoted=${demotedCount} revived=${revivedCount}`);
    }

    // Group rows by channel so we message each channel once with its own summary.
    const byChannel = new Map();
    for (const r of rows) {
      if (!byChannel.has(r.channel_id)) byChannel.set(r.channel_id, []);
      byChannel.get(r.channel_id).push(r);
    }

    let pendingTotal = 0, cancelledTotal = 0;
    for (const [channelId, group] of byChannel) {
      const pending = group.filter(j => j.status === "PENDING").sort((a, b) => a.scheduled_time.localeCompare(b.scheduled_time));
      const cancelled = group.filter(j => j.status === "CANCELLED");
      pendingTotal += pending.length;
      cancelledTotal += cancelled.length;

      const shopName = pending[0] ? (SHOP[pending[0].shop]?.name || pending[0].shop) : "";
      let msg = `✅ **Batch đã xác nhận** (${shopName})\n`;
      msg += `• **${pending.length}** bài sẽ đăng`;
      if (pending.length) {
        const first = DateTime.fromISO(pending[0].scheduled_time, { zone: "Asia/Ho_Chi_Minh" });
        const last = DateTime.fromISO(pending[pending.length - 1].scheduled_time, { zone: "Asia/Ho_Chi_Minh" });
        msg += first.isValid && last.isValid ? ` (${first.toFormat("HH:mm")} → ${last.toFormat("HH:mm")})` : "";
      }
      msg += `\n• **${cancelled.length}** bài đã bỏ`;
      if (cancelled.length) {
        const skus = cancelled.map(j => j.sku).join(", ");
        msg += `: ${skus}`;
      }
      if (demotedCount) {
        msg += `\n• ♻️ **${demotedCount}** bài SUCCESS cũ đã đánh dấu **REPOSTED** (để bài mới không bị chặn dedup)`;
      }
      if (pending.length) {
        const list = pending.slice(0, 15).map(j => {
          const dt = DateTime.fromISO(j.scheduled_time, { zone: "Asia/Ho_Chi_Minh" });
          return `• ${dt.isValid ? dt.toFormat("HH:mm") : j.scheduled_time} — **${j.sku}**`;
        }).join("\n");
        msg += `\n\n${list}`;
        if (pending.length > 15) msg += `\n… (+${pending.length - 15} bài)`;
      }
      if (msg.length > 1900) msg = msg.slice(0, 1900) + "\n… (truncated)";

      try {
        const channel = await client.channels.fetch(channelId);
        if (channel) await channel.send(msg);
      } catch (e) {
        console.error(`[BATCH-CONFIRM] send failed channel=${channelId}: ${e.message}`);
      }
    }

    console.log(`[BATCH-CONFIRM] batch=${batchId} pending=${pendingTotal} cancelled=${cancelledTotal} demoted=${demotedCount} revived=${revivedCount}`);
    res.statusCode = 200;
    res.setHeader("Content-Type", "application/json");
    return res.end(JSON.stringify({ ok: true, pending: pendingTotal, cancelled: cancelledTotal, demoted: demotedCount, revived: revivedCount }));
  } catch (e) {
    console.error(`[BATCH-CONFIRM] error: ${e.message}`);
    res.statusCode = 500;
    return res.end("error");
  }
}

// ===== /ig_folder_schedule handler =====
async function handleIgFolderSchedule(interaction, { sheets, drive }) {
  const t0 = Date.now();
  const user = interaction.user?.tag || "unknown";
  console.log(`[CMD] /ig_folder_schedule from ${user} (ws_ping=${interaction.client.ws.ping}ms)`);

  try {
    await interaction.deferReply();
    console.log(`[CMD] deferReply OK (${Date.now() - t0}ms)`);
  } catch (deferErr) {
    console.error(`[CMD] deferReply FAILED after ${Date.now() - t0}ms: ${deferErr.message}`);
    return;
  }

  try {
    const shopKey = interaction.options.getString("shop", true);
    const timeStr = interaction.options.getString("time", true);
    const folderUrl = interaction.options.getString("folder", true);
    if (!SHOP[shopKey]) throw new Error(`Shop "${shopKey}" chưa cấu hình.`);

    const startDt = parseVnDatetime(timeStr);
    if (startDt < nowVn().minus({ minutes: 1 })) throw new Error("Giờ đăng ở quá khứ.");

    const parentFolderId = parseFolderIdFromUrl(folderUrl);
    const childFolders = await listChildFolders(drive, parentFolderId);

    if (childFolders.length === 0) throw new Error("Folder cha không có subfolder nào.");

    console.log(`[CMD] Found ${childFolders.length} subfolders in parent (${Date.now() - t0}ms)`);

    // Batch id links all rows that belong to this folder schedule invocation so the
    // Apps Script sorter can pick them up via ?batch=<id> query.
    const batchId = `${Date.now()}_${interaction.user.id}`;

    const results = [];
    const errors = [];

    for (let i = 0; i < childFolders.length; i++) {
      const child = childFolders[i];
      const dt = startDt.plus({ minutes: i * 5 });

      try {
        const sku = deriveSkuFromFolderName(child.name);
        const skuResult = await findCaptionBySku({ sheets, shopKey, sku });
        if (!skuResult?.caption) throw new Error(`Không tìm caption SKU=${sku}`);

        const mediaFiles = await listMediaFiles(drive, child.id);
        const childFolderUrl = `https://drive.google.com/drive/folders/${child.id}`;
        // media[0] after naturalSortByName in listMediaFiles matches the first slide
        // the bot will actually publish — store it so the sorter shows the right thumbnail.
        const firstMediaId = mediaFiles[0]?.id || "";

        await appendJob(sheets, { queueSheetId: QUEUE_SHEET_ID, job: {
          created_at: nowVn().toISO(), requester_id: interaction.user.id, requester_tag: interaction.user.tag,
          shop: shopKey, scheduled_time: dt.toISO(), folder_url: childFolderUrl, folder_id: child.id, sku, channel_id: interaction.channelId,
          status: "DRAFT", batch_id: batchId, first_media_id: firstMediaId
        }});

        results.push({ sku, dt, mediaCount: mediaFiles.length, folderUrl: childFolderUrl });
        console.log(`[CMD] Drafted subfolder ${i + 1}/${childFolders.length} SKU=${sku} at ${dt.toFormat("HH:mm")} batch=${batchId}`);
      } catch (err) {
        errors.push({ folderName: child.name, error: err.message || String(err) });
        console.log(`[CMD] Skipped subfolder "${child.name}": ${err.message}`);
      }
    }

    // Build Discord response
    const shopName = SHOP[shopKey].name;
    let msg = `📁 **Folder Schedule (nháp)** — Shop: **${shopName}**\nFolder cha: ${folderUrl}\n`;

    if (results.length > 0) {
      msg += `\n📋 **${results.length} bài đã tạo nháp:**\n`;
      for (const r of results) {
        msg += `• ${r.dt.toFormat("HH:mm")} — SKU: **${r.sku}** — ${r.mediaCount} file\n`;
      }
      if (IG_SORTER_WEB_APP_URL) {
        msg += `\n🔗 **Mở trình sắp xếp để xem thumbnail, đổi thứ tự hoặc bỏ bài:**\n${IG_SORTER_WEB_APP_URL}?batch=${batchId}\n⚠️ Bài chỉ được đăng sau khi bấm **Done** trong trình sắp xếp.`;
      } else {
        msg += `\n⚠️ \`IG_SORTER_WEB_APP_URL\` chưa cấu hình — bài ở trạng thái DRAFT sẽ không đăng cho tới khi bạn đổi status sang PENDING thủ công. Batch ID: \`${batchId}\``;
      }
    }

    if (errors.length > 0) {
      msg += `\n\n❌ **${errors.length} subfolder lỗi (đã bỏ qua):**\n`;
      for (const e of errors) {
        msg += `• **${e.folderName}**: ${e.error}\n`;
      }
    }

    if (results.length === 0) {
      msg += `\n⚠️ Không có subfolder nào hợp lệ để lên lịch.`;
    }

    // Discord message limit 2000 chars
    if (msg.length > 1900) {
      msg = msg.slice(0, 1900) + "\n… (truncated)";
    }

    console.log(`[CMD] ✅ Folder schedule done: ${results.length} drafted, ${errors.length} errors batch=${batchId} (${Date.now() - t0}ms total)`);
    await interaction.editReply(msg);
  } catch (e) {
    console.error(`[CMD] ❌ Folder schedule FAILED after ${Date.now() - t0}ms: ${e.message}`);
    try {
      await interaction.editReply(`❌ ${e.message || String(e)}`);
    } catch (replyErr) {
      console.error(`[CMD] editReply also failed: ${replyErr.message}`);
    }
  }
}

async function main() {
  // ===== Global error handlers — catch mọi lỗi bị nuốt =====
  process.on("unhandledRejection", (reason) => {
    console.error("[UNHANDLED_REJECTION]", reason);
  });
  process.on("uncaughtException", (err) => {
    console.error("[UNCAUGHT_EXCEPTION]", err);
  });

  const client = new Client({ intents: [GatewayIntentBits.Guilds] });
  const { sheets, drive } = await getClients();

  // ===== Discord connection monitoring =====
  client.on("warn", (msg) => console.warn("[DISCORD_WARN]", msg));
  client.on("error", (err) => console.error("[DISCORD_ERROR]", err.message));
  client.on("shardDisconnect", (ev, id) => console.warn(`[DISCORD] Shard ${id} disconnected (code ${ev.code})`));
  client.on("shardReconnecting", (id) => console.log(`[DISCORD] Shard ${id} reconnecting...`));
  client.on("shardResume", (id, replayed) => console.log(`[DISCORD] Shard ${id} resumed (replayed ${replayed})`));

  client.on("ready", async () => {
    console.log(`✅ Logged in as ${client.user.tag}`);
    setTimeout(() => startTokenReminder(client), 30000);
    registerTestTokenCommand(client);

    if (!global.__MEDIA_PROXY_STARTED__) {
      global.__MEDIA_PROXY_STARTED__ = true;
      const handler = createMediaHandler({ drive });
      const port = process.env.PORT || 3000;
      http.createServer(async (req, res) => {
        try {
          const u = new URL(req.url, `http://${req.headers.host || "localhost"}`);
          if (u.pathname === "/batch-confirm") {
            return handleBatchConfirm(req, res, u, { client, sheets });
          }
        } catch (e) {
          console.error("[HTTP] dispatcher error:", e.message);
        }
        return handler(req, res);
      }).listen(port, () => console.log(`🌐 Proxy :${port}`));
    }

    setInterval(() => tick({ client, sheets, drive }).catch(console.error), 60_000);
    tick({ client, sheets, drive }).catch(console.error);

    // ===== Heartbeat log mỗi 30 phút — chứng minh bot còn sống =====
    setInterval(() => {
      const mem = process.memoryUsage();
      console.log(`[HEARTBEAT] RSS=${(mem.rss / 1024 / 1024).toFixed(0)}MB heap=${(mem.heapUsed / 1024 / 1024).toFixed(0)}MB uptime=${(process.uptime() / 3600).toFixed(1)}h ws=${client.ws.ping}ms`);
    }, 30 * 60 * 1000);
  });

  client.on("interactionCreate", async (interaction) => {
    if (!interaction.isChatInputCommand()) return;
    if (interaction.commandName === "testtoken") return handleTestTokenSlash(interaction, client);
    if (interaction.commandName === "ig_cancel") return handleIgCancel(interaction, { sheets });
    if (interaction.commandName === "ig_folder_schedule") return handleIgFolderSchedule(interaction, { sheets, drive });
    if (interaction.commandName !== "ig_schedule") return;

    const t0 = Date.now();
    const user = interaction.user?.tag || "unknown";
    console.log(`[CMD] /ig_schedule from ${user} (ws_ping=${client.ws.ping}ms)`);

    try {
      await interaction.deferReply();
      console.log(`[CMD] deferReply OK (${Date.now() - t0}ms)`);
    } catch (deferErr) {
      // deferReply failed = Discord đã timeout (>3s) hoặc network lỗi
      console.error(`[CMD] deferReply FAILED after ${Date.now() - t0}ms: ${deferErr.message}`);
      return; // Không thể editReply nếu defer fail
    }

    try {
      const shopKey = interaction.options.getString("shop", true);
      const timeStr = interaction.options.getString("time", true);
      const folderUrl = interaction.options.getString("folder", true);
      if (!SHOP[shopKey]) throw new Error(`Shop "${shopKey}" chưa cấu hình.`);

      const dt = parseVnDatetime(timeStr);
      if (dt < nowVn().minus({ minutes: 1 })) throw new Error("Giờ đăng ở quá khứ.");

      const folderId = parseFolderIdFromUrl(folderUrl);
      const folderName = await getFolderName(drive, folderId);
      const sku = deriveSkuFromFolderName(folderName);
      console.log(`[CMD] SKU=${sku} shop=${shopKey} (${Date.now() - t0}ms)`);

      const skuResult = await findCaptionBySku({ sheets, shopKey, sku });
      if (!skuResult?.caption) throw new Error(`Không tìm caption SKU=${sku} shop ${shopKey}`);
      const mediaFiles = await listMediaFiles(drive, folderId);

      // Rate limit check
      const { items: queueItems } = await fetchAllJobs(sheets, { queueSheetId: QUEUE_SHEET_ID });
      const rateLimitWarning = buildRateLimitWarning(queueItems, shopKey, dt);

      await appendJob(sheets, { queueSheetId: QUEUE_SHEET_ID, job: {
        created_at: nowVn().toISO(), requester_id: interaction.user.id, requester_tag: interaction.user.tag,
        shop: shopKey, scheduled_time: dt.toISO(), folder_url: folderUrl, folder_id: folderId, sku, channel_id: interaction.channelId
      }});

      console.log(`[CMD] ✅ Queued SKU=${sku} (${Date.now() - t0}ms total)`);
      const successMsg = `✅ Đã tạo lịch\n- Shop: **${SHOP[shopKey].name}**\n- Giờ: **${dt.toFormat("yyyy-MM-dd HH:mm")}**\n- SKU: **${sku}**\n- Media: **${mediaFiles.length}** file\n- Folder: ${folderUrl}`;
      await interaction.editReply(rateLimitWarning ? `${rateLimitWarning}\n\n${successMsg}` : successMsg);
    } catch (e) {
      console.error(`[CMD] ❌ FAILED after ${Date.now() - t0}ms: ${e.message}`);
      try {
        await interaction.editReply(`❌ ${e.message || String(e)}`);
      } catch (replyErr) {
        console.error(`[CMD] editReply also failed: ${replyErr.message}`);
      }
    }
  });

  await client.login(DISCORD_TOKEN);
}

main().catch(e => { console.error(e); process.exit(1); });
