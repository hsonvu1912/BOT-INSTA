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
  listMediaFiles, driveDirectDownloadUrl
} = require("./drive");

const { parseVnDatetime, appendJob, fetchAllJobs, updateRow, nowVn } = require("./queue");

const {
  igCreateMediaContainerWithRetry, igCreateCarouselContainer,
  igPublishWithRetry, igGetPermalink,
  waitUntilFinished, waitAllUntilFinished, isRateLimitError
} = require("./ig");

const DISCORD_TOKEN = mustEnv("DISCORD_TOKEN");
const QUEUE_SHEET_ID = mustEnv("SHEET_ID_QUEUE");

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
    sheetId: testSheetId, sheetTab: null, sheetRange: "A:C",
    captionColIndexInRange: 2, codeColIndexInRange: 1, khoStatusCol: "A"
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
      http.createServer((req, res) => handler(req, res)).listen(port, () => console.log(`🌐 Proxy :${port}`));
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

      await appendJob(sheets, { queueSheetId: QUEUE_SHEET_ID, job: {
        created_at: nowVn().toISO(), requester_id: interaction.user.id, requester_tag: interaction.user.tag,
        shop: shopKey, scheduled_time: dt.toISO(), folder_url: folderUrl, folder_id: folderId, sku, channel_id: interaction.channelId
      }});

      console.log(`[CMD] ✅ Queued SKU=${sku} (${Date.now() - t0}ms total)`);
      await interaction.editReply(
        `✅ Đã tạo lịch\n- Shop: **${SHOP[shopKey].name}**\n- Giờ: **${dt.toFormat("yyyy-MM-dd HH:mm")}**\n- SKU: **${sku}**\n- Media: **${mediaFiles.length}** file\n- Folder: ${folderUrl}`
      );
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
