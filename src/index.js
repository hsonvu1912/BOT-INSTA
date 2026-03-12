const axios = require("axios");
const http = require("http");

let tokenReminder;
try {
  tokenReminder = require("./tokenReminder");
} catch {
  tokenReminder = require("../tokenReminder");
}
const { startTokenReminder, registerTestTokenCommand, handleTestTokenSlash } = tokenReminder;


const { Client, GatewayIntentBits } = require("discord.js");
const { DateTime } = require("luxon");

const { mustEnv } = require("./utils");
const { getClients } = require("./google");
const { createMediaHandler } = require("./media-server");

const {
  parseFolderIdFromUrl,
  getFolderName,
  deriveSkuFromFolderName,
  listMediaFiles,
  driveDirectDownloadUrl
} = require("./drive");

const {
  parseVnDatetime,
  appendJob,
  fetchAllJobs,
  updateRow,
  nowVn
} = require("./queue");

const {
  igCreateMediaContainerWithRetry,
  igCreateCarouselContainer,
  igPublishWithRetry,
  igGetPermalink,
  waitUntilFinished,
  waitAllUntilFinished,
  isRateLimitError
} = require("./ig");

const DISCORD_TOKEN = mustEnv("DISCORD_TOKEN");
const QUEUE_SHEET_ID = mustEnv("SHEET_ID_QUEUE");

// ===== v4: Timing config =====
const DELAY_BETWEEN_JOBS_MS = 5000;
const COOLDOWN_EVERY_N_JOBS = 5;
const COOLDOWN_MS = 30000;
const DELAY_BETWEEN_CHILDREN_MS = 500;

// Auto-retry config
const MAX_AUTO_RETRIES = 3;
const RETRY_BACKOFF_MS = 10 * 1000;

// ===== Env helper: optional env (không throw nếu thiếu) =====
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
    sheetTab: null,
    // Range E:L — code ở cột L (index 7), caption ở cột E (index 0)
    sheetRange: "E:L",
    captionColIndexInRange: 0,
    codeColIndexInRange: 7,
    khoStatusCol: "B"
  },
  BURGER: {
    name: "Burger",
    igUserId: mustEnv("IG_USER_ID_BURGER"),
    pageToken: mustEnv("FB_PAGE_TOKEN_BURGER"),
    sheetId: mustEnv("SHEET_ID_BURGER"),
    sheetTab: null,
    // Range F:I — code ở cột F (index 0), caption ở cột I (index 3)
    sheetRange: "F:I",
    captionColIndexInRange: 3,
    codeColIndexInRange: 0,
    khoStatusCol: "D"
  }
};

// ===== TEST shop: chỉ thêm nếu env vars có đủ =====
const testIgUserId = optEnv("IG_USER_ID_TEST");
const testPageToken = optEnv("FB_PAGE_TOKEN_TEST");
const testSheetId = optEnv("SHEET_ID_TEST");

if (testIgUserId && testPageToken && testSheetId) {
  SHOP.TEST = {
    name: "Test",
    igUserId: testIgUserId,
    pageToken: testPageToken,
    sheetId: testSheetId,
    sheetTab: null,
    // Range A:C — code ở cột B (index 1), caption ở cột C (index 2)
    sheetRange: "A:C",
    captionColIndexInRange: 2,
    codeColIndexInRange: 1,
    khoStatusCol: "A"
  };
  console.log("[CONFIG] TEST shop enabled");
} else {
  console.log("[CONFIG] TEST shop not configured (missing IG_USER_ID_TEST / FB_PAGE_TOKEN_TEST / SHEET_ID_TEST)");
}

// ===== Multi-tab SKU lookup =====
const TAB_CACHE_TTL_MS = 10 * 60 * 1000;
const tabCache = new Map();

function canonSku(s) {
  return String(s ?? "")
    .trim()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[đĐ]/g, (m) => (m === "đ" ? "d" : "D"))
    .replace(/[^A-Za-z0-9]/g, "")
    .toUpperCase();
}

function tabIsLikelyInventory(title) {
  const t = String(title || "").trim();
  const up = t.toUpperCase();
  const denyExact = new Set(["QUEUE", "DASHBOARD", "CONFIG", "README", "LOG", "SETTING", "SETTINGS"]);
  if (denyExact.has(up)) return false;
  if (t.startsWith("_")) return false;
  return true;
}

function sortTabsNewestFirst(titles) {
  return [...titles].sort((a, b) => b.localeCompare(a, "en", { numeric: true, sensitivity: "base" }));
}

async function getTabTitles(sheets, spreadsheetId) {
  const cached = tabCache.get(spreadsheetId);
  if (cached && Date.now() - cached.ts < TAB_CACHE_TTL_MS) return cached.titles;

  const meta = await sheets.spreadsheets.get({
    spreadsheetId,
    fields: "sheets(properties(title))"
  });

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

  // Dùng sheetRange từ config thay vì hardcode
  const ranges = titles.map(t => `${t}!${cfg.sheetRange}`);

  const CHUNK = 80;
  for (let i = 0; i < ranges.length; i += CHUNK) {
    const chunkRanges = ranges.slice(i, i + CHUNK);
    const chunkTitles = titles.slice(i, i + CHUNK);

    const resp = await sheets.spreadsheets.values.batchGet({
      spreadsheetId: cfg.sheetId,
      ranges: chunkRanges,
      majorDimension: "ROWS"
    });

    const valueRanges = resp.data.valueRanges || [];
    for (let vrIdx = 0; vrIdx < valueRanges.length; vrIdx++) {
      const vr = valueRanges[vrIdx];
      const tabName = chunkTitles[vrIdx];
      const rows = vr.values || [];
      for (let rowIdx = 0; rowIdx < rows.length; rowIdx++) {
        const row = rows[rowIdx];
        const codeRaw = (row[cfg.codeColIndexInRange] ?? "").toString();
        const codeCanon = canonSku(codeRaw);
        if (codeCanon === targetCanon) {
          const caption = (row[cfg.captionColIndexInRange] ?? "").toString();
          return { caption, tabName, rowNum: rowIdx + 1 };
        }
      }
    }
  }

  return null;
}

// ===== Data validation cache =====
const khoValidationCache = new Map();
const KHO_VALIDATION_CACHE_TTL_MS = 30 * 60 * 1000;
const POSTED_KEYWORDS = ["đã đăng", "da dang", "đăng rồi", "dang roi", "done", "posted", "đã up", "da up"];

async function readCellDropdownValues({ sheets, spreadsheetId, tabName, col, rowNum }) {
  const cacheKey = `${spreadsheetId}:${tabName}:${col}`;
  const cached = khoValidationCache.get(cacheKey);
  if (cached && Date.now() - cached.ts < KHO_VALIDATION_CACHE_TTL_MS) {
    return cached.values;
  }

  const resp = await sheets.spreadsheets.get({
    spreadsheetId,
    ranges: [`${tabName}!${col}${rowNum}`],
    fields: "sheets.data.rowData.values.dataValidation"
  });

  const cellData = resp.data.sheets?.[0]?.data?.[0]?.rowData?.[0]?.values?.[0];
  const validation = cellData?.dataValidation;

  if (!validation || validation.condition?.type !== "ONE_OF_LIST") {
    khoValidationCache.set(cacheKey, { ts: Date.now(), values: null });
    return null;
  }

  const values = (validation.condition.values || [])
    .map(v => v.userEnteredValue)
    .filter(Boolean);

  khoValidationCache.set(cacheKey, { ts: Date.now(), values });
  console.log(`[KHO] Loaded dropdown values for ${tabName}!${col}: [${values.join(", ")}]`);
  return values;
}

function pickPostedValue(dropdownValues) {
  if (!dropdownValues || dropdownValues.length === 0) return null;

  function norm(s) {
    return String(s).trim().toLowerCase().normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "").replace(/[đĐ]/g, "d");
  }

  for (const kw of POSTED_KEYWORDS) {
    for (const val of dropdownValues) {
      if (norm(val).includes(norm(kw))) {
        return { value: val, matched: kw };
      }
    }
  }
  return null;
}

async function updateKhoPostStatus({ sheets, shopKey, tabName, rowNum }) {
  const cfg = SHOP[shopKey];
  const col = cfg.khoStatusCol;
  const range = `${tabName}!${col}${rowNum}`;

  const dropdownValues = await readCellDropdownValues({
    sheets, spreadsheetId: cfg.sheetId, tabName, col, rowNum
  });

  let valueToWrite;
  if (dropdownValues && dropdownValues.length > 0) {
    const pick = pickPostedValue(dropdownValues);
    if (pick) {
      valueToWrite = pick.value;
      console.log(`[KHO] Matched dropdown value "${pick.value}" via keyword "${pick.matched}"`);
    } else {
      throw new Error(
        `Không tìm thấy giá trị "đã đăng" trong dropdown [${dropdownValues.join(", ")}]. ` +
        `Nhờ cập nhật thủ công hoặc kiểm tra lại dropdown trong sheet kho.`
      );
    }
  } else {
    valueToWrite = "Đã đăng";
    console.log(`[KHO] No dropdown validation found, writing default: "${valueToWrite}"`);
  }

  await sheets.spreadsheets.values.update({
    spreadsheetId: cfg.sheetId, range,
    valueInputOption: "RAW",
    requestBody: { values: [[valueToWrite]] }
  });

  console.log(`[KHO] Updated ${cfg.name} kho status: ${tabName}!${col}${rowNum} = "${valueToWrite}"`);
  return valueToWrite;
}

function isVideoName(name) {
  return /\.mp4$/i.test(name || "");
}

function prioritizeVideosFirst(mediaFiles) {
  const videos = [];
  const images = [];
  for (const f of mediaFiles) {
    if (isVideoName(f.name)) videos.push(f);
    else images.push(f);
  }
  return videos.length ? [...videos, ...images] : mediaFiles;
}

// ===== v4: publishJob — batch create + batch poll =====
async function publishJob({ shopKey, caption, mediaFiles }) {
  const cfg = SHOP[shopKey];
  const ordered = prioritizeVideosFirst(mediaFiles);

  // Single file
  if (ordered.length === 1) {
    const f = ordered[0];
    const isVideo = isVideoName(f.name);
    const mediaUrl = driveDirectDownloadUrl(f.id);

    const creationId = await igCreateMediaContainerWithRetry({
      igUserId: cfg.igUserId,
      pageToken: cfg.pageToken,
      imageUrl: isVideo ? null : mediaUrl,
      videoUrl: isVideo ? mediaUrl : null,
      caption,
      isCarouselItem: false
    });

    await waitUntilFinished({ creationId, pageToken: cfg.pageToken, isVideo });

    const mediaId = await igPublishWithRetry({
      igUserId: cfg.igUserId,
      pageToken: cfg.pageToken,
      creationId
    });

    const permalink = await igGetPermalink({ mediaId, pageToken: cfg.pageToken });
    return { mediaId, permalink };
  }

  // Carousel — batch approach
  console.log(`[PUBLISH] Creating ${ordered.length} child containers...`);
  const children = [];

  for (let idx = 0; idx < ordered.length; idx++) {
    const f = ordered[idx];
    const isVideo = isVideoName(f.name);
    const mediaUrl = driveDirectDownloadUrl(f.id);

    const childCreationId = await igCreateMediaContainerWithRetry({
      igUserId: cfg.igUserId,
      pageToken: cfg.pageToken,
      imageUrl: isVideo ? null : mediaUrl,
      videoUrl: isVideo ? mediaUrl : null,
      caption: null,
      isCarouselItem: true
    });

    children.push({ creationId: childCreationId, isVideo });

    if (idx < ordered.length - 1) {
      await new Promise(r => setTimeout(r, DELAY_BETWEEN_CHILDREN_MS));
    }
  }

  console.log(`[PUBLISH] All ${children.length} containers created. Batch polling...`);

  await waitAllUntilFinished({
    items: children,
    pageToken: cfg.pageToken
  });

  console.log(`[PUBLISH] All children FINISHED. Creating carousel parent...`);

  const childrenIds = children.map(c => c.creationId);
  const parentCreationId = await igCreateCarouselContainer({
    igUserId: cfg.igUserId,
    pageToken: cfg.pageToken,
    childrenIds,
    caption
  });

  await waitUntilFinished({ creationId: parentCreationId, pageToken: cfg.pageToken, isVideo: false });

  const mediaId = await igPublishWithRetry({
    igUserId: cfg.igUserId,
    pageToken: cfg.pageToken,
    creationId: parentCreationId
  });

  const permalink = await igGetPermalink({ mediaId, pageToken: cfg.pageToken });
  return { mediaId, permalink };
}

// Lock tick
let tickRunning = false;

async function tick({ client, sheets, drive }) {
  if (tickRunning) {
    console.log("[TICK] Skipped — previous tick still running");
    return;
  }
  tickRunning = true;

  try {
    const { items } = await fetchAllJobs(sheets, { queueSheetId: QUEUE_SHEET_ID });
    const now = nowVn();

    const due = [];
    const retryable = [];

    for (const j of items) {
      // Kiểm tra shop có được cấu hình không
      if (!SHOP[j.shop]) continue;

      if (j.status === "PENDING") {
        const dt = DateTime.fromISO(j.scheduled_time, { zone: "Asia/Ho_Chi_Minh" });
        if (dt.isValid && dt <= now) {
          due.push(j);
        }
      }

      if (j.status === "FAILED" && j.attempts < MAX_AUTO_RETRIES) {
        const scheduledDt = DateTime.fromISO(j.scheduled_time, { zone: "Asia/Ho_Chi_Minh" });
        if (!scheduledDt.isValid) continue;
        const retryAfter = scheduledDt.plus({ milliseconds: j.attempts * RETRY_BACKOFF_MS });
        if (now >= retryAfter) {
          retryable.push(j);
        }
      }
    }

    const allJobs = [...due, ...retryable];

    if (retryable.length > 0) {
      console.log(`[TICK] Found ${due.length} due + ${retryable.length} retryable jobs`);
    }

    let consecutiveJobCount = 0;

    for (let jobIdx = 0; jobIdx < allJobs.length; jobIdx++) {
      const job = allJobs[jobIdx];
      const isRetry = job.status === "FAILED";
      const channel = await client.channels.fetch(job.channel_id).catch(() => null);

      try {
        const statusLabel = isRetry ? `RETRYING (attempt ${job.attempts + 1}/${MAX_AUTO_RETRIES})` : "RUNNING";
        await updateRow(sheets, {
          queueSheetId: QUEUE_SHEET_ID,
          rowNum: job.rowNum,
          patch: { status: statusLabel, attempts: job.attempts + 1, last_error: "" }
        });

        if (isRetry && channel) {
          await channel.send(
            `🔄 Auto-retry lần ${job.attempts + 1}/${MAX_AUTO_RETRIES} (${SHOP[job.shop]?.name || job.shop}) | SKU: **${job.sku}**`
          );
        }

        const folderName = await getFolderName(drive, job.folder_id);
        const sku = job.sku || deriveSkuFromFolderName(folderName);

        const skuResult = await findCaptionBySku({ sheets, shopKey: job.shop, sku });
        if (!skuResult || !skuResult.caption) {
          throw new Error(`Không tìm thấy caption cho SKU=${sku} trong sheet shop ${job.shop}`);
        }
        const { caption, tabName: khoTab, rowNum: khoRow } = skuResult;

        const mediaFiles = await listMediaFiles(drive, job.folder_id);

        const { mediaId, permalink } = await publishJob({
          shopKey: job.shop,
          caption,
          mediaFiles
        });

        const publishedAt = nowVn().toISO();

        await updateRow(sheets, {
          queueSheetId: QUEUE_SHEET_ID,
          rowNum: job.rowNum,
          patch: {
            status: "SUCCESS",
            attempts: job.attempts + 1,
            ig_media_id: mediaId,
            ig_permalink: permalink,
            published_at: publishedAt
          }
        });

        try {
          const statusValue = await updateKhoPostStatus({
            sheets, shopKey: job.shop, tabName: khoTab, rowNum: khoRow
          });
          if (channel) {
            const retryNote = isRetry ? " (auto-retry thành công)" : "";
            await channel.send(`✅ Đăng thành công${retryNote} (${SHOP[job.shop].name}) | SKU: **${sku}** | ${permalink}\n📋 Kho: cập nhật → **${statusValue}**`);
          }
        } catch (khoErr) {
          console.error(`[KHO] Failed to update kho status for SKU=${sku}:`, khoErr.message);
          if (channel) {
            await channel.send(
              `✅ Đăng IG thành công (${SHOP[job.shop].name}) | SKU: **${sku}** | ${permalink}\n` +
              `⚠️ Không cập nhật được tình trạng trong sheet kho: ${khoErr.message}\n` +
              `**Nhờ cập nhật cột trạng thái đăng bài trong kho thủ công nhé!**`
            );
          }
        }

        consecutiveJobCount++;

      } catch (e) {
        const msg = (e.response?.data && JSON.stringify(e.response.data))
          ? JSON.stringify(e.response.data)
          : (e.message || String(e));

        const isRL = isRateLimitError(e);

        let newStatus;
        if (isRL) {
          newStatus = "PENDING";
        } else if (job.attempts + 1 < MAX_AUTO_RETRIES) {
          newStatus = "FAILED";
        } else {
          newStatus = "GIVE_UP";
        }

        await updateRow(sheets, {
          queueSheetId: QUEUE_SHEET_ID,
          rowNum: job.rowNum,
          patch: { status: newStatus, attempts: job.attempts + 1, last_error: msg.slice(0, 5000) }
        });

        if (channel) {
          if (isRL) {
            await channel.send(
              `⏳ Rate-limit, sẽ thử lại lần sau (${SHOP[job.shop].name}) | SKU: **${job.sku}**\nLý do: \`\`\`${msg.slice(0, 1800)}\`\`\``
            );
          } else if (newStatus === "FAILED") {
            await channel.send(
              `⚠️ Lỗi lần ${job.attempts + 1}/${MAX_AUTO_RETRIES}, sẽ tự retry sau ${RETRY_BACKOFF_MS / 1000}s (${SHOP[job.shop].name}) | SKU: **${job.sku}**\nLý do: \`\`\`${msg.slice(0, 1200)}\`\`\``
            );
          } else {
            await channel.send(
              `❌ Đăng thất bại sau ${MAX_AUTO_RETRIES} lần thử (${SHOP[job.shop].name}) | SKU: **${job.sku}**\nLý do: \`\`\`${msg.slice(0, 1500)}\`\`\`\n` +
              `⚠️ **Nhờ mọi người đăng bài thủ công lên IG và cập nhật cột tình trạng đăng bài trong sheet kho nhé!**`
            );
          }
        }

        if (isRL) {
          console.log("[TICK] Rate-limited — stopping remaining jobs, will retry next tick");
          break;
        }
      }

      // Smart delay giữa jobs
      if (jobIdx < allJobs.length - 1) {
        if (consecutiveJobCount > 0 && consecutiveJobCount % COOLDOWN_EVERY_N_JOBS === 0) {
          console.log(`[TICK] Cooldown ${COOLDOWN_MS / 1000}s after ${consecutiveJobCount} jobs...`);
          await new Promise(r => setTimeout(r, COOLDOWN_MS));
        } else {
          await new Promise(r => setTimeout(r, DELAY_BETWEEN_JOBS_MS));
        }
      }
    }
  } finally {
    tickRunning = false;
  }
}

async function main() {
  const client = new Client({ intents: [GatewayIntentBits.Guilds] });
  const { sheets, drive } = await getClients();

  client.on("ready", async () => {
    console.log(`✅ Logged in as ${client.user.tag}`);

    setTimeout(() => {
      startTokenReminder(client);
    }, 30000);

    registerTestTokenCommand(client);
    
    if (!global.__MEDIA_PROXY_STARTED__) {
      global.__MEDIA_PROXY_STARTED__ = true;

      const handler = createMediaHandler({ drive });
      const port = process.env.PORT || 3000;

      http.createServer((req, res) => handler(req, res)).listen(port, () => {
        console.log(`🌐 Media proxy listening on :${port}`);
      });
    }

    setInterval(() => tick({ client, sheets, drive }).catch(console.error), 60_000);
    tick({ client, sheets, drive }).catch(console.error);
  });

  client.on("interactionCreate", async (interaction) => {
    if (!interaction.isChatInputCommand()) return;

    if (interaction.commandName === "testtoken") {
      return handleTestTokenSlash(interaction, client);
    }

    if (interaction.commandName !== "ig_schedule") return;

    await interaction.deferReply();

    try {
      const shopKey = interaction.options.getString("shop", true);
      const timeStr = interaction.options.getString("time", true);
      const folderUrl = interaction.options.getString("folder", true);

      if (!SHOP[shopKey]) {
        throw new Error(`Shop "${shopKey}" chưa được cấu hình. Kiểm tra env vars trên Railway.`);
      }

      const dt = parseVnDatetime(timeStr);
      if (dt < nowVn().minus({ minutes: 1 })) throw new Error("Giờ đăng đang ở quá khứ.");

      const folderId = parseFolderIdFromUrl(folderUrl);
      const folderName = await getFolderName(drive, folderId);
      const sku = deriveSkuFromFolderName(folderName);

      const skuResult = await findCaptionBySku({ sheets, shopKey, sku });
      if (!skuResult || !skuResult.caption) {
        throw new Error(`Không tìm thấy caption cho SKU=${sku} trong sheet shop ${shopKey}`);
      }

      const mediaFiles = await listMediaFiles(drive, folderId);

      await appendJob(sheets, {
        queueSheetId: QUEUE_SHEET_ID,
        job: {
          created_at: nowVn().toISO(),
          requester_id: interaction.user.id,
          requester_tag: interaction.user.tag,
          shop: shopKey,
          scheduled_time: dt.toISO(),
          folder_url: folderUrl,
          folder_id: folderId,
          sku,
          channel_id: interaction.channelId
        }
      });

      await interaction.editReply(
        `✅ Đã tạo lịch\n- Shop: **${SHOP[shopKey].name}**\n- Giờ đăng (VN): **${dt.toFormat("yyyy-MM-dd HH:mm")}**\n- SKU: **${sku}**\n- Media: **${mediaFiles.length}** file (<=10)\n- Folder: ${folderUrl}`
      );
    } catch (e) {
      await interaction.editReply(`❌ Tạo lịch thất bại: ${e.message || String(e)}`);
    }
  });

  await client.login(DISCORD_TOKEN);
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});
