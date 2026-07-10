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

const { parseVnDatetime, appendJob, fetchAllJobs, updateRow, readRowCreatedAt, nowVn } = require("./queue");

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
const RATE_LIMIT_COOLDOWN_MS = 30 * 60 * 1000; // 30 phút pause sau khi gặp RL (app-level limit chia chung Màu Mè + Burger)

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

// v8: Tab kho đặt tên "Tháng M/YYYY" — so chuỗi thuần thì "Tháng 12/2025" đứng
// TRƯỚC "Tháng 7/2026" (12 > 7, năm nằm sau tháng nên không được xét) → SKU trùng
// ở 2 tab sẽ lấy caption/ghi kho nhầm tab CŨ. Parse (year, month) để sort đúng.
function tabDateKey(title) {
  const m = String(title).match(/(\d{1,2})\s*\/\s*(\d{4})/);
  return m ? Number(m[2]) * 100 + Number(m[1]) : null; // YYYYMM
}

function sortTabsNewestFirst(titles) {
  return [...titles].sort((a, b) => {
    const ka = tabDateKey(a), kb = tabDateKey(b);
    if (ka !== null && kb !== null && ka !== kb) return kb - ka;
    if (ka !== null && kb === null) return -1; // tab có tháng/năm ưu tiên trước
    if (ka === null && kb !== null) return 1;
    return b.localeCompare(a, "en", { numeric: true, sensitivity: "base" });
  });
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

// v8 #3+#4: quyết định bỏ 1 job (thuần, không I/O — dễ test). Trả "cancelled" nếu row
// vừa bị /ig_cancel giữa tick, "dup" nếu shop+SKU đã đăng trong chính tick này, else null.
function jobSkipReason(job, { successKeys, cancelledRowNums }) {
  if (cancelledRowNums.has(job.rowNum)) return "cancelled";
  if (successKeys.has(`${job.shop}::${canonSku(job.sku)}`)) return "dup";
  return null;
}

// ===== v8: Watchdog helpers =====
// Bọc 1 promise trong timeout — chống awaited-call treo vĩnh viễn giữ lock tick
// (sự cố 09/07). Timeout → job FAILED + auto-retry thay vì bot liệt.
function withTimeout(promise, ms, label) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timeout sau ${Math.round(ms / 60000)} phút`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

// Retry ghi sổ (Sheets hay 503) — dùng cho các update KHÔNG được phép phá job.
async function updateRowWithRetry(sheets, args, { tries = 3, gapMs = 5000 } = {}) {
  let lastErr;
  for (let i = 0; i < tries; i++) {
    try { return await updateRow(sheets, args); }
    catch (e) {
      lastErr = e;
      if (i < tries - 1) await new Promise(r => setTimeout(r, gapMs));
    }
  }
  throw lastErr;
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
      // v8: video cần Meta transcode lâu — publish lại CÙNG container (sống ~24h) nhiều
      // lần thay vì để job-retry tạo container mới (reset đồng hồ transcode). 20×15s=5phút.
      const mediaId = await igPublishWithRetry({ igUserId: cfg.igUserId, pageToken: cfg.pageToken, creationId, retries: isVideo ? 20 : 8 });
      // v8: từ đây bài ĐÃ lên IG — lỗi lấy permalink không được phá job (throw →
      // FAILED → retry → đăng TRÙNG). Fallback URL là đủ.
      const permalink = await igGetPermalink({ igUserId: cfg.igUserId, mediaId, pageToken: cfg.pageToken })
        .catch(() => `https://www.instagram.com/?mediaid=${mediaId}`);
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

    // v8: carousel chứa video → cho publish container CHA nhiều lần hơn
    const carouselHasVideo = children.some(c => c.isVideo);
    const mediaId = await igPublishWithRetry({ igUserId: cfg.igUserId, pageToken: cfg.pageToken, creationId: parentCreationId, retries: carouselHasVideo ? 20 : 8 });
    // v8: bài đã lên IG — permalink lỗi chỉ fallback, không throw (tránh đăng trùng)
    const permalink = await igGetPermalink({ igUserId: cfg.igUserId, mediaId, pageToken: cfg.pageToken })
      .catch(() => `https://www.instagram.com/?mediaid=${mediaId}`);
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
let tickStartedAt = 0; // v8: watchdog — biết lock bị giữ bao lâu
let rateLimitUntilByShop = {};  // { MAUME: timestamp, BURGER: timestamp } — mỗi shop có cooldown riêng vì token riêng

// v8: 1 job publish tối đa 20 phút (bình thường <3 phút kể cả video retry).
// Quá hạn → job FAILED + auto-retry, lock được nhả — bot KHÔNG BAO GIỜ liệt như 09/07.
const PUBLISH_JOB_TIMEOUT_MS = 20 * 60 * 1000;

// v8: rows đã đăng IG thành công nhưng ghi SUCCESS vào sheet thất bại (Sheets 503).
// Recovery phải bỏ qua các row này để không retry → đăng trùng.
const bookkeepingFailedRows = new Set();

// v8 #3: rowNum bị /ig_cancel thu hồi TRONG LÚC tick đang chạy batch. Tick chốt
// danh sách due lúc T0 rồi xử lý tuần tự nhiều phút — nếu user thu hồi giữa chừng,
// row này phải được bỏ qua trước khi publish (TOCTOU). handleIgCancel ghi vào đây.
const cancelledRowNums = new Set();

// ===== Manual pause state =====
// pausedShops: Set chứa các shop key đang bị pause. Nếu chứa "ALL" thì pause toàn bộ.
// State chỉ lưu trong RAM → bot restart sẽ tự resume. Đây là pause thủ công, khác với rate-limit cooldown.
const pausedShops = new Set();
const pauseMetaByShop = {}; // { [shopKey|"ALL"]: { by, at, reason } }

function isShopPaused(shopKey) {
  return pausedShops.has("ALL") || pausedShops.has(shopKey);
}

async function tick({ client, sheets, drive }) {
  if (tickRunning) {
    // v8: watchdog — nếu lock bị giữ quá lâu thì la lên thay vì Skipped âm thầm
    const heldMin = Math.round((Date.now() - tickStartedAt) / 60000);
    if (heldMin >= 10) console.error(`[TICK] Skipped — lock held ${heldMin}m (nghi treo; watchdog publishJob sẽ tự nhả trong tối đa ${PUBLISH_JOB_TIMEOUT_MS / 60000}m)`);
    else console.log("[TICK] Skipped");
    return;
  }
  tickRunning = true;
  tickStartedAt = Date.now();

  try {
    const { items } = await fetchAllJobs(sheets, { queueSheetId: QUEUE_SHEET_ID });
    const now = nowVn();

    // ===== v8: Recovery — job kẹt RUNNING/RETRYING do bot restart giữa chừng =====
    // Tick là single-flight (lock): tại ĐẦU tick không thể có job nào đang chạy thật
    // trong process này → mọi row RUNNING/RETRYING là xác chết từ trước restart.
    // Bản cũ bỏ mặc chúng vĩnh viễn → bài âm thầm không bao giờ đăng.
    for (const j of items) {
      if (j.status !== "RUNNING" && !j.status.startsWith("RETRYING")) continue;
      if (bookkeepingFailedRows.has(j.rowNum)) continue; // bài đã lên IG, chỉ lỗi ghi sổ
      const newStatus = j.attempts >= MAX_AUTO_RETRIES ? "GIVE_UP" : "FAILED";
      try {
        await updateRow(sheets, { queueSheetId: QUEUE_SHEET_ID, rowNum: j.rowNum,
          patch: { status: newStatus, last_error: `Auto-recover: kẹt ${j.status} (bot restart giữa chừng publish)` } });
        j.status = newStatus;
        console.log(`[RECOVER] row=${j.rowNum} SKU=${j.sku} → ${newStatus}${newStatus === "FAILED" ? " (sẽ tự retry)" : ""}`);
        if (newStatus === "GIVE_UP") {
          const ch = await client.channels.fetch(j.channel_id).catch(() => null);
          if (ch) await ch.send(`❌ Job kẹt giữa chừng do bot restart, đã hết ${MAX_AUTO_RETRIES} lần thử (${SHOP[j.shop]?.name || j.shop}) | SKU: **${j.sku}**\n⚠️ **Nhờ đăng tay + cập nhật kho!**`).catch(() => {});
        }
      } catch (e) {
        console.error(`[RECOVER] update failed row=${j.rowNum}: ${e.message}`);
      }
    }

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

      // Manual pause: skip job nếu shop (hoặc toàn bộ) đang bị pause thủ công
      if (isShopPaused(job.shop)) {
        console.log(`[TICK] Skip ${job.shop} job (manually paused) | SKU=${job.sku}`);
        continue;
      }

      // Per-shop cooldown: skip job nếu shop đang trong RL cooldown
      const shopCooldownUntil = rateLimitUntilByShop[job.shop] || 0;
      if (Date.now() < shopCooldownUntil) {
        const remainingMin = Math.ceil((shopCooldownUntil - Date.now()) / 60000);
        console.log(`[TICK] Skip ${job.shop} job (cooldown ${remainingMin}m left) | SKU=${job.sku}`);
        continue;
      }

      // v8 #3+#4: guard chống đăng nhầm/đăng trùng, tính TRONG vòng lặp (in-RAM, không tốn quota)
      const skipReason = jobSkipReason(job, { successKeys, cancelledRowNums });
      if (skipReason === "cancelled") {
        // #3: bị /ig_cancel thu hồi giữa chừng tick
        console.log(`[TICK] Skip row=${job.rowNum} (đã /ig_cancel giữa chừng) | SKU=${job.sku}`);
        continue;
      }
      if (skipReason === "dup") {
        // #4: cùng shop+SKU đã đăng ở vòng trước của CHÍNH tick này (pha phân loại chỉ check 1 lần đầu)
        console.log(`[TICK] Skip row=${job.rowNum} (SKU=${job.sku} đã đăng trong tick này) → CANCELLED_DUP`);
        await updateRow(sheets, { queueSheetId: QUEUE_SHEET_ID, rowNum: job.rowNum,
          patch: { status: "CANCELLED_DUP", last_error: "Đã đăng bởi row khác cùng tick" } }).catch(() => {});
        continue;
      }

      // v8 #11: sheet QUEUE bị sửa tay (xoá/chèn/sort hàng) giữa lúc tick chạy →
      // rowNum snapshot trỏ sang hàng KHÁC. Verify created_at (cột A) trước khi ghi/đăng;
      // lệch thì bỏ qua an toàn (tick sau fetch lại rowNum đúng) thay vì ghi nhầm hàng.
      if (job.created_at) {
        const liveCreatedAt = await readRowCreatedAt(sheets, { queueSheetId: QUEUE_SHEET_ID, rowNum: job.rowNum }).catch(() => null);
        if (liveCreatedAt !== null && liveCreatedAt !== job.created_at) {
          console.error(`[GUARD] row=${job.rowNum} created_at lệch ("${liveCreatedAt}" != "${job.created_at}") — sheet bị sửa tay? Bỏ job này để không ghi/đăng nhầm hàng.`);
          continue;
        }
      }

      const isRetry = job.status === "FAILED";
      const channel = await client.channels.fetch(job.channel_id).catch(() => null);

      // v8: set sau khi IG publish OK — từ đó trở đi catch KHÔNG được đánh FAILED
      // (bài đã lên IG, retry sẽ đăng trùng)
      let published = null;

      try {
        await updateRow(sheets, { queueSheetId: QUEUE_SHEET_ID, rowNum: job.rowNum,
          patch: { status: isRetry ? `RETRYING (${job.attempts+1}/${MAX_AUTO_RETRIES})` : "RUNNING", attempts: job.attempts+1, last_error: "" }
        });

        if (isRetry && channel) {
          // v8 #5: notify KHÔNG được throw ra ngoài (mất quyền SendMessages → job FAILED oan)
          await channel.send(`🔄 Auto-retry ${job.attempts+1}/${MAX_AUTO_RETRIES} (${SHOP[job.shop]?.name}) | SKU: **${job.sku}**`).catch(() => {});
        }

        const folderName = await getFolderName(drive, job.folder_id);
        const sku = job.sku || deriveSkuFromFolderName(folderName);
        const skuResult = await findCaptionBySku({ sheets, shopKey: job.shop, sku });
        if (!skuResult?.caption) throw new Error(`Không tìm caption SKU=${sku} shop ${job.shop}`);
        const { caption, tabName: khoTab, rowNum: khoRow } = skuResult;
        const mediaFiles = await listMediaFiles(drive, job.folder_id);

        // v8: watchdog — publishJob treo (socket đơ, promise mồ côi) tối đa 20 phút
        // là bị cắt thành lỗi thường → job FAILED + auto-retry, lock được nhả.
        published = await withTimeout(
          publishJob({ shopKey: job.shop, caption, mediaFiles, drive }),
          PUBLISH_JOB_TIMEOUT_MS, `publishJob SKU=${sku}`
        );
        const { mediaId, permalink } = published;

        // ===== v8: từ đây bài ĐÃ lên IG — lỗi ghi sổ không được phá job =====
        let successWritten = false;
        try {
          await updateRowWithRetry(sheets, { queueSheetId: QUEUE_SHEET_ID, rowNum: job.rowNum,
            patch: { status: "SUCCESS", attempts: job.attempts+1, last_error: "", ig_media_id: mediaId, ig_permalink: permalink, published_at: nowVn().toISO() }
          });
          successWritten = true;
        } catch {
          // fallback tối thiểu: chỉ ghi status để chặn retry
          try {
            await updateRowWithRetry(sheets, { queueSheetId: QUEUE_SHEET_ID, rowNum: job.rowNum, patch: { status: "SUCCESS" } });
            successWritten = true;
          } catch {}
        }
        if (!successWritten) {
          bookkeepingFailedRows.add(job.rowNum);
          console.error(`[BOOKKEEPING] Ghi SUCCESS thất bại row=${job.rowNum} — bài ĐÃ lên IG: ${permalink}. Chặn auto-retry để tránh đăng trùng.`);
          if (channel) await channel.send(`⚠️ Bài **ĐÃ ĐĂNG** (${permalink}) nhưng ghi sheet thất bại nhiều lần.\n👉 **Sửa tay row ${job.rowNum} trong QUEUE thành SUCCESS** kẻo bot restart có thể đăng lại.`).catch(() => {});
        }

        const cancelled = await cancelSiblingFailedJobs({ sheets, allItems: items, currentJob: job });
        if (cancelled) console.log(`[DEDUP] Cancelled ${cancelled} dups for SKU=${sku}`);
        successKeys.add(`${job.shop}::${canonSku(sku)}`);

        try {
          const sv = await updateKhoPostStatus({ sheets, shopKey: job.shop, tabName: khoTab, rowNum: khoRow });
          if (channel) {
            const note = isRetry ? " (auto-retry OK)" : "";
            await channel.send(`✅ Thành công${note} (${SHOP[job.shop].name}) | SKU: **${sku}** | ${permalink}\n📋 Kho → **${sv}**`).catch(() => {});
          }
        } catch (khoErr) {
          if (channel) await channel.send(`✅ IG OK (${SHOP[job.shop].name}) | ${permalink}\n⚠️ Lỗi kho: ${khoErr.message}`).catch(() => {});
        }

        consecutiveJobCount++;

      } catch (e) {
        // v8: bài đã lên IG mà lỗi ở khâu sau (kho/notify/dedup) → KHÔNG đánh FAILED
        // (row đã là SUCCESS hoặc đã được bookkeepingFailedRows chặn retry)
        if (published) {
          console.error(`[TICK] Lỗi hậu-publish (bài đã lên IG ${published.permalink}) row=${job.rowNum}: ${e.message}`);
          consecutiveJobCount++;
          continue;
        }
        const msg = (e.response?.data && JSON.stringify(e.response.data)) ? JSON.stringify(e.response.data) : (e.message || String(e));
        const isRL = isRateLimitError(e);
        let newStatus = isRL ? "PENDING" : (job.attempts+1 < MAX_AUTO_RETRIES ? "FAILED" : "GIVE_UP");
        // RL không phải lỗi của job → không tốn quota retry. Job sẽ tự thử lại sau cooldown.
        const newAttempts = isRL ? job.attempts : job.attempts + 1;

        await updateRow(sheets, { queueSheetId: QUEUE_SHEET_ID, rowNum: job.rowNum,
          patch: { status: newStatus, attempts: newAttempts, last_error: msg.slice(0,5000) }
        });

        if (channel) {
          // v8 #5: send trong catch nếu throw sẽ thoát catch → huỷ cả vòng for (bỏ các job due còn lại). Bọc .catch.
          if (isRL) {
            const resumeAt = DateTime.fromMillis(Date.now() + RATE_LIMIT_COOLDOWN_MS).setZone("Asia/Ho_Chi_Minh");
            await channel.send(`⏸️ **Instagram rate limit (${SHOP[job.shop].name})** | SKU: **${job.sku}**\nShop này tạm dừng **${RATE_LIMIT_COOLDOWN_MS/60000} phút** (đến **${resumeAt.toFormat("HH:mm")}**). Shop khác vẫn chạy. Job sẽ tự retry sau cooldown.\n\`\`\`${msg.slice(0,1500)}\`\`\``).catch(() => {});
          }
          else if (newStatus === "FAILED") await channel.send(`⚠️ Lỗi ${job.attempts+1}/${MAX_AUTO_RETRIES}, retry ${RETRY_BACKOFF_MS/1000}s (${SHOP[job.shop].name}) | SKU: **${job.sku}**\n\`\`\`${msg.slice(0,1200)}\`\`\``).catch(() => {});
          else await channel.send(`❌ Thất bại ${MAX_AUTO_RETRIES} lần (${SHOP[job.shop].name}) | SKU: **${job.sku}**\n\`\`\`${msg.slice(0,1500)}\`\`\`\n⚠️ **Nhờ đăng tay + cập nhật kho!**`).catch(() => {});
        }

        if (isRL) {
          // Per-shop cooldown: chỉ pause shop này, shop khác tiếp tục chạy
          rateLimitUntilByShop[job.shop] = Date.now() + RATE_LIMIT_COOLDOWN_MS;
          console.log(`[TICK] Rate-limited ${job.shop} — cooldown ${RATE_LIMIT_COOLDOWN_MS/60000}m until ${new Date(rateLimitUntilByShop[job.shop]).toISOString()}`);
          continue; // tiếp tục xử lý jobs của shop khác
        }
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
  } finally {
    // v8 #3: dọn Set sau mỗi tick — row đã ghi CANCELLED vào sheet nên tick sau tự loại,
    // không cần giữ rowNum trong RAM (tránh Set phình vô hạn).
    cancelledRowNums.clear();
    tickRunning = false;
  }
}

function formatPauseScope(scope) {
  return scope === "ALL" ? "**TẤT CẢ shop**" : `shop **${SHOP[scope]?.name || scope}**`;
}

async function handleIgPause(interaction) {
  try { await interaction.deferReply(); } catch { return; }
  try {
    const shopOpt = interaction.options.getString("shop") || "ALL";
    const reason = interaction.options.getString("reason") || "";
    if (shopOpt !== "ALL" && !SHOP[shopOpt]) throw new Error(`Shop "${shopOpt}" chưa cấu hình.`);

    if (pausedShops.has(shopOpt)) {
      const meta = pauseMetaByShop[shopOpt];
      const since = meta ? DateTime.fromISO(meta.at).setZone("Asia/Ho_Chi_Minh").toFormat("yyyy-MM-dd HH:mm") : "?";
      await interaction.editReply(`ℹ️ ${formatPauseScope(shopOpt)} đã ở trạng thái tạm dừng từ **${since}** (bởi ${meta?.by || "?"}).`);
      return;
    }

    pausedShops.add(shopOpt);
    pauseMetaByShop[shopOpt] = { by: interaction.user.tag, at: nowVn().toISO(), reason };
    console.log(`[PAUSE] ${shopOpt} paused by ${interaction.user.tag}${reason ? ` — ${reason}` : ""}`);

    let msg = `⏸️ Đã tạm dừng ${formatPauseScope(shopOpt)}.\nCác job PENDING sẽ không được đăng cho tới khi dùng \`/ig_resume\`.`;
    if (reason) msg += `\nLý do: ${reason}`;
    await interaction.editReply(msg);
  } catch (e) {
    try { await interaction.editReply(`❌ ${e.message}`); } catch {}
  }
}

async function handleIgResume(interaction) {
  try { await interaction.deferReply(); } catch { return; }
  try {
    const shopOpt = interaction.options.getString("shop") || "ALL";
    if (shopOpt !== "ALL" && !SHOP[shopOpt]) throw new Error(`Shop "${shopOpt}" chưa cấu hình.`);

    if (!pausedShops.has(shopOpt)) {
      // Nếu user resume 1 shop nhưng đang pause ALL → cảnh báo
      if (shopOpt !== "ALL" && pausedShops.has("ALL")) {
        await interaction.editReply(`⚠️ Bot đang pause **TẤT CẢ shop**. Dùng \`/ig_resume shop:ALL\` (hoặc không truyền shop) để resume toàn bộ trước.`);
        return;
      }
      await interaction.editReply(`ℹ️ ${formatPauseScope(shopOpt)} không ở trạng thái tạm dừng.`);
      return;
    }

    pausedShops.delete(shopOpt);
    delete pauseMetaByShop[shopOpt];
    console.log(`[PAUSE] ${shopOpt} resumed by ${interaction.user.tag}`);

    await interaction.editReply(`▶️ Đã tiếp tục ${formatPauseScope(shopOpt)}. Bot sẽ xử lý lại job ở lần tick tới (tối đa 60s).`);
  } catch (e) {
    try { await interaction.editReply(`❌ ${e.message}`); } catch {}
  }
}

async function handleIgStatus(interaction) {
  try { await interaction.deferReply(); } catch { return; }
  try {
    const lines = ["📊 **Trạng thái bot**"];
    if (pausedShops.size === 0) {
      lines.push("• Pause thủ công: ✅ không có shop nào bị pause");
    } else {
      lines.push("• Pause thủ công:");
      for (const scope of pausedShops) {
        const meta = pauseMetaByShop[scope];
        const since = meta ? DateTime.fromISO(meta.at).setZone("Asia/Ho_Chi_Minh").toFormat("yyyy-MM-dd HH:mm") : "?";
        const tail = meta?.reason ? ` — ${meta.reason}` : "";
        lines.push(`  - ⏸️ ${formatPauseScope(scope)} từ ${since} (bởi ${meta?.by || "?"})${tail}`);
      }
    }

    const cooldownLines = [];
    for (const [shop, until] of Object.entries(rateLimitUntilByShop)) {
      if (Date.now() < until) {
        const remainMin = Math.ceil((until - Date.now()) / 60000);
        cooldownLines.push(`  - ⏳ ${SHOP[shop]?.name || shop}: còn ${remainMin} phút`);
      }
    }
    if (cooldownLines.length) {
      lines.push("• Rate-limit cooldown:");
      lines.push(...cooldownLines);
    } else {
      lines.push("• Rate-limit cooldown: không");
    }

    await interaction.editReply(lines.join("\n"));
  } catch (e) {
    try { await interaction.editReply(`❌ ${e.message}`); } catch {}
  }
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
    // v8 #3: thu hồi cả job đang chờ auto-retry (FAILED / "RETRYING (x/y)") — trước đây
    // chỉ huỷ PENDING/DRAFT nên user thu hồi xong bot vẫn đăng ở lần retry kế tiếp.
    const pending = items.filter(j =>
      j.shop === shopKey && canonSku(j.sku) === targetCanon &&
      (j.status === "PENDING" || j.status === "DRAFT" || j.status === "FAILED" || j.status.startsWith("RETRYING"))
    );

    if (!pending.length) {
      await interaction.editReply(`❌ Không tìm thấy lịch đang chờ (PENDING/DRAFT/đang retry) cho SKU=**${sku}** shop **${SHOP[shopKey].name}**`);
      return;
    }

    for (const job of pending) {
      // v8 #3: chặn tick đang chạy publish row này giữa chừng (TOCTOU) — set TRƯỚC khi ghi sheet
      cancelledRowNums.add(job.rowNum);
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

// Pack lines into Discord-safe chunks without splitting mid-line (so markdown ** pairs stay balanced).
function chunkLinesForDiscord(lines, maxChars) {
  const chunks = [];
  let buf = "";
  for (const line of lines) {
    const addLen = buf ? 1 + line.length : line.length; // 1 for "\n"
    if (buf && buf.length + addLen > maxChars) {
      chunks.push(buf);
      buf = line;
    } else {
      buf = buf ? buf + "\n" + line : line;
    }
  }
  if (buf) chunks.push(buf);
  return chunks;
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

        const mediaFiles = await listMediaFiles(drive, child.id, { skipLimitCheck: true });
        const childFolderUrl = `https://drive.google.com/drive/folders/${child.id}`;
        // v8: bot đăng theo prioritizeVideosFirst (video lên slide 1) — thumbnail
        // sorter phải lấy đúng file đầu theo thứ tự ĐĂNG THẬT, không phải theo tên.
        const firstMediaId = prioritizeVideosFirst(mediaFiles)[0]?.id || "";

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

    // Build Discord response — split across messages so the sorter link never gets truncated by Discord's 2000-char cap
    const shopName = SHOP[shopKey].name;
    const firstTime = results[0]?.dt.toFormat("HH:mm");
    const lastTime = results[results.length - 1]?.dt.toFormat("HH:mm");

    let headerMsg = `📁 **Folder Schedule (nháp)** — Shop: **${shopName}**\nFolder cha: ${folderUrl}\n`;
    if (results.length > 0) {
      headerMsg += `\n📋 **${results.length} bài đã tạo nháp** (${firstTime} → ${lastTime}, chi tiết ở tin nhắn tiếp theo)\n`;
      if (IG_SORTER_WEB_APP_URL) {
        headerMsg += `\n🔗 **Mở trình sắp xếp để xem thumbnail, đổi thứ tự hoặc bỏ bài:**\n${IG_SORTER_WEB_APP_URL}?batch=${batchId}\n⚠️ Bài chỉ được đăng sau khi bấm **Done** trong trình sắp xếp.`;
      } else {
        headerMsg += `\n⚠️ \`IG_SORTER_WEB_APP_URL\` chưa cấu hình — bài ở trạng thái DRAFT sẽ không đăng cho tới khi bạn đổi status sang PENDING thủ công. Batch ID: \`${batchId}\``;
      }
    } else {
      headerMsg += `\n⚠️ Không có subfolder nào hợp lệ để lên lịch.`;
    }

    console.log(`[CMD] ✅ Folder schedule done: ${results.length} drafted, ${errors.length} errors batch=${batchId} (${Date.now() - t0}ms total)`);
    await interaction.editReply(headerMsg);

    if (results.length > 0) {
      const lines = results.map(r => `• ${r.dt.toFormat("HH:mm")} — SKU: **${r.sku}** — ${r.mediaCount} file`);
      const chunks = chunkLinesForDiscord(lines, 1900);
      for (let i = 0; i < chunks.length; i++) {
        const prefix = chunks.length > 1 ? `📋 **Danh sách (${i + 1}/${chunks.length}):**\n` : `📋 **Danh sách chi tiết:**\n`;
        await interaction.followUp(prefix + chunks[i]);
      }
    }

    if (errors.length > 0) {
      const errLines = errors.map(e => `• **${e.folderName}**: ${e.error}`);
      const errHeader = `❌ **${errors.length} subfolder lỗi (đã bỏ qua):**\n`;
      const chunks = chunkLinesForDiscord(errLines, 1900 - errHeader.length);
      for (let i = 0; i < chunks.length; i++) {
        const prefix = i === 0 ? errHeader : `❌ **Tiếp theo (${i + 1}/${chunks.length}):**\n`;
        await interaction.followUp(prefix + chunks[i]);
      }
    }
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

    // Tự đăng ký slash commands khi bot start để không phải chạy `npm run deploy:commands` thủ công.
    // clientId lấy từ client.user.id (= application ID), guildId vẫn cần env.
    try {
      const { registerCommands } = require("./deploy-commands");
      const guildId = process.env.DISCORD_GUILD_ID;
      if (guildId) {
        const n = await registerCommands({ token: DISCORD_TOKEN, clientId: client.user.id, guildId });
        console.log(`✅ Registered ${n} slash commands to guild ${guildId}`);
      } else {
        console.warn("[STARTUP] DISCORD_GUILD_ID not set — skipping slash command registration");
      }
    } catch (e) {
      console.error(`[STARTUP] Slash command registration failed: ${e.message}`);
    }

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

    // === CHANNEL GUARD ===
    // Chỉ cho slash commands chạy trong channels liệt kê ở env ALLOWED_CHANNEL_IDS (comma-separated).
    // Env trống → bot hoạt động ở mọi channel (failsafe).
    const allowedChannels = (process.env.ALLOWED_CHANNEL_IDS || "")
      .split(",").map(s => s.trim()).filter(Boolean);
    if (allowedChannels.length > 0 && !allowedChannels.includes(interaction.channelId)) {
      const channelMentions = allowedChannels.map(id => `<#${id}>`).join(", ");
      try {
        await interaction.reply({
          content: `❌ Lệnh này chỉ dùng được trong: ${channelMentions}`,
          ephemeral: true,
        });
      } catch (e) {
        console.error(`[CHANNEL_GUARD] reply failed: ${e.message}`);
      }
      return;
    }

    if (interaction.commandName === "testtoken") return handleTestTokenSlash(interaction, client);
    if (interaction.commandName === "ig_cancel") return handleIgCancel(interaction, { sheets });
    if (interaction.commandName === "ig_folder_schedule") return handleIgFolderSchedule(interaction, { sheets, drive });
    if (interaction.commandName === "ig_pause") return handleIgPause(interaction);
    if (interaction.commandName === "ig_resume") return handleIgResume(interaction);
    if (interaction.commandName === "ig_status") return handleIgStatus(interaction);
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

if (require.main === module) {
  main().catch(e => { console.error(e); process.exit(1); });
}

// v8: export hàm thuần để test offline (không side-effect — main() chỉ chạy khi là entrypoint)
module.exports = {
  canonSku, sortTabsNewestFirst, tabDateKey, withTimeout,
  prioritizeVideosFirst, chunkLinesForDiscord, pickPostedValue, jobSkipReason, tick
};
