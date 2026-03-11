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
  isRateLimitError
} = require("./ig");

const DISCORD_TOKEN = mustEnv("DISCORD_TOKEN");
const QUEUE_SHEET_ID = mustEnv("SHEET_ID_QUEUE");

// FIX: Delay giữa các job để tránh rate-limit (30s)
const DELAY_BETWEEN_JOBS_MS = 30000;

// FIX: Delay giữa carousel children (3s)
const DELAY_BETWEEN_CHILDREN_MS = 3000;

// ===== Shop config =====
const SHOP = {
  MAUME: {
    name: "Màu mè",
    igUserId: mustEnv("IG_USER_ID_MAUME"),
    pageToken: mustEnv("FB_PAGE_TOKEN_MAUME"),
    sheetId: mustEnv("SHEET_ID_MAUME"),
    sheetTab: null,
    captionColIndexInRange: 0,
    codeColIndexInRange: 7,
    khoStatusCol: "B"       // Cột "tình trạng đăng bài" trong kho Màu Mè
  },
  BURGER: {
    name: "Burger",
    igUserId: mustEnv("IG_USER_ID_BURGER"),
    pageToken: mustEnv("FB_PAGE_TOKEN_BURGER"),
    sheetId: mustEnv("SHEET_ID_BURGER"),
    sheetTab: null,
    captionColIndexInRange: 3,  // Cột I (F+3)
    codeColIndexInRange: 0,     // Cột F
    khoStatusCol: "D"           // Cột "tình trạng đăng bài" trong kho Burger
  }
};

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

/**
 * Tìm caption theo SKU trong kho sheet.
 * Trả về { caption, tabName, rowNum } hoặc null nếu không tìm thấy.
 * tabName + rowNum dùng để cập nhật tình trạng đăng bài sau khi publish.
 */
async function findCaptionBySku({ sheets, shopKey, sku }) {
  const cfg = SHOP[shopKey];
  const targetCanon = canonSku(sku);

  const allTitles = await getTabTitles(sheets, cfg.sheetId);
  const titles = cfg.sheetTab ? [cfg.sheetTab] : sortTabsNewestFirst(allTitles.filter(tabIsLikelyInventory));

  const ranges = titles.map(t => (shopKey === "MAUME" ? `${t}!E:L` : `${t}!F:I`));

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

// ===== Data validation cache: key = "shopKey:tabName" =====
const khoValidationCache = new Map();
const KHO_VALIDATION_CACHE_TTL_MS = 30 * 60 * 1000; // 30 phút

// Từ khoá ưu tiên để match giá trị "đã đăng" trong dropdown (thứ tự ưu tiên giảm dần)
const POSTED_KEYWORDS = ["đã đăng", "da dang", "đăng rồi", "dang roi", "done", "posted", "đã up", "da up"];

/**
 * Đọc data validation (dropdown) từ một ô cụ thể trong kho sheet.
 * Trả về mảng string các giá trị dropdown, hoặc null nếu không có validation.
 */
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
    // Không có dropdown validation — cache null để không gọi lại liên tục
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

/**
 * Chọn giá trị "đã đăng" từ danh sách dropdown values.
 * Match bằng cách normalize tiếng Việt rồi so sánh với từ khoá.
 * Trả về { value, matched } hoặc null nếu không match được.
 */
function pickPostedValue(dropdownValues) {
  if (!dropdownValues || dropdownValues.length === 0) return null;

  // Normalize: bỏ dấu, lowercase
  function norm(s) {
    return String(s)
      .trim()
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/[đĐ]/g, "d");
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

/**
 * Cập nhật cột "tình trạng đăng bài" trong kho sheet sau khi đăng IG thành công.
 * Tự đọc data validation rules để ghi đúng giá trị dropdown.
 */
async function updateKhoPostStatus({ sheets, shopKey, tabName, rowNum }) {
  const cfg = SHOP[shopKey];
  const col = cfg.khoStatusCol;
  const range = `${tabName}!${col}${rowNum}`;

  // 1) Đọc dropdown values từ data validation
  const dropdownValues = await readCellDropdownValues({
    sheets,
    spreadsheetId: cfg.sheetId,
    tabName,
    col,
    rowNum
  });

  let valueToWrite;

  if (dropdownValues && dropdownValues.length > 0) {
    const pick = pickPostedValue(dropdownValues);
    if (pick) {
      valueToWrite = pick.value;
      console.log(`[KHO] Matched dropdown value "${pick.value}" via keyword "${pick.matched}"`);
    } else {
      // Có dropdown nhưng không match được — throw để cảnh báo
      throw new Error(
        `Không tìm thấy giá trị "đã đăng" trong dropdown [${dropdownValues.join(", ")}]. ` +
        `Nhờ cập nhật thủ công hoặc kiểm tra lại dropdown trong sheet kho.`
      );
    }
  } else {
    // Không có data validation — ghi trực tiếp
    valueToWrite = "Đã đăng";
    console.log(`[KHO] No dropdown validation found, writing default: "${valueToWrite}"`);
  }

  // 2) Ghi giá trị
  await sheets.spreadsheets.values.update({
    spreadsheetId: cfg.sheetId,
    range,
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

async function validateMediaUrl(mediaUrl, fileName) {
  const safeUrl = String(mediaUrl || "").replace(/token=[^&]+/i, "token=***");

  if (!mediaUrl || !mediaUrl.startsWith("https://")) {
    throw new Error(`MEDIA URL invalid: ${safeUrl} | file=${fileName}`);
  }

  const headResp = await axios.head(mediaUrl, { timeout: 15000 }).catch(err => err.response);
  const headStatus = headResp?.status;
  const headCt = headResp?.headers?.["content-type"] || "";
  console.log("[MEDIA-HEAD]", headStatus, headCt, safeUrl, fileName);

  if (headStatus === 200 && (headCt.startsWith("image/") || headCt.startsWith("video/"))) return;

  const getResp = await axios.get(mediaUrl, {
    timeout: 20000,
    responseType: "arraybuffer",
    headers: { Range: "bytes=0-1023" },
    validateStatus: () => true
  });

  const st = getResp.status;
  const ct = getResp.headers?.["content-type"] || "";
  console.log("[MEDIA-GET]", st, ct, safeUrl, fileName);

  if ((st === 200 || st === 206) && (ct.startsWith("image/") || ct.startsWith("video/"))) return;

  throw new Error(`MEDIA URL not serving media: status=${st} ct=${ct} url=${safeUrl}`);
}

async function publishJob({ shopKey, caption, mediaFiles }) {
  const cfg = SHOP[shopKey];
  const ordered = prioritizeVideosFirst(mediaFiles);

  // Single
  if (ordered.length === 1) {
    const f = ordered[0];
    const isVideo = isVideoName(f.name);

    const mediaUrl = driveDirectDownloadUrl(f.id);
    await validateMediaUrl(mediaUrl, f.name);

    const creationId = await igCreateMediaContainerWithRetry({
      igUserId: cfg.igUserId,
      pageToken: cfg.pageToken,
      imageUrl: isVideo ? null : mediaUrl,
      videoUrl: isVideo ? mediaUrl : null,
      caption,
      isCarouselItem: false
    });

    await waitUntilFinished({ creationId, pageToken: cfg.pageToken });

    const mediaId = await igPublishWithRetry({
      igUserId: cfg.igUserId,
      pageToken: cfg.pageToken,
      creationId
    });

    const permalink = await igGetPermalink({ mediaId, pageToken: cfg.pageToken });
    return { mediaId, permalink };
  }

  // Carousel
  const childrenIds = [];
  for (let idx = 0; idx < ordered.length; idx++) {
    const f = ordered[idx];
    const isVideo = isVideoName(f.name);

    const mediaUrl = driveDirectDownloadUrl(f.id);
    await validateMediaUrl(mediaUrl, f.name);

    const childCreationId = await igCreateMediaContainerWithRetry({
      igUserId: cfg.igUserId,
      pageToken: cfg.pageToken,
      imageUrl: isVideo ? null : mediaUrl,
      videoUrl: isVideo ? mediaUrl : null,
      caption: null,
      isCarouselItem: true
    });

    await waitUntilFinished({ creationId: childCreationId, pageToken: cfg.pageToken });
    childrenIds.push(childCreationId);

    // FIX: Delay giữa các children để giảm burst
    if (idx < ordered.length - 1) {
      await new Promise(r => setTimeout(r, DELAY_BETWEEN_CHILDREN_MS));
    }
  }

  const parentCreationId = await igCreateCarouselContainer({
    igUserId: cfg.igUserId,
    pageToken: cfg.pageToken,
    childrenIds,
    caption
  });

  await waitUntilFinished({ creationId: parentCreationId, pageToken: cfg.pageToken });

  const mediaId = await igPublishWithRetry({
    igUserId: cfg.igUserId,
    pageToken: cfg.pageToken,
    creationId: parentCreationId
  });

  const permalink = await igGetPermalink({ mediaId, pageToken: cfg.pageToken });
  return { mediaId, permalink };
}

// FIX: Lock để tick() không chồng chéo khi job chạy lâu
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

    const due = items.filter(j => {
      if (j.status !== "PENDING") return false;
      const dt = DateTime.fromISO(j.scheduled_time, { zone: "Asia/Ho_Chi_Minh" });
      if (!dt.isValid) return false;
      return dt <= now;
    });

    // FIX: Xử lý tuần tự từng job, có delay giữa các job
    for (let jobIdx = 0; jobIdx < due.length; jobIdx++) {
      const job = due[jobIdx];
      const channel = await client.channels.fetch(job.channel_id).catch(() => null);

      try {
        await updateRow(sheets, {
          queueSheetId: QUEUE_SHEET_ID,
          rowNum: job.rowNum,
          patch: { status: "RUNNING", attempts: job.attempts + 1, last_error: "" }
        });

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

        // ===== Cập nhật tình trạng đăng bài trong kho sheet =====
        try {
          const statusValue = await updateKhoPostStatus({
            sheets,
            shopKey: job.shop,
            tabName: khoTab,
            rowNum: khoRow
          });
          if (channel) {
            await channel.send(`✅ Đăng thành công (${SHOP[job.shop].name}) | SKU: **${sku}** | ${permalink}\n📋 Kho: cập nhật → **${statusValue}**`);
          }
        } catch (khoErr) {
          console.error(`[KHO] Failed to update kho status for SKU=${sku}:`, khoErr.message);
          // Không throw — job IG vẫn thành công, chỉ cảnh báo lỗi kho
          if (channel) {
            await channel.send(
              `✅ Đăng IG thành công (${SHOP[job.shop].name}) | SKU: **${sku}** | ${permalink}\n` +
              `⚠️ Không cập nhật được tình trạng trong sheet kho: ${khoErr.message}\n` +
              `**Nhờ cập nhật cột trạng thái đăng bài trong kho thủ công nhé!**`
            );
          }
        }
      } catch (e) {
        const msg = (e.response?.data && JSON.stringify(e.response.data))
          ? JSON.stringify(e.response.data)
          : (e.message || String(e));

        // FIX: Nếu rate-limit, đánh dấu PENDING lại thay vì FAILED để retry lần sau
        const isRL = isRateLimitError(e);
        const newStatus = isRL ? "PENDING" : "FAILED";

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
          } else {
            await channel.send(
              `❌ Đăng thất bại (${SHOP[job.shop].name}) | SKU: **${job.sku}**\nLý do: \`\`\`${msg.slice(0, 1500)}\`\`\`\n` +
              `⚠️ **Nhờ mọi người đăng bài thủ công lên IG và cập nhật cột tình trạng đăng bài trong sheet kho nhé!**`
            );
          }
        }

        // FIX: Nếu rate-limit, dừng xử lý các job còn lại trong tick này
        if (isRL) {
          console.log("[TICK] Rate-limited — stopping remaining jobs, will retry next tick");
          break;
        }
      }

      // FIX: Delay giữa các job
      if (jobIdx < due.length - 1) {
        console.log(`[TICK] Waiting ${DELAY_BETWEEN_JOBS_MS / 1000}s before next job...`);
        await new Promise(r => setTimeout(r, DELAY_BETWEEN_JOBS_MS));
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

    // FIX: Delay token check 30s sau khi start để không burst cùng lúc
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

    // FIX: Tick mỗi 90s thay vì 60s
    setInterval(() => tick({ client, sheets, drive }).catch(console.error), 90_000);
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
