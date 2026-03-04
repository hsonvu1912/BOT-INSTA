const axios = require("axios");
const http = require("http");

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
  waitUntilFinished
} = require("./ig");

const DISCORD_TOKEN = mustEnv("DISCORD_TOKEN");
const QUEUE_SHEET_ID = mustEnv("SHEET_ID_QUEUE");

// ===== Shop config =====
const SHOP = {
  MAUME: {
    name: "Màu mè",
    igUserId: mustEnv("IG_USER_ID_MAUME"),
    pageToken: mustEnv("FB_PAGE_TOKEN_MAUME"),
    sheetId: mustEnv("SHEET_ID_MAUME"),
    sheetTab: process.env.SHEET_TAB_MAUME || null,
    // MauMe: caption col E, code col L -> range E:L
    captionColIndexInRange: 0,
    codeColIndexInRange: 7
  },
  BURGER: {
    name: "Burger",
    igUserId: mustEnv("IG_USER_ID_BURGER"),
    pageToken: mustEnv("FB_PAGE_TOKEN_BURGER"),
    sheetId: mustEnv("SHEET_ID_BURGER"),
    sheetTab: process.env.SHEET_TAB_BURGER || null,
    // Burger: code col F, caption col H -> range F:H
    captionColIndexInRange: 2,
    codeColIndexInRange: 0
  }
};

// ===== Multi-tab SKU lookup (no monthly variable changes) =====
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
  const targetCanon = canonSku(sku);

  const allTitles = await getTabTitles(sheets, cfg.sheetId);
  const titles = cfg.sheetTab ? [cfg.sheetTab] : sortTabsNewestFirst(allTitles.filter(tabIsLikelyInventory));

  const ranges = titles.map(t => (shopKey === "MAUME" ? `${t}!E:L` : `${t}!F:H`));

  const CHUNK = 80;
  for (let i = 0; i < ranges.length; i += CHUNK) {
    const chunkRanges = ranges.slice(i, i + CHUNK);

    const resp = await sheets.spreadsheets.values.batchGet({
      spreadsheetId: cfg.sheetId,
      ranges: chunkRanges,
      majorDimension: "ROWS"
    });

    const valueRanges = resp.data.valueRanges || [];
    for (const vr of valueRanges) {
      const rows = vr.values || [];
      for (const row of rows) {
        const codeRaw = (row[cfg.codeColIndexInRange] ?? "").toString();
        const codeCanon = canonSku(codeRaw);
        if (codeCanon === targetCanon) {
          return (row[cfg.captionColIndexInRange] ?? "").toString();
        }
      }
    }
  }

  return null;
}
// ===== End multi-tab SKU lookup =====

function isVideoName(name) {
  return /\.mp4$/i.test(name || "");
}

// Nếu folder có mp4: bắt buộc mp4 đứng đầu
function prioritizeVideosFirst(mediaFiles) {
  const videos = [];
  const images = [];
  for (const f of mediaFiles) {
    if (isVideoName(f.name)) videos.push(f);
    else images.push(f);
  }
  return videos.length ? [...videos, ...images] : mediaFiles;
}

// Validate media URL: HEAD trước, fail thì GET Range
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
  for (const f of ordered) {
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

async function tick({ client, sheets, drive }) {
  const { items } = await fetchAllJobs(sheets, { queueSheetId: QUEUE_SHEET_ID });
  const now = nowVn();

  const due = items.filter(j => {
    if (j.status !== "PENDING") return false;
    const dt = DateTime.fromISO(j.scheduled_time, { zone: "Asia/Ho_Chi_Minh" });
    if (!dt.isValid) return false;
    return dt <= now;
  });

  for (const job of due) {
    const channel = await client.channels.fetch(job.channel_id).catch(() => null);

    try {
      await updateRow(sheets, {
        queueSheetId: QUEUE_SHEET_ID,
        rowNum: job.rowNum,
        patch: { status: "RUNNING", attempts: job.attempts + 1, last_error: "" }
      });

      const folderName = await getFolderName(drive, job.folder_id);
      const sku = job.sku || deriveSkuFromFolderName(folderName);

      const caption = await findCaptionBySku({ sheets, shopKey: job.shop, sku });
      if (!caption) throw new Error(`Không tìm thấy caption cho SKU=${sku} trong sheet shop ${job.shop}`);

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

      if (channel) {
        await channel.send(`✅ Đăng thành công (${SHOP[job.shop].name}) | SKU: **${sku}** | ${permalink}`);
      }
    } catch (e) {
      const msg = (e.response?.data && JSON.stringify(e.response.data))
        ? JSON.stringify(e.response.data)
        : (e.message || String(e));

      await updateRow(sheets, {
        queueSheetId: QUEUE_SHEET_ID,
        rowNum: job.rowNum,
        patch: { status: "FAILED", attempts: job.attempts + 1, last_error: msg.slice(0, 5000) }
      });

      if (channel) {
        await channel.send(`❌ Đăng thất bại (${SHOP[job.shop].name}) | SKU: **${job.sku}**\nLý do: \`\`\`${msg.slice(0, 1800)}\`\`\``);
      }
    }
  }
}

async function main() {
  const client = new Client({ intents: [GatewayIntentBits.Guilds] });
  const { sheets, drive } = await getClients();

  client.on("ready", async () => {
    console.log(`✅ Logged in as ${client.user.tag}`);

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
    if (interaction.commandName !== "ig_schedule") return;

    await interaction.deferReply({ ephemeral: true });

    try {
      const shopKey = interaction.options.getString("shop", true);
      const timeStr = interaction.options.getString("time", true);
      const folderUrl = interaction.options.getString("folder", true);

      const dt = parseVnDatetime(timeStr);
      if (dt < nowVn().minus({ minutes: 1 })) throw new Error("Giờ đăng đang ở quá khứ.");

      const folderId = parseFolderIdFromUrl(folderUrl);
      const folderName = await getFolderName(drive, folderId);
      const sku = deriveSkuFromFolderName(folderName);

      const caption = await findCaptionBySku({ sheets, shopKey, sku });
      if (!caption) throw new Error(`Không tìm thấy caption cho SKU=${sku} trong sheet shop ${shopKey}`);

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
