const { Client, GatewayIntentBits } = require("discord.js");
const { DateTime } = require("luxon");

const { mustEnv } = require("./utils");
const { getClients } = require("./google");
const { parseFolderIdFromUrl, getFolderName, deriveSkuFromFolderName, listMediaFiles, driveDirectDownloadUrl } = require("./drive");
const { parseVnDatetime, appendJob, fetchAllJobs, updateRow, nowVn } = require("./queue");
const { igCreateMediaContainer, igCreateCarouselContainer, igPublish, igGetPermalink, waitUntilFinished } = require("./ig");

const DISCORD_TOKEN = mustEnv("DISCORD_TOKEN");
const QUEUE_SHEET_ID = mustEnv("SHEET_ID_QUEUE");

// Shop config
const SHOP = {
  MAUME: {
    name: "Màu mè",
    igUserId: mustEnv("IG_USER_ID_MAUME"),
    pageToken: mustEnv("FB_PAGE_TOKEN_MAUME"),
    sheetId: mustEnv("SHEET_ID_MAUME"),
    sheetTab: process.env.SHEET_TAB_MAUME || null,
    // MauMe: caption col E, code col L
    captionColIndexInRange: 0, // E in E:L
    codeColIndexInRange: 7       // L in E:L
  },
  BURGER: {
    name: "Burger",
    igUserId: mustEnv("IG_USER_ID_BURGER"),
    pageToken: mustEnv("FB_PAGE_TOKEN_BURGER"),
    sheetId: mustEnv("SHEET_ID_BURGER"),
    sheetTab: process.env.SHEET_TAB_BURGER || null,
    // Burger: code col F, caption col H -> range F:H
    captionColIndexInRange: 2, // H in F:H
    codeColIndexInRange: 0     // F in F:H
  }
};

async function getFirstSheetTitle(sheets, spreadsheetId) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId });
  const first = meta.data.sheets?.[0]?.properties?.title;
  if (!first) throw new Error("Không lấy được tên tab trong Google Sheet");
  return first;
}

async function findCaptionBySku({ sheets, shopKey, sku }) {
  const cfg = SHOP[shopKey];
  const tab = cfg.sheetTab || await getFirstSheetTitle(sheets, cfg.sheetId);

  const range = shopKey === "MAUME" ? `${tab}!E:L` : `${tab}!F:H`;
  const r = await sheets.spreadsheets.values.get({ spreadsheetId: cfg.sheetId, range });
  const rows = r.data.values || [];

  const target = String(sku).trim();
  for (const row of rows) {
    const code = (row[cfg.codeColIndexInRange] ?? "").toString().trim();
    if (code === target) {
      const caption = (row[cfg.captionColIndexInRange] ?? "").toString();
      return caption;
    }
  }
  return null;
}

async function publishJob({ client, channelId, shopKey, caption, mediaFiles }) {
  const cfg = SHOP[shopKey];

  // Single media
  if (mediaFiles.length === 1) {
    const f = mediaFiles[0];
    const isVideo = /\.mp4$/i.test(f.name);
    const creationId = await igCreateMediaContainer({
      igUserId: cfg.igUserId,
      pageToken: cfg.pageToken,
      imageUrl: isVideo ? null : driveDirectDownloadUrl(f.id),
      videoUrl: isVideo ? driveDirectDownloadUrl(f.id) : null,
      caption,
      isCarouselItem: false
    });

    if (isVideo) {
      await waitUntilFinished({ creationId, pageToken: cfg.pageToken });
    }

    const mediaId = await igPublish({ igUserId: cfg.igUserId, pageToken: cfg.pageToken, creationId });
    const permalink = await igGetPermalink({ mediaId, pageToken: cfg.pageToken });
    return { mediaId, permalink };
  }

  // Carousel: create children first, then parent carousel (<=10). :contentReference[oaicite:15]{index=15}
  const childrenIds = [];
  for (const f of mediaFiles) {
    const isVideo = /\.mp4$/i.test(f.name);
    const childCreationId = await igCreateMediaContainer({
      igUserId: cfg.igUserId,
      pageToken: cfg.pageToken,
      imageUrl: isVideo ? null : driveDirectDownloadUrl(f.id),
      videoUrl: isVideo ? driveDirectDownloadUrl(f.id) : null,
      caption: null,
      isCarouselItem: true
    });
    if (isVideo) {
      await waitUntilFinished({ creationId: childCreationId, pageToken: cfg.pageToken });
    }
    childrenIds.push(childCreationId);
  }

  const parentCreationId = await igCreateCarouselContainer({
    igUserId: cfg.igUserId,
    pageToken: cfg.pageToken,
    childrenIds,
    caption
  });

  const mediaId = await igPublish({ igUserId: cfg.igUserId, pageToken: cfg.pageToken, creationId: parentCreationId });
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
        client,
        channelId: job.channel_id,
        shopKey: job.shop,
        caption,
        mediaFiles
      });

      const publishedAt = nowVn().toISO();

      await updateRow(sheets, {
        queueSheetId: QUEUE_SHEET_ID,
        rowNum: job.rowNum,
        patch: { status: "SUCCESS", attempts: job.attempts + 1, ig_media_id: mediaId, ig_permalink: permalink, published_at: publishedAt }
      });

      if (channel) {
        await channel.send(`✅ Đăng thành công (${SHOP[job.shop].name}) | SKU: **${sku}** | ${permalink}`);
      }
    } catch (e) {
      const msg = (e.response?.data && JSON.stringify(e.response.data)) ? JSON.stringify(e.response.data) : (e.message || String(e));
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

    // Worker tick mỗi 60s (Railway Cron không hợp vì cron service phải exit, và min 5 phút). :contentReference[oaicite:16]{index=16}
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
      const msg = (e.message || String(e));
      await interaction.editReply(`❌ Tạo lịch thất bại: ${msg}`);
    }
  });

  await client.login(DISCORD_TOKEN);
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});