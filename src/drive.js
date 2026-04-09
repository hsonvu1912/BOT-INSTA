const { parseDriveFolderId, naturalSortByName, isJpeg, isMp4 } = require("./utils");

async function listChildFolders(drive, parentFolderId) {
  const folders = [];
  let pageToken = undefined;
  do {
    const r = await drive.files.list({
      q: `'${parentFolderId}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false`,
      fields: "nextPageToken, files(id,name)",
      pageSize: 100,
      pageToken
    });
    folders.push(...(r.data.files || []));
    pageToken = r.data.nextPageToken;
  } while (pageToken);
  folders.sort(naturalSortByName);
  return folders;
}

async function getFolderName(drive, folderId) {
  const r = await drive.files.get({ fileId: folderId, fields: "id,name" });
  return r.data.name;
}

async function listMediaFiles(drive, folderId) {
  const files = [];
  let pageToken = undefined;

  do {
    const r = await drive.files.list({
      q: `'${folderId}' in parents and trashed=false`,
      fields: "nextPageToken, files(id,name,mimeType)",
      pageSize: 1000,
      pageToken
    });

    files.push(...(r.data.files || []));
    pageToken = r.data.nextPageToken;
  } while (pageToken);

  files.sort(naturalSortByName);

  // Only keep supported types for this bot:
  const media = files.filter(f => isJpeg(f.name) || isMp4(f.name));

  if (media.length === 0) {
    throw new Error("Folder không có file .jpg/.jpeg hoặc .mp4");
  }

  if (media.length > 10) {
    // IG carousel API limit is 10 items. :contentReference[oaicite:10]{index=10}
    throw new Error("Folder có hơn 10 media. API carousel chỉ cho tối đa 10.");
  }

  // Enforce JPEG for images (IG content publishing notes JPEG). :contentReference[oaicite:11]{index=11}
  const nonJpegImages = media.filter(f => !isMp4(f.name) && !isJpeg(f.name));
  if (nonJpegImages.length) {
    throw new Error("Ảnh phải là JPG/JPEG (không dùng PNG).");
  }

  return media;
}

function driveDirectDownloadUrl(fileId) {
  const base = process.env.PUBLIC_BASE_URL;
  const token = process.env.MEDIA_PROXY_TOKEN;

  // Bắt buộc phải có, để không “rụng” về Drive nữa
  if (!base || !token) {
    throw new Error("Missing PUBLIC_BASE_URL or MEDIA_PROXY_TOKEN (Option C required)");
  }

  const cleanBase = base.replace(/\/$/, "");
  return `${cleanBase}/media/${fileId}?token=${encodeURIComponent(token)}`;
}

function deriveSkuFromFolderName(folderName) {
  // Rule mặc định: lấy token đầu tiên trước dấu cách
  // Ví dụ: "MM-2403-001 Sản phẩm A" -> "MM-2403-001"
  const t = String(folderName).trim();
  return t.split(/\s+/)[0];
}

function parseFolderIdFromUrl(folderUrl) {
  return parseDriveFolderId(folderUrl);
}

module.exports = {
  getFolderName,
  listMediaFiles,
  listChildFolders,
  driveDirectDownloadUrl,
  deriveSkuFromFolderName,
  parseFolderIdFromUrl
};
