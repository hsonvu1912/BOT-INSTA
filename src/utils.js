function mustEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env: ${name}`);
  return v;
}

function parseDriveFolderId(url) {
  // Accept:
  // - https://drive.google.com/drive/folders/<ID>
  // - https://drive.google.com/drive/u/0/folders/<ID>
  // - https://drive.google.com/open?id=<ID>
  // - raw <ID>
  if (!url) throw new Error("Folder URL is empty");

  const trimmed = String(url).trim();
  if (/^[a-zA-Z0-9_-]{10,}$/.test(trimmed)) return trimmed;

  const m1 = trimmed.match(/\/folders\/([a-zA-Z0-9_-]+)/);
  if (m1) return m1[1];

  const m2 = trimmed.match(/[?&]id=([a-zA-Z0-9_-]+)/);
  if (m2) return m2[1];

  throw new Error("Không parse được Folder ID từ link Drive");
}

function naturalSortByName(a, b) {
  return a.name.localeCompare(b.name, "en", { numeric: true, sensitivity: "base" });
}

function isJpeg(name) {
  return /\.(jpe?g)$/i.test(name);
}
function isMp4(name) {
  return /\.mp4$/i.test(name);
}

module.exports = { mustEnv, parseDriveFolderId, naturalSortByName, isJpeg, isMp4 };