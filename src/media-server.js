// src/media-server.js
const { URL } = require("url");
const { mustEnv } = require("./utils");

// ===== In-memory buffer =====
const fileBuffer = new Map();
const BUFFER_TTL_MS = 5 * 60 * 1000; // 5 phút (giảm từ 10)
const MAX_BUFFER_TOTAL_BYTES = 500 * 1024 * 1024; // 500MB (Hobby plan 8GB, thoải mái)

// Metadata cache
const metaCache = new Map();
const META_CACHE_TTL_MS = 30 * 60 * 1000;

function getCachedMeta(fileId) {
  const entry = metaCache.get(fileId);
  if (!entry) return null;
  if (Date.now() - entry.ts > META_CACHE_TTL_MS) { metaCache.delete(fileId); return null; }
  return entry.data;
}

function setCachedMeta(fileId, data) {
  if (metaCache.size > 500) metaCache.delete(metaCache.keys().next().value);
  metaCache.set(fileId, { ts: Date.now(), data });
}

function getBuffered(fileId) {
  const entry = fileBuffer.get(fileId);
  if (!entry) return null;
  if (Date.now() - entry.ts > BUFFER_TTL_MS) { fileBuffer.delete(fileId); return null; }
  return entry;
}

function getTotalBufferSize() {
  let total = 0;
  for (const [, entry] of fileBuffer) total += entry.buffer.length;
  return total;
}

function evictOldest() {
  let oldestKey = null, oldestTs = Infinity;
  for (const [key, entry] of fileBuffer) {
    if (entry.ts < oldestTs) { oldestTs = entry.ts; oldestKey = key; }
  }
  if (oldestKey) fileBuffer.delete(oldestKey);
}

function setBuffered(fileId, buffer, mimeType) {
  while (getTotalBufferSize() + buffer.length > MAX_BUFFER_TOTAL_BYTES && fileBuffer.size > 0) {
    evictOldest();
  }
  fileBuffer.set(fileId, { buffer, mimeType, size: buffer.length, ts: Date.now() });
}

// ===== v7: Xoá buffer theo danh sách file IDs (gọi sau khi publish xong) =====
function clearBufferedFiles(fileIds) {
  let cleared = 0;
  for (const id of fileIds) {
    if (fileBuffer.delete(id)) cleared++;
  }
  if (cleared > 0) {
    console.log(`[BUFFER] Cleared ${cleared} files. Remaining buffer: ${(getTotalBufferSize() / 1024 / 1024).toFixed(1)}MB`);
  }
}

// Cleanup expired buffers mỗi 2 phút
setInterval(() => {
  const now = Date.now();
  for (const [key, entry] of fileBuffer) {
    if (now - entry.ts > BUFFER_TTL_MS) fileBuffer.delete(key);
  }
}, 2 * 60 * 1000);

// ===== JPEG/MP4 magic bytes check =====
function detectMediaType(buffer) {
  if (!buffer || buffer.length < 12) return null;

  // JPEG: starts with FF D8 FF
  if (buffer[0] === 0xFF && buffer[1] === 0xD8 && buffer[2] === 0xFF) return "image/jpeg";

  // PNG: starts with 89 50 4E 47
  if (buffer[0] === 0x89 && buffer[1] === 0x50 && buffer[2] === 0x4E && buffer[3] === 0x47) return "image/png";

  // GIF: starts with GIF8
  if (buffer[0] === 0x47 && buffer[1] === 0x49 && buffer[2] === 0x46 && buffer[3] === 0x38) return "image/gif";

  // MP4: has 'ftyp' at offset 4
  if (buffer[4] === 0x66 && buffer[5] === 0x74 && buffer[6] === 0x79 && buffer[7] === 0x70) return "video/mp4";

  // WebP: starts with RIFF....WEBP
  if (buffer[0] === 0x52 && buffer[1] === 0x49 && buffer[2] === 0x46 && buffer[3] === 0x46 &&
      buffer[8] === 0x57 && buffer[9] === 0x45 && buffer[10] === 0x42 && buffer[11] === 0x50) return "image/webp";

  return null;
}

// ===== Prefetch: download + verify =====
async function prefetchFile(drive, fileId) {
  const existing = getBuffered(fileId);
  if (existing) return { mimeType: existing.mimeType, size: existing.size, cached: true };

  // Metadata
  let mimeType, declaredSize;
  const cachedMeta = getCachedMeta(fileId);
  if (cachedMeta) {
    mimeType = cachedMeta.mimeType;
    declaredSize = cachedMeta.size;
  } else {
    const meta = await drive.files.get({
      fileId, fields: "id,name,mimeType,size", supportsAllDrives: true,
    });
    mimeType = meta.data?.mimeType;
    declaredSize = meta.data?.size;
    if (mimeType) setCachedMeta(fileId, { mimeType, size: declaredSize });
  }

  if (!mimeType || (!mimeType.startsWith("image/") && !mimeType.startsWith("video/"))) {
    throw new Error(`Unsupported media type: ${mimeType}`);
  }

  // Download toàn bộ
  const file = await drive.files.get(
    { fileId, alt: "media", supportsAllDrives: true },
    { responseType: "arraybuffer" }
  );

  const buffer = Buffer.from(file.data);

  // ===== v7: Verify file integrity =====
  // Check 1: size > 0
  if (buffer.length === 0) {
    throw new Error(`Prefetch got empty file for ${fileId}`);
  }

  // Check 2: magic bytes match expected media type
  const detectedType = detectMediaType(buffer);
  if (detectedType) {
    // Dùng detected type thay vì Drive mimeType (chính xác hơn)
    mimeType = detectedType;
  } else if (buffer.length < 1024) {
    // File quá nhỏ + không detect được type → likely error page
    throw new Error(`Prefetch: file ${fileId} too small (${buffer.length}B) and unrecognized format`);
  }

  setBuffered(fileId, buffer, mimeType);

  console.log(`[PREFETCH] ${fileId} (${mimeType}, ${(buffer.length / 1024).toFixed(0)}KB). Total: ${(getTotalBufferSize() / 1024 / 1024).toFixed(1)}MB`);

  return { mimeType, size: buffer.length, cached: false };
}

function createMediaHandler({ drive }) {
  const token = mustEnv("MEDIA_PROXY_TOKEN");

  return async (req, res) => {
    try {
      if (req.method !== "GET" && req.method !== "HEAD") {
        res.statusCode = 405;
        return res.end("method not allowed");
      }

      const u = new URL(req.url, `http://${req.headers.host || "localhost"}`);

      if (u.pathname === "/health") {
        res.statusCode = 200;
        return res.end("ok");
      }

      // Serve media
      const m = u.pathname.match(/^\/media\/([a-zA-Z0-9_-]+)$/);
      if (!m) { res.statusCode = 404; return res.end("not found"); }
      if (u.searchParams.get("token") !== token) { res.statusCode = 403; return res.end("forbidden"); }

      const fileId = m[1];

      // Serve từ buffer (prefetch đã download sẵn)
      const buffered = getBuffered(fileId);
      if (buffered) {
        res.statusCode = 200;
        res.setHeader("Content-Type", buffered.mimeType);
        res.setHeader("Content-Length", String(buffered.size));
        res.setHeader("Cache-Control", "public, max-age=3600");
        if (req.method === "HEAD") return res.end();
        return res.end(buffered.buffer);
      }

      // Fallback: stream từ Drive (nếu chưa prefetch)
      let mimeType, size;
      const cached = getCachedMeta(fileId);
      if (cached) { mimeType = cached.mimeType; size = cached.size; }
      else {
        const meta = await drive.files.get({ fileId, fields: "id,name,mimeType,size", supportsAllDrives: true });
        mimeType = meta.data?.mimeType;
        size = meta.data?.size;
        if (mimeType && (mimeType.startsWith("image/") || mimeType.startsWith("video/"))) {
          setCachedMeta(fileId, { mimeType, size });
        }
      }

      if (!mimeType || (!mimeType.startsWith("image/") && !mimeType.startsWith("video/"))) {
        res.statusCode = 415;
        return res.end("unsupported media type");
      }

      if (req.method === "HEAD") {
        res.statusCode = 200;
        res.setHeader("Content-Type", mimeType);
        if (size) res.setHeader("Content-Length", String(size));
        return res.end();
      }

      const file = await drive.files.get(
        { fileId, alt: "media", supportsAllDrives: true },
        { responseType: "stream" }
      );

      res.statusCode = 200;
      res.setHeader("Content-Type", mimeType);
      if (size) res.setHeader("Content-Length", String(size));
      file.data.on("error", (e) => { try { res.destroy(e); } catch {} });
      file.data.pipe(res);
    } catch (e) {
      const status = e?.response?.status || 500;
      console.error("MEDIA proxy error:", status, e.message);
      res.statusCode = status === 404 ? 404 : 500;
      res.end("error");
    }
  };
}

module.exports = { createMediaHandler, prefetchFile, clearBufferedFiles };
