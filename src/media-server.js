// src/media-server.js
const { URL } = require("url");
const { mustEnv } = require("./utils");

// Cache metadata để khi IG fetch không cần gọi Drive 2 lần
const metaCache = new Map();
const META_CACHE_TTL_MS = 30 * 60 * 1000;

function getCachedMeta(fileId) {
  const entry = metaCache.get(fileId);
  if (!entry) return null;
  if (Date.now() - entry.ts > META_CACHE_TTL_MS) {
    metaCache.delete(fileId);
    return null;
  }
  return entry.data;
}

function setCachedMeta(fileId, data) {
  if (metaCache.size > 500) {
    const oldest = metaCache.keys().next().value;
    metaCache.delete(oldest);
  }
  metaCache.set(fileId, { ts: Date.now(), data });
}

function createMediaHandler({ drive }) {
  const token = mustEnv("MEDIA_PROXY_TOKEN");

  return async (req, res) => {
    try {
      if (req.method !== "GET" && req.method !== "HEAD") {
        res.statusCode = 405;
        res.setHeader("Content-Type", "text/plain; charset=utf-8");
        return res.end("method not allowed");
      }

      const u = new URL(req.url, `http://${req.headers.host || "localhost"}`);

      if (u.pathname === "/health") {
        res.statusCode = 200;
        res.setHeader("Content-Type", "text/plain; charset=utf-8");
        return res.end("ok");
      }

      const m = u.pathname.match(/^\/media\/([a-zA-Z0-9_-]+)$/);
      if (!m) {
        res.statusCode = 404;
        res.setHeader("Content-Type", "text/plain; charset=utf-8");
        return res.end("not found");
      }

      if (u.searchParams.get("token") !== token) {
        res.statusCode = 403;
        res.setHeader("Content-Type", "text/plain; charset=utf-8");
        return res.end("forbidden");
      }

      const fileId = m[1];

      let mimeType, size;
      const cached = getCachedMeta(fileId);

      if (cached) {
        mimeType = cached.mimeType;
        size = cached.size;
      } else {
        const meta = await drive.files.get({
          fileId,
          fields: "id,name,mimeType,size",
          supportsAllDrives: true,
        });

        mimeType = meta.data?.mimeType;
        size = meta.data?.size;

        if (mimeType && (mimeType.startsWith("image/") || mimeType.startsWith("video/"))) {
          setCachedMeta(fileId, { mimeType, size });
        }
      }

      if (!mimeType || (!mimeType.startsWith("image/") && !mimeType.startsWith("video/"))) {
        res.statusCode = 415;
        res.setHeader("Content-Type", "text/plain; charset=utf-8");
        return res.end("unsupported media type");
      }

      if (req.method === "HEAD") {
        res.statusCode = 200;
        res.setHeader("Content-Type", mimeType);
        if (size) res.setHeader("Content-Length", String(size));
        res.setHeader("Cache-Control", "public, max-age=3600");
        return res.end();
      }

      const file = await drive.files.get(
        { fileId, alt: "media", supportsAllDrives: true },
        { responseType: "stream" }
      );

      res.statusCode = 200;
      res.setHeader("Content-Type", mimeType);
      if (size) res.setHeader("Content-Length", String(size));
      res.setHeader("Cache-Control", "public, max-age=3600");

      file.data.on("error", (e) => {
        console.error("Drive stream error:", e);
        try { res.destroy(e); } catch {}
      });

      file.data.pipe(res);
    } catch (e) {
      const status = e?.response?.status || 500;
      const details = e?.response?.data ? JSON.stringify(e.response.data) : (e.message || String(e));
      console.error("MEDIA proxy error:", status, details);

      res.statusCode = status === 404 ? 404 : 500;
      res.setHeader("Content-Type", "text/plain; charset=utf-8");
      res.end("error");
    }
  };
}

module.exports = { createMediaHandler };
