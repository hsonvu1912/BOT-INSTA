// src/media-server.js
const { URL } = require("url");
const { mustEnv } = require("./utils");

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

      // Health check
      if (u.pathname === "/health") {
        res.statusCode = 200;
        res.setHeader("Content-Type", "text/plain; charset=utf-8");
        return res.end("ok");
      }

      // /media/<fileId>
      const m = u.pathname.match(/^\/media\/([a-zA-Z0-9_-]+)$/);
      if (!m) {
        res.statusCode = 404;
        res.setHeader("Content-Type", "text/plain; charset=utf-8");
        return res.end("not found");
      }

      // Token guard
      if (u.searchParams.get("token") !== token) {
        res.statusCode = 403;
        res.setHeader("Content-Type", "text/plain; charset=utf-8");
        return res.end("forbidden");
      }

      const fileId = m[1];

      // 1) Get metadata
      const meta = await drive.files.get({
        fileId,
        fields: "id,name,mimeType,size",
        supportsAllDrives: true,
      });

      const { mimeType, size } = meta.data || {};
      if (!mimeType || (!mimeType.startsWith("image/") && !mimeType.startsWith("video/"))) {
        res.statusCode = 415;
        res.setHeader("Content-Type", "text/plain; charset=utf-8");
        return res.end("unsupported media type");
      }

      // 2) HEAD request: return headers only (no stream)
      if (req.method === "HEAD") {
        res.statusCode = 200;
        res.setHeader("Content-Type", mimeType);
        if (size) res.setHeader("Content-Length", String(size));
        res.setHeader("Cache-Control", "public, max-age=3600");
        return res.end();
      }

      // 3) Open stream from Drive
      const file = await drive.files.get(
        { fileId, alt: "media", supportsAllDrives: true },
        { responseType: "stream" }
      );

      // 4) Set correct headers (NO Content-Disposition to avoid invalid chars)
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
