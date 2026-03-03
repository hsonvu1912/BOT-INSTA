// src/media-server.js
const express = require("express");
const { mustEnv } = require("./utils");

function createMediaServer({ drive }) {
  const app = express();

  const token = mustEnv("MEDIA_PROXY_TOKEN");

  app.get("/health", (req, res) => res.status(200).send("ok"));

  // Public URL for Meta to fetch:
  // GET /media/:fileId?token=...
  app.get("/media/:fileId", async (req, res) => {
    try {
      if (req.query.token !== token) {
        return res.status(403).send("forbidden");
      }

      const fileId = req.params.fileId;

      // Get metadata for mimeType + name
      const meta = await drive.files.get({
        fileId,
        fields: "id,name,mimeType,size",
        supportsAllDrives: true,
      });

      const { name, mimeType } = meta.data || {};
      if (!mimeType || (!mimeType.startsWith("image/") && !mimeType.startsWith("video/"))) {
        return res.status(415).send("unsupported media type");
      }

      res.setHeader("Content-Type", mimeType);
      res.setHeader("Content-Disposition", `inline; filename="${name || fileId}"`);
      // Cache a bit so Meta can retry without re-pulling from Drive every time
      res.setHeader("Cache-Control", "public, max-age=3600");

      // Stream file bytes from Drive using alt=media :contentReference[oaicite:2]{index=2}
      const file = await drive.files.get(
        { fileId, alt: "media", supportsAllDrives: true },
        { responseType: "stream" }
      );

      file.data.on("error", (e) => {
        console.error("Drive stream error:", e);
        if (!res.headersSent) res.status(502);
        res.end();
      });

      file.data.pipe(res);
    } catch (e) {
      const status = e?.response?.status || 500;
      console.error("media proxy error:", status, e?.message || e);
      res.status(status === 404 ? 404 : 500).send("error");
    }
  });

  return app;
}

module.exports = { createMediaServer };
