const axios = require("axios");
const { mustEnv } = require("./utils");

const GRAPH_API_VERSION = process.env.GRAPH_API_VERSION || "v25.0";
const BASE = `https://graph.facebook.com/${GRAPH_API_VERSION}`;

async function igCreateMediaContainer({ igUserId, pageToken, imageUrl, videoUrl, caption, isCarouselItem }) {
  const params = new URLSearchParams();
  if (caption) params.set("caption", caption);
  if (imageUrl) params.set("image_url", imageUrl);
  if (videoUrl) params.set("video_url", videoUrl);
  if (isCarouselItem) params.set("is_carousel_item", "true");
  params.set("access_token", pageToken);

  const url = `${BASE}/${igUserId}/media`;
  const r = await axios.post(url, params);
  return r.data.id; // creation_id
}

async function igGetContainerStatus({ creationId, pageToken }) {
  const url = `${BASE}/${creationId}`;
  const r = await axios.get(url, {
    params: { fields: "status_code,status", access_token: pageToken }
  });
  return r.data;
}

// Chờ FINISHED cho cả ảnh/video/parent carousel (không chỉ video)
async function waitUntilFinished({ creationId, pageToken, timeoutMs = 15 * 60 * 1000 }) {
  const started = Date.now();

  while (true) {
    const st = await igGetContainerStatus({ creationId, pageToken });
    const code = st.status_code || st.status; // fallback

    if (code === "FINISHED") return;
    if (code === "ERROR") throw new Error(`IG container ERROR: ${JSON.stringify(st)}`);

    if (Date.now() - started > timeoutMs) {
      throw new Error(`Timeout chờ IG container FINISHED: ${creationId} | ${JSON.stringify(st)}`);
    }

    await new Promise(res => setTimeout(res, 5000));
  }
}

async function igCreateCarouselContainer({ igUserId, pageToken, childrenIds, caption }) {
  const params = new URLSearchParams();
  params.set("media_type", "CAROUSEL");
  params.set("children", childrenIds.join(","));
  if (caption) params.set("caption", caption);
  params.set("access_token", pageToken);

  const url = `${BASE}/${igUserId}/media`;
  const r = await axios.post(url, params);
  return r.data.id;
}

async function igPublish({ igUserId, pageToken, creationId }) {
  const params = new URLSearchParams();
  params.set("creation_id", creationId);
  params.set("access_token", pageToken);

  const url = `${BASE}/${igUserId}/media_publish`;
  const r = await axios.post(url, params);
  return r.data.id; // published media id
}

// Retry publish nếu gặp 9007/2207027 (media chưa sẵn sàng)
async function igPublishWithRetry({ igUserId, pageToken, creationId, retries = 6, delayMs = 8000 }) {
  for (let i = 0; i <= retries; i++) {
    try {
      return await igPublish({ igUserId, pageToken, creationId });
    } catch (e) {
      const err = e?.response?.data?.error;
      const code = err?.code;
      const sub = err?.error_subcode;

      if (code === 9007 && sub === 2207027 && i < retries) {
        console.log(`[IG] Media not ready (9007/2207027). Retry publish in ${delayMs}ms (attempt ${i + 1}/${retries})`);
        await new Promise(r => setTimeout(r, delayMs));
        continue;
      }
      throw e;
    }
  }
}

async function igGetPermalink({ mediaId, pageToken }) {
  const url = `${BASE}/${mediaId}`;
  const r = await axios.get(url, {
    params: { fields: "permalink", access_token: pageToken }
  });
  return r.data.permalink;
}

module.exports = {
  igCreateMediaContainer,
  igCreateCarouselContainer,
  igPublish,
  igPublishWithRetry,
  igGetPermalink,
  waitUntilFinished
};
