const axios = require("axios");

const GRAPH_API_VERSION = process.env.GRAPH_API_VERSION || "v25.0";
const BASE = `https://graph.facebook.com/${GRAPH_API_VERSION}`;

function pickMediaUrls(payload) {
  const img = payload.imageUrl ?? payload.image_url ?? null;
  const vid = payload.videoUrl ?? payload.video_url ?? null;
  return { img, vid };
}

async function igCreateMediaContainer(payload) {
  const { igUserId, pageToken, caption, isCarouselItem, mediaType } = payload;
  const { img, vid } = pickMediaUrls(payload);

  if (!img && !vid) {
    throw new Error("Missing media URL: need imageUrl/image_url or videoUrl/video_url");
  }

  const params = new URLSearchParams();
  if (caption) params.set("caption", caption);

  // optional: nếu sau này bạn muốn ép reels/video type
  if (mediaType) params.set("media_type", mediaType);

  if (img) params.set("image_url", img);
  if (vid) params.set("video_url", vid);
  if (isCarouselItem) params.set("is_carousel_item", "true");

  params.set("access_token", pageToken);

  const url = `${BASE}/${igUserId}/media`;
  const r = await axios.post(url, params);
  return r.data.id; // creation_id
}

// Retry create container nếu Meta báo media chưa tải được (9004/2207052) hoặc lỗi mạng tạm
async function igCreateMediaContainerWithRetry(payload, { retries = 3, delayMs = 6000 } = {}) {
  for (let i = 0; i <= retries; i++) {
    try {
      return await igCreateMediaContainer(payload);
    } catch (e) {
      const err = e?.response?.data?.error;
      const code = err?.code;
      const sub = err?.error_subcode;
      const status = e?.response?.status;

      const isMediaFetchFail = code === 9004 && sub === 2207052;
      const isTransientHttp = status && status >= 500;
      const isNetwork = !status && (e.code || e.message);

      if ((isMediaFetchFail || isTransientHttp || isNetwork) && i < retries) {
        console.log(`[IG] create container retry in ${delayMs}ms (attempt ${i + 1}/${retries})`, err?.message || e.message);
        await new Promise(r => setTimeout(r, delayMs));
        continue;
      }
      throw e;
    }
  }
}

async function igGetContainerStatus({ creationId, pageToken }) {
  const url = `${BASE}/${creationId}`;
  const r = await axios.get(url, {
    params: { fields: "status_code,status", access_token: pageToken }
  });
  return r.data;
}

// Chờ FINISHED cho cả ảnh/video/parent carousel
async function waitUntilFinished({ creationId, pageToken, timeoutMs = 15 * 60 * 1000 }) {
  const started = Date.now();

  while (true) {
    const st = await igGetContainerStatus({ creationId, pageToken });
    const code = st.status_code || st.status;

    if (code === "FINISHED") return;
    if (code === "ERROR") throw new Error(`IG container ERROR: ${JSON.stringify(st)}`);

    if (Date.now() - started > timeoutMs) {
      throw new Error(`Timeout chờ FINISHED: ${creationId} | ${JSON.stringify(st)}`);
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
  return r.data.id; // parent creation_id
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
async function igPublishWithRetry({ igUserId, pageToken, creationId, retries = 8, delayMs = 7000 }) {
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
  igCreateMediaContainerWithRetry,
  igCreateCarouselContainer,
  igPublishWithRetry,
  igGetPermalink,
  waitUntilFinished
};
