const axios = require("axios");

const GRAPH_API_VERSION = process.env.GRAPH_API_VERSION || "v25.0";
const BASE = `https://graph.facebook.com/${GRAPH_API_VERSION}`;

function pickMediaUrls(payload) {
  const img = payload.imageUrl ?? payload.image_url ?? null;
  const vid = payload.videoUrl ?? payload.video_url ?? null;
  return { img, vid };
}

function isImageUrlRequiredError(e) {
  const err = e?.response?.data?.error;
  return err?.code === 100 && typeof err?.message === "string" && err.message.includes("image_url is required");
}

function isVideoUrlRequiredError(e) {
  const err = e?.response?.data?.error;
  return err?.code === 100 && typeof err?.message === "string" && err.message.includes("video_url is required");
}

function isMediaFetchFail(e) {
  const err = e?.response?.data?.error;
  return err?.code === 9004 && err?.error_subcode === 2207052;
}

function isServerError(e) {
  const status = e?.response?.status;
  return status && status >= 500;
}

async function igCreateMediaContainer(payload) {
  const { igUserId, pageToken, caption, isCarouselItem, mediaType } = payload;
  const { img, vid } = pickMediaUrls(payload);

  if (!img && !vid) {
    throw new Error("Missing media URL: need imageUrl/image_url or videoUrl/video_url");
  }

  const params = new URLSearchParams();
  if (caption) params.set("caption", caption);

  // media_type chỉ set khi có (để hỗ trợ fallback)
  if (mediaType) params.set("media_type", mediaType);

  if (img) params.set("image_url", img);
  if (vid) params.set("video_url", vid);

  if (isCarouselItem) params.set("is_carousel_item", "true");

  params.set("access_token", pageToken);

  const url = `${BASE}/${igUserId}/media`;
  const r = await axios.post(url, params);
  return r.data.id; // creation_id
}

function buildVideoVariants(payload) {
  // Meta thay đổi khá thất thường:
  // - Single video: thường cần media_type=REELS :contentReference[oaicite:3]{index=3}
  // - Carousel item: docs nói reels không supported, nên thử theo thứ tự: (no media_type) -> VIDEO -> REELS :contentReference[oaicite:4]{index=4}
  const base = { ...payload };

  if (payload.isCarouselItem) {
    return [
      { ...base, mediaType: undefined },
      { ...base, mediaType: "VIDEO" },  // nếu Meta vẫn chấp nhận cho carousel item
      { ...base, mediaType: "REELS" }   // last resort (có thể fail nếu Meta cứng)
    ];
  }

  return [
    { ...base, mediaType: "REELS" },    // chuẩn nhất cho single video :contentReference[oaicite:5]{index=5}
    { ...base, mediaType: "VIDEO" },    // fallback (có thể bị deprecated tùy thời điểm) :contentReference[oaicite:6]{index=6}
    { ...base, mediaType: undefined }   // last resort
  ];
}

// Retry + fallback variants để giảm lỗi kiểu “image_url required”
async function igCreateMediaContainerWithRetry(payload, { retries = 3, delayMs = 6000 } = {}) {
  const { vid } = pickMediaUrls(payload);

  const variants = vid ? buildVideoVariants(payload) : [payload];

  let lastErr = null;

  for (const v of variants) {
    for (let i = 0; i <= retries; i++) {
      try {
        return await igCreateMediaContainer(v);
      } catch (e) {
        lastErr = e;

        // Nếu lỗi “image_url required” hoặc “video_url required” thường do kiểu payload không hợp lệ -> thử variant khác
        if (isImageUrlRequiredError(e) || isVideoUrlRequiredError(e)) {
          break;
        }

        // Media fetch fail / 5xx: retry cùng variant vì có thể Meta fetch chậm hoặc server chập
        if ((isMediaFetchFail(e) || isServerError(e)) && i < retries) {
          console.log(`[IG] create container retry in ${delayMs}ms (attempt ${i + 1}/${retries})`, e?.response?.data?.error?.message || e.message);
          await new Promise(r => setTimeout(r, delayMs));
          continue;
        }

        // Lỗi khác: ném ra luôn
        throw e;
      }
    }
  }

  // Nếu đi hết variants mà vẫn fail
  throw lastErr || new Error("Failed to create media container");
}

async function igGetContainerStatus({ creationId, pageToken }) {
  const url = `${BASE}/${creationId}`;
  const r = await axios.get(url, {
    params: { fields: "status_code,status", access_token: pageToken }
  });
  return r.data;
}

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
  return r.data.id;
}

async function igPublishWithRetry({ igUserId, pageToken, creationId, retries = 8, delayMs = 7000 }) {
  for (let i = 0; i <= retries; i++) {
    try {
      return await igPublish({ igUserId, pageToken, creationId });
    } catch (e) {
      const err = e?.response?.data?.error;
      const code = err?.code;
      const sub = err?.error_subcode;

      // 9007/2207027 = media chưa sẵn sàng đăng
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
