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

// ===== FIX: Detect rate-limit error =====
function isRateLimitError(e) {
  const err = e?.response?.data?.error;
  return err?.code === 4 || err?.code === 32 || err?.code === 613;
}

async function igCreateMediaContainer(payload) {
  const { igUserId, pageToken, caption, isCarouselItem, mediaType } = payload;
  const { img, vid } = pickMediaUrls(payload);

  if (!img && !vid) {
    throw new Error("Missing media URL: need imageUrl/image_url or videoUrl/video_url");
  }

  const params = new URLSearchParams();
  if (caption) params.set("caption", caption);

  if (mediaType) params.set("media_type", mediaType);

  if (img) params.set("image_url", img);
  if (vid) params.set("video_url", vid);

  if (isCarouselItem) params.set("is_carousel_item", "true");

  params.set("access_token", pageToken);

  const url = `${BASE}/${igUserId}/media`;
  const r = await axios.post(url, params);
  return r.data.id;
}

function buildVideoVariants(payload) {
  const base = { ...payload };

  if (payload.isCarouselItem) {
    return [
      { ...base, mediaType: undefined },
      { ...base, mediaType: "VIDEO" },
      { ...base, mediaType: "REELS" }
    ];
  }

  return [
    { ...base, mediaType: "REELS" },
    { ...base, mediaType: "VIDEO" },
    { ...base, mediaType: undefined }
  ];
}

// FIX: Tăng delay retry lên 15s, thêm xử lý rate-limit (chờ 60s)
async function igCreateMediaContainerWithRetry(payload, { retries = 3, delayMs = 15000 } = {}) {
  const { vid } = pickMediaUrls(payload);

  const variants = vid ? buildVideoVariants(payload) : [payload];

  let lastErr = null;

  for (const v of variants) {
    for (let i = 0; i <= retries; i++) {
      try {
        return await igCreateMediaContainer(v);
      } catch (e) {
        lastErr = e;

        // FIX: Rate limit -> chờ lâu hơn rồi retry
        if (isRateLimitError(e) && i < retries) {
          const waitMs = 60000 * (i + 1); // 60s, 120s, 180s
          console.log(`[IG] Rate limit hit. Waiting ${waitMs / 1000}s before retry (attempt ${i + 1}/${retries})`);
          await new Promise(r => setTimeout(r, waitMs));
          continue;
        }

        if (isImageUrlRequiredError(e) || isVideoUrlRequiredError(e)) {
          break;
        }

        if ((isMediaFetchFail(e) || isServerError(e)) && i < retries) {
          console.log(`[IG] create container retry in ${delayMs}ms (attempt ${i + 1}/${retries})`, e?.response?.data?.error?.message || e.message);
          await new Promise(r => setTimeout(r, delayMs));
          continue;
        }

        throw e;
      }
    }
  }

  throw lastErr || new Error("Failed to create media container");
}

async function igGetContainerStatus({ creationId, pageToken }) {
  const url = `${BASE}/${creationId}`;
  const r = await axios.get(url, {
    params: { fields: "status_code,status", access_token: pageToken }
  });
  return r.data;
}

// FIX: Poll mỗi 20s thay vì 5s để giảm API calls
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

    // FIX: 20s thay vì 5s — giảm 75% API calls cho polling
    await new Promise(res => setTimeout(res, 20000));
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
  return r.data.id;
}

// FIX: Tăng delay publish retry lên 15s, thêm xử lý rate-limit
async function igPublishWithRetry({ igUserId, pageToken, creationId, retries = 8, delayMs = 15000 }) {
  for (let i = 0; i <= retries; i++) {
    try {
      return await igPublish({ igUserId, pageToken, creationId });
    } catch (e) {
      const err = e?.response?.data?.error;
      const code = err?.code;
      const sub = err?.error_subcode;

      // FIX: Rate limit -> chờ 60s+
      if (isRateLimitError(e) && i < retries) {
        const waitMs = 60000 * (i + 1);
        console.log(`[IG] Rate limit on publish. Waiting ${waitMs / 1000}s (attempt ${i + 1}/${retries})`);
        await new Promise(r => setTimeout(r, waitMs));
        continue;
      }

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
  waitUntilFinished,
  isRateLimitError
};
