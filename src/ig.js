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

function isOnlyPhotoOrVideoError(e) {
  const err = e?.response?.data?.error;
  if (!err) return false;
  return (typeof err.message === "string" && err.message.includes("Only photo or video can be accepted"));
}

// ===== v7: Detect "image format is not supported" (code 36001) =====
function isImageFormatError(e) {
  const err = e?.response?.data?.error;
  return err?.code === 36001 && err?.error_subcode === 2207083;
}

function isRateLimitError(e) {
  const err = e?.response?.data?.error;
  return err?.code === 4 || err?.code === 32 || err?.code === 613;
}

// ===== v7: Gộp tất cả lỗi "Meta fetch fail" để retry chung =====
function isMetaFetchRelatedError(e) {
  return isOnlyPhotoOrVideoError(e) || isImageFormatError(e) || isMediaFetchFail(e);
}

async function igCreateMediaContainer(payload) {
  const { igUserId, pageToken, caption, isCarouselItem, mediaType } = payload;
  const { img, vid } = pickMediaUrls(payload);

  if (!img && !vid) throw new Error("Missing media URL");

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

        // Rate limit → throw ngay, để tick-level xử lý (tránh spam API trong loop)
        if (isRateLimitError(e)) {
          throw e;
        }

        // ===== v7: Tất cả lỗi Meta fetch (photo/video, format, 9004) → retry với delay =====
        if (isMetaFetchRelatedError(e) && i < retries) {
          const waitMs = 15000 * (i + 1); // 15s, 30s, 45s
          const errMsg = e?.response?.data?.error?.message || e.message;
          console.log(`[IG] Meta fetch error: "${errMsg}". Retry in ${waitMs / 1000}s (attempt ${i + 1}/${retries})`);
          await new Promise(r => setTimeout(r, waitMs));
          continue;
        }

        if (isImageUrlRequiredError(e) || isVideoUrlRequiredError(e)) break;

        if (isServerError(e) && i < retries) {
          console.log(`[IG] Server error. Retry in ${delayMs}ms (attempt ${i + 1}/${retries})`);
          await new Promise(r => setTimeout(r, delayMs));
          continue;
        }

        throw e;
      }
    }
  }

  throw lastErr || new Error("Failed to create media container");
}

// Meta deprecated `GET /{container_id}?fields=status_code` for Page Tokens around 2026-05.
// Polling now returns Authorization Error 100/33 even when the container is fine and publish
// would succeed. We replace polling with a fixed wait sized to the typical processing time.
// If the wait is too short, igPublishWithRetry already retries on code 9007 (not ready yet).
async function waitUntilFinished({ creationId, pageToken, isVideo = false }) {
  const ms = isVideo ? 25000 : 6000;
  await new Promise(res => setTimeout(res, ms));
}

async function waitAllUntilFinished({ items, pageToken }) {
  const hasVideo = items.some(it => it.isVideo);
  const ms = hasVideo ? 45000 : 12000;
  await new Promise(res => setTimeout(res, ms));
}

async function igCreateCarouselContainer({ igUserId, pageToken, childrenIds, caption }) {
  const params = new URLSearchParams();
  params.set("media_type", "CAROUSEL");
  params.set("children", childrenIds.join(","));
  if (caption) params.set("caption", caption);
  params.set("access_token", pageToken);
  const r = await axios.post(`${BASE}/${igUserId}/media`, params);
  return r.data.id;
}

async function igPublish({ igUserId, pageToken, creationId }) {
  const params = new URLSearchParams();
  params.set("creation_id", creationId);
  params.set("access_token", pageToken);
  const r = await axios.post(`${BASE}/${igUserId}/media_publish`, params);
  return r.data.id;
}

async function igPublishWithRetry({ igUserId, pageToken, creationId, retries = 8, delayMs = 15000 }) {
  for (let i = 0; i <= retries; i++) {
    try {
      return await igPublish({ igUserId, pageToken, creationId });
    } catch (e) {
      const err = e?.response?.data?.error;
      // Rate limit → throw ngay, để tick-level xử lý cooldown
      if (isRateLimitError(e)) {
        throw e;
      }
      if (err?.code === 9007 && err?.error_subcode === 2207027 && i < retries) {
        console.log(`[IG] Not ready. Retry publish in ${delayMs}ms (${i + 1}/${retries})`);
        await new Promise(r => setTimeout(r, delayMs));
        continue;
      }
      throw e;
    }
  }
}

// Meta deprecated `GET /{media_id}?fields=permalink` for Page Tokens (~2026-05) —
// returns code 10 "Insufficient permissions". The list endpoint /{ig_user_id}/media
// still exposes permalink, so we fetch recent media and pick by id.
async function igGetPermalink({ igUserId, mediaId, pageToken }) {
  const r = await axios.get(`${BASE}/${igUserId}/media`, {
    params: { fields: "id,permalink", limit: 10, access_token: pageToken }
  });
  const found = (r.data?.data || []).find(m => String(m.id) === String(mediaId));
  return found?.permalink || `https://www.instagram.com/?mediaid=${mediaId}`;
}

module.exports = {
  igCreateMediaContainerWithRetry,
  igCreateCarouselContainer,
  igPublishWithRetry,
  igGetPermalink,
  waitUntilFinished,
  waitAllUntilFinished,
  isRateLimitError
};
