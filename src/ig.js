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
  const msg = typeof err.message === "string" ? err.message : "";
  return msg.includes("Only photo or video can be accepted");
}

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

        if (isRateLimitError(e) && i < retries) {
          const waitMs = 60000 * (i + 1);
          console.log(`[IG] Rate limit hit. Waiting ${waitMs / 1000}s (attempt ${i + 1}/${retries})`);
          await new Promise(r => setTimeout(r, waitMs));
          continue;
        }

        if (isOnlyPhotoOrVideoError(e) && i < retries) {
          const waitMs = 20000 * (i + 1);
          console.log(`[IG] "Only photo or video" — proxy likely cold. Retry in ${waitMs / 1000}s (attempt ${i + 1}/${retries})`);
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

// ===== v4: waitUntilFinished — smart single-poll cho ảnh =====
// Ảnh JPEG: chờ 2s → check 1 lần → nếu FINISHED thì xong (1 API call thay vì 3-5)
//           nếu chưa → vào loop backoff bình thường
// Video: vào loop backoff luôn (8s → 15s)
async function waitUntilFinished({ creationId, pageToken, timeoutMs = 15 * 60 * 1000, isVideo = false }) {
  const started = Date.now();

  // Ảnh: chờ 2s rồi check 1 shot trước
  if (!isVideo) {
    await new Promise(res => setTimeout(res, 2000));
    const st = await igGetContainerStatus({ creationId, pageToken });
    const code = st.status_code || st.status;
    if (code === "FINISHED") return;
    if (code === "ERROR") throw new Error(`IG container ERROR: ${JSON.stringify(st)}`);
    // Chưa FINISHED → fall through vào loop (hiếm khi xảy ra với ảnh)
  }

  // Loop với progressive backoff
  let pollInterval = isVideo ? 8000 : 5000;
  const maxInterval = isVideo ? 15000 : 10000;
  const backoffFactor = 1.5;

  while (true) {
    await new Promise(res => setTimeout(res, pollInterval));

    const st = await igGetContainerStatus({ creationId, pageToken });
    const code = st.status_code || st.status;

    if (code === "FINISHED") return;
    if (code === "ERROR") throw new Error(`IG container ERROR: ${JSON.stringify(st)}`);

    if (Date.now() - started > timeoutMs) {
      throw new Error(`Timeout chờ FINISHED: ${creationId} | ${JSON.stringify(st)}`);
    }

    pollInterval = Math.min(Math.round(pollInterval * backoffFactor), maxInterval);
  }
}

// ===== v4: Batch poll — chờ nhiều containers cùng lúc =====
// Thay vì poll từng child tuần tự, poll tất cả trong 1 vòng lặp
// Mỗi vòng check tất cả chưa FINISHED, tốn ít API calls hơn tổng cộng
// vì nhiều container sẽ FINISHED cùng lúc (nhất là ảnh)
async function waitAllUntilFinished({ items, pageToken, timeoutMs = 15 * 60 * 1000 }) {
  // items = [{ creationId, isVideo }]
  const started = Date.now();
  const pending = new Map(); // creationId → { isVideo }
  for (const it of items) {
    pending.set(it.creationId, { isVideo: it.isVideo });
  }

  // Bước 1: chờ 2s rồi check tất cả 1 shot (ảnh thường xong ngay)
  await new Promise(res => setTimeout(res, 2500));

  for (const [cid] of pending) {
    const st = await igGetContainerStatus({ creationId: cid, pageToken });
    const code = st.status_code || st.status;
    if (code === "FINISHED") {
      pending.delete(cid);
    } else if (code === "ERROR") {
      throw new Error(`IG container ERROR: ${cid} | ${JSON.stringify(st)}`);
    }
  }

  if (pending.size === 0) return; // Tất cả ảnh xong sau 1 lần poll

  console.log(`[BATCH-POLL] ${items.length - pending.size}/${items.length} done after first check. ${pending.size} remaining (likely videos)...`);

  // Bước 2: poll remaining với backoff
  let pollInterval = 8000;
  const maxInterval = 15000;

  while (pending.size > 0) {
    await new Promise(res => setTimeout(res, pollInterval));

    for (const [cid] of pending) {
      const st = await igGetContainerStatus({ creationId: cid, pageToken });
      const code = st.status_code || st.status;
      if (code === "FINISHED") {
        pending.delete(cid);
      } else if (code === "ERROR") {
        throw new Error(`IG container ERROR: ${cid} | ${JSON.stringify(st)}`);
      }
    }

    if (Date.now() - started > timeoutMs) {
      const remaining = [...pending.keys()].join(", ");
      throw new Error(`Timeout chờ FINISHED cho: ${remaining}`);
    }

    pollInterval = Math.min(Math.round(pollInterval * 1.4), maxInterval);
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

async function igPublishWithRetry({ igUserId, pageToken, creationId, retries = 8, delayMs = 15000 }) {
  for (let i = 0; i <= retries; i++) {
    try {
      return await igPublish({ igUserId, pageToken, creationId });
    } catch (e) {
      const err = e?.response?.data?.error;
      const code = err?.code;
      const sub = err?.error_subcode;

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
  waitAllUntilFinished,
  isRateLimitError
};
