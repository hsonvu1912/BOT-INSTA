const axios = require("axios");
const { mustEnv } = require("./utils");

const GRAPH_API_VERSION = process.env.GRAPH_API_VERSION || "v25.0"; // latest noted in changelog :contentReference[oaicite:12]{index=12}
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
  // Official reference mentions status_code for video readiness (FINISHED). :contentReference[oaicite:13]{index=13}
  const url = `${BASE}/${creationId}`;
  const r = await axios.get(url, {
    params: { fields: "status_code,status", access_token: pageToken }
  });
  return r.data;
}

async function waitUntilFinished({ creationId, pageToken, timeoutMs = 5 * 60 * 1000 }) {
  const started = Date.now();
  while (true) {
    const st = await igGetContainerStatus({ creationId, pageToken });
    const code = st.status_code;

    if (code === "FINISHED") return;
    if (code === "ERROR") throw new Error(`IG container ERROR: ${JSON.stringify(st)}`);

    if (Date.now() - started > timeoutMs) {
      throw new Error("Timeout chờ IG container FINISHED");
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
  // Media publish edge reference :contentReference[oaicite:14]{index=14}
  const params = new URLSearchParams();
  params.set("creation_id", creationId);
  params.set("access_token", pageToken);

  const url = `${BASE}/${igUserId}/media_publish`;
  const r = await axios.post(url, params);
  return r.data.id; // published media id
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
  igGetPermalink,
  waitUntilFinished
};