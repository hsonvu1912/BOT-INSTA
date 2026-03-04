/* =========================
   FILE 1: tokenReminder.js
   (Tạo file mới ở ROOT repo: tokenReminder.js)
   ========================= */
const https = require("https");

const DAY_MS = 24 * 60 * 60 * 1000;
const WARN_DAYS = 7;

function env(name, fallback = "") {
  const v = process.env[name];
  return v && String(v).trim() ? String(v).trim() : fallback;
}

function previewToken(t) {
  if (!t || typeof t !== "string") return "(no-token)";
  return `${t.slice(0, 6)}…${t.slice(-4)}`;
}

function httpsGetJson(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, (res) => {
        let buf = "";
        res.on("data", (c) => (buf += c));
        res.on("end", () => {
          let json;
          try {
            json = JSON.parse(buf);
          } catch {
            return reject(new Error("Response is not JSON"));
          }

          if (res.statusCode >= 200 && res.statusCode < 300) return resolve(json);
          const msg = json?.error?.message || `HTTP ${res.statusCode}`;
          reject(new Error(msg));
        });
      })
      .on("error", reject);
  });
}

async function debugToken(inputToken, appAccessToken) {
  const url =
    "https://graph.facebook.com/debug_token" +
    `?input_token=${encodeURIComponent(inputToken)}` +
    `&access_token=${encodeURIComponent(appAccessToken)}`;

  const json = await httpsGetJson(url);
  return json?.data;
}

function formatDateFromUnix(unixSeconds) {
  if (!unixSeconds || unixSeconds <= 0) return "không rõ";
  const d = new Date(unixSeconds * 1000);
  return d.toISOString().replace("T", " ").replace("Z", " UTC");
}

function daysLeft(unixSeconds) {
  if (!unixSeconds || unixSeconds <= 0) return null;
  const ms = unixSeconds * 1000 - Date.now();
  return Math.floor(ms / DAY_MS);
}

async function sendToDiscord(client, message) {
  const channelId =
    env("DISCORD_ALERT_CHANNEL_ID") ||
    env("DISCORD_LOG_CHANNEL_ID") ||
    env("LOG_CHANNEL_ID") ||
    env("REPORT_CHANNEL_ID");

  // Không có channel thì log ra console cho đỡ “mất tích”
  if (!channelId) {
    console.log("[tokenReminder] " + message);
    return;
  }

  const ch = await client.channels.fetch(channelId).catch(() => null);
  if (!ch) {
    console.log("[tokenReminder] (cannot fetch channel) " + message);
    return;
  }

  await ch.send(message);
}

function collectShopTokens() {
  const maume = env("FB_PAGE_TOKEN_MAUME");
  const burger = env("FB_PAGE_TOKEN_BURGER");

  return [
    { name: "MauMe", token: maume },
    { name: "Burger", token: burger },
  ].filter((x) => x.token && x.token.length > 20);
}

async function checkOnce(client) {
  const appId = env("FB_APP_ID") || env("META_APP_ID") || env("APP_ID");
  const appSecret = env("FB_APP_SECRET") || env("META_APP_SECRET") || env("APP_SECRET");

  if (!appId || !appSecret) {
    await sendToDiscord(
      client,
      "⚠️ Thiếu FB_APP_ID / FB_APP_SECRET (hoặc META_APP_ID / META_APP_SECRET). Không debug token được."
    );
    return;
  }

  const tokens = collectShopTokens();
  if (tokens.length < 2) {
    await sendToDiscord(
      client,
      `⚠️ Bot chỉ thấy **${tokens.length}/2** token shop. Cần có đủ:\n- FB_PAGE_TOKEN_MAUME\n- FB_PAGE_TOKEN_BURGER`
    );
    return;
  }

  const appAccessToken = `${appId}|${appSecret}`;

  for (const it of tokens) {
    const label = it.name;
    const tok = it.token;
    const p = previewToken(tok);

    let data;
    try {
      data = await debugToken(tok, appAccessToken);
    } catch (e) {
      await sendToDiscord(client, `⚠️ Debug token thất bại cho **${label}** (${p}): \`${e.message}\``);
      continue;
    }

    const isValid = !!data?.is_valid;
    const expiresAt = Number(data?.expires_at || 0);

    if (!isValid) {
      await sendToDiscord(
        client,
        `❌ Token **${label}** (${p}) đang **KHÔNG hợp lệ**. Tạo/refresh token mới trước khi bot chết giữa chợ.`
      );
      continue;
    }

    // Nếu API không trả expires_at thì không tính “sắp hết hạn” được. Im lặng cho gọn.
    if (!expiresAt) {
      console.log(`[tokenReminder] OK ${label} (${p}) but expires_at not provided`);
      continue;
    }

    const dLeft = daysLeft(expiresAt);
    if (dLeft !== null && dLeft <= WARN_DAYS) {
      await sendToDiscord(
        client,
        `⚠️ Token **${label}** (${p}) còn **${dLeft} ngày** sẽ hết hạn.\nHết hạn lúc: **${formatDateFromUnix(expiresAt)}**`
      );
    } else {
      console.log(`[tokenReminder] OK ${label} (${p}) expires: ${formatDateFromUnix(expiresAt)} (~${dLeft} days left)`);
    }
  }
}

function startDailyTokenReminder(client) {
  // chạy ngay khi bot ready
  checkOnce(client).catch(() => {});

  // rồi mỗi 24h chạy lại (đơn giản, ít lỗi)
  setInterval(() => {
    checkOnce(client).catch(() => {});
  }, DAY_MS);

  console.log("[tokenReminder] daily token check enabled (interval 24h)");
}

module.exports = { startDailyTokenReminder };


/* =========================
   FILE 2: PATCH file main bot (index.js / bot.js / src/bot.js)
   Dán 2 đoạn dưới đây vào ĐÚNG file entry của bạn
   ========================= */

/*
(1) Ở gần đầu file main, dưới các require khác, thêm đoạn này:

let startDailyTokenReminder;
try {
  ({ startDailyTokenReminder } = require("./tokenReminder"));
} catch {
  ({ startDailyTokenReminder } = require("../tokenReminder"));
}

(2) Trong client.once("ready", ...) hoặc client.on("ready", ...), thêm 1 dòng:

startDailyTokenReminder(client);

Ví dụ:

client.once("ready", () => {
  console.log("Bot ready");
  startDailyTokenReminder(client);
});

*/
