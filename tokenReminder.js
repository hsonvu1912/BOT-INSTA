/* =========================
   tokenReminder.js (ROOT)
   - Check token 12h/lần và LUÔN gửi report lên channel đã set
   - Slash command: /testtoken (check ngay, gửi report lên channel + reply ephemeral)
   - Không đổi variables:
     FB_PAGE_TOKEN_MAUME, FB_PAGE_TOKEN_BURGER
     FB_APP_ID, FB_APP_SECRET (hoặc META_APP_ID/META_APP_SECRET, APP_ID/APP_SECRET)
     DISCORD_ALERT_CHANNEL_ID (hoặc DISCORD_LOG_CHANNEL_ID, LOG_CHANNEL_ID, REPORT_CHANNEL_ID)
   ========================= */

const https = require("https");

const H12_MS = 12 * 60 * 60 * 1000; // 12h
const WARN_DAYS = 7;
const TZ = "Asia/Ho_Chi_Minh";

function env(name, fallback = "") {
  const v = process.env[name];
  return v && String(v).trim() ? String(v).trim() : fallback;
}

function previewToken(t) {
  if (!t || typeof t !== "string") return "(no-token)";
  return `${t.slice(0, 6)}…${t.slice(-4)}`;
}

function nowVN() {
  try {
    return new Date().toLocaleString("vi-VN", { timeZone: TZ });
  } catch {
    return new Date().toISOString();
  }
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
  return Math.floor(ms / (24 * 60 * 60 * 1000));
}

async function sendToDiscord(client, message) {
  const channelId =
    env("DISCORD_ALERT_CHANNEL_ID") ||
    env("DISCORD_LOG_CHANNEL_ID") ||
    env("LOG_CHANNEL_ID") ||
    env("REPORT_CHANNEL_ID");

  if (!channelId) {
    console.log("[tokenReminder] " + message);
    return false;
  }

  const ch = await client.channels.fetch(channelId).catch(() => null);
  if (!ch) {
    console.log("[tokenReminder] (cannot fetch channel) " + message);
    return false;
  }

  await ch.send(message);
  return true;
}

function collectShopTokens() {
  const maume = env("FB_PAGE_TOKEN_MAUME");
  const burger = env("FB_PAGE_TOKEN_BURGER");

  return [
    { name: "MauMe", token: maume },
    { name: "Burger", token: burger },
  ].filter((x) => x.token && x.token.length > 20);
}

function getAppCreds() {
  const appId = env("FB_APP_ID") || env("META_APP_ID") || env("APP_ID");
  const appSecret = env("FB_APP_SECRET") || env("META_APP_SECRET") || env("APP_SECRET");
  return { appId, appSecret };
}

function buildReport({ mode, results, warnings }) {
  const header = `🧾 **Token check (${mode})** • ${nowVN()} (VN)`;

  const lines = results.map((r) => {
    if (r.status === "ERROR") {
      return `- **${r.name}** (${r.preview}): ⚠️ lỗi debug \`${r.error}\``;
    }
    if (r.status === "INVALID") {
      return `- **${r.name}** (${r.preview}): ❌ **KHÔNG hợp lệ** (is_valid=false)`;
    }
    // VALID
    if (!r.expiresAt) {
      return `- **${r.name}** (${r.preview}): ✅ hợp lệ • \`expires_at\` không có (không tính được ngày hết hạn)`;
    }
    const warn = r.daysLeft !== null && r.daysLeft <= WARN_DAYS ? " ⚠️" : "";
    return `- **${r.name}** (${r.preview}): ✅ hợp lệ • hết hạn: **${formatDateFromUnix(
      r.expiresAt
    )}** • còn **${r.daysLeft} ngày**${warn}`;
  });

  const warnBlock = warnings.length
    ? `\n\n⚠️ **Ghi chú:**\n${warnings.map((w) => `- ${w}`).join("\n")}`
    : "";

  return `${header}\n${lines.join("\n")}${warnBlock}`;
}

/**
 * Core: check tokens và (tuỳ chọn) gửi report lên Discord channel
 * @param {object} client Discord client
 * @param {object} opts { mode: "auto" | "manual", postToChannel: boolean }
 * @returns {Promise<{report: string, okPost: boolean, results: any[]}>}
 */
async function checkTokens(client, opts = {}) {
  const mode = opts.mode || "auto";
  const postToChannel = opts.postToChannel !== false; // default true

  const warnings = [];
  const { appId, appSecret } = getAppCreds();

  if (!appId || !appSecret) {
    const report =
      "⚠️ Thiếu FB_APP_ID / FB_APP_SECRET (hoặc META_APP_ID/META_APP_SECRET). Không debug token được.";
    const okPost = postToChannel ? await sendToDiscord(client, report) : false;
    return { report, okPost, results: [] };
  }

  const tokens = collectShopTokens();
  if (tokens.length < 2) {
    warnings.push(
      `Bot chỉ thấy **${tokens.length}/2** token shop. Cần có đủ: FB_PAGE_TOKEN_MAUME và FB_PAGE_TOKEN_BURGER.`
    );
  }

  const appAccessToken = `${appId}|${appSecret}`;
  const results = [];

  for (const it of tokens) {
    const name = it.name;
    const tok = it.token;
    const prev = previewToken(tok);

    try {
      const data = await debugToken(tok, appAccessToken);
      const isValid = !!data?.is_valid;
      const expiresAt = Number(data?.expires_at || 0);

      if (!isValid) {
        results.push({ name, preview: prev, status: "INVALID" });
      } else {
        results.push({
          name,
          preview: prev,
          status: "VALID",
          expiresAt: expiresAt || 0,
          daysLeft: expiresAt ? daysLeft(expiresAt) : null,
        });
      }
    } catch (e) {
      results.push({ name, preview: prev, status: "ERROR", error: e?.message || String(e) });
    }
  }

  // Nếu vì lý do nào đó tokens rỗng, vẫn report để bạn biết bot đang làm gì
  if (results.length === 0) {
    results.push({
      name: "Tokens",
      preview: "(none)",
      status: "ERROR",
      error: "Không tìm thấy token để check",
    });
  }

  const report = buildReport({ mode, results, warnings });
  const okPost = postToChannel ? await sendToDiscord(client, report) : false;
  return { report, okPost, results };
}

/* =========================
   12h scheduler
   ========================= */
let _intervalId = null;

function startTokenReminder(client) {
  // chạy ngay khi bot ready
  checkTokens(client, { mode: "auto", postToChannel: true }).catch(() => {});

  // rồi mỗi 12h chạy lại
  if (_intervalId) clearInterval(_intervalId);
  _intervalId = setInterval(() => {
    checkTokens(client, { mode: "auto", postToChannel: true }).catch(() => {});
  }, H12_MS);

  console.log("[tokenReminder] enabled: check every 12 hours + always post to channel");
}

/* =========================
   Slash: /testtoken
   - tự register (global hoặc guild nếu có DISCORD_GUILD_ID/GUILD_ID)
   - handle interaction
   ========================= */
async function registerTestTokenCommand(client) {
  const cmd = {
    name: "testtoken",
    description: "Kiểm tra token MauMe/Burger ngay lập tức và gửi report lên kênh log.",
  };

  // Đợi client.application sẵn sàng
  if (!client.application) await client.application?.fetch?.().catch(() => {});

  const guildId = env("DISCORD_GUILD_ID") || env("GUILD_ID"); // không bắt buộc, không đổi biến
  try {
    if (guildId) {
      const guild = await client.guilds.fetch(guildId);
      const existing = await guild.commands.fetch().catch(() => null);
      if (existing && existing.some((c) => c.name === cmd.name)) return;
      await guild.commands.create(cmd);
      console.log("[tokenReminder] /testtoken registered (guild)");
      return;
    }

    const existing = await client.application.commands.fetch().catch(() => null);
    if (existing && existing.some((c) => c.name === cmd.name)) return;
    await client.application.commands.create(cmd);
    console.log("[tokenReminder] /testtoken registered (global, may take time to appear)");
  } catch (e) {
    console.log("[tokenReminder] register command failed:", e?.message || e);
  }
}

async function handleTestTokenSlash(interaction, client) {
  try {
    if (!interaction.isChatInputCommand()) return;
    if (interaction.commandName !== "testtoken") return;

    // phản hồi nhanh để Discord khỏi timeout
    await interaction.reply({ content: "🔎 Đang check token và gửi report lên kênh log...", ephemeral: true });

    const { okPost } = await checkTokens(client, { mode: "manual", postToChannel: true });

    if (okPost) {
      await interaction.editReply("✅ Xong. Report đã gửi lên kênh log đã cấu hình.");
    } else {
      await interaction.editReply(
        "✅ Xong. Nhưng bot không gửi được vào channel (không fetch được channelId hoặc thiếu quyền). Xem Railway logs để biết chi tiết."
      );
    }
  } catch (e) {
    // Nếu reply fail thì cố gắng báo qua channel/log
    const msg = `⚠️ /testtoken lỗi: \`${e?.message || e}\``;
    await sendToDiscord(client, msg).catch(() => {});
    try {
      if (!interaction.replied) await interaction.reply({ content: msg, ephemeral: true });
      else await interaction.editReply(msg);
    } catch {}
  }
}

module.exports = {
  startTokenReminder,
  registerTestTokenCommand,
  handleTestTokenSlash,
  // export thêm nếu bạn muốn dùng nơi khác
  checkTokens,
};

/* =========================
   PATCH: dán vào FILE MAIN (index.js / bot.js / src/bot.js)
   (Bạn đã nói variables đúng, nên chỉ cần dán đúng chỗ là chạy.)
   ========================= */

/*
1) Ở đầu file main (dưới các require/import khác):

let tokenReminder;
try {
  tokenReminder = require("./tokenReminder");
} catch {
  tokenReminder = require("../tokenReminder");
}
const { startTokenReminder, registerTestTokenCommand, handleTestTokenSlash } = tokenReminder;

2) Trong client.once("ready", ...) hoặc client.on("ready", ...) thêm:

client.once("ready", async () => {
  console.log("Bot ready");
  startTokenReminder(client);                 // check 12h/lần + luôn post report
  registerTestTokenCommand(client);           // tự tạo /testtoken (guild nếu có DISCORD_GUILD_ID/GUILD_ID)
});

3) Trong interactionCreate (nếu bạn đã có rồi thì chỉ thêm IF này vào trong đó, đừng tạo handler mới):

client.on("interactionCreate", async (interaction) => {
  if (interaction.isChatInputCommand() && interaction.commandName === "testtoken") {
    await handleTestTokenSlash(interaction, client);
  }
});
*/
