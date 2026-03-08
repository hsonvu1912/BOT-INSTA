const https = require("https");

const CHECK_INTERVAL_MS = 12 * 60 * 60 * 1000; // 12h
const WARN_DAYS = 7;
const TZ = "Asia/Ho_Chi_Minh";

function env(name, fallback = "") {
  const v = process.env[name];
  return v && String(v).trim() ? String(v).trim() : fallback;
}

function nowVN() {
  try {
    return new Date().toLocaleString("vi-VN", { timeZone: TZ });
  } catch {
    return new Date().toISOString();
  }
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

function buildReport({ mode, results, notes }) {
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

  const noteBlock = notes.length ? `\n\n📝 **Ghi chú:**\n${notes.map((n) => `- ${n}`).join("\n")}` : "";

  return `${header}\n${lines.join("\n")}${noteBlock}`;
}

/**
 * Check if any result has a problem (invalid, error, or expiring soon)
 */
function hasProblems(results) {
  return results.some((r) => {
    if (r.status === "ERROR") return true;
    if (r.status === "INVALID") return true;
    if (r.status === "VALID" && r.daysLeft !== null && r.daysLeft <= WARN_DAYS) return true;
    return false;
  });
}

/**
 * Check tokens.
 * - mode "auto": only post to Discord if there's a problem
 * - mode "manual": always post full report
 */
async function checkTokensAndReport(client, mode = "auto") {
  const notes = [];
  const { appId, appSecret } = getAppCreds();

  if (!appId || !appSecret) {
    const report =
      "⚠️ Thiếu FB_APP_ID / FB_APP_SECRET (hoặc META_APP_ID/META_APP_SECRET). Không debug token được.";
    // Always alert if credentials are missing
    const okPost = await sendToDiscord(client, report);
    return { report, okPost };
  }

  const tokens = collectShopTokens();
  if (tokens.length < 2) {
    notes.push(
      `Bot chỉ thấy **${tokens.length}/2** token shop. Cần có đủ: FB_PAGE_TOKEN_MAUME và FB_PAGE_TOKEN_BURGER.`
    );
  }

  const appAccessToken = `${appId}|${appSecret}`;
  const results = [];

  for (const it of tokens) {
    const name = it.name;
    const prev = previewToken(it.token);

    try {
      const data = await debugToken(it.token, appAccessToken);
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

  if (results.length === 0) {
    results.push({ name: "Tokens", preview: "(none)", status: "ERROR", error: "Không tìm thấy token để check" });
  }

  const report = buildReport({ mode, results, notes });

  // AUTO mode: only post if there's a problem
  if (mode === "auto") {
    if (hasProblems(results)) {
      const okPost = await sendToDiscord(client, report);
      return { report, okPost };
    }
    // All good — just log quietly, don't spam Discord
    console.log("[tokenReminder] All tokens OK — no alert needed.");
    return { report, okPost: false };
  }

  // MANUAL mode (/testtoken): always post full report
  const okPost = await sendToDiscord(client, report);
  return { report, okPost };
}

/* ===== Scheduler 12h ===== */
let _interval = null;

function startTokenReminder(client) {
  // Run immediately
  checkTokensAndReport(client, "auto").catch(() => {});

  // Every 12h
  if (_interval) clearInterval(_interval);
  _interval = setInterval(() => {
    checkTokensAndReport(client, "auto").catch(() => {});
  }, CHECK_INTERVAL_MS);

  console.log("[tokenReminder] enabled: check every 12h, only alert on problems");
}

/* ===== Slash command: /testtoken ===== */
async function registerTestTokenCommand(client) {
  const cmd = {
    name: "testtoken",
    description: "Check token MauMe/Burger ngay và gửi report lên kênh log.",
  };

  try {
    if (!client.application) await client.application?.fetch?.().catch(() => {});
    const guildId = env("DISCORD_GUILD_ID") || env("GUILD_ID");

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
    console.log("[tokenReminder] register /testtoken failed:", e?.message || e);
  }
}

async function handleTestTokenSlash(interaction, client) {
  if (!interaction.isChatInputCommand()) return;
  if (interaction.commandName !== "testtoken") return;

  await interaction.reply({ content: "🔎 Đang check token và gửi report lên kênh log...", ephemeral: true });

  const { okPost } = await checkTokensAndReport(client, "manual");

  if (okPost) {
    await interaction.editReply("✅ Xong. Report đã gửi lên kênh log đã cấu hình.");
  } else {
    await interaction.editReply(
      "✅ Xong. Nhưng bot không gửi được vào channel (sai channelId hoặc thiếu quyền). Xem Railway logs để biết chi tiết."
    );
  }
}

module.exports = { startTokenReminder, registerTestTokenCommand, handleTestTokenSlash };
