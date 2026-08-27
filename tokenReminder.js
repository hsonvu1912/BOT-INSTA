/**
 * tokenReminder v2 — 22/08/2026
 *
 * Vì sao viết lại: bản v1 bắn Discord "Burger: KHÔNG hợp lệ (is_valid=false)" lúc
 * 16:21 VN 13/08 và 22/08/2026, trong khi token vẫn dùng được (debug_token trả
 * is_valid=true 8/8 lần ngay sau đó, IG Graph API trả HTTP 200, quota 0/50).
 * Đối chiếu 25 ngày log: 48/50 khe check có log, 2 khe khuyết đúng là 2 lần báo động giả.
 *
 * v1 sai ở 4 chỗ, mỗi chỗ tự nó đủ gây báo động giả:
 *  1. Tin mù vào is_valid, không hề kiểm chứng bằng một lời gọi API thật.
 *  2. Không retry — một cú hiccup mạng là kết luận luôn.
 *  3. debugToken() trả json?.data; nếu Meta trả 200 mà thiếu field data thì
 *     !!undefined === false ⇒ tự bịa ra "token hỏng" từ response rỗng.
 *  4. Vứt bỏ data.error của Meta ⇒ mất sạch lý do, không truy vết được.
 * Và một lỗi vận hành: nhánh có sự cố KHÔNG log gì lên Railway, nên muốn biết bot
 * đã cảnh báo lúc nào phải đi đếm khe 12h bị KHUYẾT trong log. v2 luôn log 1 dòng.
 */

const axios = require("axios");
const crypto = require("crypto");

const CHECK_INTERVAL_MS = 12 * 60 * 60 * 1000; // 12h
const WARN_DAYS = 7;
const TZ = "Asia/Ho_Chi_Minh";

// Bám convention src/ig.js: pin version + MỌI call Meta phải có timeout.
const GRAPH_API_VERSION = process.env.GRAPH_API_VERSION || "v25.0";
const BASE = `https://graph.facebook.com/${GRAPH_API_VERSION}`;
const HTTP_TIMEOUT_MS = 20 * 1000;
const ax = axios.create({ timeout: HTTP_TIMEOUT_MS });

const DEBUG_ATTEMPTS = 3; // debug_token: thử 3 lần trước khi kết luận
const PROBE_ATTEMPTS = 2; // probe IG: thử 2 lần
const RETRY_BASE_MS = 2000;

const REALERT_MS = 24 * 60 * 60 * 1000; // hỏng thật kéo dài: nhắc lại mỗi 24h, không phải mỗi 12h
const UNKNOWN_STREAK_TO_ALERT = 2; // "không kiểm tra được" phải lặp 2 kỳ liên tiếp mới báo
const FLAP_WINDOW_MS = 30 * 24 * 60 * 60 * 1000;
const FLAP_ALERT_THRESHOLD = 3; // báo động giả ≥3 lần/30 ngày thì báo 1 lần cho biết
const FLAP_DAMP_WINDOW_MS = 48 * 60 * 60 * 1000;
const FLAP_DAMP_CHANGES = 4; // đổi trạng thái ≥4 lần/48h = dao động, phải hạ nhiệt
const DISCORD_CHUNK = 1900; // Discord chặn ở 2000 ký tự

/* ===== Trạng thái ===== */
const ST = {
  OK: "OK",
  SAP_HET_HAN: "SẮP HẾT HẠN",
  BAO_DONG_GIA: "BÁO ĐỘNG GIẢ",
  HONG: "HỎNG",
  CAU_HINH_SAI: "CẤU HÌNH SAI",
  THIEU_QUYEN: "THIẾU QUYỀN",
  THIEU_TOKEN: "THIẾU TOKEN",
  KHONG_RO: "KHÔNG KIỂM TRA ĐƯỢC",
};

// Probe fields=username chỉ cần instagram_basic. Đăng bài cần instagram_content_publish.
// Token bị thu hồi RIÊNG quyền đăng vẫn qua được cả debug_token lẫn probe ⇒ phải soi scopes,
// nếu không bot sẽ báo "OK" trong khi không đăng nổi bài nào.
const REQUIRED_SCOPES = ["instagram_basic", "instagram_content_publish"];

// Trần thời gian cho CẢ lượt check. axios timeout chỉ là deadline từng request;
// 3 shop × 5 lần thử × 30s có thể kéo rất dài mà không ai chặn.
// Trần cho CẢ lượt check (axios timeout chỉ là deadline TỪNG request). Phải lớn hơn
// worst case: mỗi shop = debug 3×20s + nghỉ 6s + 2 probe × (2×20s + nghỉ 2s) = ~150s;
// 3 shop = ~450s. Để 5 phút thì shop cuối (TEST) chắc chắn bị bỏ oan thành KHÔNG RÕ.
const ROUND_DEADLINE_MS = 10 * 60 * 1000;
// Chỉ 3 trạng thái này mới đáng làm phiền người dùng.
const CAN_ALERT = new Set([ST.SAP_HET_HAN, ST.HONG, ST.CAU_HINH_SAI, ST.THIEU_QUYEN, ST.THIEU_TOKEN]);
const CONFIG_KEY = "__CONFIG__";

function env(name, fallback = "") {
  const v = process.env[name];
  return v && String(v).trim() ? String(v).trim() : fallback;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function nowVN() {
  try {
    return new Date().toLocaleString("vi-VN", { timeZone: TZ });
  } catch {
    return new Date().toISOString();
  }
}

// Vân tay không phục hồi được. Hơn hẳn việc in 6 ký tự đầu token: mọi page token Meta
// đều bắt đầu "EAA" nên tiền tố gần như giống nhau giữa các shop — chịu rủi ro rò rỉ mà
// không thu lại giá trị chẩn đoán. Vân tay còn phân biệt được "người đã xoay token" với
// "TỰ khỏi, token y nguyên ⇒ nhiều khả năng Meta báo sai".
function fingerprint(t) {
  if (!t || typeof t !== "string") return "(không có)";
  return crypto.createHash("sha256").update(t).digest("hex").slice(0, 8);
}

// Chuỗi ngoại lai (message của Meta) đi thẳng vào tin nhắn Discord: cắt ngắn, bỏ ký tự
// điều khiển, bỏ dấu ` (markdown injection) và @ (ping @everyone).
const FOREIGN_MAX = 200;
function safe(s, max = FOREIGN_MAX) {
  const t = String(s == null ? "" : s)
    .replace(/[\u0000-\u001F\u007F]/g, " ")
    .replace(/[`@]/g, "")
    .replace(/\s+/g, " ")
    .trim();
  return t.length > max ? t.slice(0, max) + "\u2026" : t;
}

function formatDateFromUnix(unixSeconds) {
  if (!unixSeconds || unixSeconds <= 0) return "không rõ";
  return new Date(unixSeconds * 1000).toISOString().replace("T", " ").replace("Z", " UTC");
}

function daysLeft(unixSeconds) {
  if (!unixSeconds || unixSeconds <= 0) return null;
  return Math.floor((unixSeconds * 1000 - Date.now()) / (24 * 60 * 60 * 1000));
}

/* ===== Phân loại lỗi Meta (đo thực nghiệm 22/08/2026) ===== */
function metaError(e) {
  return (e && e.response && e.response.data && e.response.data.error) || null;
}

// Token thật sự hỏng. Đo được: token sai chữ ký ⇒ 400 + {code:190,type:"OAuthException"}.
function isAuthError(err) {
  if (!err) return false;
  if (err.code === 190) return true;
  const sub = err.error_subcode;
  return err.type === "OAuthException" && [458, 459, 460, 463, 464, 467, 492].includes(sub);
}

// Gỡ quyền app ⇒ code 10 hoặc 200-299. Vĩnh viễn, không retry được. Nếu để rơi xuống
// nhánh "lỗi lạ" thì phải đủ 2 kỳ (24h) mới báo, và báo bằng thông điệp SAI.
function isPermissionError(err) {
  if (!err) return false;
  const c = err.code;
  return c === 10 || (typeof c === "number" && c >= 200 && c <= 299);
}

// Sai IG_USER_ID ⇒ 400 + {code:100, error_subcode:33, type:"GraphMethodException"}.
// KHÔNG phải token hỏng — gộp chung sẽ báo oan, nên tách hẳn ra.
function isConfigError(err) {
  return !!err && err.code === 100 && err.error_subcode === 33;
}

// Tạm thời: mạng, 5xx, throttle. Đáng retry, KHÔNG đáng kết luận token hỏng.
function isTransient(e) {
  if (e && e.__retryable) return true;
  if (!e || !e.response) return true; // timeout / DNS / socket
  const status = e.response.status;
  if (status >= 500 || status === 429) return true;
  const err = metaError(e);
  const c = err && err.code;
  if (c === 1 || c === 2 || c === 4 || c === 17 || c === 32 || c === 613) return true;
  if (typeof c === "number" && c >= 80001 && c <= 80007) return true;
  return false;
}

function describeErr(e) {
  const err = metaError(e);
  if (err) return safe(`code=${err.code}${err.error_subcode ? "/" + err.error_subcode : ""} ${err.message || ""}`);
  return safe((e && e.message) || String(e));
}

async function withRetry(fn, attempts) {
  let last;
  for (let i = 1; i <= attempts; i++) {
    try {
      return await fn();
    } catch (e) {
      last = e;
      if (i === attempts || !isTransient(e)) break;
      await sleep(RETRY_BASE_MS * i);
    }
  }
  throw last;
}

/* ===== Gọi Meta ===== */
async function debugTokenOnce(inputToken, appAccessToken) {
  // v1 gọi debug_token KHÔNG pin version ⇒ Meta tự chọn version mặc định ⇒ bộ giám sát
  // đo trên đường khác đường bot dùng để đăng. Bot này đã bị Meta đổi API cắn 05/2026.
  const r = await ax.get(`${BASE}/debug_token`, {
    params: { input_token: inputToken, access_token: appAccessToken },
  });
  const data = r && r.data && r.data.data;
  if (!data || typeof data !== "object") {
    // Bug v1: chỗ này trả undefined ⇒ !!undefined === false ⇒ "token hỏng" từ hư không.
    const err = new Error("debug_token trả 200 nhưng thiếu field data");
    err.__retryable = true;
    throw err;
  }
  return data;
}

// Kiểm chứng bằng lời gọi API THẬT mà bot dùng để đăng bài.
// Đây là trọng tài: Meta nói token hỏng mà endpoint này trả 200 thì Meta nói sai.
// Trọng tài 2: token còn ĐĂNG được không. fields=username chỉ chứng minh quyền ĐỌC
// (instagram_basic); đăng bài cần instagram_content_publish. Token bị thu hồi RIÊNG
// quyền đăng vẫn qua được probe đọc ⇒ báo "OK" trong khi không đăng nổi bài nào.
// Endpoint này tiện thể cho luôn quota để in vào log.
async function probePublish(igUserId, token) {
  const r = await ax.get(`${BASE}/${igUserId}/content_publishing_limit`, {
    params: { fields: "quota_usage,config", access_token: token },
  });
  return (r && r.data && r.data.data && r.data.data[0]) || {};
}

async function probeIg(igUserId, token) {
  const r = await ax.get(`${BASE}/${igUserId}`, {
    params: { fields: "username", access_token: token },
  });
  return (r && r.data) || {};
}

/* ===== Danh sách shop ===== */
// v2: thêm TEST. v1 hardcode 2 shop nên token TEST chạy thật mà không ai giám sát.
function collectShops() {
  const defs = [
    { key: "MAUME", name: "MauMe", tokenVar: "FB_PAGE_TOKEN_MAUME", idVar: "IG_USER_ID_MAUME", required: true },
    { key: "BURGER", name: "Burger", tokenVar: "FB_PAGE_TOKEN_BURGER", idVar: "IG_USER_ID_BURGER", required: true },
    { key: "TEST", name: "Test", tokenVar: "FB_PAGE_TOKEN_TEST", idVar: "IG_USER_ID_TEST", required: false },
    { key: "AO", name: "Áo", tokenVar: "FB_PAGE_TOKEN_AO", idVar: "IG_USER_ID_AO", required: false },
  ];
  const shops = [];
  const missing = [];
  for (const d of defs) {
    const token = env(d.tokenVar);
    const igUserId = env(d.idVar);
    if (token && token.length > 20) shops.push({ key: d.key, name: d.name, token, igUserId });
    // TEST không bắt buộc, nhưng cấu hình một nửa (có IG_USER_ID mà mất token) là MẤT
    // token chứ không phải "chưa dùng" — im lặng ở đây là giấu sự cố.
    else if (d.required || igUserId)
      missing.push({ key: d.key, name: d.name, tokenVar: d.tokenVar, why: token ? `token méo, chỉ ${token.length} ký tự` : "biến trống" });
  }
  return { shops, missing };
}

function getAppCreds() {
  return {
    appId: env("FB_APP_ID") || env("META_APP_ID") || env("APP_ID"),
    appSecret: env("FB_APP_SECRET") || env("META_APP_SECRET") || env("APP_SECRET"),
  };
}

/* ===== Kiểm tra 1 shop ===== */
// alwaysProbe: giữ tham số cho tương thích; v2 luôn probe nên nó không còn điều khiển gì.
async function checkShop(shop, appAccessToken, alwaysProbe) {
  const t0 = Date.now();
  const r = {
    key: shop.key,
    name: shop.name,
    fp: fingerprint(shop.token),
    isValid: null,
    expiresAt: 0,
    daysLeft: null,
    dataAccessExpiresAt: 0,
    metaErr: null, // lỗi Meta nhúng trong data — v1 vứt mất, v2 giữ để truy vết
    debugErr: null,
    probeOk: false,
    probeUser: null,
    probeErr: null,
    pubOk: false,
    quota: null,
    pubErr: null,
    pubErrObj: null,
    ms: 0,
    status: ST.KHONG_RO,
    reason: "",
  };

  let data = null;
  try {
    data = await withRetry(() => debugTokenOnce(shop.token, appAccessToken), DEBUG_ATTEMPTS);
  } catch (e) {
    r.debugErr = describeErr(e);
  }

  if (data) {
    r.isValid = !!data.is_valid;
    r.expiresAt = Number(data.expires_at || 0);
    r.daysLeft = r.expiresAt ? daysLeft(r.expiresAt) : null;
    r.dataAccessExpiresAt = Number(data.data_access_expires_at || 0);
    r.metaErr = data.error ? `code=${data.error.code} ${data.error.message || ""}`.trim() : null;
    // scopes RỖNG cũng là KHÔNG BIẾT, không phải "mất sạch quyền": chính body này đã nói
    // dối 2 lần (13/08, 22/08), tin nó mà bắn THIẾU QUYỀN là đẻ ra báo động giả kiểu mới.
    r.scopes = Array.isArray(data.scopes) && data.scopes.length ? data.scopes : null;
    // null = KHÔNG BIẾT, khác hẳn "đủ quyền" — nên để null, đừng suy ra [].
    r.missingScopes = r.scopes ? REQUIRED_SCOPES.filter((x) => !r.scopes.includes(x)) : null;
  }

  // v2: LUÔN probe, kể cả khi debug_token nói OK.
  // debug_token nói dối được CẢ HAI CHIỀU. Chiều 22/08 (nói hỏng mà API chạy) chỉ
  // gây phiền; chiều ngược lại — nói OK mà API thật từ chối — mới là hỏng CÂM,
  // nguy hiểm hơn nhiều vì bot cứ tưởng lành. Giá: 3 call thừa/12h, không đáng kể.
  if (shop.igUserId) {
    try {
      const p = await withRetry(() => probeIg(shop.igUserId, shop.token), PROBE_ATTEMPTS);
      r.probeOk = true;
      r.probeUser = p.username || null;
    } catch (e) {
      r.probeErr = describeErr(e);
      r.probeErrObj = metaError(e);
      r.probeTransient = isTransient(e);
    }
    try {
      const q = await withRetry(() => probePublish(shop.igUserId, shop.token), PROBE_ATTEMPTS);
      r.pubOk = true;
      r.quota = typeof q.quota_usage === "number" ? q.quota_usage : null;
    } catch (e) {
      r.pubErr = describeErr(e);
      r.pubErrObj = metaError(e);
      r.pubTransient = isTransient(e);
    }
  }
  r.ms = Date.now() - t0;

  // ===== Phân loại: probe (API thật) là TRỌNG TÀI, debug_token chỉ là nhân chứng =====
  const expiring = r.daysLeft !== null && r.daysLeft <= WARN_DAYS;
  const anyAuth = isAuthError(r.probeErrObj) || isAuthError(r.pubErrObj);
  const anyPerm = isPermissionError(r.probeErrObj) || isPermissionError(r.pubErrObj);
  const firstErr = r.probeErr || r.pubErr;

  if (!shop.igUserId) {
    // Có token mà thiếu IG_USER_ID thì shop KHÔNG đăng được bài. Báo "OK" ở đây là nói
    // dối: OK không nằm trong CAN_ALERT nên bot sẽ im tuyệt đối trong khi shop chết.
    r.status = ST.CAU_HINH_SAI;
    r.reason = `có token nhưng thiếu IG_USER_ID_${shop.key} — không kiểm chứng được và shop không đăng được bài`;
  } else if (anyAuth) {
    // Kể cả khi debug nói is_valid=true: API thật từ chối thì token coi như hỏng.
    r.status = ST.HONG;
    r.reason =
      r.isValid === true
        ? `debug_token nói hợp lệ NHƯNG IG từ chối: ${firstErr} — tin API thật`
        : `cả debug_token lẫn IG đều từ chối: ${firstErr}`;
  } else if (anyPerm) {
    r.status = ST.THIEU_QUYEN;
    r.reason = `mất quyền phía app: ${firstErr} — cần cấp lại permission`;
  } else if (isConfigError(r.probeErrObj)) {
    r.status = ST.CAU_HINH_SAI;
    r.reason = `IG_USER_ID_${shop.key} sai hoặc thiếu quyền: ${r.probeErr}`;
  } else if (!r.pubOk && r.missingScopes && r.missingScopes.length) {
    // Chỉ tin scopes khi probe ĐĂNG không chứng minh được điều ngược lại: content_publishing_limit
    // trả 200 nghĩa là quyền đăng còn nguyên — probe là trọng tài, scopes chỉ là nhân chứng.
    r.status = ST.THIEU_QUYEN;
    r.reason = `token hợp lệ nhưng THIẾU scope: ${r.missingScopes.join(", ")} — bot sẽ không đăng được bài`;
  } else if (r.probeOk && r.pubOk) {
    if (r.isValid === true) {
      r.status = expiring ? ST.SAP_HET_HAN : ST.OK;
      r.reason = r.expiresAt ? `còn ${r.daysLeft} ngày` : "vĩnh viễn";
    } else if (r.isValid === false) {
      // Ca 13/08 và 22/08: Meta KHẲNG ĐỊNH token hỏng, mà cả hai probe đều chạy được.
      // Đây mới là mâu thuẫn thật, mới đáng đếm vào sổ chập chờn.
      r.status = ST.BAO_DONG_GIA;
      r.reason = `debug_token nói is_valid=false${r.metaErr ? " (" + r.metaErr + ")" : " (không kèm lý do)"} nhưng IG đọc+đăng đều OK (@${r.probeUser || "?"})`;
    } else {
      // debug_token KHÔNG trả lời được (throttle code=4, 5xx, timeout). "Không hỏi được"
      // KHÁC HẲN "Meta nói hỏng": probe vừa chứng minh token chạy được, nên đây là OK.
      // Xếp vào BÁO ĐỘNG GIẢ sẽ bơm sổ chập chờn và kích heuristic app-credentials sai.
      r.status = ST.OK;
      r.reason = `IG đọc+đăng đều OK (@${r.probeUser || "?"}) nhưng không đọc được metadata token: ${r.debugErr}`;
    }
  } else if (r.isValid === true) {
    // Probe hỏng vì lý do tạm thời mà debug nói OK ⇒ bằng chứng dương, đừng hoảng.
    r.status = expiring ? ST.SAP_HET_HAN : ST.OK;
    r.reason = `probe lỗi tạm thời (${firstErr}) nhưng debug_token nói hợp lệ`;
  } else {
    r.status = ST.KHONG_RO;
    r.reason = safe(firstErr || r.debugErr || "không rõ");
  }

  return r;
}

/* ===== Máy trạng thái chống spam =====
 * Lưu in-memory: Railway có filesystem ephemeral, ghi file thêm đường hỏng mà
 * không chắc bền. Bot chạy 43 ngày không restart nên in-memory là đủ; giá phải
 * trả khi restart chỉ là tối đa 1 cảnh báo lặp. Đánh đổi này là cố ý.
 */
const state = new Map(); // key -> { status, since, lastAlertAt, flaps: number[] }

// lastAlertAt = null nghĩa là CHƯA TỪNG báo. Đừng dùng 0 làm sentinel: 0 vừa mang
// nghĩa "chưa bao giờ" vừa là một mốc thời gian hợp lệ, và `now - 0 >= REALERT_MS`
// chỉ vô tình đúng trên production vì now là epoch ~1.7e12 — chạy được vì lý do sai.
function dueForRealert(s, now) {
  return s.lastAlertAt === null || now - s.lastAlertAt >= REALERT_MS;
}

function getState(key) {
  if (!state.has(key))
    state.set(key, { status: null, since: 0, lastAlertAt: null, hasAlerted: false, fpAtAlert: null, flaps: [], changes: [], unknownStreak: 0, lastAlertStatus: null });
  return state.get(key);
}

// decide() CHỈ quan sát và đề xuất. Việc chốt "đã báo" nằm ở commitAlert(), chỉ gọi
// sau khi Discord nhận tin THẬT. Trước đây decide() ghi lastAlertAt ngay tại chỗ: gửi
// hỏng (bot mất quyền xem kênh, hoặc đúng lúc shard disconnect — bot này reconnect liên
// tục) là state vẫn ghi "đã báo" ⇒ im lặng 24h đúng lúc token hỏng thật.
function decide(r, now) {
  const s = getState(r.key);
  const changed = s.status !== r.status;
  if (changed) {
    s.since = now;
    s.changes = (s.changes || []).filter((t) => now - t < FLAP_DAMP_WINDOW_MS);
    s.changes.push(now);
  }
  // Dao động HỎNG↔OK làm `changed` luôn đúng ⇒ 2 tin/ngày vĩnh viễn. Đổi trạng thái quá
  // nhiều lần trong 48h thì hạ nhiệt: bắt cả nhánh `changed` phải qua hàng rào 24h.
  const flapping = (s.changes || []).length >= FLAP_DAMP_CHANGES;

  let alert = false;
  let note = "";
  let clearFlaps = false;

  if (r.status === ST.BAO_DONG_GIA) {
    s.flaps = (s.flaps || []).filter((t) => now - t < FLAP_WINDOW_MS);
    s.flaps.push(now);
    // KHÔNG reset unknownStreak ở đây: Meta chập chờn hay cho chuỗi xen kẽ
    // KHÔNG_RÕ → BÁO ĐỘNG GIẢ → KHÔNG_RÕ; reset ở đây thì streak không bao giờ đạt 2
    // và nhánh "không kiểm tra được" KHÔNG BAO GIỜ báo.
    if (s.flaps.length >= FLAP_ALERT_THRESHOLD && dueForRealert(s, now)) {
      alert = true;
      clearFlaps = true; // báo xong xoá sổ, nếu không từ lần thứ 3 trở đi báo mãi
      note = `Đã ${s.flaps.length} lần báo động giả trong 30 ngày — Meta trả kết quả chập chờn cho token này. Bot vẫn đăng bài bình thường, nhưng nên để ý.`;
    }
  } else if (r.status === ST.KHONG_RO) {
    s.unknownStreak += 1;
    if (s.unknownStreak >= UNKNOWN_STREAK_TO_ALERT && dueForRealert(s, now)) {
      alert = true;
      note = `Không kiểm tra được ${s.unknownStreak} kỳ liên tiếp. Đây KHÔNG có nghĩa token hỏng — chỉ là bot không xác minh nổi.`;
    }
  } else if (CAN_ALERT.has(r.status)) {
    s.unknownStreak = 0;
    // Hạ nhiệt dao động KHÔNG được nuốt một LOẠI sự cố mới. HỎNG↔OK lặp đi lặp lại thì
    // đáng im, nhưng OK → THIẾU QUYỀN là vấn đề khác hẳn, phải cho qua ngay.
    const kindMoi = r.status !== s.lastAlertStatus;
    if (dueForRealert(s, now) || (changed && (!flapping || kindMoi))) alert = true;
  } else {
    // OK. Đóng sự cố theo cờ hasAlerted, KHÔNG suy từ CAN_ALERT: BÁO ĐỘNG GIẢ và
    // KHÔNG KIỂM TRA ĐƯỢC cũng báo được (qua ngưỡng) mà KHÔNG nằm trong CAN_ALERT, nên
    // suy từ tập đó thì cảnh báo đã lộ ra Discord sẽ không bao giờ được đóng.
    // Nguyên tắc: đã lộ ra Discord thì PHẢI đóng ra Discord.
    s.unknownStreak = 0;
    s.flaps = (s.flaps || []).filter((t) => now - t < FLAP_WINDOW_MS);
    // Tin hồi phục cũng phải hạ nhiệt khi đang dao động, nếu không HỎNG→OK→HỎNG vẫn
    // ra 2 tin mỗi vòng. hasAlerted giữ nguyên nên sự cố vẫn được đóng, chỉ là muộn hơn.
    if (s.hasAlerted && (!flapping || dueForRealert(s, now))) {
      alert = true;
      const rotated = s.fpAtAlert && r.fp && s.fpAtAlert !== r.fp;
      note = rotated
        ? `✅ Đã trở lại bình thường — token ĐÃ ĐƯỢC XOAY (vân tay ${s.fpAtAlert} → ${r.fp}).`
        : `✅ Đã trở lại bình thường — token KHÔNG đổi (vân tay vẫn ${r.fp || "?"}), nhiều khả năng Meta báo sai chứ không phải đã sửa được gì.`;
    }
  }

  // Quan sát thì chốt ngay; "đã báo" thì đợi Discord nhận được.
  s.status = r.status;

  const commitAlert = () => {
    s.lastAlertAt = now;
    // CHỈ ghi nhớ loại SỰ CỐ. Nếu ghi cả OK thì tin hồi phục sẽ đặt lastAlertStatus=OK,
    // khiến lần HỎNG kế tiếp luôn bị coi là "loại mới" và bộ hạ nhiệt mất tác dụng.
    if (r.status !== ST.OK) s.lastAlertStatus = r.status;
    if (clearFlaps) s.flaps = [];
    if (r.status === ST.OK) {
      s.hasAlerted = false;
      s.fpAtAlert = null;
    } else {
      s.hasAlerted = true;
      s.fpAtAlert = r.fp || null;
    }
  };

  return { alert, note, changed, commitAlert };
}

/* ===== Báo cáo ===== */
function iconOf(status) {
  return {
    [ST.OK]: "✅",
    [ST.SAP_HET_HAN]: "⏳",
    [ST.BAO_DONG_GIA]: "🟡",
    [ST.HONG]: "❌",
    [ST.CAU_HINH_SAI]: "🔧",
    [ST.THIEU_QUYEN]: "🚫",
    [ST.THIEU_TOKEN]: "🕳️",
    [ST.KHONG_RO]: "❓",
  }[status] || "•";
}

function lineOf(r) {
  const bits = [`${iconOf(r.status)} **${r.name}** (${r.fp || "—"}): **${r.status}**`];
  if (r.reason) bits.push(`• ${safe(r.reason, 600)}`);
  if (r.status === ST.OK && r.expiresAt) bits.push(`• hết hạn ${formatDateFromUnix(r.expiresAt)}`);
  return "- " + bits.join(" ");
}

function buildReport({ mode, results, notes, checkId }) {
  const header = `🧾 **Token check (${mode})** • ${nowVN()} (VN) • \`${checkId}\` • ${GRAPH_API_VERSION}`;
  const lines = results.map(lineOf);
  const noteBlock = notes.length ? `\n\n📝 **Ghi chú:**\n${notes.map((n) => `- ${n}`).join("\n")}` : "";
  return `${header}\n${lines.join("\n")}${noteBlock}`;
}

// Phải CẮT CỨNG dòng dài hơn giới hạn. reason có nhúng message do Meta kiểm soát, độ dài
// không giới hạn; Discord chặn 2000 ⇒ DiscordAPIError 50035 ⇒ mất trắng cảnh báo.
function chunk(text, size = DISCORD_CHUNK) {
  const out = [];
  for (const raw of String(text).split("\n")) {
    const pieces = [];
    let l = raw;
    while (l.length > size) {
      pieces.push(l.slice(0, size));
      l = l.slice(size);
    }
    pieces.push(l);
    for (const piece of pieces) {
      if (out.length && (out[out.length - 1] + "\n" + piece).length <= size) out[out.length - 1] += "\n" + piece;
      else out.push(piece);
    }
  }
  return out.filter((x) => x.length > 0);
}

async function sendToDiscord(client, message) {
  const channelId =
    env("DISCORD_ALERT_CHANNEL_ID") ||
    env("DISCORD_LOG_CHANNEL_ID") ||
    env("LOG_CHANNEL_ID") ||
    env("REPORT_CHANNEL_ID");

  if (!channelId) {
    console.error("[TOKEN] THIẾU channel id, không gửi được Discord");
    return false;
  }
  const ch = await client.channels.fetch(channelId).catch((e) => {
    console.error(`[TOKEN] fetch channel FAIL: ${(e && e.message) || e}`);
    return null;
  });
  if (!ch) return false;

  try {
    // allowedMentions rỗng: chuỗi lỗi của Meta đi thẳng vào tin nhắn, đừng để nó
    // ping được @everyone nếu Meta trả về nội dung lạ.
    for (const part of chunk(message)) await ch.send({ content: part, allowedMentions: { parse: [] } });
    return true;
  } catch (e) {
    console.error(`[TOKEN] send FAIL: ${(e && e.message) || e}`);
    return false;
  }
}

/* ===== Vòng kiểm tra ===== */
async function checkTokensAndReport(client, mode = "auto") {
  // checkId nối tin nhắn Discord về đúng dòng log Railway.
  const checkId = `T-${Date.now().toString(36)}`;
  const notes = [];
  const results = [];
  const { appId, appSecret } = getAppCreds();
  const { shops, missing } = collectShops();

  // Cấu hình thiếu là trạng thái KÉO DÀI. Gửi Discord vô điều kiện mỗi 12h ⇒ 2 tin/ngày
  // vĩnh viễn ⇒ người dùng tắt não với kênh cảnh báo ⇒ cảnh báo token THẬT sau đó bị bỏ
  // lỡ. Cho nó đi qua cùng máy trạng thái như mọi thứ khác.
  if (!appId || !appSecret) {
    results.push({ key: CONFIG_KEY, name: "Cấu hình", fp: null, status: ST.CAU_HINH_SAI, reason: "thiếu FB_APP_ID / FB_APP_SECRET — không debug token được" });
  } else if (!shops.length && !missing.length) {
    results.push({ key: CONFIG_KEY, name: "Cấu hình", fp: null, status: ST.CAU_HINH_SAI, reason: "không tìm thấy token shop nào để kiểm tra" });
  }

  const appAccessToken = `${appId}|${appSecret}`;
  const alwaysProbe = mode === "manual";

  // Shop bắt buộc mà mất token là SỰ CỐ, không phải ghi chú. v1 nhét vào notes mà
  // hasProblems() không hề đọc notes ⇒ mất trắng token vẫn im lặng tuyệt đối.
  for (const mi of missing) {
    results.push({
      key: mi.key, name: mi.name, fp: null,
      status: ST.THIEU_TOKEN, reason: `${mi.tokenVar}: ${mi.why}`,
    });
  }

  const deadline = Date.now() + ROUND_DEADLINE_MS;
  for (const shop of (appId && appSecret ? shops : [])) {
    if (Date.now() > deadline) {
      results.push({ key: shop.key, name: shop.name, fp: fingerprint(shop.token), status: ST.KHONG_RO, reason: "hết thời gian cho lượt check" });
      continue;
    }
    try {
      results.push(await checkShop(shop, appAccessToken, alwaysProbe));
    } catch (e) {
      // Không để 1 shop lỗi làm chết cả vòng kiểm tra.
      results.push({
        key: shop.key, name: shop.name, fp: fingerprint(shop.token),
        status: ST.KHONG_RO, reason: `lỗi không lường trước: ${(e && e.message) || e}`,
      });
    }
  }

  // App secret bị xoay ⇒ debug_token trả code 190 cho MỌI shop trong khi probe vẫn 200
  // ⇒ mọi shop thành BÁO ĐỘNG GIẢ ⇒ im lặng vĩnh viễn, mất luôn chức năng giám sát.
  // Nhiều shop cùng "giả" một lúc thì thủ phạm gần như chắc chắn là app credentials.
  // Chỉ quy tội app credentials khi Meta KHẲNG ĐỊNH is_valid=false cho mọi shop (hình
  // dạng đo thật khi app secret sai: HTTP 200 + {error:{code:190}, is_valid:false}).
  // Nếu chỉ là throttle/5xx thì isValid=null, không được đổ oan.
  const shopResults = results.filter((r) => r.key !== CONFIG_KEY);
  if (shopResults.length >= 2 && shopResults.every((r) => r.status === ST.BAO_DONG_GIA && r.isValid === false)) {
    results.push({
      key: CONFIG_KEY, name: "Cấu hình", fp: null, status: ST.CAU_HINH_SAI,
      reason: `cả ${shopResults.length} shop cùng báo động giả một lúc — nhiều khả năng FB_APP_ID/FB_APP_SECRET sai hoặc app bị đổi trạng thái, không phải token từng shop`,
    });
  }

  const now = Date.now();
  // /testtoken (manual) KHÔNG được đụng vào state: bấm vài lần trong 1 phút sẽ bơm
  // unknownStreak/flaps và tiêu mất mốc 24h ⇒ vừa tự sinh cảnh báo giả vừa làm câm kỳ
  // auto kế tiếp. Manual chỉ đọc và in.
  const decisions = mode === "manual"
    ? results.map(() => ({ alert: false, note: "", changed: false, commitAlert: () => {} }))
    : results.map((r) => decide(r, now));
  // Gắn tên shop vào ghi chú, nếu không 3 shop cùng hồi phục sẽ ra 3 dòng giống hệt nhau.
  decisions.forEach((d, i) => { if (d.note) notes.push(`**${results[i].name}**: ${d.note}`); });

  const report = buildReport({ mode, results, notes, checkId });

  // LUÔN log 1 dòng — dù có sự cố hay không. Đây là bài học từ v1: nhánh cảnh báo
  // im lặng khiến phải đi đếm khe log bị khuyết mới biết bot đã báo lúc nào.
  // Giữ đúng 1 dòng ngắn vì Railway drop log khi buffer bị flood.
  const willPost = mode === "manual" || decisions.some((d) => d.alert);
  const summary = results.map((r) => `${r.name}=${r.status}`).join(" ");
  console.log(`[TOKEN] ${checkId} ${summary} | gửi Discord: ${willPost ? "CÓ" : "không"}`);
  // console.error cho sự cố thật: CLAUDE.md dùng `railway logs --filter "@level:error"`,
  // mà console.log thì filter đó không bao giờ thấy.
  // 1 dòng/shop, thật gọn: Railway drop log khi buffer bị flood (CLAUDE.md).
  for (const r of results) {
    if (r.status === ST.OK) continue;
    const line =
      `[TOKEN] ${checkId} ${r.name} state=${r.status} valid=${r.isValid} ` +
      `probe=${r.probeOk ? "ok" : "fail"}/${r.pubOk ? "ok" : "fail"} quota=${r.quota == null ? "?" : r.quota} ms=${r.ms || 0} — ${r.reason}`;
    if (CAN_ALERT.has(r.status)) console.error(line);
    else console.log(line);
  }

  if (!willPost) return { report, okPost: false, results, checkId };

  const okPost = await sendToDiscord(client, report);
  if (okPost) {
    // CHỈ chốt "đã báo" khi Discord thật sự nhận được. Gửi hỏng mà vẫn chốt thì kỳ sau
    // dueForRealert chặn ⇒ im lặng 24h đúng lúc token hỏng thật.
    decisions.forEach((d) => { if (d.alert) d.commitAlert(); });
  } else {
    console.error(`[TOKEN] ${checkId} KHÔNG gửi được Discord — GIỮ NGUYÊN state để kỳ sau báo lại. Nội dung: ${summary}`);
  }
  return { report, okPost, results, checkId };
}

/* ===== Scheduler 12h ===== */
let _interval = null;

function startTokenReminder(client) {
  const run = () =>
    checkTokensAndReport(client, "auto").catch((e) =>
      console.error(`[TOKEN] vòng kiểm tra CHẾT: ${(e && e.message) || e}`)
    );

  // index.js:1098 dùng client.on("ready") chứ KHÔNG phải once ⇒ mỗi lần gateway
  // re-identify là ready bắn lại. Nếu ở đây clearInterval+setInterval thì đồng hồ 12h
  // BỊ RESET VỀ 0 mỗi lần, và với bot reconnect liên tục thì lượt check có thể không
  // bao giờ tới. Đã hẹn giờ rồi thì tuyệt đối không đụng vào nữa.
  if (_interval) {
    console.log("[TOKEN] đã chạy sẵn, bỏ qua lần gọi lại (ready bắn lại sau reconnect)");
    return;
  }
  run();
  _interval = setInterval(run, CHECK_INTERVAL_MS);

  const { shops, missing } = collectShops();
  console.log(
    `[TOKEN] v2 bật (${GRAPH_API_VERSION}): ${shops.length} shop [${shops.map((s) => s.name).join(", ")}]` +
      `${missing.length ? " | THIẾU: " + missing.map((m) => m.tokenVar).join(", ") : ""} | check mỗi 12h`
  );
}

/* ===== Slash command /testtoken ===== */
// 22/08/2026: /testtoken nay nằm trong src/deploy-commands.js — MỘT nguồn sự thật duy
// nhất, đăng ký cùng 6 lệnh kia lúc boot. Bản cũ tự tạo lệnh riêng ở đây, mà rest.put
// trong deploy-commands là GHI ĐÈ TRỌN BỘ ⇒ chạy `npm run deploy:commands` là xoá mất
// /testtoken tới lần boot sau. Giữ export vì index.js:1119 vẫn gọi.
// Bản cũ còn 2 lỗi: guard `if (!client.application) await (client.application && ...)`
// là no-op tự mâu thuẫn, và nhánh global `client.application.commands` ném TypeError
// khi thiếu DISCORD_GUILD_ID.
async function registerTestTokenCommand() {
  const guildId = env("DISCORD_GUILD_ID") || env("GUILD_ID");
  if (guildId) {
    console.log("[TOKEN] /testtoken đăng ký qua deploy-commands.js (bộ lệnh chính thức)");
    return;
  }
  console.error("[TOKEN] chưa đặt DISCORD_GUILD_ID — /testtoken sẽ KHÔNG được đăng ký");
}

async function handleTestTokenSlash(interaction, client) {
  if (!interaction.isChatInputCommand()) return;
  if (interaction.commandName !== "testtoken") return;

  await interaction.reply({ content: "🔎 Đang check token (có probe API thật)...", flags: 64 }).catch(() => {});

  try {
    const { okPost, checkId } = await checkTokensAndReport(client, "manual");
    await interaction
      .editReply(okPost ? `✅ Xong (\`${checkId}\`). Report đã gửi lên kênh log.` : `⚠️ Xong (\`${checkId}\`) nhưng không gửi được vào channel. Xem Railway logs.`)
      .catch(() => {});
  } catch (e) {
    // v1 để lỗi này rơi tự do ⇒ interaction treo "đang suy nghĩ" mãi.
    console.error(`[TOKEN] /testtoken lỗi: ${(e && e.message) || e}`);
    await interaction.editReply(`❌ Lỗi khi check: ${safe((e && e.message) || e, 300)}`).catch(() => {});
  }
}

module.exports = {
  startTokenReminder,
  registerTestTokenCommand,
  handleTestTokenSlash,
  // export thêm để test được mà không cần Discord client
  __runFor: (client, mode) => checkTokensAndReport(client, mode || "auto"),
  __internal: { checkShop, collectShops, decide, ST, CAN_ALERT, isPermissionError, fingerprint, safe, isAuthError, isConfigError, isTransient, state, ax, chunk, buildReport },
};
