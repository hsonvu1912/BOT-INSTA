/**
 * Bo test cho tokenReminder v2 — 22/08/2026
 *
 * Chay:  cd /tmp && railway run --service BOT-INSTA node tokenReminder.test.js
 * (can env that de kiem chung token that o nhom C; cac nhom khac dung stub adapter
 *  nen tat dinh va KHONG dot han muc app cua Meta — chay bo test nhieu lan da tung
 *  dinh code=4 Application request limit reached.)
 */
const assert = require("assert");
const m = require("./tokenReminder.js");
const { checkShop, collectShops, decide, ST, isAuthError, isConfigError, isPermissionError, isTransient, state, ax, chunk, safe, fingerprint } = m.__internal;
const e = (n) => (process.env[n] || "").trim();
const app = `${e("FB_APP_ID")}|${e("FB_APP_SECRET")}`;
let pass = 0, fail = 0;

// decide() nay chỉ ĐỀ XUẤT; production chốt state qua commitAlert() SAU KHI Discord nhận.
const dec = (r, now) => { const d = decide(r, now); if (d.alert) d.commitAlert(); return d; };
const t = (n, f) => { try { f(); console.log(`  OK  ${n}`); pass++; } catch (err) { console.log(`  FAIL ${n}\n       ${err.message.split("\n")[0]}`); fail++; } };
const ta = async (n, f) => { try { await f(); console.log(`  OK  ${n}`); pass++; } catch (err) { console.log(`  FAIL ${n}\n       ${err.message.split("\n")[0]}`); fail++; } };

// Stub adapter: test logic phân loại KHÔNG cần gọi Meta thật. Tất định, nhanh, và không
// đốt hạn mức app (chạy bộ test nhiều lần đã từng dính code=4 Application request limit).
const ERR = (code, sub, msg) => ({ __err: { code, error_subcode: sub, message: msg || "stub", type: "OAuthException" } });
const NET = (msg) => ({ __net: msg || "ETIMEDOUT" });
function stub(map) {
  const prev = ax.defaults.adapter;
  ax.defaults.adapter = async (config) => {
    const u = String(config.url || "");
    const key = u.includes("debug_token") ? "debug" : u.includes("content_publishing_limit") ? "pub" : "id";
    const r = map[key];
    if (r && r.__err) { const err = new Error("stub"); err.config = config; err.response = { status: 400, data: { error: r.__err } }; throw err; }
    if (r && r.__net) { const err = new Error(r.__net); err.config = config; throw err; }
    return { data: r === undefined ? {} : r, status: 200, config, headers: {} };
  };
  return () => { ax.defaults.adapter = prev; };
}
const DBG = (over) => ({ data: Object.assign({ is_valid: true, type: "PAGE", expires_at: 0, scopes: ["instagram_basic", "instagram_content_publish"] }, over || {}) });
const ID_OK = { username: "burger_2nd" };
const PUB_OK = { data: [{ quota_usage: 0 }] };

(async () => {
console.log("\n-- A. Phan loai loi Meta (hinh dang DO THAT) --");
t("code 190 = token hong", () => assert(isAuthError({ code: 190, type: "OAuthException" })));
t("code 100/33 = cau hinh sai, KHONG phai token hong", () => { assert(isConfigError({ code: 100, error_subcode: 33 })); assert(!isAuthError({ code: 100, error_subcode: 33 })); });
t("code 10 va 200-299 = mat quyen", () => { assert(isPermissionError({ code: 10 })); assert(isPermissionError({ code: 200 })); assert(isPermissionError({ code: 299 })); assert(!isPermissionError({ code: 190 })); });
t("code 4/17/32 = throttle tam thoi", () => { for (const c of [4, 17, 32, 613, 80004]) assert(isTransient({ response: { status: 400, data: { error: { code: c } } } }), "code " + c); });
t("loi mang = tam thoi", () => assert(isTransient(new Error("ETIMEDOUT"))));
t("HTTP 500 = tam thoi", () => assert(isTransient({ response: { status: 500, data: {} } })));
t("code 190 KHONG phai tam thoi", () => assert(!isTransient({ response: { status: 400, data: { error: { code: 190 } } } })));

console.log("\n-- B. Shop duoc giam sat --");
const { shops } = collectShops();
t("du 3 shop ke ca TEST (v1 bo sot TEST)", () => { assert.strictEqual(shops.length, 3, "thay " + shops.map((s) => s.name)); assert(shops.some((s) => s.key === "TEST")); });
t("moi shop deu co igUserId de probe", () => shops.forEach((s) => assert(s.igUserId, s.name + " thieu igUserId")));
const burger = shops.find((s) => s.key === "BURGER");

console.log("\n-- C. Token THAT (goi Meta that) --");
for (const s of shops) await ta(`${s.name} = OK`, async () => { const r = await checkShop(s, app); assert.strictEqual(r.status, ST.OK, `ra ${r.status}: ${r.reason}`); });

console.log("\n-- D. TAI HIEN 22/08: debug noi is_valid=false, IG van 200 --");
await ta("=> BAO DONG GIA, khong phai HONG", async () => {
  const un = stub({ debug: DBG({ is_valid: false }), id: ID_OK, pub: PUB_OK });
  try { const r = await checkShop(burger, app); assert.strictEqual(r.status, ST.BAO_DONG_GIA, `ra ${r.status}: ${r.reason}`); } finally { un(); }
});
await ta("debug_token 200 nhung THIEU field data => KHONG ket luan token hong (bug v1 tu bia)", async () => {
  // v1: json?.data undefined => !!undefined === false => "token hong" tu hu khong.
  // v2: coi la khong doc duoc metadata; probe da chung minh token chay => OK.
  const un = stub({ debug: {}, id: ID_OK, pub: PUB_OK });
  try {
    const r = await checkShop(burger, app);
    assert.notStrictEqual(r.status, ST.HONG, 'khong duoc ket luan HONG');
    assert.strictEqual(r.status, ST.OK, `ra ${r.status}: ${r.reason}`);
    assert(r.reason.indexOf('metadata') >= 0, r.reason);
  } finally { un(); }
});

console.log("\n-- E. Hong THAT van phai bat duoc --");
await ta("ca debug lan probe deu 190 => HONG", async () => {
  const un = stub({ debug: DBG({ is_valid: false, error: { code: 190, message: "could not be decrypted" } }), id: ERR(190), pub: ERR(190) });
  try { const r = await checkShop(burger, app); assert.strictEqual(r.status, ST.HONG, `ra ${r.status}: ${r.reason}`); } finally { un(); }
});
await ta("debug noi HOP LE nhung IG tu choi 190 => HONG, tin API that (hong CAM)", async () => {
  const un = stub({ debug: DBG(), id: ERR(190), pub: ERR(190) });
  try { const r = await checkShop(burger, app); assert.strictEqual(r.status, ST.HONG, `ra ${r.status}: ${r.reason}`); assert(r.reason.indexOf("tin API that") >= 0 || r.reason.indexOf("tin API th") >= 0, r.reason); } finally { un(); }
});
await ta("IG_USER_ID sai (100/33) => CAU HINH SAI, khong do oan token", async () => {
  const un = stub({ debug: DBG(), id: ERR(100, 33), pub: ERR(100, 33) });
  try { const r = await checkShop(burger, app); assert.strictEqual(r.status, ST.CAU_HINH_SAI, `ra ${r.status}: ${r.reason}`); } finally { un(); }
});
await ta("probe tra code 10 => THIEU QUYEN (truoc roi vao 'loi la')", async () => {
  const un = stub({ debug: DBG(), id: ERR(10), pub: ERR(10) });
  try { const r = await checkShop(burger, app); assert.strictEqual(r.status, ST.THIEU_QUYEN, `ra ${r.status}: ${r.reason}`); } finally { un(); }
});
await ta("probe loi MANG + debug noi hop le => van OK (khong hoang)", async () => {
  const un = stub({ debug: DBG(), id: NET(), pub: NET() });
  try { const r = await checkShop(burger, app); assert.strictEqual(r.status, ST.OK, `ra ${r.status}: ${r.reason}`); } finally { un(); }
});
await ta("thieu IG_USER_ID => CAU HINH SAI (truoc bao OK trong khi shop khong dang duoc)", async () => {
  const un = stub({ debug: DBG(), id: ID_OK, pub: PUB_OK });
  try { const r = await checkShop({ ...burger, igUserId: "" }, app); assert.strictEqual(r.status, ST.CAU_HINH_SAI, `ra ${r.status}: ${r.reason}`); } finally { un(); }
});

console.log("\n-- F. scopes: probe la TRONG TAI, scopes chi la nhan chung --");
await ta("scopes thieu NHUNG probe dang van 200 => OK", async () => {
  const un = stub({ debug: DBG({ scopes: ["instagram_basic"] }), id: ID_OK, pub: PUB_OK });
  try { const r = await checkShop(burger, app); assert.strictEqual(r.status, ST.OK, `ra ${r.status}: ${r.reason}`); } finally { un(); }
});
await ta("scopes thieu VA probe dang hong => THIEU QUYEN", async () => {
  const un = stub({ debug: DBG({ scopes: ["instagram_basic"] }), id: ID_OK, pub: ERR(1, null, "unknown") });
  try { const r = await checkShop(burger, app); assert.strictEqual(r.status, ST.THIEU_QUYEN, `ra ${r.status}: ${r.reason}`); } finally { un(); }
});
await ta("scopes RONG => coi la KHONG BIET, khong ban THIEU QUYEN", async () => {
  const un = stub({ debug: DBG({ scopes: [] }), id: ID_OK, pub: PUB_OK });
  try { const r = await checkShop(burger, app); assert.strictEqual(r.status, ST.OK, `ra ${r.status}: ${r.reason}`); } finally { un(); }
});

await ta("debug_token bi THROTTLE (code 4) + probe 200 => OK, khong phai BAO DONG GIA", async () => {
  const un = stub({ debug: ERR(4, null, "Application request limit reached"), id: ID_OK, pub: PUB_OK });
  try { const r = await checkShop(burger, app); assert.strictEqual(r.status, ST.OK, `ra ${r.status}: ${r.reason}`); } finally { un(); }
});
await ta("throttle toan bo => KHONG do oan app credentials", async () => {
  state.clear();
  const got = [];
  const fake = { channels: { fetch: async () => ({ send: async (o) => got.push(o.content) }) } };
  const un = stub({ debug: ERR(4, null, "Application request limit reached"), id: ID_OK, pub: PUB_OK });
  try { const out = await m.__runFor(fake, "auto"); assert(!out.results.some((r) => r.status === ST.CAU_HINH_SAI), "do oan cau hinh khi chi la throttle"); assert.strictEqual(got.length, 0, "khong duoc bao Discord"); } finally { un(); state.clear(); }
});

console.log("\n-- G. May trang thai chong spam --");
const H = 3600000, T = 1700000000000;
state.clear();
t("HONG lan dau => BAO", () => assert(dec({ key: "X", status: ST.HONG, fp: "a1" }, T).alert));
t("van HONG sau 12h => IM (v1 bao lai)", () => assert(!dec({ key: "X", status: ST.HONG, fp: "a1" }, T + 12 * H).alert));
t("van HONG sau 24h => nhac lai", () => assert(dec({ key: "X", status: ST.HONG, fp: "a1" }, T + 25 * H).alert));
t("tu khoi => bao da tro lai binh thuong", () => { const d = dec({ key: "X", status: ST.OK, fp: "a1" }, T + 26 * H); assert(d.alert); assert(d.note.indexOf("binh thuong") >= 0 || d.note.indexOf("bình thường") >= 0, d.note); });
t("OK lien tuc => im", () => assert(!dec({ key: "X", status: ST.OK, fp: "a1" }, T + 40 * H).alert));
state.clear();
t("BAO DONG GIA lan 1,2 => im hoan toan", () => { assert(!dec({ key: "Y", status: ST.BAO_DONG_GIA, fp: "b" }, T).alert); assert(!dec({ key: "Y", status: ST.BAO_DONG_GIA, fp: "b" }, T + 9 * 24 * H).alert); });
t("lan 3 trong 30 ngay => bao 1 lan", () => assert(dec({ key: "Y", status: ST.BAO_DONG_GIA, fp: "b" }, T + 18 * 24 * H).alert));
t("sau khi bao thi XOA SO, lan ke tiep im (truoc bao mai mai)", () => { assert(!dec({ key: "Y", status: ST.BAO_DONG_GIA, fp: "b" }, T + 20 * 24 * H).alert); assert(!dec({ key: "Y", status: ST.BAO_DONG_GIA, fp: "b" }, T + 22 * 24 * H).alert); });
state.clear();
t("KHONG RO 1 ky => im", () => assert(!dec({ key: "Z", status: ST.KHONG_RO, fp: "c" }, T).alert));
t("2 ky lien tiep => bao, noi ro KHONG phai token hong", () => { const d = dec({ key: "Z", status: ST.KHONG_RO, fp: "c" }, T + 12 * H); assert(d.alert); assert(d.note.indexOf("token") >= 0); });
state.clear();
t("xen ke KHONG RO <-> BAO DONG GIA van bao duoc (truoc streak bi reset, im vinh vien)", () => {
  assert(!dec({ key: "Q", status: ST.KHONG_RO, fp: "d" }, T).alert);
  dec({ key: "Q", status: ST.BAO_DONG_GIA, fp: "d" }, T + 12 * H);
  assert(dec({ key: "Q", status: ST.KHONG_RO, fp: "d" }, T + 24 * H).alert, "khong bao");
});
state.clear();
t("dao dong HONG<->OK bi ha nhiet sau 4 lan doi trong 48h", () => {
  let n = 0;
  for (let i = 0; i < 8; i++) { const st = i % 2 === 0 ? ST.HONG : ST.OK; if (dec({ key: "P", status: st, fp: "e" }, T + i * 6 * H).alert) n++; }
  assert(n <= 4, "bao " + n + " lan / 8 ky, chua ha nhiet");
});
state.clear();
t("token KHONG doi => noi ro nhieu kha nang Meta bao sai", () => { dec({ key: "V", status: ST.HONG, fp: "same1234" }, T); const d = dec({ key: "V", status: ST.OK, fp: "same1234" }, T + 25 * H); assert(d.note.indexOf("KH") >= 0 && d.note.indexOf("i") >= 0, d.note); assert(d.note.indexOf("same1234") >= 0, d.note); });
t("token DA XOAY => noi ro da xoay", () => { state.clear(); dec({ key: "U", status: ST.HONG, fp: "old11111" }, T); const d = dec({ key: "U", status: ST.OK, fp: "new22222" }, T + 25 * H); assert(d.note.indexOf("old11111") >= 0 && d.note.indexOf("new22222") >= 0, d.note); });
t("BAO DONG GIA chua tung bao => khoi thi dong IM LANG", () => { state.clear(); dec({ key: "S", status: ST.BAO_DONG_GIA, fp: "f" }, T); assert(!dec({ key: "S", status: ST.OK, fp: "f" }, T + 12 * H).alert); });

t("ha nhiet dao dong KHONG duoc nuot mot LOAI su co MOI", () => {
  state.clear();
  // dao dong HONG<->OK 8 ky => da vao che do ha nhiet
  for (let i = 0; i < 8; i++) dec({ key: "N", status: i % 2 === 0 ? ST.HONG : ST.OK, fp: "h" }, T + i * 6 * H);
  // ngay sau do xuat hien MOT LOAI su co KHAC
  const d = dec({ key: "N", status: ST.THIEU_QUYEN, fp: "h" }, T + 8 * 6 * H + 60000);
  assert(d.alert, "loai su co moi bi ha nhiet nuot mat");
});
t("cung mot loai su co lap lai khi dang dao dong => van ha nhiet", () => {
  state.clear();
  let n = 0;
  for (let i = 0; i < 10; i++) { if (dec({ key: "M", status: i % 2 === 0 ? ST.HONG : ST.OK, fp: "k" }, T + i * 6 * H).alert) n++; }
  assert(n <= 5, "bao " + n + "/10 ky, chua ha nhiet");
});

console.log("\n-- H. Gui hong thi KHONG duoc chot state --");
t("gui hong => ky sau van bao lai", () => {
  state.clear();
  const d1 = decide({ key: "R", status: ST.HONG, fp: "g" }, T);
  assert(d1.alert, "lan dau phai bao");
  // KHONG goi commitAlert() => mo phong sendToDiscord that bai
  const d2 = decide({ key: "R", status: ST.HONG, fp: "g" }, T + 12 * H);
  assert(d2.alert, "gui hong ma ky sau van im => mat canh bao");
});

console.log("\n-- I. Chunk / safe / fingerprint --");
t("chunk cat CUNG 1 dong 5000 ky tu (truoc push nguyen dong => Discord 50035)", () => { const p = chunk("x".repeat(5000)); assert(p.length >= 3); p.forEach((x) => assert(x.length <= 1900, "dai " + x.length)); assert.strictEqual(p.join("").length, 5000); });
t("chunk gop nhieu dong ngan", () => { const p = chunk(Array.from({ length: 200 }, (_, i) => "- dong " + i).join("\n")); assert(p.length > 1); p.forEach((x) => assert(x.length <= 1900)); });
t("safe bo @ va backtick", () => { const o = safe("loi @everyone va `code`"); assert(o.indexOf("@") < 0, o); assert(o.indexOf("`") < 0, o); });
t("safe cat ve 200 ky tu", () => assert(safe("y".repeat(5000)).length <= 201));
t("fingerprint on dinh, khong lo token", () => { const f = fingerprint("EAAtokenbimat"); assert.strictEqual(f.length, 8); assert.strictEqual(f, fingerprint("EAAtokenbimat")); assert("EAAtokenbimat".indexOf(f) < 0); });
t("fingerprint doi khi token bi xoay", () => assert.notStrictEqual(fingerprint("aaa"), fingerprint("bbb")));

console.log("\n-- J. Tich hop toan vong --");
await ta("mat FB_PAGE_TOKEN_BURGER => BAO Discord (v1 im lang tuyet doi)", async () => {
  const keep = process.env.FB_PAGE_TOKEN_BURGER; delete process.env.FB_PAGE_TOKEN_BURGER;
  state.clear();
  const got = [];
  const fake = { channels: { fetch: async () => ({ send: async (o) => got.push(o.content) }) } };
  const un = stub({ debug: DBG(), id: ID_OK, pub: PUB_OK });
  try {
    const out = await m.__runFor(fake, "auto");
    assert(out.results.some((r) => r.status === ST.THIEU_TOKEN), "khong co ket qua THIEU TOKEN");
    assert(got.length > 0, "khong gui Discord");
  } finally { un(); process.env.FB_PAGE_TOKEN_BURGER = keep; state.clear(); }
});
await ta("thieu FB_APP_ID => bao 1 lan roi IM (truoc spam 2 tin/ngay vinh vien)", async () => {
  const keep = process.env.FB_APP_ID; delete process.env.FB_APP_ID;
  state.clear();
  const got = [];
  const fake = { channels: { fetch: async () => ({ send: async (o) => got.push(o.content) }) } };
  try {
    await m.__runFor(fake, "auto");
    assert.strictEqual(got.length, 1, "lan dau phai bao, nhan " + got.length);
    await m.__runFor(fake, "auto");
    assert.strictEqual(got.length, 1, "lan 2 KHONG duoc bao lai, nhan " + got.length);
  } finally { process.env.FB_APP_ID = keep; state.clear(); }
});
await ta("ca 3 shop cung BAO DONG GIA => quy toi app credentials, khong im lang", async () => {
  state.clear();
  const got = [];
  const fake = { channels: { fetch: async () => ({ send: async (o) => got.push(o.content) }) } };
  const un = stub({ debug: DBG({ is_valid: false }), id: ID_OK, pub: PUB_OK });
  try {
    const out = await m.__runFor(fake, "auto");
    assert(out.results.some((r) => r.status === ST.CAU_HINH_SAI), "khong quy toi cau hinh: " + out.results.map((r) => r.name + "=" + r.status).join(" "));
    assert(got.length > 0, "phai bao Discord");
  } finally { un(); state.clear(); }
});
await ta("/testtoken (manual) KHONG lam ban state chong spam", async () => {
  state.clear();
  const fake = { channels: { fetch: async () => ({ send: async () => {} }) } };
  const un = stub({ debug: DBG({ is_valid: false }), id: ID_OK, pub: PUB_OK });
  try {
    await m.__runFor(fake, "manual");
    await m.__runFor(fake, "manual");
    await m.__runFor(fake, "manual");
    const st = state.get("BURGER");
    assert(!st || !st.flaps || st.flaps.length === 0, "manual da bom flaps: " + JSON.stringify(st && st.flaps));
  } finally { un(); state.clear(); }
});
await ta("send dung allowedMentions rong", async () => {
  state.clear();
  let opts = null;
  const fake = { channels: { fetch: async () => ({ send: async (o) => { opts = o; } }) } };
  const un = stub({ debug: DBG({ is_valid: false, error: { code: 190 } }), id: ERR(190), pub: ERR(190) });
  try { await m.__runFor(fake, "auto"); assert(opts && typeof opts === "object", "send nhan string"); assert.deepStrictEqual(opts.allowedMentions, { parse: [] }); } finally { un(); state.clear(); }
});
await ta("report co checkId + Graph version", async () => {
  state.clear();
  const got = [];
  const fake = { channels: { fetch: async () => ({ send: async (o) => got.push(o.content) }) } };
  const un = stub({ debug: DBG(), id: ID_OK, pub: PUB_OK });
  try { const out = await m.__runFor(fake, "manual"); assert(out.checkId && out.checkId.indexOf("T-") === 0); assert(got.join("").indexOf(out.checkId) >= 0, "checkId khong co trong tin"); assert(got.join("").indexOf("v25.0") >= 0, "thieu version"); } finally { un(); state.clear(); }
});

console.log(`\n===== KET QUA: ${pass} pass, ${fail} fail =====`);
process.exit(fail ? 1 : 0);
})();
