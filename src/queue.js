const { DateTime } = require("luxon");
const { mustEnv } = require("./utils");

const QUEUE_TAB = process.env.SHEET_TAB_QUEUE || "QUEUE";

function nowVn() {
  return DateTime.now().setZone("Asia/Ho_Chi_Minh");
}

function parseVnDatetime(input) {
  // expects "YYYY-MM-DD HH:mm"
  const dt = DateTime.fromFormat(String(input).trim(), "yyyy-MM-dd HH:mm", { zone: "Asia/Ho_Chi_Minh" });
  if (!dt.isValid) throw new Error("Giờ đăng sai format. Dùng: YYYY-MM-DD HH:mm (giờ VN)");
  return dt;
}

async function appendJob(sheets, { queueSheetId, job }) {
  // Columns A-O unchanged (existing schema). P=batch_id, Q=first_media_id added for the
  // folder-schedule sorter web app. Single-shot /ig_schedule leaves them empty and keeps
  // default status=PENDING; /ig_folder_schedule passes status=DRAFT + batch info so the
  // sorter can pick the rows up and convert them to PENDING after user confirms.
  const values = [[
    job.created_at,
    job.requester_id,
    job.requester_tag,
    job.shop,
    job.scheduled_time,
    job.folder_url,
    job.folder_id,
    job.sku,
    job.channel_id,
    job.status || "PENDING",
    "0",
    "",
    "",
    "",
    "",
    job.batch_id || "",
    job.first_media_id || ""
  ]];

  await sheets.spreadsheets.values.append({
    spreadsheetId: queueSheetId,
    range: `${QUEUE_TAB}!A:Q`,
    valueInputOption: "RAW",
    requestBody: { values }
  });
}

async function fetchAllJobs(sheets, { queueSheetId }) {
  const r = await sheets.spreadsheets.values.get({
    spreadsheetId: queueSheetId,
    range: `${QUEUE_TAB}!A:Q`
  });
  const rows = r.data.values || [];
  if (rows.length <= 1) return { header: rows[0] || [], items: [] };

  const header = rows[0];
  const items = rows.slice(1).map((row, idx) => {
    const rowNum = idx + 2;
    const get = (i) => (row[i] ?? "").toString();
    return {
      rowNum,
      created_at: get(0),
      requester_id: get(1),
      requester_tag: get(2),
      shop: get(3),
      scheduled_time: get(4),
      folder_url: get(5),
      folder_id: get(6),
      sku: get(7),
      channel_id: get(8),
      status: get(9),
      attempts: Number(get(10) || 0),
      last_error: get(11),
      ig_media_id: get(12),
      ig_permalink: get(13),
      published_at: get(14),
      batch_id: get(15),
      first_media_id: get(16)
    };
  });

  return { header, items };
}

// Columns: J=status, K=attempts, L=last_error, M=ig_media_id, N=ig_permalink, O=published_at
const PATCH_COLS = {
  status: "J",
  attempts: "K",
  last_error: "L",
  ig_media_id: "M",
  ig_permalink: "N",
  published_at: "O"
};

async function updateRow(sheets, { queueSheetId, rowNum, patch }) {
  // v8: CHỈ ghi các field có mặt trong patch. Bản cũ ghi nguyên khối J:O nên
  // patch thiếu field nào là field đó bị xoá trắng (mất attempts/permalink âm thầm).
  const data = [];
  for (const [key, col] of Object.entries(PATCH_COLS)) {
    if (!Object.prototype.hasOwnProperty.call(patch, key)) continue;
    const v = patch[key];
    data.push({
      range: `${QUEUE_TAB}!${col}${rowNum}`,
      values: [[key === "attempts" ? String(v ?? "") : (v ?? "")]]
    });
  }
  if (!data.length) return;

  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: queueSheetId,
    requestBody: { valueInputOption: "RAW", data }
  });
}

// v8: đọc created_at (cột A) của 1 row để verify rowNum còn trỏ đúng job trước khi ghi.
async function readRowCreatedAt(sheets, { queueSheetId, rowNum }) {
  const r = await sheets.spreadsheets.values.get({
    spreadsheetId: queueSheetId,
    range: `${QUEUE_TAB}!A${rowNum}`
  });
  return (r.data.values?.[0]?.[0] ?? "").toString();
}

module.exports = { nowVn, parseVnDatetime, appendJob, fetchAllJobs, updateRow, readRowCreatedAt };
