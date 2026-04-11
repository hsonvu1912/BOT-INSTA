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

async function updateRow(sheets, { queueSheetId, rowNum, patch }) {
  // Columns: J=status, K=attempts, L=last_error, M=ig_media_id, N=ig_permalink, O=published_at
  const range = `${QUEUE_TAB}!J${rowNum}:O${rowNum}`;
  const values = [[
    patch.status ?? "",
    String(patch.attempts ?? ""),
    patch.last_error ?? "",
    patch.ig_media_id ?? "",
    patch.ig_permalink ?? "",
    patch.published_at ?? ""
  ]];

  await sheets.spreadsheets.values.update({
    spreadsheetId: queueSheetId,
    range,
    valueInputOption: "RAW",
    requestBody: { values }
  });
}

module.exports = { nowVn, parseVnDatetime, appendJob, fetchAllJobs, updateRow };
