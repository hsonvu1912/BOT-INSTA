const { google } = require("googleapis");
const { mustEnv } = require("./utils");

function getGoogleAuth() {
  const b64 = mustEnv("GOOGLE_SERVICE_ACCOUNT_JSON_BASE64");
  const json = JSON.parse(Buffer.from(b64, "base64").toString("utf8"));

  const scopes = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets"
  ];

  return new google.auth.JWT({
    email: json.client_email,
    key: json.private_key,
    scopes
  });
}

async function getClients() {
  const auth = getGoogleAuth();
  await auth.authorize();

  // v8: timeout toàn cục cho mọi call Sheets/Drive — không có timeout thì 1 request
  // đơ (Google 503/network stall) giữ lock tick vĩnh viễn. 120s đủ cho cả
  // download video ~10MB từ Drive (thực tế <2s trên Railway).
  google.options({ timeout: 120 * 1000 });

  return {
    sheets: google.sheets({ version: "v4", auth }),
    drive: google.drive({ version: "v3", auth })
  };
}

module.exports = { getClients };