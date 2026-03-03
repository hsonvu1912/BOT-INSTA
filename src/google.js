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

  return {
    sheets: google.sheets({ version: "v4", auth }),
    drive: google.drive({ version: "v3", auth })
  };
}

module.exports = { getClients };