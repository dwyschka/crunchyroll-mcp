#!/usr/bin/env node
// Sends the same password login request as crunchyroll-rs, for checking credentials by hand.
//
// Usage: node crunchy-login.js
// Reads CRUNCHYROLL_EMAIL and CRUNCHYROLL_PASSWORD from the environment or from ./.env.

const fs = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");

const CREDENTIALS_URL =
  "https://raw.githubusercontent.com/crunchy-labs/artifacts/refs/heads/main/credentials.json";
const TOKEN_URL = "https://www.crunchyroll.com/auth/v1/token";

function loadEnvFile(file) {
  if (!fs.existsSync(file)) return {};
  const env = {};
  for (const line of fs.readFileSync(file, "utf8").split(/\r?\n/)) {
    const match = line.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$/);
    if (!match) continue;
    let value = match[2].trim();
    if (/^(['"]).*\1$/.test(value)) value = value.slice(1, -1);
    env[match[1]] = value;
  }
  return env;
}

async function main() {
  const fileEnv = loadEnvFile(path.join(__dirname, ".env"));
  const email = process.env.CRUNCHYROLL_EMAIL || fileEnv.CRUNCHYROLL_EMAIL;
  const password = process.env.CRUNCHYROLL_PASSWORD || fileEnv.CRUNCHYROLL_PASSWORD;
  if (!email || !password) {
    console.error("CRUNCHYROLL_EMAIL and CRUNCHYROLL_PASSWORD must be set (env or .env)");
    process.exit(1);
  }

  const credentials = (await (await fetch(CREDENTIALS_URL)).json()).android_tv;
  const userAgent =
    `Crunchyroll/ANDROIDTV/${credentials.version}_${credentials.version_code} ` +
    "(Android 13.0; en-US; TCL-S5400AF Build/TP1A.220624.014)";
  const deviceId = crypto.randomUUID();

  const body = new URLSearchParams({
    username: email,
    password,
    grant_type: "password",
    scope: "offline_access",
    device_id: deviceId,
    device_type: "ANDROIDTV",
  });

  console.log(`App version:  ${credentials.version} (${credentials.version_code})`);
  console.log(`E-Mail:       ${email}`);
  console.log(`Password:     <${[...password].length} characters>`);
  console.log(`Device ID:    ${deviceId}`);
  console.log(`POST ${TOKEN_URL}\n`);

  const response = await fetch(TOKEN_URL, {
    method: "POST",
    headers: {
      Authorization: `Basic ${credentials.basic_auth_token}`,
      "Content-Type": "application/x-www-form-urlencoded",
      "User-Agent": userAgent,
      "ETP-Anonymous-ID": deviceId,
    },
    body,
  });

  const text = await response.text();
  console.log(`-> ${response.status} ${response.statusText}`);
  try {
    const json = JSON.parse(text);
    // Shorten tokens so a successful login does not dump them in full.
    for (const key of ["access_token", "refresh_token"]) {
      if (typeof json[key] === "string") json[key] = `${json[key].slice(0, 12)}...`;
    }
    console.log(JSON.stringify(json, null, 2));
  } catch {
    console.log(text.slice(0, 500));
  }

  if (response.ok) {
    console.log("\nLogin successful.");
  } else if (text.includes("invalid_credentials")) {
    console.log("\nCrunchyroll rejects e-mail/password for this login method.");
  } else if (text.includes("client_inactive")) {
    console.log("\nThe Android TV app credentials are outdated.");
  } else if (text.includes("Just a moment")) {
    console.log("\nBlocked by Cloudflare (bot check), the request did not reach Crunchyroll.");
  }
  process.exit(response.ok ? 0 : 1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
