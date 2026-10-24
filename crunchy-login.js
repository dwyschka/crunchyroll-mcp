#!/usr/bin/env node
// Sends the same password login request as crunchyroll-rs, for checking credentials by hand.
//
// Usage: node crunchy-login.js
// Reads CRUNCHYROLL_EMAIL and CRUNCHYROLL_PASSWORD from the environment or from ./.env.

const fs = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");
const http = require("node:http");
const https = require("node:https");

const CREDENTIALS_URL =
  "https://raw.githubusercontent.com/crunchy-labs/artifacts/refs/heads/main/credentials.json";
const INDEX_URL = "https://www.crunchyroll.com";
const TOKEN_URL = "https://www.crunchyroll.com/auth/v1/token";

// Plain node:https instead of fetch, which adds sec-fetch-mode and accept-encoding on its own.
// This sends exactly the headers given, like the reqwest client of crunchyroll-rs.
function request(method, url, headers, body) {
  const client = url.startsWith("https:") ? https : http;
  return new Promise((resolve, reject) => {
    const req = client.request(url, { method, headers }, (res) => {
      const chunks = [];
      res.on("data", (chunk) => chunks.push(chunk));
      res.on("end", () =>
        resolve({
          status: res.statusCode,
          statusText: res.statusMessage,
          ok: res.statusCode >= 200 && res.statusCode < 300,
          setCookies: res.headers["set-cookie"] ?? [],
          text: Buffer.concat(chunks).toString("utf8"),
        }),
      );
    });
    req.on("error", reject);
    if (body) req.write(body);
    req.end();
  });
}

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
  // Same default headers as crunchyroll-rs.
  const defaultHeaders = {
    accept: "*/*",
    "accept-language": "en-US,en;q=0.5",
    connection: "keep-alive",
    "user-agent": userAgent,
  };

  // crunchyroll-rs requests the index page first to get the cookies Cloudflare's bot check wants.
  const index = await request("GET", INDEX_URL, defaultHeaders);
  const cookies = index.setCookies.map((cookie) => cookie.split(";")[0]).join("; ");

  const body = new URLSearchParams({
    username: email,
    password: password,
    grant_type: "password",
    scope: "offline_access",
    device_id: deviceId,
    device_type: "ANDROIDTV",
  }).toString();

  console.log(`App version:  ${credentials.version} (${credentials.version_code})`);
  console.log(`E-Mail:       ${email}`);
  console.log(`Password:     <${[...password].length} characters>`);
  console.log(`Device ID:    ${deviceId}`);
  console.log(`Index page:   ${index.status}, cookies: ${cookies || "none"}`);
  console.log(`POST ${TOKEN_URL}\n`);

  // Same headers and order as Executor::auth_with_credentials plus the client defaults.
  const response = await request(
    "POST",
    TOKEN_URL,
    {
      authorization: `Basic ${credentials.basic_auth_token}`,
      "content-type": "application/x-www-form-urlencoded",
      "etp-anonymous-id": deviceId,
      ...defaultHeaders,
      ...(cookies && { cookie: cookies }),
      "content-length": Buffer.byteLength(body),
    },
    body,
  );

  const text = response.text;
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
