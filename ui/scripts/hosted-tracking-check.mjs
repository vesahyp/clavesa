// The hosted UI's tracking proof (ADR-032 slice 7; jeeves
// practices/web-tracking.md, "Minimum error reporting", "Proven"): a build
// throws one error and the `error` beacon leaves the page.
//
// Run via `make hosted-tracking-check` (it runs make build-ui first).
//
// Local (default): the script lays out what hosted/scripts/deploy-spa.sh
// uploads (the build, index.html rendered by render-index.mjs with a
// dummy Cognito config, web-tracker/tracker.js, hosted/static/
// tracker-config.js and t.gif) in a temp dir, serves it on a free port
// with the SPA fallback CloudFront has, and checks:
//   1. hosted, /?__throw=1: the crash screen shows, and a /t.gif beacon
//      leaves the page with e=error, type=boundary and a msg that sanitize
//      cleaned (":run" in place of the run id and the execution ARN).
//   2. hosted: a pageview beacon leaves too, and no beacon carries the id
//      token or the email of the session in sessionStorage (planted here
//      before the page loads).
//   3. local mode (the same build's index.html without the hosted config,
//      which is what `clavesa ui` serves): no request for tracker.js and no
//      beacon at all, with ?__throw=1 or without.
//
// LIVE_URL=https://ui.clavesa.dev: check 1 against the deployed site
// instead (the beacon goes to CloudFront and lands in its access log;
// headless Chromium's user agent makes the pipeline file it as a bot).
//
// Exit status 1 when a check fails.

import { chromium } from "playwright";
import { execFileSync } from "node:child_process";
import { cpSync, existsSync, mkdtempSync, readFileSync, rmSync, statSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { extname, join, normalize } from "node:path";

const REPO = new URL("../../", import.meta.url).pathname;
const DIST = join(REPO, "internal/ui/dist");
const LIVE_URL = process.env.LIVE_URL?.replace(/\/$/, "");
const TIMEOUT = 15_000;

// The run id and ARN ThrowProbe (ui/src/hosted/CrashBoundary.tsx) throws.
const RAW_ID = "3f2b8c1e-9d4a-4e7b-8c6f-1a2b3c4d5e6f";
const FAKE_TOKEN = "eyJraWQiOiJjaGVjayJ9.eyJlbWFpbCI6ImNoZWNrQGV4YW1wbGUuY29tIn0.c2lnbmF0dXJl";
const FAKE_EMAIL = "check-person@example.com";

let failures = 0;
const ok = (m) => console.log(`ok    ${m}`);
const fail = (m) => {
  failures++;
  console.log(`FAIL  ${m}`);
};

const TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css",
  ".gif": "image/gif",
  ".svg": "image/svg+xml",
  ".txt": "text/plain",
  ".json": "application/json",
};

/** A static server with CloudFront's SPA rule: an extensionless path gets
 * /index.html. /t.gif answers any method, as a beacon is a POST. */
function serve(root) {
  const srv = createServer((req, res) => {
    const url = new URL(req.url, "http://x");
    let p = url.pathname;
    if (!extname(p)) p = "/index.html";
    const file = normalize(join(root, p));
    if (!file.startsWith(root) || !existsSync(file) || !statSync(file).isFile()) {
      res.writeHead(404, { "Content-Type": "application/json" });
      res.end('{"error":"not found"}');
      return;
    }
    res.writeHead(200, { "Content-Type": TYPES[extname(file)] ?? "application/octet-stream" });
    res.end(readFileSync(file));
  });
  return new Promise((resolve) =>
    srv.listen(0, "127.0.0.1", () => resolve({ srv, base: `http://127.0.0.1:${srv.address().port}` })),
  );
}

function layOut() {
  if (!existsSync(join(DIST, "index.html"))) throw new Error(`no ${DIST}/index.html: run make build-ui`);
  const tmp = mkdtempSync(join(tmpdir(), "clavesa-tracking-"));
  const hosted = join(tmp, "hosted");
  const local = join(tmp, "local");
  cpSync(DIST, hosted, { recursive: true });
  cpSync(DIST, local, { recursive: true });
  execFileSync("node", [
    join(REPO, "hosted/scripts/render-index.mjs"),
    join(DIST, "index.html"),
    join(hosted, "index.html"),
    "eu-north-1",
    "eu-north-1_check",
    "checkclient",
    "check.auth.eu-north-1.amazoncognito.com",
    "false",
  ]);
  cpSync(join(REPO, "web-tracker/tracker.js"), join(hosted, "tracker.js"));
  cpSync(join(REPO, "hosted/static/tracker-config.js"), join(hosted, "tracker-config.js"));
  cpSync(join(REPO, "hosted/static/t.gif"), join(hosted, "t.gif"));
  return { tmp, hosted, local };
}

/** Opens url and collects every request for tracker.js and every beacon
 * (as parsed query params). */
async function visit(browser, url, { plantSession = false, settle = 1500 } = {}) {
  const ctx = await browser.newContext();
  if (plantSession) {
    await ctx.addInitScript(
      ([token, email]) => {
        sessionStorage.setItem(
          "clavesa.hosted.session",
          JSON.stringify({ idToken: token, accessToken: token, expiresAt: Date.now() + 3600_000, email }),
        );
        localStorage.setItem("clavesa.hosted.refresh", token);
      },
      [FAKE_TOKEN, FAKE_EMAIL],
    );
  }
  const page = await ctx.newPage();
  const beacons = [];
  let trackerLoaded = false;
  page.on("request", (r) => {
    const u = new URL(r.url());
    if (u.pathname === "/tracker.js") trackerLoaded = true;
    if (u.pathname === "/t.gif") beacons.push({ raw: r.url(), q: Object.fromEntries(u.searchParams) });
  });
  const errors = [];
  page.on("pageerror", (e) => errors.push(e.message));
  await page.goto(url, { waitUntil: "load", timeout: TIMEOUT });
  await page.waitForTimeout(settle);
  return { ctx, page, beacons, trackerLoaded, errors };
}

function checkBoundaryBeacon(beacons, where) {
  const err = beacons.find((b) => b.q.e === "error" && b.q.type === "boundary");
  if (!err) {
    fail(`${where}: no e=error type=boundary beacon (saw: ${beacons.map((b) => b.q.e).join(", ") || "none"})`);
    return;
  }
  ok(`${where}: error beacon left the page: ${err.raw}`);
  const msg = err.q.msg ?? "";
  if (msg.includes(":run") && !msg.includes(RAW_ID) && !msg.includes("arn:aws")) {
    ok(`${where}: msg sanitized: "${msg}"`);
  } else {
    fail(`${where}: msg not sanitized: "${msg}"`);
  }
  if (msg.length > 200) fail(`${where}: msg is ${msg.length} chars, over 200`);
}

async function main() {
  const browser = await chromium.launch();
  try {
    if (LIVE_URL) {
      const v = await visit(browser, `${LIVE_URL}/?__throw=1`, { settle: 3000 });
      v.trackerLoaded ? ok("live: tracker.js loaded") : fail("live: tracker.js was never requested");
      checkBoundaryBeacon(v.beacons, "live");
      await v.ctx.close();
      return;
    }

    const { tmp, hosted, local } = layOut();
    const H = await serve(hosted);
    const L = await serve(local);
    try {
      // 1. hosted, a throw.
      const t = await visit(browser, `${H.base}/?__throw=1`);
      (await t.page.getByTestId("crash-screen").count())
        ? ok("hosted: crash screen with a reload button")
        : fail("hosted: no crash screen after ?__throw=1");
      checkBoundaryBeacon(t.beacons, "hosted");
      await t.ctx.close();

      // 2. hosted, a plain load with a session planted: a pageview, and no
      //    beacon carries the token or the email.
      const p = await visit(browser, `${H.base}/w/smoke2/pipelines`, { plantSession: true });
      const pv = p.beacons.find((b) => b.q.e === "pageview");
      pv ? ok(`hosted: pageview beacon, p=${pv.q.p}`) : fail("hosted: no pageview beacon");
      const leaked = p.beacons.filter(
        (b) => b.raw.includes("eyJ") || decodeURIComponent(b.raw).includes(FAKE_EMAIL),
      );
      leaked.length === 0
        ? ok(`hosted: none of ${p.beacons.length} beacons carries the session token or email`)
        : fail(`hosted: a beacon carries the session: ${leaked[0].raw}`);
      await p.ctx.close();

      // 3. local mode: no tracker, no beacon.
      for (const path of ["/", "/?__throw=1"]) {
        const l = await visit(browser, `${L.base}${path}`);
        !l.trackerLoaded && l.beacons.length === 0
          ? ok(`local ${path}: no tracker.js, no beacon`)
          : fail(`local ${path}: tracker loaded=${l.trackerLoaded}, ${l.beacons.length} beacons`);
        await l.ctx.close();
      }
    } finally {
      H.srv.close();
      L.srv.close();
      rmSync(tmp, { recursive: true, force: true });
    }
  } finally {
    await browser.close();
  }
}

await main();
if (failures) {
  console.log(`\n${failures} check(s) failed`);
  process.exit(1);
}
console.log("\nall tracking checks passed");
