// Phone screenshots of the four hosted pages (ADR-032 section 1): the
// Workspaces list, one workspace's home, one pipeline and one run of it,
// on an emulated iPhone 16. One shot per page: the app has one theme
// (dark, ui/src/index.css), so a light colour scheme draws the same page
// and is not taken.
//
// Run via `make ui-phone-shots` (it builds bin/clavesa first). The script
// starts `bin/clavesa ui --no-browser --workspaces <dir> --read-only` on a
// free port, opens each page, waits for its charts (by data-testid), and
// checks two things per page:
//   - no console error and no uncaught exception. A workspace whose
//     summary fails answers 500 and the browser logs that as a console
//     error; that one is expected when the page shows the workspace's
//     error card, so it is counted apart and does not fail the run.
//   - no horizontal scroll: neither the document nor the app's own scroll
//     container is wider than the screen.
// Shots go to .shots/phone/<page>.png (gitignored; the directory is
// emptied first), taken full length: the viewport grows to the height of
// the page's scroll content first.
//
// Env (all optional):
//   WS_DIR      the workspaces directory (default ~/clavesa-workspaces)
//   WS_NAME     the workspace for pages 2 to 4 (default smoke2)
//   PIPELINE    its pipeline dir for pages 3 and 4 (default taxi)
//   RUN         the run for page 4 (default: the pipeline's last run, from
//               the workspace summary)
//   CLAVESA_BIN the binary (default bin/clavesa in the repo)
//   BASE_URL    a server that is already running (make hosted-local, for
//               a local-warehouse workspace served from its published
//               summary); no clavesa ui is started
//   AWS_PROFILE passed through to clavesa, as for any command
//
// Exit status 1 when a page fails a check or its charts never appear.

import { chromium, devices } from "playwright";
import { spawn } from "node:child_process";
import { mkdirSync, rmSync } from "node:fs";
import { createServer } from "node:net";
import { homedir } from "node:os";

const REPO = new URL("../../", import.meta.url).pathname;
const env = (k, d) => process.env[k] || d;
const BIN = env("CLAVESA_BIN", `${REPO}bin/clavesa`);
const WS_DIR = env("WS_DIR", `${homedir()}/clavesa-workspaces`);
const WS_NAME = env("WS_NAME", "smoke2");
const PIPELINE = env("PIPELINE", "taxi");
const OUT = `${REPO}.shots/phone/`;
// Summaries of cloud workspaces read Athena; the first one is slow.
const WAIT_MS = Number(env("WAIT_MS", "180000"));

const iphone = devices["iPhone 16"];
if (!iphone) throw new Error('playwright has no "iPhone 16" device descriptor');

async function freePort() {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.once("error", reject);
    srv.listen(0, "127.0.0.1", () => {
      const { port } = srv.address();
      srv.close(() => resolve(port));
    });
  });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function startUI(port) {
  const ui = spawn(BIN, ["ui", "--no-browser", "--workspaces", WS_DIR, "--read-only"], {
    cwd: WS_DIR,
    env: { ...process.env, CLAVESA_ADDR: `127.0.0.1:${port}` },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let log = "";
  ui.stdout.on("data", (d) => (log += d));
  ui.stderr.on("data", (d) => (log += d));
  let exited = false;
  ui.on("exit", () => (exited = true));
  for (let i = 0; i < 120; i++) {
    if (exited) break;
    try {
      const r = await fetch(`http://127.0.0.1:${port}/api/workspaces`);
      if (r.ok) return ui;
    } catch {}
    await sleep(500);
  }
  ui.kill();
  throw new Error(`clavesa ui did not start on :${port}\n${log.slice(-4000)}`);
}

// The browser first: a missing Chromium fails before a server is running.
const browser = await chromium.launch();
rmSync(OUT, { recursive: true, force: true });
mkdirSync(OUT, { recursive: true });
let BASE = process.env.BASE_URL?.replace(/\/$/, "");
let ui = { kill() {} };
if (BASE) {
  console.log(`using the server at ${BASE}`);
} else {
  const port = await freePort();
  BASE = `http://127.0.0.1:${port}`;
  console.log(`clavesa ui --workspaces ${WS_DIR} --read-only on ${BASE}`);
  ui = await startUI(port);
}
process.on("exit", () => ui.kill());

let runId = process.env.RUN;
if (!runId) {
  const r = await fetch(`${BASE}/w/${encodeURIComponent(WS_NAME)}/api/workspace/summary`, {
    signal: AbortSignal.timeout(WAIT_MS),
  });
  if (!r.ok) throw new Error(`summary of ${WS_NAME}: HTTP ${r.status}`);
  const s = await r.json();
  const p = s.pipelines.find((x) => x.dir === PIPELINE || x.name === PIPELINE);
  runId = p?.last_run?.run_id;
  if (!runId) throw new Error(`${WS_NAME}/${PIPELINE} has no last run; set RUN=<id>`);
}

const dash = `/w/${encodeURIComponent(WS_NAME)}/pipelines/dashboard?dir=${encodeURIComponent(PIPELINE)}`;
const PAGES = [
  {
    name: "01-workspaces",
    path: "/",
    testids: ["workspaces-page", "workspace-card"],
    // Every card has left its skeleton (ready or error).
    ready: () =>
      document.querySelectorAll('[data-testid="workspace-card"]').length > 0 &&
      document.querySelectorAll('[data-testid="workspace-card"][data-state="loading"]').length === 0,
  },
  {
    name: "02-workspace",
    path: `/w/${encodeURIComponent(WS_NAME)}/`,
    testids: [
      "workspace-home",
      "workspace-home-runs-chart",
      "workspace-home-cost-chart",
      "pipeline-card",
      "pipeline-card-duration-sparkline",
      "phone-nav",
    ],
  },
  {
    name: "03-pipeline",
    path: dash,
    testids: [
      "deployment-status",
      "pipeline-phone-duration-bars",
      "pipeline-phone-cost-sparkline",
      "run-timings-chart",
      "pipeline-phone-run-row",
      "phone-nav",
    ],
    // The phase chart renders empty (no rows) while its query loads.
    ready: () =>
      Number(document.querySelector('[data-testid="run-timings-chart"]')?.dataset.pointCount ?? 0) > 0,
  },
  {
    name: "04-run",
    path: `${dash}&run=${encodeURIComponent(runId)}`,
    testids: ["run-detail-sheet", "run-phone-timeline-chart", "run-phone-node-row"],
    fit: '[data-testid="run-phone-view"]',
  },
];

// Grow the viewport to the page's full scroll length, so the shot shows
// everything: the app scrolls inside its own containers, which a
// fullPage screenshot does not expand.
// `only` limits it to one scroller (the run sheet, not the page under it).
async function fitHeight(page, only) {
  for (let i = 0; i < 3; i++) {
    const extra = await page.evaluate((only) => {
      if (only) {
        const el = document.querySelector(only);
        return el ? Math.max(0, Math.ceil(el.scrollHeight - el.clientHeight)) : 0;
      }
      let max = document.scrollingElement.scrollHeight - innerHeight;
      for (const el of document.querySelectorAll("*")) {
        const oy = getComputedStyle(el).overflowY;
        if ((oy === "auto" || oy === "scroll") && el.clientHeight > 0)
          max = Math.max(max, el.scrollHeight - el.clientHeight);
      }
      return Math.max(0, Math.ceil(max));
    }, only);
    if (extra <= 1) return;
    const vp = page.viewportSize();
    await page.setViewportSize({ width: vp.width, height: Math.min(vp.height + extra, 8000) });
    await sleep(300);
  }
}

const failures = [];
try {
  const ctx = await browser.newContext({ ...iphone, colorScheme: "dark" });
  for (const pg of PAGES) {
    const page = await ctx.newPage();
    const errors = [];
    const expected = [];
    page.on("console", (m) => {
      if (m.type() !== "error") return;
      const url = m.location()?.url ?? "";
      const summary500 =
        /Failed to load resource/.test(m.text()) && /\/w\/[^/]+\/api\/workspace\/summary/.test(url);
      (summary500 ? expected : errors).push(`${m.text()} ${url}`.trim());
    });
    page.on("pageerror", (e) => errors.push(`pageerror: ${e.message}`));

    const label = pg.name;
    let ok = true;
    try {
      await page.goto(BASE + pg.path, { waitUntil: "domcontentloaded" });
      for (const id of pg.testids) {
        await page.locator(`[data-testid="${id}"]`).first().waitFor({ state: "visible", timeout: WAIT_MS });
      }
      if (pg.ready) await page.waitForFunction(pg.ready, null, { timeout: WAIT_MS });
      await sleep(800);
    } catch (e) {
      ok = false;
      failures.push(`${label}: ${e.message.split("\n")[0]}`);
    }

    // The summary 500s are expected only when the page shows error cards.
    if (expected.length) {
      const errorCards = await page.locator('[data-testid="workspace-card"][data-state="error"]').count();
      if (errorCards === 0) errors.push(...expected);
    }

    const scroll = await page.evaluate(() => {
      const doc = document.scrollingElement.scrollWidth;
      const wide = [...document.querySelectorAll("main, [data-testid='run-phone-view']")]
        .filter((el) => el.scrollWidth > el.clientWidth + 1)
        .map((el) => `${el.tagName.toLowerCase()}${el.dataset.testid ? `[${el.dataset.testid}]` : ""} ${el.scrollWidth}>${el.clientWidth}`);
      return { doc, inner: innerWidth, wide };
    });
    const hScroll = scroll.doc > scroll.inner || scroll.wide.length > 0;
    if (hScroll) failures.push(`${label}: horizontal scroll (document ${scroll.doc} > ${scroll.inner}; ${scroll.wide.join(", ")})`);
    if (errors.length) failures.push(`${label}: ${errors.length} console error(s):\n    ${errors.join("\n    ")}`);

    await fitHeight(page, pg.fit);
    const file = `${OUT}${label}.png`;
    await page.screenshot({ path: file });
    console.log(
      `${ok && !hScroll && errors.length === 0 ? "ok  " : "FAIL"} ${label}: ${errors.length} console errors` +
        `${expected.length ? ` (+${expected.length} expected summary 500s)` : ""}, ` +
        `scrollWidth ${scroll.doc}/${scroll.inner} -> ${file}`,
    );
    await page.close();
  }
  await ctx.close();
} finally {
  await browser.close();
  ui.kill();
}

if (failures.length) {
  console.error(`\n${failures.length} failure(s):\n  ${failures.join("\n  ")}`);
  process.exit(1);
}
console.log(`\nAll pages pass. Shots in ${OUT}`);
