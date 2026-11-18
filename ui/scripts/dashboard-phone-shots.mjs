// Dashboards on a phone: iPhone 16 shots of whole dashboards and of every
// widget, with the checks that catch what Vesa saw on 2026-11-18 ("Some y
// axis labels are missing/weird and the layout on mobile is strange to
// have 2 widgets when there clearly is not enough room").
//
// Run via `make ui-dashboard-shots`. It starts
// `bin/clavesa ui --no-browser --workspaces <WS_DIR> --read-only` on a free
// port (or uses BASE_URL), opens /w/<WS_NAME>/dashboards/<slug> for each
// slug in DASHBOARDS, waits until no widget is loading, and writes
//   .shots/dashboards/<slug>.png            the whole dashboard
//   .shots/dashboards/<slug>/NN-<title>.png  each widget
// (gitignored; emptied first).
//
// Checks, per widget (exit 1, one line per failure):
//   - y axis: every chart with a y axis has at least two tick labels, none
//     empty, none like 0.30000000000000004 or 1e3, none cut off at the
//     left edge of the chart;
//   - width: below NARROW px (640) every widget takes the full row, except
//     stat tiles (big_number), which may share a row two by two;
//   - no text inside a widget is wider than its box (a cut number), and
//     no table breaks a word (a table that still does not fit scrolls
//     sideways, printed as a note);
//   - legends name their series (never "y", "x", "value", "line", "bar");
//   - lines are straight between points (no smoothing);
//   - no horizontal scroll, no console error.
//
// Env (all optional):
//   WS_DIR      the workspaces directory (default ~/clavesa-workspaces)
//   WS_NAME     the workspace (default web-traffic)
//   DASHBOARDS  comma-separated slugs (default heineli,heineli-technical)
//   BASE_URL    a server that is already running
//   WAIT_MS     how long a dashboard may load (default 240000)
//   AWS_PROFILE passed through to clavesa, as for any command

import { chromium, devices } from "playwright";
import { spawn } from "node:child_process";
import { mkdirSync, rmSync } from "node:fs";
import { createServer } from "node:net";
import { homedir } from "node:os";

const REPO = new URL("../../", import.meta.url).pathname;
const env = (k, d) => process.env[k] || d;
const BIN = env("CLAVESA_BIN", `${REPO}bin/clavesa`);
const WS_DIR = env("WS_DIR", `${homedir()}/clavesa-workspaces`);
const WS_NAME = env("WS_NAME", "web-traffic");
const DASHBOARDS = env("DASHBOARDS", "heineli,heineli-technical").split(",").map((s) => s.trim()).filter(Boolean);
const WAIT_MS = Number(env("WAIT_MS", "240000"));
const NARROW = 640;
const OUT = `${REPO}.shots/dashboards/`;

const iphone = devices["iPhone 16"];
if (!iphone) throw new Error('playwright has no "iPhone 16" device descriptor');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

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

// Grow the viewport to the page's full scroll length, so one shot shows it all.
async function fitHeight(page) {
  for (let i = 0; i < 3; i++) {
    const extra = await page.evaluate(() => {
      let max = document.scrollingElement.scrollHeight - innerHeight;
      for (const el of document.querySelectorAll("*")) {
        const oy = getComputedStyle(el).overflowY;
        if ((oy === "auto" || oy === "scroll") && el.clientHeight > 0) max = Math.max(max, el.scrollHeight - el.clientHeight);
      }
      return Math.max(0, Math.ceil(max));
    });
    if (extra <= 1) return;
    const vp = page.viewportSize();
    await page.setViewportSize({ width: vp.width, height: Math.min(vp.height + extra, 16000) });
    await sleep(400);
  }
}

const fileSafe = (s) => s.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 50) || "widget";

const browser = await chromium.launch();
rmSync(OUT, { recursive: true, force: true });
mkdirSync(OUT, { recursive: true });
let BASE = process.env.BASE_URL?.replace(/\/$/, "");
let ui = { kill() {} };
if (BASE) console.log(`using the server at ${BASE}`);
else {
  const port = await freePort();
  BASE = `http://127.0.0.1:${port}`;
  console.log(`clavesa ui --workspaces ${WS_DIR} --read-only on ${BASE}`);
  ui = await startUI(port);
}
process.on("exit", () => ui.kill());

const failures = [];
try {
  const ctx = await browser.newContext({ ...iphone, colorScheme: "dark" });
  for (const slug of DASHBOARDS) {
    const page = await ctx.newPage();
    const errors = [];
    page.on("console", (m) => m.type() === "error" && errors.push(`${m.text()} ${m.location()?.url ?? ""}`.trim()));
    page.on("pageerror", (e) => errors.push(`pageerror: ${e.message}`));
    await page.goto(`${BASE}/w/${encodeURIComponent(WS_NAME)}/dashboards/${encodeURIComponent(slug)}`, {
      waitUntil: "domcontentloaded",
    });
    await page.locator('[data-testid="dashboard-widget"]').first().waitFor({ state: "visible", timeout: WAIT_MS });
    // Loaded: no skeleton in any widget, and the charts have drawn.
    await page.waitForFunction(
      () =>
        [...document.querySelectorAll('[data-testid="dashboard-widget"]')].every(
          (w) => !w.querySelector(".animate-pulse"),
        ),
      null,
      { timeout: WAIT_MS },
    );
    await sleep(1500);

    const info = await page.evaluate((NARROW) => {
      const vw = innerWidth;
      const widgets = [...document.querySelectorAll('[data-testid="dashboard-widget"]')];
      const main = widgets[0]?.parentElement;
      const rowW = main ? main.getBoundingClientRect().width : vw;
      return {
        vw,
        doc: document.scrollingElement.scrollWidth,
        widgets: widgets.map((w) => {
          const r = w.getBoundingClientRect();
          const svg = w.querySelector(".recharts-wrapper svg.recharts-surface");
          const sr = svg?.getBoundingClientRect();
          const ticks = [...w.querySelectorAll(".recharts-yAxis-tick-labels text.recharts-cartesian-axis-tick-value")].map((t) => {
            const b = t.getBoundingClientRect();
            const s = t.ownerSVGElement?.getBoundingClientRect() ?? sr;
            return { text: (t.textContent ?? "").trim(), cut: s ? b.left < s.left - 0.5 || b.right > s.right + 0.5 : false };
          });
          const yAxes = w.querySelectorAll(".recharts-yAxis").length;
          const wide = [...w.querySelectorAll("*")]
            .filter((el) => {
              if (el.closest("svg") || el.closest("table") || el.closest('[data-scroll-x]')) return false;
              const cs = getComputedStyle(el);
              if (cs.overflowX === "auto" || cs.overflowX === "scroll") return false;
              return el.scrollWidth > el.clientWidth + 1 && el.clientWidth > 0 && el.children.length === 0;
            })
            .map((el) => (el.textContent ?? "").trim().slice(0, 30));
          const table = w.querySelector("table");
          const tableBox = table?.parentElement;
          const tableCut = tableBox ? tableBox.scrollWidth > tableBox.clientWidth + 1 : false;
          // A one-word cell on more than one line is a word broken in two.
          const splitWords = [...w.querySelectorAll("td")]
            .filter((td) => {
              const t = (td.textContent ?? "").trim();
              if (!/^[\p{L}\p{N}]{1,12}$/u.test(t)) return false;
              const range = document.createRange();
              range.selectNodeContents(td);
              const tops = new Set([...range.getClientRects()].map((r) => Math.round(r.top)));
              return tops.size > 1;
            })
            .map((td) => (td.textContent ?? "").trim());
          const legend = [...w.querySelectorAll(".recharts-legend-item-text")].map((e) => (e.textContent ?? "").trim());
          // A smoothed line is a path with cubic segments ("C").
          const curved = [...w.querySelectorAll("path.recharts-line-curve, .recharts-line path.recharts-curve")].some((p) =>
            /C/.test(p.getAttribute("d") ?? ""),
          );
          return {
            legend,
            curved,
            tableCut,
            splitWords: [...new Set(splitWords)],
            title: w.dataset.widgetTitle ?? "",
            type: w.dataset.widgetType ?? "",
            width: Math.round(r.width),
            full: r.width >= rowW - 2,
            yAxes,
            ticks,
            wide,
            empty: /No rows/.test(w.textContent ?? ""),
            failed: /Query failed/.test(w.textContent ?? ""),
          };
        }),
      };
    }, NARROW);

    const label = slug;
    if (info.doc > info.vw) failures.push(`${label}: horizontal scroll (${info.doc} > ${info.vw})`);
    if (errors.length) failures.push(`${label}: ${errors.length} console error(s): ${errors.slice(0, 5).join(" | ")}`);
    console.log(`${label}: ${info.widgets.length} widgets at ${info.vw} px`);
    info.widgets.forEach((w, i) => {
      const n = String(i + 1).padStart(2, "0");
      const probs = [];
      if (w.yAxes > 0) {
        const texts = w.ticks.map((t) => t.text);
        if (texts.length < 2) probs.push(`y axis has ${texts.length} tick label(s)`);
        for (const t of w.ticks) {
          if (t.text === "") probs.push("empty y tick");
          else if (/\d[eE][+-]?\d/.test(t.text)) probs.push(`y tick "${t.text}" in e notation`);
          else if (/\.\d{3,}/.test(t.text)) probs.push(`y tick "${t.text}" has too many decimals`);
          if (t.cut) probs.push(`y tick "${t.text}" cut off`);
        }
      }
      const tile = w.type === "big_number";
      if (info.vw < NARROW && !w.full && !tile) probs.push(`takes ${w.width} px, not the full row`);
      for (const t of w.wide) probs.push(`text "${t}" wider than its box`);
      // A table too wide for a phone even with its text wrapped scrolls
      // sideways in its widget: said, not failed.
      if (w.tableCut) console.log(`         note: table scrolls sideways`);
      for (const t of w.splitWords) probs.push(`table word "${t}" broken over two lines`);
      if (w.failed) probs.push("query failed");
      for (const l of w.legend) if (/^(x|y|value|val|line|bar|bars)$/i.test(l)) probs.push(`legend says "${l}", a placeholder`);
      if (w.curved) probs.push("line is smoothed (a curve between points)");
      const ticks =
        (w.ticks.length ? ` y[${w.ticks.map((t) => t.text).join(" ")}]` : "") +
        (w.legend.length ? ` legend[${w.legend.join(", ")}]` : "");
      console.log(`  ${probs.length ? "FAIL" : "ok  "} ${n} ${w.type || "?"} "${w.title}" ${w.width}px${w.empty ? " (no rows)" : ""}${ticks}`);
      for (const p of probs) {
        console.log(`         ${p}`);
        failures.push(`${label} ${n} "${w.title}": ${p}`);
      }
    });

    await fitHeight(page);
    await page.screenshot({ path: `${OUT}${slug}.png` });
    mkdirSync(`${OUT}${slug}`, { recursive: true });
    const els = page.locator('[data-testid="dashboard-widget"]');
    const count = await els.count();
    for (let i = 0; i < count; i++) {
      const title = info.widgets[i]?.title ?? `widget-${i + 1}`;
      await els.nth(i).screenshot({ path: `${OUT}${slug}/${String(i + 1).padStart(2, "0")}-${fileSafe(title)}.png` });
    }
    await page.close();
  }
} finally {
  await browser.close();
  ui.kill();
}

if (failures.length) {
  console.log(`\n${failures.length} problem(s):\n  ${failures.join("\n  ")}`);
  process.exit(1);
}
console.log(`\nAll dashboards pass. Shots in ${OUT}`);
