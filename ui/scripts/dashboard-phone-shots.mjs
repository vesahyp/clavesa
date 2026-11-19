// Dashboards on a phone: iPhone 16 shots of whole dashboards and of every
// widget, with the checks that catch what Vesa saw on 2026-11-18 ("Some y
// axis labels are missing/weird and the layout on mobile is strange to
// have 2 widgets when there clearly is not enough room").
//
// Run via `make ui-dashboard-shots`. It starts
// `bin/clavesa ui --no-browser --workspaces <WS_DIR> --read-only` on a free
// port (or uses BASE_URL), opens /w/<WS_NAME>/dashboards/<slug> for each
// slug in DASHBOARDS on an iPhone 16 and on a 1440 x 900 desktop, waits
// until no widget is loading, and writes
//   .shots/dashboards/<LABEL>/<view>/<slug>.png             the whole dashboard
//   .shots/dashboards/<LABEL>/<view>/<slug>/NN-<title>.png   each widget
//   .shots/dashboards/<LABEL>/<view>/<slug>/NN-<title>-off.png
//        a chart with its first series turned off
// where <view> is phone or desktop (gitignored; <LABEL> emptied first).
// LABEL=before on the old build and LABEL=after on the new one give the
// before and after shots of a change side by side.
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
//   - no horizontal scroll, no console error;
//   - series can be turned off (Vesa, 2026-11-19: "Clavesa dashboard
//     graphs dont allow deselecting things"): every chart with more than
//     one series (stacked_bar, bar_line, pie, donut) has a legend of
//     toggle buttons. A tap turns a series off (aria-pressed=false, the
//     chart draws one series fewer), the last series left on cannot be
//     turned off, and a second tap brings each one back. On the phone
//     every legend button is at least 44 px tall.
//
// Env (all optional):
//   WS_DIR      the workspaces directory (default ~/Repositories/monorepo/analytics)
//   WS_NAME     the workspace (default web-traffic)
//   LABEL       the shots' folder under .shots/dashboards (default current)
//   VIEWS       comma-separated, phone and/or desktop (default both)
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
const WS_DIR = env("WS_DIR", `${homedir()}/Repositories/monorepo/analytics`);
const WS_NAME = env("WS_NAME", "web-traffic");
const DASHBOARDS = env("DASHBOARDS", "heineli,heineli-technical").split(",").map((s) => s.trim()).filter(Boolean);
const WAIT_MS = Number(env("WAIT_MS", "240000"));
const NARROW = 640;
const LABEL = env("LABEL", "current").replace(/[^a-zA-Z0-9_-]/g, "");
const ROOT = `${REPO}.shots/dashboards/${LABEL}/`;
const VIEWS = env("VIEWS", "phone,desktop").split(",").map((s) => s.trim()).filter(Boolean);
// Chart types whose series a legend can turn off.
const MULTI = new Set(["stacked_bar", "bar_line", "pie", "donut"]);

const iphone = devices["iPhone 16"];
if (!iphone) throw new Error('playwright has no "iPhone 16" device descriptor');
const VIEWPORTS = {
  phone: { ...iphone, colorScheme: "dark" },
  desktop: { viewport: { width: 1440, height: 900 }, deviceScaleFactor: 2, colorScheme: "dark" },
};
for (const v of VIEWS) if (!VIEWPORTS[v]) throw new Error(`unknown view "${v}" (phone, desktop)`);

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
rmSync(ROOT, { recursive: true, force: true });
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
  for (const view of VIEWS) {
  const OUT = `${ROOT}${view}/`;
  mkdirSync(OUT, { recursive: true });
  const ctx = await browser.newContext(VIEWPORTS[view]);
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

    const label = `${view} ${slug}`;
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

    // Series on and off, per chart with more than one series.
    for (let i = 0; i < count; i++) {
      const w = info.widgets[i];
      if (!w || !MULTI.has(w.type) || w.empty || w.failed) continue;
      const n = String(i + 1).padStart(2, "0");
      const name = `${label} ${n} "${w.title}"`;
      const el = els.nth(i);
      const items = el.locator('[data-testid="legend-item"]');
      const k = await items.count();
      if (k < 2) {
        // A pie of one slice has nothing to turn off; anything else must.
        const one = await el.locator(".recharts-pie-sector, .recharts-bar-rectangle, .recharts-line").count();
        if (!(k === 1 && one <= 1)) failures.push(`${name}: series cannot be turned off (no toggle legend)`);
        continue;
      }
      const frame = el.locator('[data-testid="widget-chart"]');
      const visible = async () => Number(await frame.getAttribute("data-series-visible"));
      const pressed = async (j) => (await items.nth(j).getAttribute("aria-pressed")) === "true";
      const probs = [];
      if (view === "phone") {
        for (let j = 0; j < k; j++) {
          const b = await items.nth(j).boundingBox();
          if (b && b.height < 43.5) probs.push(`legend button ${j + 1} is ${Math.round(b.height)} px tall, under 44`);
        }
      }
      if ((await visible()) !== k) probs.push(`chart says ${await visible()} series shown, legend has ${k}`);
      await items.nth(0).click();
      await sleep(250);
      if (await pressed(0)) probs.push("first legend button still on after a tap");
      if ((await visible()) !== k - 1) probs.push(`after one tap the chart shows ${await visible()} series, not ${k - 1}`);
      await el.screenshot({ path: `${OUT}${slug}/${n}-${fileSafe(w.title)}-off.png` });
      for (let j = 1; j < k - 1; j++) await items.nth(j).click();
      await items.nth(k - 1).click({ force: true });
      await sleep(250);
      if (!(await pressed(k - 1))) probs.push("the last series left on was turned off");
      if ((await visible()) !== 1) probs.push(`with all but one off the chart shows ${await visible()} series`);
      for (let j = 0; j < k - 1; j++) await items.nth(j).click();
      await sleep(250);
      if ((await visible()) !== k) probs.push(`after turning all back on the chart shows ${await visible()} of ${k}`);
      console.log(`  ${probs.length ? "FAIL" : "ok  "} ${n} legend: ${k} series turn off and on`);
      for (const p of probs) failures.push(`${name}: ${p}`);
    }
    await page.close();
  }
  await ctx.close();
  }
} finally {
  await browser.close();
  ui.kill();
}

if (failures.length) {
  console.log(`\n${failures.length} problem(s):\n  ${failures.join("\n  ")}`);
  process.exit(1);
}
console.log(`\nAll dashboards pass. Shots in ${ROOT}`);
