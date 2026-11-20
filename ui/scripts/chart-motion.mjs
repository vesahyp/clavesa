// Dashboard chart motion on a phone (Vesa, 2026-11-19: "I do like the
// animation changes of the ecarbrowser graphs"). Run via
// `make ui-chart-motion`. On an iPhone 16 it opens one dashboard, records a
// video, and samples the chart SVG every frame to check that:
//   - a chart draws in on first show (several frames, not one jump);
//   - a chart off screen when its data arrives draws final, and does not
//     draw again when it is scrolled into view (review 2026-11-19: all 35
//     heineli charts animated at once);
//   - a donut or pie slice turned off from the legend shrinks away while
//     the others widen, and grows back when turned on;
//   - bars turned off in a bar_line chart shrink into the axis, and its
//     line fades out and draws back in from the left;
//   - a period change moves the line to the new values (the old chart
//     stays, dimmed, while the query runs; no skeleton in between);
//   - with prefers-reduced-motion the same legend tap changes the chart in
//     one step.
// Writes (gitignored, emptied first):
//   .shots/motion/<slug>-phone.webm   the whole run (and .mp4 when ffmpeg is on PATH)
//   .shots/motion/frames/NN.png       the donut, one shot about every 100 ms
//                                     while a slice turns off
//
// Env (all optional): WS_DIR, WS_NAME, DASHBOARD (default heineli),
// PERIOD (the preset picked, default "Last 7 days"), BASE_URL, WAIT_MS,
// AWS_PROFILE, as for make ui-dashboard-shots.

import { chromium, devices } from "playwright";
import { execFileSync } from "node:child_process";
import { mkdirSync, renameSync, rmSync } from "node:fs";
import { homedir } from "node:os";

import { uiServer } from "./lib/ui-server.mjs";

const REPO = new URL("../../", import.meta.url).pathname;
const env = (k, d) => process.env[k] || d;
const BIN = env("CLAVESA_BIN", `${REPO}bin/clavesa`);
const WS_DIR = env("WS_DIR", `${homedir()}/Repositories/monorepo/analytics`);
const WS_NAME = env("WS_NAME", "web-traffic");
const SLUG = env("DASHBOARD", "heineli");
const PERIOD = env("PERIOD", "Last 7 days");
const WAIT_MS = Number(env("WAIT_MS", "240000"));
const OUT = `${REPO}.shots/motion/`;
const iphone = devices["iPhone 16"];
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const failures = [];
const check = (ok, what) => {
  console.log(`${ok ? "ok  " : "FAIL"} ${what}`);
  if (!ok) failures.push(what);
};

// In the page: sample the chart of the first widget matching `sel` every
// frame. Starts when `start()` is true (default at once), runs `ms` after
// that, and returns how many distinct pictures the chart went through.
// A picture is every path's d, every rect's geometry and every series
// group's opacity, rounded to whole pixels.
const SAMPLER = `
window.__sampleChart = (sel, ms, startExpr, maxWaitMs) => new Promise((done) => {
  const start = startExpr ? new Function("return (" + startExpr + ")") : () => true;
  const sig = () => {
    const w = document.querySelector(sel);
    const svg = w && w.querySelector("svg.recharts-surface");
    if (!svg) return "-";
    const r = (v) => Math.round(Number(v) || 0);
    const parts = [];
    for (const p of svg.querySelectorAll("path")) parts.push((p.getAttribute("d") || "").replace(/-?\\d+\\.\\d+/g, (m) => String(Math.round(m))));
    for (const p of svg.querySelectorAll("rect")) parts.push([p.getAttribute("x"), p.getAttribute("y"), p.getAttribute("width"), p.getAttribute("height")].map(r).join(","));
    for (const g of svg.querySelectorAll(".chart-series")) parts.push("o" + Math.round(getComputedStyle(g).opacity * 10));
    return parts.join("|");
  };
  const seen = [];
  const t0 = performance.now();
  let from = null;
  const tick = () => {
    const now = performance.now();
    if (from === null && start()) from = now;
    if (from !== null) {
      const s = sig();
      if (seen[seen.length - 1] !== s) seen.push(s);
    }
    if ((from !== null && now - from > ms) || now - t0 > maxWaitMs) done({ states: seen.length, started: from !== null });
    else requestAnimationFrame(tick);
  };
  tick();
});
`;

const sample = (page, sel, ms, startExpr = "", maxWaitMs = WAIT_MS) =>
  page.evaluate(([sel, ms, startExpr, maxWaitMs]) => window.__sampleChart(sel, ms, startExpr, maxWaitMs), [sel, ms, startExpr, maxWaitMs]);

const widgetSel = (type) => `[data-testid="dashboard-widget"][data-widget-type="${type}"]`;

async function openDashboard(ctx) {
  const page = await ctx.newPage();
  const errors = [];
  page.on("console", (m) => m.type() === "error" && errors.push(m.text()));
  page.on("pageerror", (e) => errors.push(e.message));
  await page.addInitScript(SAMPLER);
  return { page, errors };
}

async function loaded(page) {
  await page.waitForFunction(
    () => {
      const ws = [...document.querySelectorAll('[data-testid="dashboard-widget"]')];
      return ws.length > 0 && ws.every((w) => !w.querySelector(".animate-pulse"));
    },
    null,
    { timeout: WAIT_MS },
  );
}

rmSync(OUT, { recursive: true, force: true });
mkdirSync(`${OUT}frames`, { recursive: true });
const server = await uiServer(BIN, WS_DIR);
const url = `${server.base}/w/${encodeURIComponent(WS_NAME)}/dashboards/${encodeURIComponent(SLUG)}`;
const browser = await chromium.launch();
let video = null;
try {
  // One load first, so the server side is warm and the recorded run
  // shows charts drawing rather than a long first query.
  {
    const ctx = await browser.newContext({ ...iphone, colorScheme: "dark" });
    const { page } = await openDashboard(ctx);
    await page.goto(url, { waitUntil: "domcontentloaded" });
    await loaded(page);
    await ctx.close();
  }

  const ctx = await browser.newContext({
    ...iphone,
    colorScheme: "dark",
    reducedMotion: "no-preference",
    recordVideo: { dir: `${OUT}video`, size: iphone.viewport },
  });
  const { page, errors } = await openDashboard(ctx);

  // 1. First draw: the first line chart, from the moment it has an SVG.
  const firstLine = widgetSel("line");
  await page.goto(url, { waitUntil: "domcontentloaded" });
  await page.locator('[data-testid="dashboard-widget"]').first().waitFor({ timeout: WAIT_MS });
  const drawn = await page.locator(`${firstLine} svg.recharts-surface`).count();
  // The last chart well below the fold, marked for the sampler.
  const far = await page.evaluate(() => {
    const ws = [...document.querySelectorAll('[data-testid="dashboard-widget"]')].filter(
      (w) => /^(line|bar|bar_line|stacked_bar)$/.test(w.dataset.widgetType ?? "") && w.getBoundingClientRect().top > innerHeight * 1.5,
    );
    const w = ws[ws.length - 1];
    if (w) w.dataset.motionProbe = "far";
    return w?.dataset.widgetTitle ?? null;
  });
  const farSel = '[data-motion-probe="far"]';
  const farDraw = far
    ? sample(page, farSel, 1200, `!!document.querySelector('${farSel} svg.recharts-surface')`)
    : null;
  // On a phone the first line chart is below the stat tiles: bring its
  // card (still loading) into view, so it is on screen when its data comes.
  await page.locator(firstLine).first().scrollIntoViewIfNeeded();
  const fd = await sample(page, firstLine, 1200, `!!document.querySelector('${firstLine} svg.recharts-surface')`);
  if (drawn) console.log("note: the line chart had drawn before sampling began");
  check(fd.started && fd.states >= 5, `first draw: the line chart goes through ${fd.states} pictures in 1.2 s (5 or more)`);
  if (farDraw) {
    const r0 = await farDraw;
    check(r0.started && r0.states <= 2, `off screen: "${far}" draws final (${r0.states - 1} change(s) after its first picture)`);
  }
  await loaded(page);
  await sleep(1600);
  if (far) {
    const s = sample(page, farSel, 1500);
    await page.locator(farSel).scrollIntoViewIfNeeded();
    const r1 = await s;
    check(r1.states <= 1, `off screen: "${far}" does not draw again when scrolled into view (${r1.states} picture(s))`);
    await page.evaluate(() => scrollTo(0, 0));
    await sleep(300);
  }

  // 2. Donut (or pie): a slice off and back on.
  const pieType = (await page.locator(widgetSel("donut")).count()) ? "donut" : "pie";
  const pie = page.locator(widgetSel(pieType)).first();
  await pie.scrollIntoViewIfNeeded();
  await sleep(500);
  const pieSel = `${widgetSel(pieType)}`;
  let off = sample(page, pieSel, 1200);
  const shots = (async () => {
    for (let i = 0; i < 16; i++) {
      await pie.screenshot({ path: `${OUT}frames/${String(i).padStart(2, "0")}.png` });
      await sleep(60);
    }
  })();
  await pie.locator('[data-testid="legend-item"]').first().click();
  let r = await off;
  await shots;
  check(r.states >= 5, `${pieType}: a slice turned off moves through ${r.states} pictures (5 or more)`);
  await sleep(800);
  off = sample(page, pieSel, 1200);
  await pie.locator('[data-testid="legend-item"]').first().click();
  r = await off;
  check(r.states >= 5, `${pieType}: the slice turned back on moves through ${r.states} pictures (5 or more)`);
  await sleep(600);

  // 3. bar_line: bars off and on, the line off and on.
  if (await page.locator(widgetSel("bar_line")).count()) {
    const bl = page.locator(widgetSel("bar_line")).first();
    const blSel = widgetSel("bar_line");
    await bl.scrollIntoViewIfNeeded();
    await sleep(400);
    const items = bl.locator('[data-testid="legend-item"]');
    for (const [i, what] of [[0, "bars"], [1, "line"]]) {
      let s = sample(page, blSel, 600);
      await items.nth(i).click();
      r = await s;
      check(r.states >= 3, `bar_line: ${what} turned off move through ${r.states} pictures (3 or more)`);
      await sleep(1600);
      // The plot widens when the bars' axis goes: the line must still
      // reach its last point once the animation is over.
      const cut = await bl.evaluate((w) =>
        [...w.querySelectorAll("path.recharts-line-curve")].map((p) => {
          const d = p.getAttribute("stroke-dasharray");
          return d && /px/.test(d) ? Math.round(p.getTotalLength() - parseFloat(d)) : 0;
        }),
      );
      check(cut.every((c) => c <= 1), `bar_line: with ${what} off the line reaches its end (short by ${cut.join(", ") || 0} px)`);
      s = sample(page, blSel, 900);
      await items.nth(i).click();
      r = await s;
      check(r.states >= 3, `bar_line: ${what} turned back on move through ${r.states} pictures (3 or more)`);
      await sleep(1200);
    }
  }

  // 4. Period change: the line chart keeps its old values (dimmed) while
  // the query runs, then moves to the new ones.
  // Only a chart on screen when its new data comes animates. The picker
  // is at the top, so the first line chart the period change reruns
  // (shown dimmed while its query runs) is scrolled into view before the
  // new rows come. (A taller viewport does not do it: under phone
  // emulation innerHeight stays the phone's.)
  const combo = page.locator('[data-testid="time-range-picker"]').first();
  if (await combo.count()) {
    await page.evaluate(() => scrollTo(0, 0));
    await combo.click();
    const option = page.getByRole("option", { name: PERIOD });
    if (await option.count()) {
      await option.click();
      const sawSkeleton = page
        .waitForSelector(`${widgetSel("line")} .animate-pulse`, { timeout: 3000 })
        .then(() => true, () => false);
      // Every line chart the period change reruns (shown dimmed while it
      // runs); one whose rows come back the same has nothing to move.
      const marked = await page
        .waitForFunction(
          () => {
            const ws = [...document.querySelectorAll('[data-testid="dashboard-widget"][data-widget-type="line"]')].filter((x) =>
              x.querySelector("[data-stale]"),
            );
            ws.forEach((w, i) => (w.dataset.motionProbe = `period-${i}`));
            return ws.length;
          },
          null,
          { timeout: 10000 },
        )
        .then((h) => h.jsonValue(), () => 0);
      check(marked > 0, `period change: ${marked} line chart(s) keep their old values, dimmed, while the new query runs`);
      if (marked) {
        const sel = '[data-motion-probe="period-0"]';
        const s = sample(page, sel, 1200, `!document.querySelector('${sel} [data-stale]')`);
        await page.locator(sel).scrollIntoViewIfNeeded();
        const still = await page.locator(`${sel} [data-stale]`).count();
        r = await s;
        if (!still) console.log("note: the new rows came before the chart was in view");
        check(r.states >= 5, `period change: the line on screen moves to the new values through ${r.states} pictures (5 or more)`);
      }
      check(!(await sawSkeleton), "period change: no skeleton in a line chart while the new query runs");
      await sleep(800);
    } else {
      console.log(`note: no "${PERIOD}" option in the period picker; period change not checked`);
      await page.keyboard.press("Escape");
    }
  }
  check(errors.length === 0, `no console error (${errors.slice(0, 3).join(" | ") || "none"})`);
  video = page.video();
  await ctx.close();

  // 5. Reduced motion: the same slice tap is one step.
  {
    const still = await browser.newContext({ ...iphone, colorScheme: "dark", reducedMotion: "reduce" });
    const { page } = await openDashboard(still);
    await page.goto(url, { waitUntil: "domcontentloaded" });
    await loaded(page);
    await sleep(300);
    const s = sample(page, pieSel, 800);
    await page.locator(pieSel).first().locator('[data-testid="legend-item"]').first().click();
    r = await s;
    check(r.states <= 2, `reduced motion: a slice turned off changes the ${pieType} in one step (${r.states - 1} change(s))`);
    await still.close();
  }
} finally {
  await browser.close();
  server.stop();
}

if (video) {
  const webm = `${OUT}${SLUG}-phone.webm`;
  renameSync(await video.path(), webm);
  rmSync(`${OUT}video`, { recursive: true, force: true });
  console.log(`video: ${webm}`);
  try {
    execFileSync("ffmpeg", ["-y", "-loglevel", "error", "-i", webm, "-c:v", "libx264", "-pix_fmt", "yuv420p", "-movflags", "+faststart", `${OUT}${SLUG}-phone.mp4`]);
    console.log(`video: ${OUT}${SLUG}-phone.mp4`);
  } catch {
    console.log("note: no ffmpeg, so no .mp4");
  }
}
console.log(`frames: ${OUT}frames/`);
if (failures.length) {
  console.log(`\n${failures.length} problem(s):\n  ${failures.join("\n  ")}`);
  process.exit(1);
}
console.log("\nCharts animate, and stand still under reduced motion.");
