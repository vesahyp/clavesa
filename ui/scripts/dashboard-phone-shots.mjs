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
//   - x axis: no label cut off at the chart's edge (a long timestamp
//     tick shows as "19.11 10:25");
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
//     turned off, and a second tap brings each one back; no line is cut
//     short before its last point after a tap. On a touch
//     screen (phone, tablet) every legend button is at least 44 px tall;
//   - periods (Vesa, 2026-11-19: "Last 5min? Smallest should be 1h"): the
//     period picker offers Last 1 hour, 24 hours, 7, 30 and 90 days and
//     Custom, nothing under an hour; it shows the dashboard's default (or,
//     with none, a preset picked from the data); the refresh picker offers
//     Off, 5 min, 15 min, 1 hour and is Off; on the phone, a link naming a
//     retired period (?<name>.rel=now-5m) or one under an hour (now-10m)
//     opens on Last 1 hour; with auto refresh on, a tick (the clock moved
//     5 minutes on) queries the moved window and never the same one again
//     (last dashboard, first view);
//   - reduced motion: no widget body has a transition (the dim fade);
//   - tables: below 640 px no table scrolls sideways (one too wide turns
//     into a list of rows); each row's title is one line, ending in an
//     ellipsis when long, with the whole value in its title attribute.
//
// Env (all optional):
//   WS_DIR      the workspaces directory (default ~/Repositories/monorepo/analytics)
//   WS_NAME     the workspace (default web-traffic)
//   LABEL       the shots' folder under .shots/dashboards (default current)
//   VIEWS       comma-separated: phone, tablet (iPad Pro 11), desktop
//               (default phone,desktop)
//   DASHBOARDS  comma-separated slugs (default heineli,heineli-technical)
//   BASE_URL    a server that is already running
//   WAIT_MS     how long a dashboard may load (default 240000)
//   AWS_PROFILE passed through to clavesa, as for any command

import { chromium, devices } from "playwright";
import { mkdirSync, rmSync } from "node:fs";
import { homedir } from "node:os";

import { uiServer } from "./lib/ui-server.mjs";

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
const ipad = devices["iPad Pro 11"];
if (!ipad) throw new Error('playwright has no "iPad Pro 11" device descriptor');
// reducedMotion: the charts and cards draw at once (Recharts' "auto" and
// the CSS both follow prefers-reduced-motion), so a shot never catches a
// chart halfway through its animation. make ui-chart-motion checks the
// animation itself.
const still = { colorScheme: "dark", reducedMotion: "reduce" };
const VIEWPORTS = {
  phone: { ...iphone, ...still },
  tablet: { ...ipad, ...still },
  desktop: { viewport: { width: 1440, height: 900 }, deviceScaleFactor: 2, ...still },
};
for (const v of VIEWS) if (!VIEWPORTS[v]) throw new Error(`unknown view "${v}" (phone, tablet, desktop)`);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

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

// Lines whose stroke dash shows less than the whole path: a line cut short
// before its last point (Recharts kept the dash of a shorter path once).
const cutLines = (el) =>
  el.evaluate((w) =>
    [...w.querySelectorAll("path.recharts-line-curve, path.recharts-curve.recharts-area-curve")]
      .filter((p) => {
        const dash = p.getAttribute("stroke-dasharray");
        if (!dash || !/px/.test(dash)) return false;
        const shown = parseFloat(dash);
        return shown + 1 < p.getTotalLength();
      }).length,
  );

const PERIODS = ["Last 1 hour", "Last 24 hours", "Last 7 days", "Last 30 days", "Last 90 days", "Custom range"];
const PERIOD_OF = {
  "now-1h": "Last 1 hour", "now-24h": "Last 24 hours", "now-7d": "Last 7 days", "now-30d": "Last 30 days", "now-90d": "Last 90 days",
  last_24h: "Last 24 hours", last_7d: "Last 7 days", last_30d: "Last 30 days", last_90d: "Last 90 days",
  "now-5m": "Last 1 hour", "now-15m": "Last 1 hour", "now-30m": "Last 1 hour", "now-3h": "Last 1 hour", "now-12h": "Last 24 hours", "now-3d": "Last 7 days",
};
const REFRESH = ["Off", "5 min", "15 min", "1 hour"];

// The options of a Radix select: open it, read them, close it.
async function selectOptions(page, trigger) {
  await trigger.click();
  const opts = page.getByRole("option");
  await opts.first().waitFor({ timeout: 5000 });
  const texts = (await opts.allTextContents()).map((t) => t.trim());
  await page.keyboard.press("Escape");
  await sleep(200);
  return texts;
}

const fileSafe = (s) => s.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 50) || "widget";

const browser = await chromium.launch();
rmSync(ROOT, { recursive: true, force: true });
const server = await uiServer(BIN, WS_DIR);
const BASE = server.base;

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
          const xCut = [...w.querySelectorAll(".recharts-xAxis-tick-labels text.recharts-cartesian-axis-tick-value")]
            .filter((t) => {
              const b = t.getBoundingClientRect();
              const s = t.ownerSVGElement?.getBoundingClientRect() ?? sr;
              return s ? b.left < s.left - 0.5 || b.right > s.right + 0.5 : false;
            })
            .map((t) => (t.textContent ?? "").trim());
          const wide = [...w.querySelectorAll("*")]
            .filter((el) => {
              if (el.closest("svg") || el.closest("table") || el.closest('[data-scroll-x]')) return false;
              const cs = getComputedStyle(el);
              if (cs.overflowX === "auto" || cs.overflowX === "scroll") return false;
              // An ellipsis on purpose (a stacked row's title) is not a cut.
              if (cs.textOverflow === "ellipsis" && el.title === (el.textContent ?? "")) return false;
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
          // A stacked table row's title: one line, the full value in its
          // title attribute.
          const titles = [...w.querySelectorAll('[data-testid="stacked-title"]')].map((el) => {
            const lh = parseFloat(getComputedStyle(el).lineHeight) || 16;
            return {
              text: (el.textContent ?? "").trim(),
              oneLine: el.getBoundingClientRect().height <= lh * 1.5,
              titled: el.title === (el.textContent ?? ""),
            };
          });
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
            xCut,
            titles,
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
      for (const t of w.xCut) probs.push(`x label "${t}" cut off`);
      for (const t of w.titles) {
        if (!t.oneLine) probs.push(`stacked row title "${t.text.slice(0, 24)}" wraps`);
        if (!t.titled) probs.push(`stacked row title "${t.text.slice(0, 24)}" has no full-value title`);
      }
      const tile = w.type === "big_number";
      if (info.vw < NARROW && !w.full && !tile) probs.push(`takes ${w.width} px, not the full row`);
      for (const t of w.wide) probs.push(`text "${t}" wider than its box`);
      // A table too wide for a phone even with its text wrapped turns into
      // a list of rows; one that scrolls sideways is a failure.
      if (w.tableCut && info.vw < NARROW) probs.push("table scrolls sideways");
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
      if (VIEWPORTS[view].hasTouch) {
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
      if (await cutLines(el)) probs.push("with one series off, a line is cut short before its last point");
      await el.screenshot({ path: `${OUT}${slug}/${n}-${fileSafe(w.title)}-off.png` });
      for (let j = 1; j < k - 1; j++) await items.nth(j).click();
      await items.nth(k - 1).click({ force: true });
      await sleep(250);
      if (!(await pressed(k - 1))) probs.push("the last series left on was turned off");
      if ((await visible()) !== 1) probs.push(`with all but one off the chart shows ${await visible()} series`);
      for (let j = 0; j < k - 1; j++) await items.nth(j).click();
      await sleep(250);
      if ((await visible()) !== k) probs.push(`after turning all back on the chart shows ${await visible()} of ${k}`);
      if (await cutLines(el)) probs.push("after turning all back on, a line is cut short before its last point");
      console.log(`  ${probs.length ? "FAIL" : "ok  "} ${n} legend: ${k} series turn off and on`);
      for (const p of probs) failures.push(`${name}: ${p}`);
    }
    // Periods and refresh.
    {
      const probs = [];
      const spec = await (await page.request.get(`${BASE}/w/${encodeURIComponent(WS_NAME)}/api/dashboards/${encodeURIComponent(slug)}`)).json();
      const tr = (spec.controls ?? []).find((c) => c.type === "time_range");
      const picker = page.locator('[data-testid="time-range-picker"]').first();
      if (tr) {
        await page.evaluate(() => scrollTo(0, 0));
        const shown = (await picker.textContent())?.trim() ?? "";
        const want = tr.default ? PERIOD_OF[tr.default.trim()] : null;
        if (want && shown !== want) probs.push(`period picker shows "${shown}", the default is ${tr.default} ("${want}")`);
        if (!want && !PERIODS.slice(0, 5).includes(shown)) probs.push(`period picker shows "${shown}", not a preset, with no default set`);
        const opts = await selectOptions(page, picker);
        if (JSON.stringify(opts) !== JSON.stringify(PERIODS)) probs.push(`period options [${opts.join(", ")}], want [${PERIODS.join(", ")}]`);
        if (opts.some((o) => /minute/i.test(o))) probs.push("a period under an hour is offered");
      }
      const refresh = page.locator('[data-testid="refresh-picker"]').first();
      if (await refresh.count()) {
        const shown = (await refresh.textContent())?.trim();
        if (shown !== "Off") probs.push(`refresh is "${shown}", not Off`);
        const opts = await selectOptions(page, refresh);
        if (JSON.stringify(opts) !== JSON.stringify(REFRESH)) probs.push(`refresh options [${opts.join(", ")}], want [${REFRESH.join(", ")}]`);
      }
      const dashUrl = `${BASE}/w/${encodeURIComponent(WS_NAME)}/dashboards/${encodeURIComponent(slug)}`;
      if (tr && view === VIEWS[0]) {
        for (const rel of ["now-5m", "now-10m"]) {
          const old = await ctx.newPage();
          const oldErrors = [];
          old.on("pageerror", (e) => oldErrors.push(e.message));
          await old.goto(`${dashUrl}?${encodeURIComponent(tr.name)}.rel=${rel}`, { waitUntil: "domcontentloaded" });
          const p2 = old.locator('[data-testid="time-range-picker"]').first();
          await p2.waitFor({ timeout: WAIT_MS });
          const shown = (await p2.textContent())?.trim();
          if (shown !== "Last 1 hour") probs.push(`a link with ${tr.name}.rel=${rel} shows "${shown}", not "Last 1 hour"`);
          if (oldErrors.length) probs.push(`a link with ${tr.name}.rel=${rel}: ${oldErrors[0]}`);
          await old.close();
        }
      }
      // Auto refresh moves the window (review 2026-11-19: it ran the same
      // window again, an Athena scan that never showed newer data).
      if (tr && view === VIEWS[0] && slug === DASHBOARDS[DASHBOARDS.length - 1]) {
        const rp = await ctx.newPage();
        await rp.clock.install();
        const sent = [];
        rp.on("request", (r) => {
          if (r.method() === "POST" && /\/dashboards\/query$/.test(r.url())) {
            try {
              sent.push(JSON.parse(r.postData() ?? "{}"));
            } catch {}
          }
        });
        const settled = () =>
          rp.waitForFunction(
            () => !document.querySelector('[data-testid="dashboard-widget"] .animate-pulse, [data-testid="widget-body"][aria-busy]'),
            null,
            { timeout: WAIT_MS },
          );
        await rp.goto(`${dashUrl}?refresh=5m`, { waitUntil: "domcontentloaded" });
        await rp.locator('[data-testid="dashboard-widget"]').first().waitFor({ timeout: WAIT_MS });
        await settled();
        await sleep(500);
        const before = sent.splice(0);
        const key = (b) => JSON.stringify([b.dir, b.sql, b.params ?? {}]);
        const startOf = (list) => list.map((b) => b.params?.[`${tr.name}.start`]).filter(Boolean).sort()[0];
        await rp.clock.fastForward("05:01");
        for (let i = 0; i < 60 && sent.length === 0; i++) await sleep(250);
        await settled();
        const after = sent.splice(0);
        const old = new Set(before.map(key));
        if (after.length === 0) probs.push("auto refresh: a 5 min tick sent no query");
        if (after.some((b) => old.has(key(b)))) probs.push("auto refresh: a tick queried the same window again");
        if (after.length && !(startOf(after) > startOf(before))) probs.push(`auto refresh: the window did not move (${startOf(before)} → ${startOf(after)})`);
        console.log(`       refresh: ${before.length} queries, then ${after.length} on the tick, window ${startOf(before)} → ${startOf(after)}`);
        await rp.close();
      }
      const fading = await page.evaluate(() =>
        [...document.querySelectorAll('[data-testid="widget-body"]')].filter((el) => {
          const cs = getComputedStyle(el);
          return cs.transitionProperty !== "none" && cs.transitionDuration !== "0s";
        }).length,
      );
      if (fading) probs.push(`reduced motion: ${fading} widget bodies still fade (transition on)`);
      console.log(`  ${probs.length ? "FAIL" : "ok  "} periods and refresh${tr ? ` (${tr.name}, default ${tr.default || "from the data"})` : " (no period control)"}`);
      for (const p of probs) failures.push(`${label}: ${p}`);
    }
    await page.close();
  }
  await ctx.close();
  }
} finally {
  await browser.close();
  server.stop();
}

if (failures.length) {
  console.log(`\n${failures.length} problem(s):\n  ${failures.join("\n  ")}`);
  process.exit(1);
}
console.log(`\nAll dashboards pass. Shots in ${ROOT}`);
