// Screenshots for the navigation redesign (docs/ideas/hosted-navigation.md),
// at iPhone 16 and at desktop width (1440 x 900).
//
//   after   every main page of the clickable prototype,
//           docs/ideas/hosted-navigation/prototype.html, as a viewer of all
//           workspaces and as a viewer of one (?viewer=one), plus the open
//           workspace switcher and the full-path sheet.
//   before  (BEFORE=1) the same pages of the deployed hosted UI, signed in
//           as the check user (lib/hosted-session.mjs), WS default smoke2.
//   app     (APP=1) the built navigation in the real app: bin/clavesa ui
//           --workspaces ~/clavesa-workspaces --read-only (multi mode).
//   live    (LIVE=1) the built navigation on the deployed hosted UI, as the
//           check user.
//   app and live take every page of one workspace (WS, PIPELINE, CATALOG,
//   SCHEMA, TABLE; default smoke2, taxi, clavesa_smoke2, taxi, trips), the
//   open workspace switcher, and on a phone the full-path panel.
//
// Per shot it records sideways scrolling: the page itself (scrollWidth
// greater than innerWidth) and any box inside it that scrolls or clips
// sideways (content wider than the box). On a phone the prototype must
// have neither; a "before" shot only reports them.
//   TABLE   <catalog>/<schema>/<table> for the before table page
//           (default clavesa_smoke2/taxi/trips)
// Shots go to .shots/nav/{after,before}/<page>-{phone,desk}.png, full length.
// Run via `make ui-nav-shots` (BEFORE=1 for the deployed UI).
//
// Exit status 1 when a phone page of the prototype, app or live scrolls or
// clips sideways (text shortened with "…" is listed, not failed).

import { chromium, devices } from "playwright";
import { mkdirSync, rmSync } from "node:fs";
import { hostedSignIn, plantSession } from "./lib/hosted-session.mjs";
import { freePort, startUI } from "./lib/ui-server.mjs";
import { homedir } from "node:os";

const REPO = new URL("../../", import.meta.url).pathname;
const BEFORE = process.env.BEFORE === "1";
const APP = process.env.APP === "1";
const LIVE = process.env.LIVE === "1";
const WS = process.env.WS || "smoke2";
const BASE = (process.env.BASE_URL || "https://ui.clavesa.dev").replace(/\/$/, "");
const OUT = `${REPO}.shots/nav/${BEFORE ? "before" : APP ? "app" : LIVE ? "live" : "after"}/`;
const PIPELINE = process.env.PIPELINE || "taxi";
const CATALOG = process.env.CATALOG || "clavesa_smoke2";
const SCHEMA = process.env.SCHEMA || "taxi";
const TABLE_NAME = process.env.TABLE_NAME || "trips";
const PROTO = `file://${REPO}docs/ideas/hosted-navigation/prototype.html`;
const iphone = devices["iPhone 16"];
const DESK = { viewport: { width: 1440, height: 900 }, deviceScaleFactor: 1 };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const W = "/w/web-traffic";
const T = `${W}/catalog/clavesa_web_traffic/gold/fact_page_views`;
// [name, hash route, query, action]
const AFTER = [
  ["01-workspaces", "/", "", null],
  ["02-overview", W, "", null],
  ["03-pipelines", `${W}/pipelines`, "", null],
  ["04-pipeline", `${W}/pipelines/gold`, "", null],
  ["05-run", `${W}/pipelines/gold/runs/r8`, "", null],
  ["06-step", `${W}/pipelines/gold/runs/r8/steps/fact_page_views`, "", null],
  ["07-catalog", `${W}/catalog`, "", null],
  ["08-catalog-one", `${W}/catalog/clavesa_web_traffic`, "", null],
  ["09-schema", `${W}/catalog/clavesa_web_traffic/gold`, "", null],
  ["10-table", T, "", null],
  ["11-table-sample", `${T}/sample`, "", null],
  ["12-dashboards", `${W}/dashboards`, "", null],
  ["13-dashboard", `${W}/dashboards/heineli`, "", null],
  ["14-site-usage", "/stats", "", null],
  ["15-switcher", T, "", "switcher"],
  ["16-full-path", T, "", "path"],
  ["17-one-viewer-overview", W, "?viewer=one", null],
  ["18-one-viewer-switcher", `${W}/pipelines/gold`, "?viewer=one", "switcher"],
];

async function sideways(page) {
  return page.evaluate(() => {
    const out = [];
    const doc = document.scrollingElement;
    if (doc.scrollWidth > innerWidth + 1) out.push(`page ${doc.scrollWidth}/${innerWidth}`);
    for (const el of document.querySelectorAll("body *")) {
      const cs = getComputedStyle(el);
      if (cs.display === "none" || el.clientWidth === 0) continue;
      if (!["auto", "scroll", "hidden", "clip"].includes(cs.overflowX)) continue;
      if (el.scrollWidth > el.clientWidth + 1 && el.tagName !== "svg") {
        const id = el.getAttribute("data-testid") || el.className?.toString().split(" ")[0] || el.tagName;
        // Text shortened with "…" is no scroll; it is listed apart.
        const kind = cs.textOverflow === "ellipsis" ? "cut" : cs.overflowX === "hidden" || cs.overflowX === "clip" ? "clips" : "scrolls";
        out.push(`${kind} ${id} "${(el.textContent || "").trim().slice(0, 30)}" ${el.scrollWidth}/${el.clientWidth}`);
      }
    }
    return out;
  });
}

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
    await page.setViewportSize({ width: vp.width, height: Math.min(vp.height + extra, 8000) });
    await sleep(200);
  }
}

async function shoot(ctx, name, url, action, ready) {
  const page = await ctx.newPage();
  await page.goto(url, { waitUntil: "load" });
  if (ready) await ready(page);
  if (action === "switcher") await page.locator(ctx.__phone ? "[data-testid=ws-chip]" : "[data-testid=ws-switcher]").click();
  if (action === "path") {
    if (ctx.__phone) await page.locator(APP || LIVE ? "[data-testid=phone-path]" : ".phoneOnly .more").click();
    else { await page.close(); return null; }
  }
  await sleep(250);
  const side = await sideways(page);
  if (!action) await fitHeight(page);
  const file = `${OUT}${name}-${ctx.__phone ? "phone" : "desk"}.png`;
  await page.screenshot({ path: file, fullPage: !action });
  await page.close();
  return { name, file, side };
}

rmSync(OUT, { recursive: true, force: true });
mkdirSync(OUT, { recursive: true });
const browser = await chromium.launch();
const results = [];
try {
  const phone = await browser.newContext({ ...iphone, colorScheme: "dark", reducedMotion: "reduce" });
  phone.__phone = true;
  const desk = await browser.newContext({ ...DESK, colorScheme: "dark", reducedMotion: "reduce" });
  if (APP || LIVE) {
    let base = BASE;
    let ui = null;
    if (LIVE) {
      const tok = hostedSignIn({ repo: REPO, profile: process.env.PROFILE || "personal" });
      for (const ctx of [phone, desk]) await plantSession(ctx, tok);
    } else {
      const port = await freePort();
      ui = await startUI(process.env.CLAVESA_BIN || `${REPO}bin/clavesa`, process.env.WS_DIR || `${homedir()}/clavesa-workspaces`, port);
      base = `http://127.0.0.1:${port}`;
    }
    const ready = async (p) => {
      await p.waitForSelector("h1", { timeout: 90000 }).catch(() => {});
      await sleep(2500);
    };
    const w = `/w/${WS}`;
    const cat = `${w}/catalog?catalog=${encodeURIComponent(CATALOG)}`;
    const table = `${w}/tables/${CATALOG}/${SCHEMA}/${TABLE_NAME}`;
    const PAGES = [
      ["02-overview", `${w}/`, null],
      ["03-pipelines", `${w}/pipelines`, null],
      ["04-pipeline", `${w}/pipelines/dashboard?dir=${encodeURIComponent(PIPELINE)}`, null],
      ["07-catalog", `${w}/catalog`, null],
      ["08-catalog-one", cat, null],
      ["09-schema", `${cat}&schema=${encodeURIComponent(SCHEMA)}`, null],
      ["10-table", table, null],
      ["12-dashboards", `${w}/dashboards`, null],
      ["15-switcher", table, "switcher"],
      ["16-full-path", table, "path"],
    ];
    try {
      for (const ctx of [phone, desk])
        for (const [name, path, action] of PAGES) {
          const r = await shoot(ctx, name, `${base}${path}`, action, ready);
          if (r) results.push({ ...r, phone: ctx.__phone });
        }
    } finally {
      ui?.kill();
    }
  } else if (BEFORE) {
    const tok = hostedSignIn({ repo: REPO, profile: process.env.PROFILE || "personal" });
    for (const ctx of [phone, desk]) await plantSession(ctx, tok);
    // The deployed pages, reached by URL.
    const wait = (sel) => async (p) => { await p.waitForSelector(sel, { timeout: 90000 }).catch(() => {}); await sleep(1500); };
    // Catalog rows open on a tap (no link), so the table comes from TABLE.
    const table = `/w/${WS}/tables/${process.env.TABLE || "clavesa_smoke2/taxi/trips"}`;
    const BEFORE_PAGES = [
      ["01-workspaces", "/", "[data-testid=workspace-card], main"],
      ["02-overview", `/w/${WS}/`, "[data-testid=workspace-home]"],
      ["03-pipelines", `/w/${WS}/pipelines`, "main a"],
      ["07-catalog", `/w/${WS}/catalog`, "table"],
      ["10-table", table, "h1"],
      ["12-dashboards", `/w/${WS}/dashboards`, "main"],
    ];
    for (const ctx of [phone, desk])
      for (const [name, path, sel] of BEFORE_PAGES) results.push(await shoot(ctx, name, `${BASE}${path}`, null, wait(sel)));
  } else {
    for (const ctx of [phone, desk])
      for (const [name, route, query, action] of AFTER) {
        const r = await shoot(ctx, name, `${PROTO}${query}#${route}`, action, null);
        if (r) results.push({ ...r, phone: ctx.__phone });
      }
  }
} finally {
  await browser.close();
}

let bad = 0;
for (const r of results) {
  const fail = !BEFORE && r.phone && r.side.some((x) => !x.startsWith("cut "));
  if (fail) bad++;
  console.log(`${fail ? "FAIL" : r.side.length ? "note" : "ok  "} ${r.file.replace(REPO, "")}${r.side.length ? `  sideways: ${r.side.join("; ")}` : ""}`);
}
console.log(bad ? `${bad} phone page(s) scroll sideways` : `${results.length} shots in ${OUT.replace(REPO, "")}`);
process.exit(bad ? 1 : 0);
