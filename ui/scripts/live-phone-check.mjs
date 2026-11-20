// The live phone check (ADR-032 slice 9): the deployed hosted UI, opened as
// the scripted check user on an emulated iPhone 16.
//
// Run via `make hosted-live-check`. What it does:
//   1. Reads the check user, /clavesa-ui/check-user (SecureString JSON
//      {username, password}), from SSM with the AWS CLI at run time. The
//      pool id and the check client id come from `terraform output` of
//      hosted/infra. Nothing is read from the environment or the repo, and
//      the password and the tokens are never printed.
//   2. Gets tokens with `aws cognito-idp admin-initiate-auth`
//      (ADMIN_USER_PASSWORD_AUTH, the check client only) and plants them
//      the way ui/src/hosted/auth.ts keeps a session: sessionStorage
//      `clavesa.hosted.session`, localStorage `clavesa.hosted.refresh`,
//      before the app loads (addInitScript).
//   3. Reaches every page the way a person does: it opens only the start
//      page by URL, then taps through (each page's `taps`) and fails when a
//      tap target is missing or the taps lead elsewhere. Two checks type an
//      address on purpose (`typed`): a bad address, and /stats for a user
//      outside "all". The pipeline and the run come from the API (the
//      workspace summary), or from PIPELINE and RUN.
//   Per page: no console error, no uncaught exception, no failed request to
//   /api or /w/<name>/api, no horizontal scroll (scrollWidth <= innerWidth).
//   The only failed request the pages are designed to tolerate:
//   net::ERR_ABORTED, the browser cancelling a request when a page or a
//   query is replaced. It is not a server answer and is not counted.
//   The bottom bar (Vesa, 2026-11-16: "The ui bottom buttons change, which
//   is not good ux"): every page shows the same items, in the same order,
//   with the same labels, hrefs and boxes; only the active item differs,
//   and it carries the visible marker (Workspaces on pages 1 to 4, Stats
//   on page 5). Each item must be on top at its centre (not covered, so
//   tappable). Pages 2 to 4 and 6 (the Pipelines page) show a visible back button at
//   the top; pages 1 and 5 show none.
//   Review fixes (2026-11-16):
//   - times are Helsinki time: the pipeline page's newest run label in the
//     "Where the time goes" chart equals the run's start in Helsinki;
//   - dates are day.month: every x tick on /stats is "16.11", never "11.16";
//   - run counts match the CLI: each workspace in CLI_COMPARE (default
//     kivikko-traffic, ~/Repositories/kivikko/analytics/clavesa) shows the
//     14 and 30 day run counts that `clavesa workspace summary --json` gives;
//   - no "Never run" flash: the pipeline page never shows that text while
//     it loads (a MutationObserver from the first byte);
//   - an unknown pipeline URL (07-not-found) shows the plain not-found page
//     and no server text (no "hclparser", "/tmp/", "API GET").
//   - health: a workspace with a failed run in the last 14 Helsinki days
//     (counted here from GET /api/workspaces, not by the app) is not
//     "Healthy" and its card names the failed runs; every card that is not
//     Healthy says why, in plain words (never the bare "no run inside the
//     expected interval").
//   - workspaces list: a summary line first whose counts equal the cards'
//     statuses, the cards problems first, every ready card one link to
//     /w/<name>/ (the whole card tappable), and the page no taller than
//     2.1 phone screens.
//   - plain labels: no page shows "/billion", "per billion", "rate(",
//     "cron(", "no schedule", "Module:" or "1 runs"; the run page's title is
//     "Run 16.11 02:40, 1m 22s" (start in Helsinki), not the run ID.
//   - tap targets: every visible link and button is at least 44 x 44 px,
//     except the bars inside a chart (a tap there only shows a value) and
//     a link inside running text.
//   - hosted desktop (1440 x 900): the workspace sidebar has no Query,
//     Credentials or Runner entry, and /query, /credentials and /runner
//     land on the workspace home with no Run button.
//   - the second tab is "Site usage" (not "Stats"), on every page.
//   - 12-personal: LOCAL_WS is personal_data in the registry, so its
//     dashboards are kept to the Mac: GET /dashboards lists none, and a
//     link to one (LOCAL_DASHBOARD, default pipeline-runs-traffic) shows
//     one notice and no widget, never "Query failed": "This dashboard
//     holds visitor data and opens only in the local clavesa UI" when the
//     Mac publishes a snapshot, else the page's "data of this workspace is
//     on the Mac" line (no snapshot: the page runs no query at all).
//   A 503 (Lambda throttling on a cold load) on a page makes the script load
//   that page once more and say so; a failure that persists fails the run.
//
// Beacons: the page sends its /t.gif beacons with the iPhone 16 user agent
// plus a "HeadlessChrome" token, so the traffic pipeline (events_typed.py,
// AUTOMATION_INDICATORS) files the run as a bot and the human numbers on
// /stats stay clean. The iPhone descriptor's own agent has no such token.
//
// Shots: .shots/live/<page>.png (gitignored; emptied first), taken full
// length. Exit status 1, with one line per failure, when a check fails.
//
// Env (all optional):
//   BASE_URL   default https://ui.clavesa.dev
//   WS         the workspace for pages 2 to 4 (default smoke2)
//   PIPELINE   its pipeline dir (default: the first pipeline in the summary)
//   RUN        the run id (default: the newest run in the summary)
//   PROFILE    AWS profile (default personal); region is eu-north-1
//   PARAMETER  the SSM parameter (default /clavesa-ui/check-user)
//   WAIT_MS    per page wait for its content (default 90000)

import { chromium, devices } from "playwright";
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";

const REPO = new URL("../../", import.meta.url).pathname;
const env = (k, d) => process.env[k] || d;
const BASE = env("BASE_URL", "https://ui.clavesa.dev").replace(/\/$/, "");
const WS = env("WS", "smoke2");
// A local-warehouse workspace for the navigation walk (ADR-033).
const LOCAL_WS = env("LOCAL_WS", "site-traffic");
const PROFILE = env("PROFILE", "personal");
const REGION = "eu-north-1";
const PARAMETER = env("PARAMETER", "/clavesa-ui/check-user");
const WAIT_MS = Number(env("WAIT_MS", "90000"));
const OUT = `${REPO}.shots/live/`;

const iphone = devices["iPhone 16"];
if (!iphone) throw new Error('playwright has no "iPhone 16" device descriptor');

// The zone the hosted UI shows: hosted/infra var.time_zone, from its
// terraform output (set below, before any page is checked).
let HEL = "Europe/Helsinki";
const helDay = (d) => new Intl.DateTimeFormat("en-CA", { timeZone: HEL }).format(d);
const helHM = (d) =>
  new Intl.DateTimeFormat("en-GB", { timeZone: HEL, hour: "2-digit", minute: "2-digit", hour12: false }).format(d);
/** "16.11 08:00", Helsinki time: the UI's stamp(). */
function helStamp(iso) {
  const d = new Date(iso);
  const [, m, day] = helDay(d).split("-");
  return `${Number(day)}.${Number(m)} ${helHM(d)}`;
}

// CLI_COMPARE: "name=dir,name=dir". The default is kivikko when its repo
// is on this Mac; a dir that is missing is skipped with a note.
const CLI_COMPARE = (process.env.CLI_COMPARE ??
  `kivikko-traffic=${homedir()}/Repositories/kivikko/analytics/clavesa`)
  .split(",")
  .filter(Boolean)
  .map((kv) => kv.split("="));

/** Finished runs in the CLI's 14 and 30 day windows for one workspace. */
function cliCounts(dir) {
  const raw = execFileSync("clavesa", ["workspace", "summary", "--json"], {
    cwd: dir,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });
  const j = JSON.parse(raw);
  const sum = (a) => (a ?? []).reduce((n, d) => n + d.succeeded + d.failed, 0);
  return { d14: sum(j.runs_per_day_14d), d30: sum(j.runs_per_day_30d) };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const awsEnv = { ...process.env, AWS_PROFILE: PROFILE, AWS_REGION: REGION, AWS_DEFAULT_REGION: REGION };

function run(cmd, args, opts = {}) {
  return execFileSync(cmd, args, { encoding: "utf8", env: awsEnv, stdio: ["ignore", "pipe", "pipe"], ...opts });
}

// ---- credentials and tokens (secrets stay in this process) ----

function readCheckUser() {
  let raw;
  try {
    raw = run("aws", [
      "ssm", "get-parameter", "--name", PARAMETER, "--with-decryption",
      "--query", "Parameter.Value", "--output", "text",
      "--profile", PROFILE, "--region", REGION,
    ]);
  } catch (e) {
    throw new Error(`could not read ${PARAMETER} from SSM (profile ${PROFILE}): ${String(e.stderr ?? e.message).split("\n")[0]}`);
  }
  const u = JSON.parse(raw);
  if (!u.username || !u.password) throw new Error(`${PARAMETER} has no username and password`);
  return u;
}

function terraformOutputs() {
  let raw;
  try {
    raw = run("terraform", [`-chdir=${REPO}hosted/infra`, "output", "-json"]);
  } catch (e) {
    throw new Error(`terraform output of hosted/infra failed (make hosted-init first?): ${String(e.stderr ?? e.message).split("\n")[0]}`);
  }
  const o = JSON.parse(raw);
  const need = (k) => {
    const v = o[k]?.value;
    if (!v) throw new Error(`terraform output ${k} is missing`);
    return v;
  };
  return { pool: need("user_pool_id"), client: need("check_client_id"), region: need("region"), timeZone: o.time_zone?.value };
}

function signIn(user, tf) {
  // The password goes through a 0600 file, never on a command line (ps).
  const dir = mkdtempSync(join(tmpdir(), "clavesa-live-"));
  const file = join(dir, "auth.json");
  try {
    writeFileSync(
      file,
      JSON.stringify({
        UserPoolId: tf.pool,
        ClientId: tf.client,
        AuthFlow: "ADMIN_USER_PASSWORD_AUTH",
        AuthParameters: { USERNAME: user.username, PASSWORD: user.password },
      }),
      { mode: 0o600 },
    );
    const raw = run("aws", [
      "cognito-idp", "admin-initiate-auth", "--cli-input-json", `file://${file}`,
      "--region", tf.region, "--profile", PROFILE,
    ]);
    const a = JSON.parse(raw).AuthenticationResult;
    if (!a?.IdToken) throw new Error("Cognito returned no tokens");
    return {
      idToken: a.IdToken,
      accessToken: a.AccessToken,
      refreshToken: a.RefreshToken,
      expiresAt: Date.now() + a.ExpiresIn * 1000,
    };
  } catch (e) {
    // Never echo e.cmd or the input: the first stderr line is enough.
    throw new Error(`admin-initiate-auth failed: ${String(e.stderr ?? e.message).split("\n")[0]}`);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

async function api(tok, path) {
  const r = await fetch(`${BASE}${path}`, {
    headers: { Authorization: `Bearer ${tok.idToken}` },
    signal: AbortSignal.timeout(WAIT_MS),
  });
  if (!r.ok) throw new Error(`GET ${path}: HTTP ${r.status}`);
  return r.json();
}

// ---- pages ----

const ws = encodeURIComponent(WS);
let newestRuns = [];
let workspaceList = [];
async function buildPages(tok) {
  let pipeline = process.env.PIPELINE;
  let runId = process.env.RUN;
  if (!pipeline || !runId) {
    const s = await api(tok, `/w/${ws}/api/workspace/summary`);
    const pls = s.pipelines ?? [];
    const p = pipeline ? pls.find((x) => x.dir === pipeline || x.name === pipeline) : pls[0];
    if (!p) throw new Error(`workspace ${WS} has no pipeline${pipeline ? ` ${pipeline}` : ""}`);
    pipeline = p.dir ?? p.name;
    if (!runId) {
      const runs = await api(tok, `/w/${ws}/api/data/runs?pipeline=${encodeURIComponent(pipeline)}`).catch(() => null);
      const newest = Array.isArray(runs) ? runs[0]?.run_id : undefined;
      runId = newest ?? p.last_run?.run_id;
      if (!runId) throw new Error(`${WS}/${pipeline} has no run; set RUN=<id>`);
    }
  }
  // The dashboards to open (ADR-034): DASHBOARDS=a,b, else the
  // workspace's first one.
  const dl = await api(tok, `/w/${ws}/api/dashboards`).catch(() => null);
  const dashSlugs = env("DASHBOARDS", "") ? env("DASHBOARDS", "").split(",") : [(dl?.dashboards ?? [])[0]?.slug].filter(Boolean);
  // TABLE=<catalog>.<schema>.<table>: its table page must show sample rows.
  const table = env("TABLE", "");
  // The workspace's first notebook, for the read-only notebook view.
  const nl = await api(tok, `/w/${ws}/api/notebooks`).catch(() => null);
  const nbName = (Array.isArray(nl) ? nl : (nl?.notebooks ?? []))[0]?.name;
  const wl = await api(tok, "/api/workspaces").catch(() => null);
  workspaceList = Array.isArray(wl) ? wl : (wl?.workspaces ?? []);
  if (!workspaceList.length) throw new Error("GET /api/workspaces gave no workspaces");
  // The runs the "Where the time goes" chart draws, with their UTC starts.
  const timings = await api(tok, `/w/${ws}/api/data/run-timings?dir=${encodeURIComponent(pipeline)}&limit=20`).catch(() => null);
  newestRuns = timings?.runs ?? [];
  const dash = `/w/${ws}/pipelines/dashboard?dir=${encodeURIComponent(pipeline)}`;
  // Taps from the start page (never a typed URL): each page lists the
  // selectors a person taps, in order, from https://<site>/.
  const toWs = [`a[data-testid="workspace-card"][data-slug="${ws}"]`];
  // The pipeline card links by its dir (WorkspaceHome), which is what
  // `pipeline` holds; its aria-label carries the name, which can differ.
  const toPipeline = [...toWs, `a[href$="/pipelines/dashboard?dir=${encodeURIComponent(pipeline)}"]`];
  return [
    {
      name: "01-workspaces",
      path: "/",
      taps: [],
      testids: ["workspaces-page", "workspace-card", "phone-nav"],
      active: "workspaces",
      back: false,
      ready: () =>
        document.querySelectorAll('[data-testid="workspace-card"]').length > 0 &&
        document.querySelectorAll('[data-testid="workspace-card"][data-state="loading"]').length === 0,
      describe: async (page) => {
        const cards = await page.locator('[data-testid="workspace-card"]').evaluateAll((els) =>
          els.map((el) => ({
            slug: el.dataset.slug,
            state: el.dataset.state,
            status: el.querySelector('[data-testid="workspace-card-status"]')?.getAttribute("aria-label") ?? "",
            kind: el.dataset.kind,
            group: el.closest('[data-testid="workspaces-group"]')?.dataset.kind ?? "",
            where: (el.querySelector('[data-testid="workspace-card-where"]')?.textContent ?? "").trim(),
            line: (el.querySelector('[data-testid="workspace-card-reasons"]')?.textContent ?? "").trim().replace(/\s+/g, " "),
          })),
        );
        const layout = await page.evaluate(() => {
          const line = document.querySelector('[data-testid="workspaces-summary"]');
          const cards = [...document.querySelectorAll('[data-testid="workspace-card"]')];
          return {
            line: line ? { text: line.textContent.trim(), fail: +line.dataset.fail, warn: +line.dataset.warn, ok: +line.dataset.ok, top: line.getBoundingClientRect().top } : null,
            firstCardTop: cards[0]?.getBoundingClientRect().top ?? 0,
            cards: cards.map((el) => ({
              slug: el.dataset.slug,
              tag: el.tagName,
              href: el.getAttribute("href"),
              h: el.getBoundingClientRect().height,
            })),
            height: document.scrollingElement.scrollHeight,
            screen: innerHeight,
          };
        });
        const counts = await page.locator('[data-testid="workspace-card"]').evaluateAll((els) =>
          Object.fromEntries(
            els.map((el) => [el.dataset.slug, { d14: el.getAttribute("data-runs-14d"), d30: el.getAttribute("data-runs-30d") }]),
          ),
        );
        return { cards, counts, layout };
      },
      extra: async (page, info) => {
        const out = [];
        const L = info.layout;
        if (!L?.line) out.push("no summary line above the cards");
        else {
          const st = { Failing: 0, "Needs attention": 0, Healthy: 0 };
          for (const c of info.cards ?? []) if (c.status in st) st[c.status]++;
          if (L.line.fail !== st.Failing || L.line.warn !== st["Needs attention"] || L.line.ok !== st.Healthy)
            out.push(`summary line "${L.line.text}" does not match the cards (${JSON.stringify(st)})`);
          if (L.line.top > L.firstCardTop) out.push("summary line is not above the cards");
          console.log(`       summary line: ${L.line.text}`);
        }
        // Local and Cloud groups (2026-11-18): problems first inside each
        // group; every card under the heading of its kind, with a line that
        // says where it runs (folder or "runs on a Mac"; AWS account and region).
        const rankOf = { Failing: 0, "Needs attention": 1, Healthy: 2 };
        for (const kind of ["local", "cloud"]) {
          const g = (info.cards ?? []).filter((c) => c.group === kind);
          const ranks = g.map((c) => rankOf[c.status] ?? 3);
          if (ranks.some((r, i) => i > 0 && r < ranks[i - 1])) out.push(`${kind} cards not problems first: ${g.map((c) => c.status).join(", ")}`);
        }
        for (const c of info.cards ?? []) {
          if (!c.group || c.group !== c.kind) out.push(`${c.slug}: ${c.kind} card is under the "${c.group || "no"}" heading`);
          // Hosted: a local card has no where line (no path is sent, and the
          // heading says "runs on a Mac"); a cloud card names its account.
          if (c.kind === "local" ? c.where !== "" : !/^Cloud · AWS \d{12} · [a-z]+-[a-z]+-\d$/.test(c.where))
            out.push(`${c.slug}: where line "${c.where}" is wrong for a hosted ${c.kind} card`);
        }
        for (const c of L?.cards ?? []) {
          if (c.tag !== "A" || c.href !== `/w/${encodeURIComponent(c.slug)}/`)
            out.push(`${c.slug}: the card is not one link to /w/${c.slug}/ (${c.tag} ${c.href})`);
        }
        if (L && L.height > 2.1 * L.screen)
          out.push(`workspaces page is ${(L.height / L.screen).toFixed(2)} screens tall, want at most 2.1`);
        else if (L) console.log(`       page height ${(L.height / L.screen).toFixed(2)} screens`);
        const firstDay = (() => {
          const [y, m, d] = helDay(new Date()).split("-").map(Number);
          return new Date(Date.UTC(y, m - 1, d - 13)).toISOString().slice(0, 10);
        })();
        for (const ws of workspaceList) {
          const s = ws.summary;
          if (!s) continue;
          // A failure counts only while it is the pipeline's newest finished
          // run. A failure that a later run fixed is history: a grey note.
          const failing = (s.pipelines ?? []).filter((p) => {
            const done = (p.runs ?? []).filter((r) => r.result === "failed" || r.result === "succeeded")
              .sort((a, b) => String(b.started_at).localeCompare(String(a.started_at)));
            return done[0]?.result === "failed" && helDay(new Date(done[0].started_at)) >= firstDay;
          }).map((p) => p.name);
          const card = (info.cards ?? []).find((c) => c.slug === ws.slug);
          if (!card) continue;
          if (failing.length && card.status === "Healthy")
            out.push(`${ws.slug}: newest run of ${failing.join(", ")} failed but the card says Healthy`);
          if (failing.length && !/failed run/.test(card.line))
            out.push(`${ws.slug}: the card does not name the failed run of ${failing.join(", ")}`);
          if (card.status !== "Healthy" && !card.line)
            out.push(`${ws.slug}: "${card.status}" with no reason on the card`);
          if (/no run inside the expected interval/.test(card.line))
            out.push(`${ws.slug}: reason is not in plain words: ${card.line}`);
        }
        for (const [name, dir] of CLI_COMPARE) {
          if (!existsSync(dir)) {
            console.log(`       note: ${dir} is not on this Mac, ${name} not compared with the CLI`);
            continue;
          }
          const ui = info.counts?.[name];
          if (!ui && !workspaceList.some((w) => w.name === name)) {
            console.log(`       note: the check user is not a viewer of ${name} (ADR-035), not compared with the CLI`);
            continue;
          }
          if (!ui) {
            out.push(`no card for ${name} to compare with the CLI`);
            continue;
          }
          const cli = cliCounts(dir);
          console.log(`       ${name}: UI ${ui.d14} runs in 14 d, ${ui.d30} in 30 d; CLI ${cli.d14} and ${cli.d30}`);
          if (Number(ui.d14) !== cli.d14 || Number(ui.d30) !== cli.d30)
            out.push(`${name} run counts differ from the CLI: UI ${ui.d14}/${ui.d30}, CLI ${cli.d14}/${cli.d30}`);
        }
        return out;
      },
    },
    {
      name: "02-workspace",
      path: `/w/${ws}/`,
      taps: toWs,
      testids: ["workspace-home", "workspace-home-runs-chart", "pipeline-card", "workspace-home-all-pipelines", "phone-nav"],
      active: "workspaces",
      back: true,
    },
    {
      name: "03-pipeline",
      path: dash,
      taps: toPipeline,
      testids: ["deployment-status", "pipeline-phone-duration-bars", "run-timings-chart", "pipeline-phone-run-row", "phone-nav"],
      neverShow: ["Never run"],
      active: "workspaces",
      back: true,
      ready: () =>
        Number(document.querySelector('[data-testid="run-timings-chart"]')?.dataset.pointCount ?? 0) > 0,
      extra: async (page) => {
        const out = [];
        const el = page.locator('[data-testid="run-timings-chart"]');
        const id = await el.getAttribute("data-newest-run");
        const label = await el.getAttribute("data-newest-label");
        const run = newestRuns.find((r) => r.run_id === id);
        if (!run) out.push(`newest chart run ${id} is not in the run-timings answer`);
        else if (label !== helStamp(run.started_at))
          out.push(`chart time ${label} is not Helsinki time ${helStamp(run.started_at)} (started_at ${run.started_at})`);
        else console.log(`       newest run ${label} Helsinki (started_at ${run.started_at})`);
        return out;
      },
    },
    {
      name: "04-run",
      path: `${dash}&run=${encodeURIComponent(runId)}`,
      taps: [...toPipeline, `[data-testid="pipeline-phone-run-row"][data-run-id="${runId}"]`],
      testids: ["run-detail-sheet", "run-phone-timeline-chart", "run-phone-node-row", "phone-nav"],
      active: "workspaces",
      back: true,
      fit: '[data-testid="run-phone-view"]',
      extra: async (page) => {
        const t = (await page.locator('[data-testid="run-title"]').first().textContent())?.trim() ?? "";
        const run = newestRuns.find((r) => r.run_id === runId);
        const out = [];
        if (!/^Run \d{1,2}\.\d{1,2} \d\d:\d\d/.test(t)) out.push(`run title "${t}" is not "Run <day.month> <time>"`);
        if (t.includes(runId.slice(0, 8))) out.push(`run title shows the run ID: ${t}`);
        if (run && !t.startsWith(`Run ${helStamp(run.started_at)}`)) out.push(`run title "${t}" is not the run's Helsinki start ${helStamp(run.started_at)}`);
        if (!out.length) console.log(`       run title: ${t}`);
        return out;
      },
    },
    {
      // The Pipelines page, reached by "All pipelines" on the workspace page.
      name: "06-pipelines",
      path: `/w/${ws}/pipelines`,
      taps: [...toWs, '[data-testid="workspace-home-all-pipelines"]'],
      testids: ["pipeline-card", "phone-nav"],
      active: "workspaces",
      back: true,
    },
    ...(seesAll
      ? [
    {
      name: "05-stats",
      path: "/stats",
      taps: ['[data-testid="phone-nav-stats"]'],
      testids: ["stats-page", "phone-nav"],
      active: "stats",
      back: false,
      // The page shows data, the empty note or an error; an error is a failure.
      ready: () =>
        !!document.querySelector('[data-testid="stats-sessions"], [data-testid="stats-empty"], [data-testid="stats-error"]'),
      extra: async (page) => {
        const out = (await page.locator('[data-testid="stats-error"]').count()) ? ["stats page shows its error card"] : [];
        const ticks = await page
          .locator(".recharts-cartesian-axis-tick-value")
          // The x ticks are the dates; the y ticks are whole counts.
          .evaluateAll((els) => els.map((e) => e.textContent.trim()).filter((t) => t.includes(".")));
        const bad = ticks.filter((t) => {
          const m = /^(\d{1,2})\.(\d{1,2})$/.exec(t);
          return !m || Number(m[1]) > 31 || Number(m[2]) > 12;
        });
        if (!ticks.length) out.push("stats: no date ticks to check");
        if (bad.length) out.push(`stats: dates not day.month: ${bad.slice(0, 5).join(", ")}`);
        else console.log(`       stats dates day.month: ${ticks.slice(0, 3).join(", ")} …`);
        return out;
      },
    },
        ]
      : [
          {
            // Not in "all" (ADR-035): /stats sends the user to Workspaces.
            name: "05-stats",
            path: "/stats",
            // Typed on purpose: a user outside "all" has no Site usage
            // button; this checks that the typed address sends them home.
            typed: "the refused /stats address",
            testids: ["workspaces-page", "phone-nav"],
            active: "workspaces",
            back: false,
          },
        ]),
    ...dashSlugs.map((dashSlug, i) => ({
            name: i ? `09-dashboard-${dashSlug}` : "09-dashboard",
            path: `/w/${ws}/dashboards/${encodeURIComponent(dashSlug)}`,
            taps: [...toWs, '[data-testid="workspace-home-dashboards"]', `a[href$="/dashboards/${encodeURIComponent(dashSlug)}"]`],
            testids: ["dashboard-widget", "phone-nav"],
            active: "workspaces",
            back: true,
            // Every widget has run its query: no skeleton left.
            ready: () =>
              document.querySelectorAll('[data-testid="dashboard-widget"]').length > 0 &&
              !document.querySelector('[data-testid="dashboard-widget"] .animate-pulse'),
            extra: async (page) => {
              const widgets = await page.locator('[data-testid="dashboard-widget"]').count();
              const failed = await page.locator('[data-testid="dashboard-widget"]', { hasText: "Query failed" }).count();
              // "No rows" can be true (no errors this month): reported, not failed.
              const empty = await page.locator('[data-testid="dashboard-widget"]', { hasText: "No rows" }).count();
              console.log(`       dashboard ${dashSlug}: ${widgets} widget(s), ${failed} failed, ${empty} with no rows`);
              return [
                ...(failed ? [`dashboard ${dashSlug}: ${failed} of ${widgets} widget(s) show "Query failed"`] : []),
              ];
            },
          })),
    ...(table
      ? [
          {
            name: "12-table",
            path: `/w/${ws}/tables/${table.split(".").map(encodeURIComponent).join("/")}`,
            taps: [...toWs, '[data-testid="workspace-home-tables"]', `[data-testid="catalog-table-row"][data-table-catalog="${table.split(".")[0]}"][data-table-schema="${table.split(".")[1]}"][data-table-name="${table.split(".")[2]}"]`],
            testids: ["table-schema", "phone-nav"],
            active: "workspaces",
            back: true,
            ready: () => /examples from first [1-9][0-9]* rows/.test(document.body.innerText),
            extra: async (page) => {
              const m = (await page.locator("body").innerText()).match(/examples from first (\d+) rows/);
              console.log(`       table ${table}: ${m ? m[1] : 0} sample row(s)`);
              return m && Number(m[1]) > 0 ? [] : [`table ${table}: no sample rows`];
            },
          },
        ]
      : []),
    ...(nbName
      ? [
          {
            name: "10-notebook",
            path: `/w/${ws}/notebooks/${encodeURIComponent(nbName)}`,
            taps: [...toWs, '[data-testid="workspace-home-notebooks"]', `a[href$="/notebooks/${encodeURIComponent(nbName)}"]`],
            testids: ["notebook-view", "phone-nav"],
            active: "workspaces",
            back: true,
            extra: async (page) => {
              const cells = await page.locator('[data-testid="notebook-view-cell"]').count();
              const runs = await page.getByRole("button", { name: /Run/ }).count();
              console.log(`       notebook ${nbName}: ${cells} cell(s)`);
              return [...(cells ? [] : ["notebook view shows no cell"]), ...(runs ? ["notebook view has a Run button"] : [])];
            },
          },
        ]
      : []),
    {
      name: "07-not-found",
      path: `/w/${ws}/pipelines/dashboard?dir=no-such-pipeline`,
      // Typed on purpose: no link leads to a pipeline that does not exist.
      typed: "a bad address",
      testids: ["pipeline-not-found", "phone-nav"],
      active: "workspaces",
      back: true,
      // The graph read of a pipeline that does not exist answers 404.
      allowHttp: [404],
      extra: async (page) => {
        const text = await page.evaluate(() => document.body.innerText);
        return ["hclparser", "/tmp/", "API GET", "read dir"]
          .filter((t) => text.includes(t))
          .map((t) => `not-found page shows server text "${t}"`);
      },
    },
  ];
}

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

/** The bottom bar and the back button as the page shows them. */
async function readNav(page) {
  return page.evaluate(() => {
    const navs = [...document.querySelectorAll('[data-testid="phone-nav"]')];
    const nav = navs[0];
    const items = nav
      ? [...nav.querySelectorAll(":scope > a")].map((a) => {
          const r = a.getBoundingClientRect();
          const marker = a.querySelector("span[aria-hidden]");
          const hit = document.elementFromPoint(r.x + r.width / 2, r.y + r.height / 2);
          return {
            testid: a.dataset.testid,
            label: a.textContent.trim(),
            href: a.getAttribute("href"),
            box: [r.x, r.y, r.width, r.height].map(Math.round).join(","),
            active: a.getAttribute("aria-current") === "page",
            marker: marker ? getComputedStyle(marker).backgroundColor : "",
            onTop: !!hit && a.contains(hit),
          };
        })
      : [];
    const back = [...document.querySelectorAll('[data-testid="phone-back"]')].some((el) => {
      const r = el.getBoundingClientRect();
      return r.width > 0 && r.height > 0 && r.top < innerHeight / 3;
    });
    return { count: navs.length, items, back };
  });
}

/** Problems with one page's bar, against the first page's bar. */
function navProblems(pg, nav, ref) {
  const out = [];
  if (nav.count !== 1) out.push(`bottom bar: ${nav.count} bars on the page, want 1`);
  if (!nav.items.length) return [...out, "bottom bar: no items"];
  const labels = nav.items.map((i) => i.label);
  const wantLabels = seesAll ? "Workspaces|Site usage" : "Workspaces";
  if (labels.join("|") !== wantLabels) out.push(`bottom bar labels ${labels.join(" | ")}, want ${wantLabels.replace("|", " | ")}`);
  const actives = nav.items.filter((i) => i.active).map((i) => i.testid);
  if (actives.length !== 1 || actives[0] !== `phone-nav-${pg.active}`)
    out.push(`bottom bar: active ${actives.join("+") || "none"}, want phone-nav-${pg.active}`);
  for (const i of nav.items) {
    const shown = i.marker && i.marker !== "rgba(0, 0, 0, 0)" && i.marker !== "transparent";
    if (i.active && !shown) out.push(`bottom bar: active ${i.testid} has no visible marker`);
    if (!i.active && shown) out.push(`bottom bar: inactive ${i.testid} shows the marker`);
    if (!i.onTop) out.push(`bottom bar: ${i.testid} is covered at its centre`);
  }
  if (ref) {
    const shape = (n) => n.items.map((i) => `${i.testid}|${i.label}|${i.href}|${i.box}`).join(" ; ");
    if (shape(nav) !== shape(ref)) out.push(`bottom bar differs from 01-workspaces: [${shape(nav)}] vs [${shape(ref)}]`);
  }
  if (nav.back !== pg.back) out.push(pg.back ? "no back button at the top" : "a back button on a top level page");
  return out;
}

const isApi = (u) => /^\/(w\/[^/]+\/)?api\//.test(new URL(u).pathname);

/** One attempt at one page. Returns {problems, beacons, throttled, info}. */
async function visit(ctx, pg) {
  const page = await ctx.newPage();
  const problems = [];
  let throttled = false;
  let beacons = 0;
  page.on("console", (m) => {
    if (m.type() !== "error") return;
    const url = m.location()?.url ?? "";
    problems.push(`console error: ${m.text()} ${url}`.trim());
  });
  page.on("pageerror", (e) => problems.push(`uncaught exception: ${e.message}`));
  page.on("response", (r) => {
    const u = r.url();
    if (new URL(u).pathname === "/t.gif") beacons++;
    if (!isApi(u) || r.status() < 400) return;
    if (pg.allowHttp?.includes(r.status())) return;
    if (r.status() === 503) throttled = true;
    problems.push(`HTTP ${r.status()} ${new URL(u).pathname}`);
  });
  page.on("requestfailed", (r) => {
    const err = r.failure()?.errorText ?? "";
    if (isApi(r.url()) && !/ERR_ABORTED/.test(err)) problems.push(`request failed (${err}) ${new URL(r.url()).pathname}`);
  });

  let info = {};
  try {
    if (pg.typed) {
      await page.goto(BASE + pg.path, { waitUntil: "domcontentloaded", timeout: WAIT_MS });
    } else {
      // A person's way in: the start page, then taps. A page with no tap
      // path fails: everything a person needs must be reachable.
      if (!pg.taps) throw new Error("no tap path from the start page");
      await page.goto(BASE + "/", { waitUntil: "domcontentloaded", timeout: WAIT_MS });
      for (const sel of pg.taps) {
        const el = page.locator(sel).first();
        await el.waitFor({ state: "visible", timeout: WAIT_MS }).catch(() => {
          throw new Error(`cannot tap ${sel} on ${new URL(page.url()).pathname}`);
        });
        await el.click();
        await page.waitForLoadState("domcontentloaded");
      }
      const at = new URL(page.url());
      const want = new URL(BASE + pg.path);
      const norm = (u) => decodeURIComponent(u.pathname.replace(/\/$/, "") + u.search);
      if (norm(at) !== norm(want)) throw new Error(`taps led to ${norm(at)}, want ${norm(want)}`);
    }
    for (const id of pg.testids) {
      await page.locator(`[data-testid="${id}"]`).first().waitFor({ state: "visible", timeout: WAIT_MS });
    }
    if (pg.ready) await page.waitForFunction(pg.ready, null, { timeout: WAIT_MS });
    await sleep(1500);
    if (pg.describe) info = await pg.describe(page);
    if (pg.extra) problems.push(...(await pg.extra(page, info)));
    {
      const text = await page.evaluate(() => document.body.innerText);
      for (const [label, re] of [
        ["/billion", /\/billion/],
        ["per billion", /per billion/i],
        ["rate(", /\brate\(/],
        ["cron(", /\bcron\(/],
        ["no schedule", /\bno schedule\b/i],
        ["Module:", /Module:/],
        ["1 runs", /(^|[^\d.])1 runs\b/],
      ])
        if (re.test(text)) problems.push(`shows the label "${label}"`);
    }
    {
      const small = await page.evaluate(() => {
        const chart = '[role="img"], [data-testid*="chart"], [data-testid*="strip"], [data-testid*="sparkline"], [data-testid*="bars"], .recharts-wrapper';
        return [...document.querySelectorAll("a[href], button")]
          .filter((el) => {
            const r = el.getBoundingClientRect();
            if (r.width === 0 || r.height === 0) return false;
            if (getComputedStyle(el).visibility === "hidden") return false;
            if (el.closest(chart)) return false;
            if (el.closest("p, li") && el.closest("p, li").textContent.trim() !== el.textContent.trim()) return false;
            return r.width < 44 || r.height < 44;
          })
          .map((el) => {
            const r = el.getBoundingClientRect();
            const name = el.dataset.testid || el.getAttribute("aria-label") || el.textContent.trim().slice(0, 24) || el.tagName;
            return `${name} ${Math.round(r.width)}x${Math.round(r.height)}`;
          });
      });
      if (small.length) problems.push(`tap targets under 44 px: ${[...new Set(small)].join("; ")}`);
    }
    if (pg.neverShow) {
      const seen = await page.evaluate(() => [...(window.__clavesaSeen ?? [])]);
      for (const t of pg.neverShow) if (seen.includes(t)) problems.push(`showed "${t}" while loading`);
    }
    info.nav = await readNav(page);
  } catch (e) {
    problems.push(`page did not finish: ${e.message.split("\n")[0]}`);
  }

  const scroll = await page
    .evaluate(() => {
      const wide = [...document.querySelectorAll("main, [data-testid='run-phone-view']")]
        .filter((el) => el.scrollWidth > el.clientWidth + 1)
        .map((el) => `${el.tagName.toLowerCase()}${el.dataset.testid ? `[${el.dataset.testid}]` : ""} ${el.scrollWidth}>${el.clientWidth}`);
      // The innermost elements that reach past the screen's right edge,
      // so a failure names what to fix.
      const past = [...document.querySelectorAll("main *")].filter(
        (el) => el.getBoundingClientRect().right > innerWidth + 1 && ![...el.children].some((c) => c.getBoundingClientRect().right > innerWidth + 1),
      );
      if (wide.length && past.length)
        wide.push(
          `past the edge (${past.length}, widest first): ` +
            past
              .sort((a, b) => b.getBoundingClientRect().right - a.getBoundingClientRect().right)
              .slice(0, 4)
              .map((el) => `${el.tagName.toLowerCase()}.${(el.getAttribute("class") ?? "").split(" ").slice(0, 3).join(".")} "${(el.textContent ?? "").trim().slice(0, 24)}" right=${Math.round(el.getBoundingClientRect().right)}`)
              .join("; "),
        );
      return { doc: document.scrollingElement.scrollWidth, inner: innerWidth, wide };
    })
    .catch(() => ({ doc: 0, inner: 0, wide: [] }));
  if (scroll.doc > scroll.inner || scroll.wide.length) {
    problems.push(`horizontal scroll (document ${scroll.doc} > ${scroll.inner}; ${scroll.wide.join(", ")})`);
  }
  return { page, problems, beacons, throttled, info, scroll };
}

// ---- main ----

const failures = [];
const user = readCheckUser();
const tf = terraformOutputs();
if (tf.timeZone) HEL = tf.timeZone;
console.log(`hosted time zone: ${HEL}`);
const tok = signIn(user, tf);
// The check user's groups (ADR-035): who sees Site usage.
const tokGroups = (() => {
  try {
    const part = tok.idToken.split(".")[1];
    return JSON.parse(Buffer.from(part, "base64url").toString("utf8"))["cognito:groups"] ?? [];
  } catch {
    return [];
  }
})();
const seesAll = tokGroups.includes("all");
user.password = "";
console.log(`signed in as the check user on ${BASE} (tokens not printed)`);

const pages = await buildPages(tok);
const browser = await chromium.launch();
rmSync(OUT, { recursive: true, force: true });
mkdirSync(OUT, { recursive: true });
try {
  const ctx = await browser.newContext({
    ...iphone,
    userAgent: `${iphone.userAgent} HeadlessChrome`,
    colorScheme: "dark",
    // Charts draw at once, so no shot catches one mid-animation.
    reducedMotion: "reduce",
  });
  // Texts a page must never show, even for a frame while it loads.
  await ctx.addInitScript(() => {
    const watch = ["Never run"];
    window.__clavesaSeen = new Set();
    const look = () => {
      const t = document.body?.textContent ?? "";
      for (const w of watch) if (t.includes(w)) window.__clavesaSeen.add(w);
    };
    new MutationObserver(look).observe(document, { childList: true, subtree: true, characterData: true });
  });
  // Exactly what auth.ts stores, before the app loads.
  await ctx.addInitScript((t) => {
    try {
      sessionStorage.setItem(
        "clavesa.hosted.session",
        JSON.stringify({ idToken: t.idToken, accessToken: t.accessToken, expiresAt: t.expiresAt }),
      );
      if (t.refreshToken) localStorage.setItem("clavesa.hosted.refresh", t.refreshToken);
    } catch {}
  }, tok);

  let refNav = null;
  for (const pg of pages) {
    let r = await visit(ctx, pg);
    let retried = false;
    if (r.problems.length && r.throttled) {
      retried = true;
      console.log(`retry ${pg.name}: HTTP 503 (Lambda throttling) on the first load, loading it once more`);
      await r.page.close();
      await sleep(3000);
      r = await visit(ctx, pg);
    }
    await fitHeight(r.page, pg.fit);
    const file = `${OUT}${pg.name}.png`;
    await r.page.screenshot({ path: file });
    await r.page.close();

    if (r.info.nav) {
      r.problems.push(...navProblems(pg, r.info.nav, refNav));
      refNav ??= r.info.nav;
    } else r.problems.push("bottom bar: not read");
    const uniq = [...new Set(r.problems)];
    console.log(
      `${uniq.length ? "FAIL" : "ok  "} ${pg.name}: ${uniq.length} problem(s), scrollWidth ${r.scroll.doc}/${r.scroll.inner}, ` +
        `${r.beacons} beacon(s)${retried ? ", after a retry" : ""} -> ${file}`,
    );
    if (r.info.nav)
      console.log(
        `       bar ${r.info.nav.items.map((i) => `${i.label}${i.active ? "*" : ""}`).join(" | ")}` +
          `${r.info.nav.back ? ", back button at the top" : ""}`,
      );
    for (const c of r.info.cards ?? []) console.log(`       card ${c.slug} [${c.state}] ${c.status}${c.line ? `: ${c.line}` : ""}`);
    if (pg.name === "01-workspaces" && !(r.info.cards ?? []).length) uniq.push("no workspace card");
    for (const p of uniq) failures.push(`${pg.name}: ${p}`);
  }
  await ctx.close();

  // Desktop: read-only shows no page whose point is to run or change.
  const desk = await browser.newContext({ viewport: { width: 1440, height: 900 }, colorScheme: "dark", reducedMotion: "reduce" });
  await desk.addInitScript((t) => {
    try {
      sessionStorage.setItem(
        "clavesa.hosted.session",
        JSON.stringify({ idToken: t.idToken, accessToken: t.accessToken, expiresAt: t.expiresAt }),
      );
      if (t.refreshToken) localStorage.setItem("clavesa.hosted.refresh", t.refreshToken);
    } catch {}
  }, tok);
  // ADR-033: the hosted navigation is the local one. Every sidebar entry
  // opens, on a cloud and on a local-warehouse workspace, with no console
  // error and no failed API call; Query, Credentials and Runner open a
  // page that says what to do instead, never a Run button.
  for (const w of [WS, LOCAL_WS]) {
    const wsq = encodeURIComponent(w);
    const page = await desk.newPage();
    const out = [];
    let current = "";
    page.on("console", (m) => {
      if (m.type() === "error") out.push(`${current}: console error: ${m.text()}`);
    });
    page.on("pageerror", (e) => out.push(`${current}: uncaught exception: ${e.message}`));
    page.on("response", (r) => {
      if (isApi(r.url()) && r.status() >= 400) out.push(`${current}: HTTP ${r.status()} ${new URL(r.url()).pathname}`);
    });
    page.on("requestfailed", (r) => {
      const err = r.failure()?.errorText ?? "";
      if (isApi(r.url()) && !/ERR_ABORTED/.test(err)) out.push(`${current}: request failed (${err}) ${new URL(r.url()).pathname}`);
    });
    current = "home";
    await page.goto(`${BASE}/w/${wsq}/`, { waitUntil: "domcontentloaded", timeout: WAIT_MS });
    await page.locator('[data-testid="workspace-home"]').first().waitFor({ state: "visible", timeout: WAIT_MS });
    const entries = (await page.locator("aside a[href]").evaluateAll((els) => els.map((a) => a.getAttribute("href"))))
      .filter((h) => h.startsWith(`/w/${wsq}`));
    for (const want of ["query", "credentials", "runner", "dashboards", "notebooks", "sources", "bundles", "unity-catalog", "pipelines"])
      if (!entries.some((h) => h.replace(/\/$/, "").endsWith(`/${want}`))) out.push(`sidebar has no ${want}`);
    for (const href of [...new Set(entries)]) {
      current = href.replace(`/w/${wsq}`, "") || "/";
      // Click the sidebar entry, as a person does: client-side navigation,
      // so the shared queries stay cached. A full load per entry fires a
      // dozen startup calls each and trips the stage's 10 rps throttle
      // (429), which a person tapping through never does.
      await page.locator(`aside a[href="${href}"]`).first().click();
      await page.waitForLoadState("networkidle", { timeout: WAIT_MS }).catch(() => {});
      await sleep(1500);
      const path = new URL(page.url()).pathname.replace(/\/$/, "");
      if (path !== href.replace(/\/$/, "")) out.push(`${current}: moved to ${path}`);
      if (/\/(query|credentials)$/.test(current)) {
        if (!(await page.locator('[data-testid="not-hosted-line"]').count())) out.push(`${current}: no not-hosted line`);
        if (await page.getByRole("button", { name: /^Run\b/ }).count()) out.push(`${current}: shows a Run button`);
      }
    }
    await page.screenshot({ path: `${OUT}08-desktop-${w}.png` });
    console.log(`${out.length ? "FAIL" : "ok  "} 08-desktop ${w}: ${entries.length} sidebar entries opened${out.length ? `, ${out.length} problem(s)` : ""}`);
    for (const p of [...new Set(out)]) failures.push(`08-desktop ${w}: ${p}`);
    await page.close();
  }
  // A personal_data local workspace: no dashboards listed, a link to one
  // shows the notice and no failing widget.
  {
    const lw = encodeURIComponent(LOCAL_WS);
    const slug = env("LOCAL_DASHBOARD", "pipeline-runs-traffic");
    const probs = [];
    const list = await api(tok, `/w/${lw}/api/dashboards`);
    if ((list.dashboards ?? []).length) probs.push(`GET /dashboards lists ${list.dashboards.length} dashboard(s), want none`);
    const page = await desk.newPage();
    await page.setViewportSize({ width: 393, height: 852 });
    await page.goto(`${BASE}/w/${lw}/dashboards/${encodeURIComponent(slug)}`, { waitUntil: "domcontentloaded", timeout: WAIT_MS });
    const notice = page.locator('[data-testid="dashboard-notice"], [data-testid="not-hosted-line"]').first();
    await notice.waitFor({ state: "visible", timeout: WAIT_MS }).catch(() => probs.push("no notice on the dashboard page"));
    const text = ((await notice.textContent().catch(() => "")) ?? "").trim();
    if (!/holds visitor data and opens only in the local clavesa UI|data of this workspace is on the Mac/.test(text)) probs.push(`notice reads "${text}"`);
    if (await page.locator('[data-testid="dashboard-widget"]').count()) probs.push("widgets are drawn");
    if (await page.getByText("Query failed").count()) probs.push('"Query failed" shown');
    await page.screenshot({ path: `${OUT}12-personal-dashboard.png` });
    console.log(`${probs.length ? "FAIL" : "ok  "} 12-personal: ${LOCAL_WS} lists no dashboards, /dashboards/${slug} shows "${text.slice(0, 60)}…"`);
    for (const p of probs) failures.push(`12-personal ${LOCAL_WS}: ${p}`);
    await page.close();
  }
  // ADR-035: the check user is a viewer of the workspaces this check
  // walks only; another one answers 404 and is not listed.
  {
    const other = env("NOT_VIEWER_WS", "ucpub");
    const r = await fetch(`${BASE}/w/${encodeURIComponent(other)}/api/pipelines`, { headers: { Authorization: `Bearer ${tok.idToken}` } });
    const listed = workspaceList.some((w) => w.name === other);
    console.log(`${r.status === 404 && !listed ? "ok  " : "FAIL"} 11-viewers: /w/${other} answers ${r.status}, listed: ${listed}`);
    if (r.status !== 404) failures.push(`11-viewers: /w/${other} answers ${r.status}, want 404`);
    if (listed) failures.push(`11-viewers: ${other} is listed for the check user`);
  }
  await desk.close();
} finally {
  await browser.close();
}

if (failures.length) {
  console.error(`\n${failures.length} failure(s):`);
  for (const f of failures) console.error(`  ${f}`);
  process.exit(1);
}
console.log(`\nAll pages pass. Shots in ${OUT}`);
