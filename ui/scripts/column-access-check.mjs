// column-access-check: what one hosted user sees of a table with column
// tags (docs/ideas/column-tags.md). Signs in as the check user
// (/clavesa-ui/check-user in SSM), reads GET /workspace/tables and opens
// the table page on an iPhone 16, and checks the hidden column:
//   EXPECT=hidden  a viewer (group ws-<ws>): the column is in neither
//   EXPECT=shown   an admin (group all): the column is in both
// The check user's groups decide which one it is; give it the groups of
// the user it stands for before the run (make hosted-viewer).
//
// Env: WS (default smoke2), TABLE <catalog>.<schema>.<table> (default
// clavesa_smoke2.taxi.trips), COLUMN (default trip_distance), EXPECT,
// BASE_URL (default https://ui.clavesa.dev), PROFILE (default personal).
// Shot: .shots/columns/<EXPECT>.png. Exit 1 on a failed check.
import { chromium, devices } from "playwright";
import { mkdirSync } from "node:fs";
import { hostedSignIn, plantSession } from "./lib/hosted-session.mjs";

const env = (k, d) => process.env[k] || d;
const REPO = new URL("../../", import.meta.url).pathname;
const BASE = env("BASE_URL", "https://ui.clavesa.dev").replace(/\/$/, "");
const WS = env("WS", "smoke2");
const [cat, schema, table] = env("TABLE", "clavesa_smoke2.taxi.trips").split(".");
const COLUMN = env("COLUMN", "trip_distance");
const EXPECT = env("EXPECT", "");
if (!["hidden", "shown"].includes(EXPECT)) throw new Error("EXPECT must be hidden or shown");
const OUT = `${REPO}.shots/columns/`;
mkdirSync(OUT, { recursive: true });

const tok = hostedSignIn({ repo: REPO, profile: env("PROFILE", "personal") });
const groups = JSON.parse(Buffer.from(tok.idToken.split(".")[1], "base64url").toString())["cognito:groups"] ?? [];
console.log(`signed in as the check user, groups: ${groups.join(" ")}`);

const problems = [];
const r = await fetch(`${BASE}/w/${WS}/api/workspace/tables`, { headers: { Authorization: `Bearer ${tok.idToken}` } });
const body = await r.json();
const t = (body.tables ?? []).find((x) => x.name === table && x.schema === schema);
if (!t) problems.push(`GET /workspace/tables (${r.status}) has no ${schema}.${table}`);
const apiCols = (t?.columns ?? []).map((c) => c.name);
const inApi = apiCols.includes(COLUMN);
console.log(`API columns of ${table}: ${apiCols.join(", ")}`);

const browser = await chromium.launch();
try {
  const ctx = await browser.newContext({ ...devices["iPhone 16"] });
  await plantSession(ctx, tok);
  const page = await ctx.newPage();
  // The sample the page loads (GET /data/table): an admin passes only when
  // its rows carry a value of COLUMN, so a page that never loads its rows
  // cannot pass.
  const sampleResp = page.waitForResponse((r) => r.url().includes("/data/table?") && r.request().method() === "GET", { timeout: 90000 });
  await page.goto(`${BASE}/w/${WS}/tables/${cat}/${schema}/${table}`, { waitUntil: "domcontentloaded" });
  await page.locator('[data-testid="table-schema"]').waitFor({ timeout: 90000 });
  let sample = null;
  try {
    const resp = await sampleResp;
    sample = resp.ok() ? await resp.json() : null;
    if (!sample) problems.push(`GET /data/table answered ${resp.status()}`);
  } catch (e) {
    problems.push(`the sample rows did not load in 90 s (${e.message.split("\n")[0]})`);
  }
  try {
    await page.waitForFunction(() => /examples from first [1-9]/.test(document.body.innerText), null, { timeout: 90000 });
  } catch {
    problems.push("the page shows no sample rows (\"examples from first N rows\") after 90 s");
  }
  if (EXPECT === "shown" && sample) {
    const idx = (sample.columns ?? []).findIndex((c) => c.name === COLUMN);
    const values = idx < 0 ? [] : (sample.rows ?? []).map((row) => row[idx]).filter((v) => v !== null && v !== undefined && v !== "");
    if (idx < 0) problems.push(`the sample rows have no ${COLUMN} column`);
    else if (values.length === 0) problems.push(`the sample rows have no ${COLUMN} value (${(sample.rows ?? []).length} rows)`);
    else console.log(`sample rows: ${values.length} of ${sample.rows.length} carry ${COLUMN}, first ${values[0]}`);
  }
  const shown = (await page.locator('[data-testid="table-schema"]').innerText()) + "\n" +
    (await page.locator('[data-testid="table-sample-rows"]').innerText().catch(() => ""));
  const onPage = new RegExp(`\\b${COLUMN}\\b`).test(shown);
  await page.screenshot({ path: `${OUT}${EXPECT}.png`, fullPage: true });
  const want = EXPECT === "shown";
  if (inApi !== want) problems.push(`${COLUMN} ${inApi ? "is" : "is not"} in GET /workspace/tables, want ${EXPECT}`);
  if (onPage !== want) problems.push(`${COLUMN} ${onPage ? "shows" : "does not show"} on the table page, want ${EXPECT}`);
  console.log(`${problems.length ? "FAIL" : "ok  "} ${COLUMN} on ${WS} ${table}: API ${inApi ? "shown" : "hidden"}, page ${onPage ? "shown" : "hidden"} (want ${EXPECT}) -> ${OUT}${EXPECT}.png`);
} finally {
  await browser.close();
}
for (const p of problems) console.log(`  ${p}`);
process.exit(problems.length ? 1 : 0);
