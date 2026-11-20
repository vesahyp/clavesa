// make ui-new-pipeline-check: the editing UI creates a pipeline and opens
// its page at once, and that page is the editor, never "No pipeline with
// this name".
//
// v2.27.2's not-found gate first applied in every mode. The pipeline list
// was cached from the Pipelines page, so the page of a pipeline created a
// moment earlier said "No pipeline with this name" (caught by
// verify-readme). The gate now applies only read-only. This check keeps
// it that way, in seconds, without the full README walk:
//
//   1. `clavesa workspace init` in a temp dir, `clavesa ui` on a free port.
//   2. Open /pipelines first, so the pipeline list is in the cache.
//   3. New pipeline → name → Create, the way the README says.
//   4. The page is /pipelines/dashboard?dir=<name>, it shows "Add node"
//      within 20 s, and it never shows the not-found page.
//   5. Reload the same URL: still the editor.
//
// CLAVESA_BIN overrides bin/clavesa. KEEP=1 keeps the temp workspace.
import { chromium } from "playwright";
import { spawn, spawnSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";

const REPO = new URL("../../", import.meta.url).pathname;
const BIN = process.env.CLAVESA_BIN || `${REPO}bin/clavesa`;
const PIPELINE = "fresh";

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

function fail(msg) {
  console.error(`✗ ${msg}`);
  process.exitCode = 1;
}

const tmp = mkdtempSync(join(tmpdir(), "clavesa-new-pipeline-"));
const ws = join(tmp, "ws");
let ui = null;
let browser = null;
try {
  // `workspace init` records the current workspace under XDG_CONFIG_HOME;
  // keep it in the temp dir, never the developer's ~/.config/clavesa/.
  const childEnv = { ...process.env, XDG_CONFIG_HOME: `${tmp}/xdg-config` };
  const init = spawnSync(BIN, ["workspace", "init", "newpipe-ws"], { cwd: tmp, encoding: "utf8", env: childEnv });
  if (init.status !== 0) throw new Error(`workspace init failed:\n${init.stdout}${init.stderr}`);
  // init creates <cwd>/<something>; find the workspace dir from its output.
  const m = /Initialized workspace "[^"]+" at (\S+)/.exec(init.stdout);
  const wsDir = m ? m[1] : ws;

  const port = await freePort();
  const base = `http://127.0.0.1:${port}`;
  let log = "";
  ui = spawn(BIN, ["ui", "--no-browser", "--workspace", wsDir], {
    cwd: wsDir,
    env: { ...childEnv, CLAVESA_ADDR: `127.0.0.1:${port}` },
    stdio: ["ignore", "pipe", "pipe"],
  });
  ui.stdout.on("data", (d) => (log += d));
  ui.stderr.on("data", (d) => (log += d));
  let up = false;
  for (let i = 0; i < 120 && !up; i++) {
    try {
      up = (await fetch(`${base}/api/pipelines`)).ok;
    } catch {}
    if (!up) await sleep(500);
  }
  if (!up) throw new Error(`clavesa ui did not start on :${port}\n${log.slice(-4000)}`);

  browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  let sawNotFound = false;
  await page.exposeFunction("__sawNotFound", () => (sawNotFound = true));
  await page.addInitScript(() => {
    new MutationObserver(() => {
      if (document.querySelector('[data-testid="pipeline-not-found"]')) window.__sawNotFound();
    }).observe(document, { childList: true, subtree: true });
  });

  // 2. The list is cached before the pipeline exists.
  await page.goto(`${base}/pipelines`);
  await page.getByText("New pipeline").first().waitFor({ timeout: 15000 });
  await page.waitForLoadState("networkidle");

  // 3. Create it from the dialog.
  await page.locator('button:has-text("New pipeline")').first().click();
  await page.locator("[data-testid=new-pipeline-name]").fill(PIPELINE);
  await page.locator('button:text-is("Create")').click();

  // 4. The editor opens, the not-found page never shows.
  await page.waitForURL(`**/pipelines/dashboard?dir=${PIPELINE}`, { timeout: 15000 });
  try {
    await page.getByText("Add node").first().waitFor({ timeout: 20000 });
  } catch {
    fail(`the editor did not open on /pipelines/dashboard?dir=${PIPELINE} after Create`);
  }
  if (sawNotFound) fail('the new pipeline\'s page showed "No pipeline with this name"');

  // 5. A reload of the same URL is still the editor.
  sawNotFound = false;
  await page.reload();
  try {
    await page.getByText("Add node").first().waitFor({ timeout: 20000 });
  } catch {
    fail("the editor did not open after a reload");
  }
  if (sawNotFound) fail('the reloaded page showed "No pipeline with this name"');

  if (!process.exitCode) console.log(`✓ a new pipeline's page opens the editor at once (create, then reload)`);
} catch (e) {
  fail(e.message);
} finally {
  await browser?.close();
  ui?.kill();
  if (process.env.KEEP) console.log(`workspace kept: ${tmp}`);
  else rmSync(tmp, { recursive: true, force: true });
}
