// day-windows-check.mjs — make ui-day-windows-check (part of make test).
//
// The UI half of the 00:10 test. It counts the runs of
// internal/service/testdata/day_windows_0010.json with the UI's own
// rebaseSummary (ui/src/lib/summary.ts), with the zone given the way the
// server gives it (window.__CLAVESA__.timeZone), and checks the counts
// against the fixture's "want". The CLI half, TestDayWindowsAt0010 in
// internal/service, checks the Go summary against the same "want", so the
// card and `clavesa workspace summary` agree at 00:10 Helsinki time.
import { build } from "esbuild";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const UI = join(dirname(fileURLToPath(import.meta.url)), "..");
const fx = JSON.parse(
  readFileSync(join(UI, "../internal/service/testdata/day_windows_0010.json"), "utf8"),
);

// The runtime config is read when the modules load, as in the browser.
globalThis.window = { __CLAVESA__: { mode: "local", timeZone: fx.time_zone }, location: { pathname: "/" } };

const tmp = mkdtempSync(join(tmpdir(), "day-windows-"));
const out = join(tmp, "summary.mjs");
try {
  await build({
    entryPoints: [join(UI, "src/lib/summary.ts")],
    bundle: true,
    platform: "node",
    format: "esm",
    outfile: out,
    alias: { "@": join(UI, "src") },
    logLevel: "error",
  });
  const { rebaseSummary } = await import(pathToFileURL(out).href);

  const summary = {
    pipelines: [{ name: "p", runs: fx.runs.map((r) => ({ ...r, cost_usd: 0, records: 0 })) }],
    runs_per_day_14d: [],
    runs_per_day_30d: [],
    cost_per_day_30d: [],
    cost_usd_30d: 0,
    records_30d: 0,
  };
  const s = rebaseSummary(summary, Date.parse(fx.now));
  const sum = (days) => days.reduce((n, d) => n + d.succeeded + d.failed, 0);
  const d14 = s.runs_per_day_14d;
  const d30 = s.runs_per_day_30d;
  const got = {
    d14: sum(d14),
    d30: sum(d30),
    first_day_14: d14[0].date,
    first_day_30: d30[0].date,
    last_day: d14.at(-1).date,
    last_day_failed: d14.at(-1).failed,
  };
  const bad = Object.keys(fx.want).filter((k) => got[k] !== fx.want[k]);
  console.log(`UI at ${fx.now} in ${fx.time_zone}: ${JSON.stringify(got)}`);
  if (bad.length) {
    for (const k of bad) console.error(`  ${k}: UI ${got[k]}, want ${fx.want[k]} (the CLI's answer)`);
    process.exit(1);
  }
  console.log("ok: the UI counts the same days as the CLI");
} finally {
  rmSync(tmp, { recursive: true, force: true });
}
