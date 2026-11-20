// time-range-check.mjs — make ui-time-range-check (part of make test).
//
// The UI half of the dashboard period rules (Vesa, 2026-11-19: "Last
// 5min? Smallest should be 1h or something. Lets have sensible dashboard
// defaults"). Checks ui/src/lib/timeRange.ts against
// internal/service/testdata/time_range_defaults.json: the picker's
// presets, what a retired or legacy period means now, the span of short
// and invalid windows, and the default a control with none takes from its
// datasets' grain. TestPeriodFixture checks the Go side against the same
// file, so `clavesa dashboards render` and the UI use the same window.
import { build } from "esbuild";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const UI = join(dirname(fileURLToPath(import.meta.url)), "..");
const fx = JSON.parse(
  readFileSync(join(UI, "../internal/service/testdata/time_range_defaults.json"), "utf8"),
);
const tmp = mkdtempSync(join(tmpdir(), "time-range-"));
const out = join(tmp, "timeRange.mjs");
const failures = [];
const check = (ok, what) => {
  if (!ok) failures.push(what);
};
try {
  await build({
    entryPoints: [join(UI, "src/lib/timeRange.ts")],
    bundle: true,
    platform: "node",
    format: "esm",
    outfile: out,
    logLevel: "error",
  });
  const t = await import(pathToFileURL(out).href);
  const presets = t.TIME_RANGE_PRESETS.map((p) => p.expr);
  check(JSON.stringify(presets) === JSON.stringify(fx.presets), `presets ${presets} ≠ ${fx.presets}`);
  check(t.FALLBACK_PERIOD === fx.fallback, `fallback ${t.FALLBACK_PERIOD} ≠ ${fx.fallback}`);
  for (const [inp, want] of Object.entries(fx.canonical)) {
    const got = t.canonicalPeriod(inp);
    check(got === want, `canonicalPeriod(${JSON.stringify(inp)}) = ${got}, want ${want}`);
  }
  const now = new Date("2026-11-19T10:00:00Z");
  for (const [inp, secs] of Object.entries(fx.spans_seconds)) {
    const { start, end } = t.resolveTimeRange(inp, now);
    const got = (Date.parse(end) - Date.parse(start)) / 1000;
    check(got === secs, `resolveTimeRange(${JSON.stringify(inp)}) spans ${got}s, want ${secs}s`);
  }
  for (const c of fx.defaults) {
    const got = t.defaultPeriod(c.control, c.sqls);
    check(got === c.want, `${c.name}: defaultPeriod = ${got}, want ${c.want}`);
  }
  // withPeriodDefaults: an empty default takes the grain's, a retired one
  // the nearest preset, a set one stays.
  const ds = [{ sql: fx.defaults[1].sqls[0] }];
  const ctl = (d) => [{ name: "period", type: "time_range", default: d }];
  check(t.withPeriodDefaults(ctl(""), ds)[0].default === "now-24h", "empty default on hourly data is not now-24h");
  check(t.withPeriodDefaults(ctl("now-5m"), ds)[0].default === "now-1h", "retired now-5m is not now-1h");
  check(t.withPeriodDefaults(ctl("last_30d"), ds)[0].default === "now-30d", "last_30d is not now-30d");
} finally {
  rmSync(tmp, { recursive: true, force: true });
}
if (failures.length) {
  console.log(`time ranges: ${failures.length} problem(s)\n  ${failures.join("\n  ")}`);
  process.exit(1);
}
console.log("time ranges: all checks pass");
