// The dashboard charts' rules, checked in node: ui/src/components/
// dashboards/chartLogic.ts, bundled with esbuild the way
// day-windows-check.mjs bundles summary.ts. Run via
// `make ui-chart-logic-check` (part of make test). Exit 1, one line per
// failure.
//
// What it holds the code to (review of 2026-11-19):
//   1. a hidden set that covers every series (a filter left only hidden
//      ones) hides nothing, so a chart never goes blank; the last series
//      on cannot be hidden;
//   2. the hover heading of a UTC timestamp is in the viewer's time zone,
//      like its axis tick;
//   3. pie slices are keyed by name and coloured from the names'
//      alphabetical order, so a change in the values' order keeps each
//      slice's colour;
//   5. daily midnight timestamps read "19.11", never "19.11 00:00";
//   and a table value is set in mono when it looks like an ID (a run ID,
//   a hash), judged from the value, never from the column name.
// The time zone is pinned to Europe/Helsinki so the local-time cases are
// fixed.

process.env.TZ = "Europe/Helsinki";

import { build } from "esbuild";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const UI = join(dirname(fileURLToPath(import.meta.url)), "..");
const tmp = mkdtempSync(join(tmpdir(), "chart-logic-"));
const out = join(tmp, "chartLogic.mjs");
const failures = [];
const eq = (name, got, want) => {
  const g = JSON.stringify(got);
  const w = JSON.stringify(want);
  if (g !== w) failures.push(`${name}: got ${g}, want ${w}`);
  else console.log(`ok   ${name}`);
};

try {
  await build({
    entryPoints: [join(UI, "src/components/dashboards/chartLogic.ts")],
    bundle: true,
    platform: "node",
    format: "esm",
    outfile: out,
    logLevel: "error",
  });
  const L = await import(pathToFileURL(out).href);
  const sorted = (s) => [...s].sort();

  // 1. Series on and off.
  eq("hidden keys that left the data do not count", sorted(L.liveHidden(new Set(["a", "gone"]), ["a", "b"])), ["a"]);
  eq("hidden covering every key hides nothing", sorted(L.liveHidden(new Set(["a", "b"]), ["a", "b"])), []);
  eq("a filter leaving only hidden series shows them", sorted(L.liveHidden(new Set(["a", "b", "c"]), ["b"])), []);
  eq("a tap hides a series", sorted(L.toggleHidden(new Set(), ["a", "b"], "a")), ["a"]);
  eq("the last series on stays on", sorted(L.toggleHidden(new Set(["a"]), ["a", "b"], "b")), ["a"]);
  eq("a second tap shows it again", sorted(L.toggleHidden(new Set(["a"]), ["a", "b"], "a")), []);

  // 2 and 5. x values.
  const ts = ["2026-11-19 10:25:11.727 UTC", "2026-11-19T07:05:00Z"];
  const f = L.xFormat(ts);
  eq("UTC timestamp tick in local time", f.tick(ts[0]), "19.11 12:25");
  eq("UTC timestamp heading in local time", f.heading(ts[0]), "19.11.2026 12:25:11");
  eq("ISO Z tick in local time", f.tick(ts[1]), "19.11 09:05");
  eq("timestamp without a zone kept as written", L.xFormat(["2026-11-19 10:25"]).tick("2026-11-19 10:25"), "19.11 10:25");
  const days = ["2026-11-18 00:00:00.000 UTC", "2026-11-19 00:00:00.000 UTC"];
  const d = L.xFormat(days);
  eq("daily midnight values are dates", L.allMidnight(days), true);
  eq("daily midnight tick has no time", d.tick(days[1]), "19.11");
  eq("daily midnight heading has no time", d.heading(days[1]), "19.11.2026");
  eq("one value past midnight keeps the times", L.allMidnight([...days, "2026-11-19 06:00:00 UTC"]), false);
  eq("plain dates pass unchanged", L.xFormat(["2026-10-27"]).tick("2026-10-27"), "2026-10-27");
  eq("categories pass unchanged", L.xFormat(["Chrome"]).heading("Chrome"), "Chrome");

  // 3. Pie slices.
  const a = L.pieSlices([{ name: "Direct", value: 90 }, { name: "Social", value: 5 }, { name: "Search", value: 3 }], 7);
  const b = L.pieSlices([{ name: "Direct", value: 2 }, { name: "Social", value: 50 }, { name: "Search", value: 30 }], 7);
  const slots = (xs) => Object.fromEntries(xs.map((s) => [s.key, s.slot]).sort());
  eq("pie keys are names", a.map((s) => s.key), ["Direct", "Social", "Search"]);
  eq("a reorder keeps each slice's colour", slots(b), slots(a));
  eq("same-name rows are summed", L.pieSlices([{ name: "x", value: 1 }, { name: "x", value: 2 }], 7).map((s) => s.value), [3]);
  const many = L.pieSlices(
    Array.from({ length: 9 }, (_, i) => ({ name: `n${i}`, value: 100 - i })),
    7,
  );
  eq("the tail folds into a grey Other", [many.length, many.at(-1).name, many.at(-1).slot], [8, "Other", -1]);
  eq("zero and negative values are dropped", L.pieSlices([{ name: "a", value: 0 }, { name: "b", value: -1 }], 7).length, 0);

  // Table values: ID-like by value, never by column name.
  for (const v of ["3eb7001f-7cf2-41e4-834a-f9b05c2c91f6", "local-4c900d2a20d43b2e6c7029463b51a60a", "a1b2c3d4"])
    eq(`"${v}" is an ID`, L.isIdLike(v), true);
  for (const v of ["SUCCEEDED", "manual", "44777", "2026-10-27", "/laitteet-ja-asennukset/", "e-C4", "two words 123abc", ""])
    eq(`"${v}" is not an ID`, L.isIdLike(v), false);
} finally {
  rmSync(tmp, { recursive: true, force: true });
}

if (failures.length) {
  console.log(`\n${failures.length} failure(s):\n  ${failures.join("\n  ")}`);
  process.exit(1);
}
console.log("\nchart logic: all checks pass");
