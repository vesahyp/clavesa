/**
 * timeRange — parser + resolver for dashboard `time_range` controls.
 *
 * Mirrors `internal/service/timerange.go` to the second. The Go side is
 * the source of truth; this file exists so the control strip can pick
 * presets, validate user-typed relative expressions, and re-evaluate
 * `now-<n><unit>` on every render without a round trip.
 *
 * **Canonical wire form: `now-<n><unit>`** (`now-1h`, `now-30m`,
 * `now-2w`). Grafana convention. Legacy preset keys (`last_24h` /
 * `last_7d` / `last_30d` / `last_90d`) accepted on read for back-compat
 * with dashboards saved before Slice E.
 *
 * Periods (Vesa, 2026-11-19: "Last 5min? Smallest should be 1h"): the
 * picker offers 1 hour, 24 hours, 7, 30 and 90 days. A preset since
 * removed takes the nearest one left, any window under an hour is an
 * hour, and a control with no default takes one from the grain of its
 * datasets' SQL (defaultPeriod). Same rules as timerange.go, checked
 * against internal/service/testdata/time_range_defaults.json by
 * `make ui-time-range-check`.
 */

const RELATIVE_RE = /^now-(\d+)([mhdw])$/;

const LEGACY_ALIAS: Record<string, string> = {
  last_24h: "now-24h",
  last_7d: "now-7d",
  last_30d: "now-30d",
  last_90d: "now-90d",
};

/** Presets the picker no longer offers, each to the nearest one left. */
const RETIRED_PRESET: Record<string, string> = {
  "now-5m": "now-1h",
  "now-15m": "now-1h",
  "now-30m": "now-1h",
  "now-3h": "now-1h",
  "now-12h": "now-24h",
  "now-3d": "now-7d",
};

/** The window for empty or invalid input, and for SQL with no grain. */
export const FALLBACK_PERIOD = "now-7d";

/**
 * A legacy key, a retired preset or a window under an hour as the
 * expression it now means, so the picker names the window that loads.
 */
export function canonicalPeriod(expr: string): string {
  let e = (expr ?? "").trim();
  e = LEGACY_ALIAS[e] ?? e;
  e = RETIRED_PRESET[e] ?? e;
  const m = /^now-(\d+)m$/.exec(e);
  return m && Number(m[1]) > 0 && Number(m[1]) < 60 ? "now-1h" : e;
}

/**
 * parseRelative — return the lookback in milliseconds for a canonical
 * `now-<n><unit>` expression (or a legacy preset). Empty input defaults
 * to `now-30d`. Malformed input throws — a typo should fail loud, not
 * silently produce 30 days.
 */
export function parseRelative(expr: string): number {
  let e = canonicalPeriod(expr);
  if (e === "") e = FALLBACK_PERIOD;
  const m = RELATIVE_RE.exec(e);
  if (!m) {
    throw new Error(
      `invalid time-range expression "${expr}" (want \`now-<n><unit>\` with unit m|h|d|w)`,
    );
  }
  const n = Number(m[1]);
  if (!Number.isInteger(n) || n <= 0) {
    throw new Error(
      `invalid time-range expression "${expr}" (n must be a positive integer)`,
    );
  }
  const minute = 60 * 1000;
  const hour = 60 * minute;
  const day = 24 * hour;
  const week = 7 * day;
  const unit = { m: minute, h: hour, d: day, w: week }[m[2]];
  if (!unit) throw new Error(`invalid time-range unit "${m[2]}"`);
  // The shortest window is an hour.
  return Math.max(n * unit, hour);
}

/**
 * resolveTimeRange — turn an expression into a `{start, end}` ISO-8601
 * pair at `now`. Falls back to FALLBACK_PERIOD on bad input so a control
 * without a typed default still renders.
 */
export function resolveTimeRange(
  expr: string,
  now: Date = new Date(),
): { start: string; end: string } {
  let ms = 7 * 24 * 60 * 60 * 1000;
  try {
    ms = parseRelative(expr);
  } catch {
    // Defensive fallback. The picker only emits canonical form, so the
    // viewer rarely hits this; keeps a saved dashboard with a hand-edited
    // bad default from crashing the page.
  }
  return {
    start: new Date(now.getTime() - ms).toISOString(),
    end: now.toISOString(),
  };
}

/** One choice in the period picker. */
export interface TimeRangePreset {
  expr: string;
  label: string;
}

/**
 * The periods the picker offers, shortest first. Nothing under an hour:
 * the data behind a dashboard is at best hourly.
 */
export const TIME_RANGE_PRESETS: TimeRangePreset[] = [
  { expr: "now-1h", label: "Last 1 hour" },
  { expr: "now-24h", label: "Last 24 hours" },
  { expr: "now-7d", label: "Last 7 days" },
  { expr: "now-30d", label: "Last 30 days" },
  { expr: "now-90d", label: "Last 90 days" },
];

// The grain of a dataset is read only from its time column: an item of
// the outer SELECT list or GROUP BY (timerange.go has the same rules).
// Table names, WHERE filters (current_date, a date cast of a bound) and
// hour(ts) or month(ts) breakdowns say nothing about it.
const ITEM_FINE = [
  /^date_trunc\s*\(\s*'(hour|minute|second)'/i,
  /^(\w+\.)?(\w+_(hour|minute)|(hour|minute)_ts)$/i,
];
const ITEM_COARSE = [
  /^date_trunc\s*\(\s*'(week|month|quarter|year)'/i,
  /^(\w+\.)?\w+_(week|month)$/i,
];
const ITEM_DAILY = [
  /^date_trunc\s*\(\s*'day'/i,
  /^(try_)?cast\s*\([\s\S]*\s+as\s+date\s*\)$/i,
  /^(to_)?date\s*\(/i,
  /^(\w+\.)?(date|day|dt)$/i,
  /^(\w+\.)?\w+_date$/i,
];

/** The outer SELECT list and GROUP BY items, aliases removed. */
export function timeItems(sql: string): string[] {
  const items: string[] = [];
  let depth = 0;
  let quote = "";
  let cur = "";
  let part: "" | "select" | "group" = "";
  let seenSelect = false;
  const flush = () => {
    const e = cur.trim().replace(/\s+as\s+["`]?\w+["`]?$/i, "").trim();
    if (part && e) items.push(e);
    cur = "";
  };
  for (let i = 0; i < sql.length; i++) {
    const ch = sql[i];
    if (quote) {
      cur += ch;
      if (ch === quote) quote = "";
      continue;
    }
    if (ch === "'" || ch === '"' || ch === "`") {
      quote = ch;
      cur += ch;
      continue;
    }
    if (ch === "(") depth++;
    if (ch === ")") depth--;
    if (depth === 0 && /\w/.test(ch) && !/\w/.test(sql[i - 1] ?? " ")) {
      const rest = sql.slice(i);
      const kw = /^(select|from|where|group\s+by|having|order\s+by|limit|union|window)\b/i.exec(rest);
      if (kw) {
        flush();
        const k = kw[1].toLowerCase().replace(/\s+/g, " ");
        // Only the first outer SELECT counts (a CTE's sit inside brackets).
        part = k === "select" ? (seenSelect ? "" : "select") : k === "group by" ? "group" : "";
        if (k === "select") seenSelect = true;
        i += kw[0].length - 1;
        continue;
      }
    }
    if (depth === 0 && ch === ",") {
      flush();
      continue;
    }
    if (depth >= 0) cur += ch;
  }
  flush();
  return items;
}

function grainOf(sql: string): string {
  const items = timeItems(sql).map((e) => (e === "current_date" ? "" : e));
  const any = (res: RegExp[]) => items.some((e) => e && res.some((r) => r.test(e)));
  if (any(ITEM_FINE)) return "now-24h";
  if (any(ITEM_COARSE)) return "now-90d";
  if (any(ITEM_DAILY)) return "now-7d";
  return "";
}

/**
 * defaultPeriod — the period a time_range control shows when the
 * dashboard sets none: 24 hours for data by the hour or finer, 7 days for
 * daily data, 90 days for weekly or monthly data, read from the time
 * column (timeItems) of the datasets that use the control (all of them
 * when none does), 7 days when none has one. With
 * several grains the longest wins, so no chart is a single bar.
 */
export function defaultPeriod(control: string, sqls: string[]): string {
  const uses = sqls.filter(
    (q) => q.includes(`{{${control}.start}}`) || q.includes(`{{${control}.end}}`),
  );
  let best = 0;
  let pick = "";
  for (const q of uses.length > 0 ? uses : sqls) {
    const p = grainOf(q);
    if (p && parseRelative(p) > best) {
      best = parseRelative(p);
      pick = p;
    }
  }
  return pick || FALLBACK_PERIOD;
}

/**
 * withPeriodDefaults — the controls as a viewer sees them: a time_range
 * with no default gets defaultPeriod, a retired default the nearest
 * preset. The editor keeps the saved spec as written.
 */
export function withPeriodDefaults<
  C extends { name: string; type: string; default: string },
>(controls: C[], datasets: { sql: string }[]): C[] {
  return controls.map((c) =>
    c.type !== "time_range"
      ? c
      : {
          ...c,
          default: c.default?.trim()
            ? canonicalPeriod(c.default)
            : defaultPeriod(c.name, datasets.map((d) => d.sql)),
        },
  );
}

/**
 * normaliseExpr — fold legacy preset keys into their canonical form so
 * a saved dashboard's old `last_24h` Default lines up against the
 * `TIME_RANGE_PRESETS` table for the picker's selected-value match.
 * Empty input returns "" so the picker can show its placeholder.
 */
export function normaliseExpr(expr: string): string {
  return canonicalPeriod(expr);
}
