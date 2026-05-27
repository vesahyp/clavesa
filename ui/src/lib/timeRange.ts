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
 */

const RELATIVE_RE = /^now-(\d+)([mhdw])$/;

const LEGACY_ALIAS: Record<string, string> = {
  last_24h: "now-24h",
  last_7d: "now-7d",
  last_30d: "now-30d",
  last_90d: "now-90d",
};

/**
 * parseRelative — return the lookback in milliseconds for a canonical
 * `now-<n><unit>` expression (or a legacy preset). Empty input defaults
 * to `now-30d`. Malformed input throws — a typo should fail loud, not
 * silently produce 30 days.
 */
export function parseRelative(expr: string): number {
  let e = (expr ?? "").trim();
  if (e === "") e = "now-30d";
  if (LEGACY_ALIAS[e]) e = LEGACY_ALIAS[e];
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
  switch (m[2]) {
    case "m":
      return n * minute;
    case "h":
      return n * hour;
    case "d":
      return n * day;
    case "w":
      return n * week;
    default:
      throw new Error(`invalid time-range unit "${m[2]}"`);
  }
}

/**
 * resolveTimeRange — turn an expression into a `{start, end}` ISO-8601
 * pair at `now`. Falls back to `now-30d` on bad input so a control
 * without a typed default still renders.
 */
export function resolveTimeRange(
  expr: string,
  now: Date = new Date(),
): { start: string; end: string } {
  let ms = 30 * 24 * 60 * 60 * 1000;
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

/**
 * AWS Console-aligned quick picks. The user explicitly nudged for an
 * AWS-shaped preset list; matches CloudWatch's quick range dropdown
 * (5m / 15m / 30m / 1h / 3h / 12h / 1d) plus our day-scale picks
 * (3d / 7d / 30d / 90d) so the operational dashboards a clavesa user
 * authors don't lose their existing presets.
 */
export interface TimeRangePreset {
  expr: string;
  label: string;
}

export const TIME_RANGE_PRESETS: TimeRangePreset[] = [
  { expr: "now-5m", label: "Last 5 minutes" },
  { expr: "now-15m", label: "Last 15 minutes" },
  { expr: "now-30m", label: "Last 30 minutes" },
  { expr: "now-1h", label: "Last 1 hour" },
  { expr: "now-3h", label: "Last 3 hours" },
  { expr: "now-12h", label: "Last 12 hours" },
  { expr: "now-24h", label: "Last 24 hours" },
  { expr: "now-3d", label: "Last 3 days" },
  { expr: "now-7d", label: "Last 7 days" },
  { expr: "now-30d", label: "Last 30 days" },
  { expr: "now-90d", label: "Last 90 days" },
];

/**
 * normaliseExpr — fold legacy preset keys into their canonical form so
 * a saved dashboard's old `last_24h` Default lines up against the
 * `TIME_RANGE_PRESETS` table for the picker's selected-value match.
 * Empty input returns "" so the picker can show its placeholder.
 */
export function normaliseExpr(expr: string): string {
  const e = (expr ?? "").trim();
  if (e === "") return "";
  return LEGACY_ALIAS[e] ?? e;
}
