/**
 * chartLogic — the dashboard charts' rules that need no React, so
 * `make ui-chart-logic-check` (ui/scripts/chart-logic-check.mjs) can test
 * them in node.
 */

// ---------------------------------------------------------------------------
// Series on and off
// ---------------------------------------------------------------------------

/**
 * The hidden series that still count: those among `keys`. When they would
 * be every key (a filter change left only series the viewer had hidden),
 * none count, so a chart never goes blank.
 */
export function liveHidden(hidden: ReadonlySet<string>, keys: readonly string[]): Set<string> {
  const ks = new Set(keys);
  const live = new Set([...hidden].filter((k) => ks.has(k)));
  if (ks.size > 0 && live.size >= ks.size) return new Set();
  return live;
}

/**
 * The hidden set after a tap on `key`: shown again if it was hidden,
 * hidden if at least one other series stays on, else unchanged.
 */
export function toggleHidden(
  hidden: ReadonlySet<string>,
  keys: readonly string[],
  key: string,
): Set<string> {
  const live = liveHidden(hidden, keys);
  if (live.has(key)) {
    live.delete(key);
    return live;
  }
  const on = keys.filter((k) => !live.has(k)).length;
  if (on <= 1) return live;
  live.add(key);
  return live;
}

// ---------------------------------------------------------------------------
// x values: timestamps
// ---------------------------------------------------------------------------

// A timestamp ("2026-11-19 10:25:11.727 UTC", or ISO with a T) is too wide
// for an axis tick. Its tick reads "19.11 12:25" (day.month, as elsewhere
// in the UI), its hover heading "19.11.2026 12:25:11"; both in the
// viewer's time zone when the value says it is UTC. When every value is
// at midnight (one row per day), the time is left out: "19.11" and
// "19.11.2026". Dates and other values pass unchanged.
const TIMESTAMP = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?/;
const UTC_SUFFIX = /(\s?UTC|Z|[+-]00:?00)$/i;
const MIDNIGHT = /^\d{4}-\d{2}-\d{2}[ T]00:00(:00(\.0+)?)?(\s?UTC|Z|[+-]00:?00)?$/i;

interface Parts {
  y: number;
  mo: number;
  d: number;
  h: string;
  mi: string;
  s: string;
}

function parts(v: unknown): Parts | null {
  const str = String(v ?? "").trim();
  const m = TIMESTAMP.exec(str);
  if (!m) return null;
  const sec = m[6] ?? "00";
  if (UTC_SUFFIX.test(str)) {
    const dt = new Date(`${m[1]}-${m[2]}-${m[3]}T${m[4]}:${m[5]}:${sec}Z`);
    if (!Number.isNaN(dt.getTime())) {
      const p2 = (n: number) => String(n).padStart(2, "0");
      return {
        y: dt.getFullYear(),
        mo: dt.getMonth() + 1,
        d: dt.getDate(),
        h: p2(dt.getHours()),
        mi: p2(dt.getMinutes()),
        s: p2(dt.getSeconds()),
      };
    }
  }
  return { y: Number(m[1]), mo: Number(m[2]), d: Number(m[3]), h: m[4], mi: m[5], s: sec };
}

/** True when there are values and every one is a timestamp at midnight. */
export function allMidnight(values: readonly unknown[]): boolean {
  return values.length > 0 && values.every((v) => MIDNIGHT.test(String(v ?? "").trim()));
}

export interface XFormat {
  /** The axis tick: "19.11 12:25", or "19.11" for daily values. */
  tick: (v: unknown) => string;
  /** The hover card's heading: "19.11.2026 12:25:11", or "19.11.2026". */
  heading: (v: unknown) => string;
}

/** How a chart writes its x values, from all of them. */
export function xFormat(values: readonly unknown[]): XFormat {
  // A daily midnight is a date: no time, and no shift into the viewer's
  // zone (00:00 UTC is the day itself, not 02:00 of it).
  if (allMidnight(values)) {
    const day = (v: unknown) => {
      const m = TIMESTAMP.exec(String(v ?? "").trim());
      return m ? { y: Number(m[1]), mo: Number(m[2]), d: Number(m[3]) } : null;
    };
    return {
      tick: (v) => {
        const p = day(v);
        return p ? `${p.d}.${p.mo}` : String(v ?? "");
      },
      heading: (v) => {
        const p = day(v);
        return p ? `${p.d}.${p.mo}.${p.y}` : String(v ?? "");
      },
    };
  }
  return {
    tick: (v) => {
      const p = parts(v);
      return p ? `${p.d}.${p.mo} ${p.h}:${p.mi}` : String(v ?? "");
    },
    heading: (v) => {
      const p = parts(v);
      return p ? `${p.d}.${p.mo}.${p.y} ${p.h}:${p.mi}:${p.s}` : String(v ?? "");
    },
  };
}

// ---------------------------------------------------------------------------
// Pie slices
// ---------------------------------------------------------------------------

export interface Slice {
  /** The slice's name, also its key: the hidden set holds names. */
  key: string;
  name: string;
  value: number;
  /** Index into the series palette, or -1 for "Other". */
  slot: number;
}

/**
 * The slices of a pie: rows with the same name summed, positive values
 * only, the biggest first, the tail past `max` folded into "Other". A
 * slice's colour slot comes from the alphabetical order of the names
 * shown, so a change in the values' order never swaps two colours.
 */
export function pieSlices(
  rows: ReadonlyArray<{ name: string; value: number }>,
  max: number,
): Slice[] {
  const sums = new Map<string, number>();
  for (const r of rows) {
    if (!Number.isFinite(r.value) || r.value <= 0) continue;
    sums.set(r.name, (sums.get(r.name) ?? 0) + r.value);
  }
  const all = [...sums].map(([name, value]) => ({ name, value })).sort((a, b) => b.value - a.value);
  const top = all.length > max ? all.slice(0, max) : all;
  const order = [...top.map((s) => s.name)].sort((a, b) => a.localeCompare(b));
  const slices: Slice[] = top.map((s) => ({
    key: s.name,
    name: s.name,
    value: s.value,
    slot: order.indexOf(s.name),
  }));
  if (all.length > max) {
    const rest = all.slice(max).reduce((n, s) => n + s.value, 0);
    // The tail's own key cannot clash with a real "Other" category.
    slices.push({ key: "\u0000other", name: "Other", value: rest, slot: -1 });
  }
  return slices;
}

// ---------------------------------------------------------------------------
// Table values
// ---------------------------------------------------------------------------

/**
 * True when a cell value looks like an identifier (a run ID, a hash, a
 * UUID): one token of 8 or more letters, digits and - _ . :, with at
 * least one letter and one digit, and not a date. Judged from the value
 * only, never from the column's name. Such values are set in a monospace
 * font so their characters line up.
 */
export function isIdLike(v: unknown): boolean {
  const s = String(v ?? "").trim();
  if (s.length < 8 || !/^[A-Za-z0-9][A-Za-z0-9_.:-]*$/.test(s)) return false;
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return false;
  return /[A-Za-z]/.test(s) && /\d/.test(s);
}
