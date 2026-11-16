/**
 * clock.ts — every time and day the UI shows is Helsinki local time, the
 * zone the CLI prints on Vesa's Mac (`clavesa workspace summary`), and
 * every date is day.month ("16.11"). The summary from the server keys its
 * days in UTC; rebaseSummary (summary.ts) rebuilds them with helsinkiDay.
 */

export const TIME_ZONE = "Europe/Helsinki";

const dayFmt = new Intl.DateTimeFormat("en-CA", {
  timeZone: TIME_ZONE,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
});
const hmFmt = new Intl.DateTimeFormat("en-GB", {
  timeZone: TIME_ZONE,
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
});

function toDate(v: string | number | Date): Date | null {
  const d = v instanceof Date ? v : new Date(v);
  return Number.isNaN(d.getTime()) ? null : d;
}

/** helsinkiDay is the Helsinki calendar day of a time, "2026-11-16". */
export function helsinkiDay(v: string | number | Date): string {
  const d = toDate(v);
  return d ? dayFmt.format(d) : "";
}

/** The `n` Helsinki days ending today, oldest first, as "YYYY-MM-DD". */
export function lastDays(n: number, now: number = Date.now()): string[] {
  const [y, m, d] = helsinkiDay(now).split("-").map(Number);
  const out: string[] = [];
  for (let i = n - 1; i >= 0; i--) {
    out.push(new Date(Date.UTC(y, m - 1, d - i)).toISOString().slice(0, 10));
  }
  return out;
}

/** dayMonth renders a "2026-11-16" day key as "16.11". */
export function dayMonth(day: string): string {
  const [, m, d] = day.split("-");
  return d && m ? `${Number(d)}.${Number(m)}` : day;
}

/** clockTime is "08:00", Helsinki time. */
export function clockTime(v: string | number | Date): string {
  const d = toDate(v);
  return d ? hmFmt.format(d) : "";
}

/** stamp is "16.11 08:00", Helsinki time. */
export function stamp(v: string | number | Date): string {
  const d = toDate(v);
  if (!d) return typeof v === "string" ? v : "";
  return `${dayMonth(helsinkiDay(d))} ${clockTime(d)}`;
}
