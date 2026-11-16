/**
 * clock.ts — every time and day the UI shows is in one zone, TIME_ZONE,
 * and every date is day.month ("16.11"). The zone comes from the runtime
 * config the server writes (`timeZone`: the hosted config's time_zone, or
 * for `clavesa ui` the workspace's or the machine's zone), the same zone
 * the Go summary counts its days in (workspace.TimeZone). With no config
 * (Vite dev server) it is the browser's zone. rebaseSummary (summary.ts)
 * rebuilds the day series with zoneDay.
 */
import { runtimeConfig } from "@/lib/runtimeConfig";

function usableZone(tz: string | undefined): string | undefined {
  if (!tz) return undefined;
  try {
    new Intl.DateTimeFormat("en-CA", { timeZone: tz });
    return tz;
  } catch {
    return undefined;
  }
}

export const TIME_ZONE: string =
  usableZone(runtimeConfig.timeZone) ?? Intl.DateTimeFormat().resolvedOptions().timeZone;

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

/** zoneDay is the calendar day of a time in TIME_ZONE, "2026-11-16". */
export function zoneDay(v: string | number | Date): string {
  const d = toDate(v);
  return d ? dayFmt.format(d) : "";
}

/** The `n` days (in TIME_ZONE) ending today, oldest first, as "YYYY-MM-DD". */
export function lastDays(n: number, now: number = Date.now()): string[] {
  const [y, m, d] = zoneDay(now).split("-").map(Number);
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

/** clockTime is "08:00" in TIME_ZONE. */
export function clockTime(v: string | number | Date): string {
  const d = toDate(v);
  return d ? hmFmt.format(d) : "";
}

/** stamp is "16.11 08:00" in TIME_ZONE. */
export function stamp(v: string | number | Date): string {
  const d = toDate(v);
  if (!d) return typeof v === "string" ? v : "";
  return `${dayMonth(zoneDay(d))} ${clockTime(d)}`;
}
