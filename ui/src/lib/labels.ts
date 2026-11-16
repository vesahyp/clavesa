/**
 * labels.ts — plain words for the values the hosted pages show (review of
 * 2026-11-16): schedules, cost per records and counts.
 */

import { clockTime } from "@/lib/clock";
import { formatUsd } from "@/lib/summary";

/** "1 run", "2 runs". */
export function plural(n: number, word: string): string {
  return `${n} ${word}${n === 1 ? "" : "s"}`;
}

/** Cost per million records, from a cost per billion: "$0.77 per million records". */
export function perMillionRecords(costPerBillion: number): string {
  return `${formatUsd(costPerBillion / 1000)} per million records`;
}

const UNITS: Record<string, [string, string]> = {
  minute: ["every minute", "minutes"],
  minutes: ["every minute", "minutes"],
  hour: ["hourly", "hours"],
  hours: ["hourly", "hours"],
  day: ["daily", "days"],
  days: ["daily", "days"],
};

/** A UTC hour and minute of a cron line as "06:30" in TIME_ZONE. */
function helsinkiClock(h: number, m: number): string {
  const now = new Date();
  return clockTime(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), h, m));
}

/**
 * scheduleText turns an EventBridge schedule into words: "rate(1 day)" is
 * "Daily", "cron(30 4 * * ? *)" is "Daily at 06:30" (cron is UTC, the
 * time is shown in TIME_ZONE), and no schedule is "Not scheduled by
 * clavesa" (the run is started from outside, such as the Mac's crontab).
 * Anything else keeps its expression after "Schedule:".
 */
export function scheduleText(schedule?: string): string {
  if (!schedule) return "Not scheduled by clavesa";
  const rate = /^rate\((\d+)\s+(\w+)\)$/.exec(schedule.trim());
  if (rate) {
    const n = Number(rate[1]);
    const u = UNITS[rate[2].toLowerCase()];
    if (u) return n === 1 ? cap(u[0]) : `Every ${n} ${u[1]}`;
  }
  const cron = /^cron\((.+)\)$/.exec(schedule.trim());
  if (cron) {
    const [min, hour, dom, mon, dow] = cron[1].trim().split(/\s+/);
    const any = (f?: string) => f === "*" || f === "?";
    if (/^\d+$/.test(min) && any(dom) && mon === "*" && any(dow)) {
      if (/^\d+$/.test(hour)) return `Daily at ${helsinkiClock(Number(hour), Number(min))}`;
      if (hour === "*") return `Hourly at :${min.padStart(2, "0")}`;
    }
  }
  return `Schedule: ${schedule}`;
}

function cap(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}
