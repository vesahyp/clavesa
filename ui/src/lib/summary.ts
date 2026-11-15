/**
 * summary.ts — derived values the phone pages (ADR-032 section 1) draw
 * from a WorkspaceSummary: the status shown on a card (the summary's own
 * status, raised to warn when the summary itself is older than the
 * workspace's expected run interval), the problems-first order of the
 * Workspaces page, and cost per day split by pipeline.
 */

import type {
  FreshnessState,
  SummaryStatus,
  WorkspaceSummary,
} from "@/lib/queries";

export const STATUS_RANK: Record<SummaryStatus, number> = {
  fail: 0,
  warn: 1,
  ok: 2,
};

export const STATUS_LABEL: Record<SummaryStatus, string> = {
  ok: "Healthy",
  warn: "Needs attention",
  fail: "Failing",
};

export const STATUS_DOT: Record<SummaryStatus, string> = {
  ok: "bg-status-success",
  warn: "bg-status-running",
  fail: "bg-status-failed",
};

const DAY_SECONDS = 86_400;

/**
 * expectedIntervalSeconds is the shortest declared trigger interval of the
 * workspace's pipelines; one day when none declares one.
 */
export function expectedIntervalSeconds(s: WorkspaceSummary): number {
  const ivs = s.pipelines
    .map((p) => p.expected_interval_seconds ?? 0)
    .filter((n) => n > 0);
  return ivs.length ? Math.min(...ivs) : DAY_SECONDS;
}

/**
 * cardStatus is the status a workspace card shows: the summary's status,
 * raised to warn when the summary is older than the expected interval (a
 * stored summary that stopped refreshing). A live-computed summary is
 * always fresh, so this only bites on stored ones.
 */
export function cardStatus(
  s: WorkspaceSummary,
  now: number = Date.now(),
): { status: SummaryStatus; reasons: string[] } {
  const reasons = [...s.status_reasons];
  let status = s.status;
  const generated = Date.parse(s.generated_at);
  if (!Number.isNaN(generated)) {
    const ageS = (now - generated) / 1000;
    if (ageS > expectedIntervalSeconds(s)) {
      reasons.push(`summary is ${formatAge(ageS)} old`);
      if (status === "ok") status = "warn";
    }
  }
  return { status, reasons };
}

/**
 * freshnessText is what a card says about freshness: never "fresh" unless
 * every table with a freshness_sla was read and is inside it.
 *   unknown: "Unknown" and the reason
 *   no_sla:  "No freshness target set"
 *   fresh:   "All N tables fresh"
 *   stale:   "N stale" and "oldest X"
 */
export function freshnessText(f: {
  freshness_state: FreshnessState;
  freshness_reason?: string;
  sla_tables: number;
  stale_tables: number;
  oldest_table_age_seconds: number;
}): { tone: "muted" | "ok" | "bad"; text: string; detail?: string } {
  switch (f.freshness_state) {
    case "stale":
      return {
        tone: "bad",
        text: `${f.stale_tables} stale`,
        detail: `oldest ${formatAge(f.oldest_table_age_seconds)}`,
      };
    case "fresh":
      return {
        tone: "ok",
        text: `All ${f.sla_tables} table${f.sla_tables === 1 ? "" : "s"} fresh`,
      };
    case "unknown":
      return { tone: "muted", text: "Unknown", detail: f.freshness_reason };
    default:
      return { tone: "muted", text: "No freshness target set" };
  }
}

/**
 * historyUnread: no pipeline's run history could be read, so "no runs"
 * and "$0" would be claims the summary cannot make.
 */
export function historyUnread(s: WorkspaceSummary): boolean {
  return s.pipelines.length > 0 && s.pipelines.every((p) => !!p.problem);
}

/** formatAge renders a duration in seconds as "12 min", "3 h" or "2 d". */
export function formatAge(seconds: number): string {
  if (seconds <= 0) return "never written";
  if (seconds < 3600) return `${Math.max(1, Math.round(seconds / 60))} min`;
  if (seconds < DAY_SECONDS) return `${Math.round(seconds / 3600)} h`;
  return `${Math.round(seconds / DAY_SECONDS)} d`;
}

/** formatUsd: cents for small sums, whole dollars past $1000. */
export function formatUsd(n: number): string {
  if (n === 0) return "$0";
  if (n < 0.01) return `$${n.toFixed(4)}`;
  if (n < 1000) return `$${n.toFixed(2)}`;
  return `$${Math.round(n).toLocaleString()}`;
}

/** utcDay is the UTC calendar day of an ISO time, the summary's day key. */
export function utcDay(iso: string): string {
  const t = Date.parse(iso);
  return Number.isNaN(t) ? "" : new Date(t).toISOString().slice(0, 10);
}

/** shortDay renders a "2026-11-14" day key as "14.11". */
export function shortDay(day: string): string {
  const [, m, d] = day.split("-");
  return d && m ? `${Number(d)}.${Number(m)}` : day;
}

/**
 * costPerDayByPipeline splits the summary's 30 cost days by pipeline,
 * from the runs each pipeline carries. Keys are the summary's own day
 * keys (UTC), so the stack totals match cost_per_day_30d.
 */
export function costPerDayByPipeline(
  s: WorkspaceSummary,
): { date: string; byPipeline: Record<string, number> }[] {
  const days = s.cost_per_day_30d.map((d) => ({
    date: d.date,
    byPipeline: {} as Record<string, number>,
  }));
  const idx = new Map(days.map((d, i) => [d.date, i]));
  for (const p of s.pipelines) {
    for (const r of p.runs) {
      const i = idx.get(utcDay(r.started_at));
      if (i === undefined || r.cost_usd <= 0) continue;
      const bp = days[i].byPipeline;
      bp[p.name] = (bp[p.name] ?? 0) + r.cost_usd;
    }
  }
  return days;
}

/**
 * Categorical hues for series that are not a status (cost per pipeline),
 * in fixed order: the dark-surface steps of the dataviz reference
 * palette, since the UI is dark-only. A ninth pipeline folds into
 * "Other" (the last, gray slot), never a generated hue.
 */
export const SERIES_COLORS = [
  "#3987e5",
  "#d95926",
  "#199e70",
  "#c98500",
  "#d55181",
  "#0ca30c",
  "#9085e9",
  "#e66767",
];
export const OTHER_COLOR = "#8a8f98";
