/**
 * summary.ts — derived values the phone pages (ADR-032 section 1) draw
 * from a WorkspaceSummary: the status shown on a card (the summary's own
 * status, raised to warn when the summary itself is older than the
 * workspace's expected run interval), the problems-first order of the
 * Workspaces page, and cost per day split by pipeline.
 */

import { dayMonth, helsinkiDay, lastDays, stamp } from "@/lib/clock";
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
 * summaryOverdue reports whether a stored summary is older than the
 * expected interval plus a quarter of it as grace (the grace a pipeline's
 * "no run inside the expected interval" gets). The hosted server computes
 * the same for a local workspace (`summary_stale` on GET /api/workspaces,
 * internal/hosted LocalSummaryStale); when it sends that, it wins.
 */
export function summaryOverdue(
  s: WorkspaceSummary,
  now: number = Date.now(),
  entry?: { summary_stale?: boolean },
): boolean {
  if (entry?.summary_stale !== undefined) return entry.summary_stale;
  const generated = Date.parse(s.generated_at);
  if (Number.isNaN(generated)) return true;
  const iv = expectedIntervalSeconds(s);
  return (now - generated) / 1000 > iv + iv / 4;
}

/** The time a card names: "14.11 08:02", Helsinki time. */
const stampTime = stamp;

/**
 * cardStatus is the status a workspace card shows: the summary's status,
 * raised to warn when the summary is overdue (a stored summary that
 * stopped refreshing). A live-computed summary is always fresh, so this
 * only bites on stored ones. A local-warehouse workspace's summary comes
 * from the Mac at run end, so an overdue one says that in plain words.
 */
export function cardStatus(
  s: WorkspaceSummary,
  now: number = Date.now(),
  entry?: { kind?: string; summary_stale?: boolean },
): { status: SummaryStatus; reasons: string[]; overdue: boolean } {
  // The verdict and its plain reasons come from clavesa itself (the
  // failed-run rule and the wording are in internal/service/
  // summary_health.go), so the CLI and this page say the same.
  const reasons = [...s.status_reasons];
  let status = s.status;
  const overdue = summaryOverdue(s, now, entry);
  if (overdue) {
    if (entry?.kind === "local" || s.kind === "local") {
      reasons.unshift(`No summary from the Mac since ${stampTime(s.generated_at)}`);
    } else {
      const ageS = (now - Date.parse(s.generated_at)) / 1000;
      reasons.push(`summary is ${Number.isNaN(ageS) ? "of unknown age" : `${formatAge(ageS)} old`}`);
    }
    if (status === "ok") status = "warn";
  }
  return { status, reasons, overdue };
}

/** The text colour of reasons under a status: amber for warn, red for fail. */
export const REASON_TONE: Record<SummaryStatus, string> = {
  ok: "text-muted-foreground",
  warn: "text-status-running",
  fail: "text-status-failed",
};

/** A pipeline's status and reasons, as the summary gives them. */
export function pipelineStatus(p: WorkspaceSummary["pipelines"][number]): {
  status: SummaryStatus;
  reasons: string[];
} {
  return { status: p.status, reasons: p.status_reasons };
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

/** shortDay renders a "2026-11-14" day key as "14.11". */
export const shortDay = dayMonth;

const FINISHED = new Set(["succeeded", "failed"]);

/**
 * rebaseSummary rebuilds the day series and the window counts of a
 * summary against now, in Helsinki days. A stored summary was computed
 * when it was written (a local workspace's at its last run's end), so its
 * windows end on that day: read the next morning, "14 runs in 14 days"
 * would still count a day the CLI has already dropped, and the last bar
 * would be yesterday labelled "today". The runs the summary carries
 * (30 days of each pipeline's) are enough to redo it.
 */
export function rebaseSummary<S extends WorkspaceSummary>(s: S, now: number = Date.now()): S {
  const d30 = lastDays(30, now);
  const d14 = d30.slice(-14);
  const first = d30[0];
  const runs = new Map(d30.map((d) => [d, { succeeded: 0, failed: 0 }]));
  const cost = new Map(d30.map((d) => [d, 0]));
  let costTotal = 0;
  let records = 0;
  const pipelines = s.pipelines.map((p) => {
    const inWindow = p.runs.filter((r) => helsinkiDay(r.started_at) >= first);
    let pCost = 0;
    let pRecords = 0;
    for (const r of inWindow) {
      const day = helsinkiDay(r.started_at);
      const slot = runs.get(day);
      if (slot && FINISHED.has(r.result)) slot[r.result as "succeeded" | "failed"]++;
      if (cost.has(day)) cost.set(day, (cost.get(day) ?? 0) + (r.cost_usd || 0));
      pCost += r.cost_usd || 0;
      pRecords += r.records || 0;
    }
    costTotal += pCost;
    records += pRecords;
    return { ...p, runs: inWindow };
  });
  const perDay = (days: string[]) => days.map((date) => ({ date, ...runs.get(date)! }));
  return {
    ...s,
    pipelines,
    runs_per_day_14d: perDay(d14),
    runs_per_day_30d: perDay(d30),
    cost_per_day_30d: d30.map((date) => ({ date, cost_usd: cost.get(date) ?? 0 })),
    cost_usd_30d: costTotal,
    records_30d: records,
    cost_per_billion: records > 0 ? (costTotal / records) * 1e9 : s.cost_per_billion,
  };
}

/**
 * costPerDayByPipeline splits the summary's 30 cost days by pipeline,
 * from the runs each pipeline carries. Keys are the summary's own day
 * keys (Helsinki days, rebaseSummary), so the stack totals match
 * cost_per_day_30d.
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
      const i = idx.get(helsinkiDay(r.started_at));
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
