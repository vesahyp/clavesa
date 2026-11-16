/**
 * WorkspaceCard — one workspace on the Workspaces page (ADR-032 section
 * 1, page 1), compact so ten fit in about two phone screens (review of
 * 2026-11-16): status dot, name and status, the reasons when it is not
 * healthy, last run, runs and (cloud) cost, and a 14-day run strip. The
 * whole card is the link. Charts, cost per day and freshness are on the
 * Workspace page.
 *
 * The page owns the summary query (it sorts the cards by it); the card
 * draws a skeleton while that query loads and an error card when the
 * workspace cannot be summarized, so one broken workspace never breaks
 * the page.
 */

import { ChevronRight } from "lucide-react";

import type { WorkspaceEntry, WorkspaceSummary } from "@/lib/queries";
import {
  STATUS_DOT,
  STATUS_LABEL,
  cardStatus,
  formatAge,
  REASON_TONE,
  formatUsd,
  freshnessText,
  historyUnread,
  pausedLines,
  shortDay,
} from "@/lib/summary";
import { cn } from "@/lib/utils";
import { FAILED_COLOR, SUCCEEDED_COLOR } from "./MiniCharts";

export interface WorkspaceCardProps {
  entry: WorkspaceEntry;
  summary?: WorkspaceSummary;
  error?: unknown;
  isLoading: boolean;
}

function href(entry: WorkspaceEntry): string {
  return `/w/${encodeURIComponent(entry.slug)}/`;
}

export function WorkspaceCard({ entry, summary, error, isLoading }: WorkspaceCardProps) {
  const shell =
    "block min-w-0 rounded-lg border border-border bg-card p-4 text-card-foreground";
  if (isLoading) {
    return (
      <section data-testid="workspace-card" data-slug={entry.slug} data-state="loading" className={shell}>
        <CardTitle entry={entry} />
        <div data-testid="workspace-card-skeleton" className="mt-3 space-y-2">
          <div className="h-4 w-2/3 animate-pulse rounded bg-muted" />
          <div className="h-8 w-full animate-pulse rounded bg-muted" />
          <div className="h-8 w-full animate-pulse rounded bg-muted" />
        </div>
      </section>
    );
  }
  if (error || !summary) {
    const msg = error instanceof Error ? error.message : error ? String(error) : "no summary";
    return (
      <section data-testid="workspace-card" data-slug={entry.slug} data-state="error" className={shell}>
        <CardTitle entry={entry} status="fail" />
        <p
          data-testid="workspace-card-error"
          className="mt-2 line-clamp-3 break-words text-xs text-muted-foreground"
          title={msg}
        >
          <span className="text-status-failed">Summary unavailable.</span> {msg}
        </p>
      </section>
    );
  }

  const s = summary;
  const { status, reasons, overdue } = cardStatus(s, Date.now(), entry);
  const last = s.last_run;
  const strip = s.runs_per_day_14d;
  const fresh = freshnessText(s);
  const unread = historyUnread(s);
  const runs14 = strip.reduce((a, d) => a + d.succeeded + d.failed, 0);
  // Freshness only when it says something: stale tables or unknown.
  const lines = [...reasons];
  if (fresh.tone === "bad") lines.push(`Freshness: ${fresh.text}${fresh.detail ? `, ${fresh.detail}` : ""}`);
  const paused = pausedLines(s);
  return (
    // The whole card opens the workspace (its own page load and basename).
    <a
      href={href(entry)}
      data-testid="workspace-card"
      data-slug={entry.slug}
      data-state="ready"
      data-overdue={overdue ? "true" : undefined}
      // Finished runs in the 14 and 30 days (TIME_ZONE) ending today, for the
      // live check's comparison with `clavesa workspace summary`.
      data-runs-14d={runs14}
      data-runs-30d={s.runs_per_day_30d.reduce((a, d) => a + d.succeeded + d.failed, 0)}
      aria-label={`${entry.name}, ${STATUS_LABEL[status]}`}
      className={cn(shell, "px-3 py-2.5 transition-colors hover:border-primary/50 active:bg-muted/40")}
    >
      <div className="flex min-w-0 items-center gap-2">
        <StatusDot status={status} />
        <span data-testid="workspace-card-link" className="min-w-0 flex-1 truncate text-base font-semibold">
          {entry.name}
        </span>
        <span className={cn("shrink-0 text-xs", status === "ok" ? "text-muted-foreground" : REASON_TONE[status])}>
          {STATUS_LABEL[status]}
        </span>
        <ChevronRight className="-mr-1 h-4 w-4 shrink-0 text-muted-foreground" />
      </div>
      {lines.length > 0 && (
        <ul data-testid="workspace-card-reasons" className={cn("mt-1 space-y-0.5 text-xs", REASON_TONE[status === "ok" ? "warn" : status])}>
          {lines.map((r) => (
            <li key={r} className="line-clamp-2 break-words">
              {r}
            </li>
          ))}
        </ul>
      )}
      {paused.length > 0 && (
        <ul data-testid="workspace-card-notes" className="mt-1 space-y-0.5 text-xs text-muted-foreground">
          {paused.map((r) => (
            <li key={r} className="line-clamp-2 break-words">
              {r}
            </li>
          ))}
        </ul>
      )}
      <div className="mt-1 flex min-w-0 items-baseline justify-between gap-3 text-xs text-muted-foreground">
        <span data-testid="workspace-card-last-run" className="min-w-0 truncate">
          {last ? (
            <>
              Last run{" "}
              <span
                className={cn(
                  last.result === "failed" && "text-status-failed",
                  last.result === "succeeded" && "text-foreground",
                )}
              >
                {last.result}
              </span>{" "}
              {formatAge((Date.now() - Date.parse(last.started_at)) / 1000)} ago
            </>
          ) : unread ? (
            "Run history unreadable"
          ) : (
            "No runs yet"
          )}
        </span>
        <span className="shrink-0 tabular-nums">
          {runs14} run{runs14 === 1 ? "" : "s"} in 14 days
        </span>
      </div>
      {s.kind !== "local" && !unread && (
        <div data-testid="workspace-card-cost" className="mt-0.5 text-xs text-muted-foreground">
          Cost {formatUsd(s.cost_usd_30d)} in 30 days
        </div>
      )}
      <RunStrip days={strip} />
    </a>
  );
}

/** 14 bars, one per day, height by runs, red part for failures. Not interactive: the card is the link. */
function RunStrip({ days }: { days: WorkspaceSummary["runs_per_day_14d"] }) {
  const max = Math.max(1, ...days.map((d) => d.succeeded + d.failed));
  return (
    <div
      data-testid="workspace-card-runs-strip"
      role="img"
      aria-label={`Runs per day, last 14 days: ${days.map((d) => `${shortDay(d.date)} ${d.succeeded} ok ${d.failed} failed`).join(", ")}`}
      className="mt-1 flex h-4 items-end gap-[2px]"
    >
      {days.map((d) => {
        const n = d.succeeded + d.failed;
        return (
          <div key={d.date} className="flex h-full min-w-0 flex-1 flex-col justify-end rounded-[2px] bg-muted/40">
            {n > 0 && (
              <div className="flex flex-col overflow-hidden rounded-[2px]" style={{ height: `${Math.max(25, (n / max) * 100)}%` }}>
                {d.failed > 0 && <div style={{ flex: d.failed, background: FAILED_COLOR }} />}
                {d.succeeded > 0 && <div style={{ flex: d.succeeded, background: SUCCEEDED_COLOR }} />}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

function StatusDot({ status }: { status?: "ok" | "warn" | "fail" }) {
  return (
    <span
      data-testid="workspace-card-status"
      data-status={status ?? "loading"}
      role="img"
      aria-label={status ? STATUS_LABEL[status] : "Loading"}
      title={status ? STATUS_LABEL[status] : "Loading"}
      className={cn(
        "inline-block size-2.5 shrink-0 rounded-full",
        status ? STATUS_DOT[status] : "animate-pulse bg-muted-foreground",
      )}
    />
  );
}

function CardTitle({
  entry,
  status,
  summary,
}: {
  entry: WorkspaceEntry;
  status?: "ok" | "warn" | "fail";
  summary?: WorkspaceSummary;
}) {
  const where = [
    summary?.account,
    summary?.region ?? entry.region,
  ].filter(Boolean);
  const kind = (summary?.kind ?? entry.kind) === "local" ? "Local warehouse" : "Cloud";
  return (
    <div className="flex min-w-0 items-start gap-2.5">
      <span
        data-testid="workspace-card-status"
        data-status={status ?? "loading"}
        role="img"
        aria-label={status ? STATUS_LABEL[status] : "Loading"}
        title={status ? STATUS_LABEL[status] : "Loading"}
        className={cn(
          "mt-1.5 inline-block size-2.5 shrink-0 rounded-full",
          status ? STATUS_DOT[status] : "animate-pulse bg-muted-foreground",
        )}
      />
      <div className="min-w-0 flex-1">
        <a
          href={href(entry)}
          data-testid="workspace-card-link"
          className="block truncate text-base font-semibold hover:underline"
        >
          {entry.name}
        </a>
        <div className="truncate text-xs text-muted-foreground">
          {kind}
          {where.length > 0 && ` · ${where.join(" · ")}`}
          {status && ` · ${STATUS_LABEL[status]}`}
        </div>
      </div>
    </div>
  );
}
