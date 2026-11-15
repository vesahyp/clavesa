/**
 * WorkspaceCard — one workspace on the Workspaces page (ADR-032 section
 * 1, page 1): status dot, name, account and region, last run, a 14-day
 * runs strip, freshness, a 30-day cost sparkline with the total, and
 * when the summary was made. Cost per billion records is on the Pipeline
 * page, where it is a chart per run.
 *
 * The page owns the summary query (it sorts the cards by it); the card
 * draws a skeleton while that query loads and an error card when the
 * workspace cannot be summarized, so one broken workspace never breaks
 * the page.
 */

import type { ReactNode } from "react";

import { formatDuration, formatRelative } from "@/lib/format";
import type { WorkspaceEntry, WorkspaceSummary } from "@/lib/queries";
import {
  STATUS_DOT,
  STATUS_LABEL,
  cardStatus,
  formatUsd,
  freshnessText,
  historyUnread,
  shortDay,
} from "@/lib/summary";
import { cn } from "@/lib/utils";
import {
  FAILED_COLOR,
  Sparkline,
  StackedBars,
  SUCCEEDED_COLOR,
} from "./MiniCharts";

const COST_COLOR = "#3987e5";

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
  const { status, reasons } = cardStatus(s);
  const last = s.last_run;
  const strip = s.runs_per_day_14d;
  const cost = s.cost_per_day_30d;
  const fresh = freshnessText(s);
  const unread = historyUnread(s);
  return (
    <section data-testid="workspace-card" data-slug={entry.slug} data-state="ready" className={shell}>
      <CardTitle entry={entry} status={status} summary={s} />
      {reasons.length > 0 && (
        <ul data-testid="workspace-card-reasons" className="mt-1 space-y-0.5 text-xs text-muted-foreground">
          {reasons.map((r) => (
            <li key={r} className="break-words">
              {r}
            </li>
          ))}
        </ul>
      )}

      <div className="mt-3 grid grid-cols-2 gap-x-4 gap-y-3">
        <Field label="Last run" testId="workspace-card-last-run">
          {last ? (
            <>
              <span
                className={cn(
                  last.result === "failed" && "text-status-failed",
                  last.result === "succeeded" && "text-status-success",
                )}
              >
                {last.result}
              </span>{" "}
              {formatRelative(last.started_at)}
              <div className="truncate text-[11px] text-muted-foreground">
                {last.pipeline} · {formatDuration(last.duration_ms)}
              </div>
            </>
          ) : (
            <span className="text-muted-foreground">{unread ? "Unknown" : "No runs yet"}</span>
          )}
        </Field>
        <Field label="Freshness" testId="workspace-card-freshness">
          <span
            data-state={s.freshness_state}
            className={cn(
              fresh.tone === "bad" && "text-status-failed",
              fresh.tone === "muted" && "text-muted-foreground",
            )}
          >
            {fresh.text}
          </span>
          {fresh.detail && (
            <div className="line-clamp-2 break-words text-[11px] text-muted-foreground">
              {fresh.detail}
            </div>
          )}
        </Field>
      </div>

      <div className="mt-3">
        <FieldLabel>Runs per day, 14 days</FieldLabel>
        <StackedBars
          testId="workspace-card-runs-strip"
          ariaLabel="Runs per day over the last 14 days, succeeded and failed"
          height={32}
          data={strip.map((d) => ({
            key: d.date,
            caption: `${shortDay(d.date)}: ${d.succeeded} succeeded, ${d.failed} failed`,
            segments: [
              { key: "succeeded", value: d.succeeded, color: SUCCEEDED_COLOR },
              { key: "failed", value: d.failed, color: FAILED_COLOR },
            ],
          }))}
          empty={unread ? "Run history unreadable" : "No runs in 14 days"}
          axis={{
            top: `max ${Math.max(0, ...strip.map((d) => d.succeeded + d.failed))} runs`,
            start: strip[0] ? shortDay(strip[0].date) : "",
            end: "today",
          }}
        />
      </div>

      <div className="mt-1" data-testid="workspace-card-cost">
        <div className="flex items-baseline justify-between gap-2">
          <FieldLabel>Cost per day, 30 days</FieldLabel>
          {s.kind === "local" ? (
            <span className="text-sm tabular-nums">
              $0{" "}
              <span className="text-[11px] text-muted-foreground">
                {Math.round(s.records_per_sec).toLocaleString()} records/s
              </span>
            </span>
          ) : (
            <span className="text-sm tabular-nums">{unread ? "Unknown" : formatUsd(s.cost_usd_30d)}</span>
          )}
        </div>
        <Sparkline
          testId="workspace-card-cost-sparkline"
          ariaLabel="Compute cost per day over the last 30 days"
          color={COST_COLOR}
          height={32}
          values={cost.map((d) => d.cost_usd)}
          captions={cost.map((d) => `${shortDay(d.date)}: ${formatUsd(d.cost_usd)}`)}
          empty={unread ? "Run history unreadable" : "No cost in 30 days"}
          axis={{
            top: `max ${formatUsd(Math.max(0, ...cost.map((d) => d.cost_usd)))}`,
            start: cost[0] ? shortDay(cost[0].date) : "",
            end: "today",
          }}
        />
      </div>

      <div
        data-testid="workspace-card-generated-at"
        className="mt-1 text-[11px] text-muted-foreground"
        title={s.generated_at}
      >
        Summary from {formatRelative(s.generated_at)}
      </div>
    </section>
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

function FieldLabel({ children }: { children: ReactNode }) {
  return (
    <div className="text-[11px] uppercase tracking-wide text-muted-foreground">
      {children}
    </div>
  );
}

function Field({
  label,
  testId,
  children,
}: {
  label: string;
  testId: string;
  children: ReactNode;
}) {
  return (
    <div data-testid={testId} className="min-w-0">
      <FieldLabel>{label}</FieldLabel>
      <div className="mt-0.5 text-sm">{children}</div>
    </div>
  );
}
