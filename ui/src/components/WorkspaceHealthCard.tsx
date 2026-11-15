/**
 * WorkspaceHealthCard — the workspace health summary (ADR-032) at the top
 * of the Catalog page: status dot, last run, a 14-day runs-per-day strip,
 * stale tables and the 30-day cost. Reads GET /api/workspace/summary, the
 * same service call as `clavesa workspace summary --json`.
 *
 * The strip is plain divs: recharts is lazy-loaded on the dashboard routes
 * only, and this card is on the eager landing page.
 */

import type { ReactNode } from "react";
import { Link } from "react-router-dom";

import { formatDuration, formatRelative } from "@/lib/format";
import {
  useWorkspaceSummary,
  type SummaryDayRuns,
  type SummaryStatus,
  type WorkspaceSummary,
} from "@/lib/queries";
import { homePath, runtimeConfig } from "@/lib/runtimeConfig";
import { freshnessText } from "@/lib/summary";
import { cn } from "@/lib/utils";

const STATUS: Record<SummaryStatus, { label: string; dot: string }> = {
  ok: { label: "Healthy", dot: "bg-status-success" },
  warn: { label: "Needs attention", dot: "bg-status-running" },
  fail: { label: "Failing", dot: "bg-status-failed" },
};

/**
 * apiBase reads another workspace's summary: the Workspaces page of a
 * multi-workspace server passes each workspace's `/w/<name>/api`. Omitted,
 * the card reads the current page's workspace.
 */
export function WorkspaceHealthCard({ apiBase }: { apiBase?: string } = {}) {
  const { data, error, isLoading } = useWorkspaceSummary(apiBase);
  if (isLoading) {
    return (
      <div
        data-testid="workspace-health-card"
        className="mb-6 h-28 animate-pulse rounded-lg border bg-muted/30"
      />
    );
  }
  if (error || !data) {
    return (
      <div
        data-testid="workspace-health-card"
        className="mb-6 rounded-lg border px-4 py-3 text-xs text-muted-foreground"
      >
        <span data-testid="workspace-health-error">
          Workspace summary unavailable
          {error instanceof Error ? `: ${error.message}` : ""}
        </span>
      </div>
    );
  }
  return <HealthCardBody summary={data} />;
}

function HealthCardBody({ summary: s }: { summary: WorkspaceSummary }) {
  const status = STATUS[s.status];
  const last = s.last_run;
  const fresh = freshnessText(s);
  return (
    <section
      data-testid="workspace-health-card"
      className="mb-6 rounded-lg border bg-card px-4 py-3"
    >
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
        <span
          data-testid="workspace-health-status"
          data-status={s.status}
          className="flex items-center gap-2 text-sm font-medium"
        >
          <span className={cn("inline-block size-2.5 rounded-full", status.dot)} />
          {status.label}
        </span>
        <span className="text-xs text-muted-foreground">
          {s.kind === "local" ? "Local warehouse" : "Cloud warehouse"}
          {s.region ? ` · ${s.region}` : ""}
        </span>
        <span
          data-testid="workspace-health-generated-at"
          className="ml-auto text-[11px] text-muted-foreground"
        >
          Summary from {formatRelative(s.generated_at)}
        </span>
        {runtimeConfig.mode === "local" && (
          <Link
            to={homePath}
            data-testid="workspace-health-overview-link"
            className="text-[11px] text-primary hover:underline"
          >
            Charts
          </Link>
        )}
      </div>
      {s.status_reasons.length > 0 && (
        <ul
          data-testid="workspace-health-reasons"
          className="mt-1 text-xs text-muted-foreground"
        >
          {s.status_reasons.map((r) => (
            <li key={r}>{r}</li>
          ))}
        </ul>
      )}

      <div className="mt-3 grid grid-cols-2 gap-4 md:grid-cols-4">
        <Stat label="Last run" testId="workspace-health-last-run">
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
              <div className="text-[11px] text-muted-foreground">
                {last.pipeline} · {formatDuration(last.duration_ms)}
              </div>
            </>
          ) : (
            "No runs yet"
          )}
        </Stat>
        <Stat label="Runs, 14 days" testId="workspace-health-runs">
          <RunsStrip days={s.runs_per_day_14d} />
        </Stat>
        <Stat label="Freshness" testId="workspace-health-freshness">
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
        </Stat>
        <Stat label="Cost, 30 days" testId="workspace-health-cost">
          {s.kind === "local" ? (
            <>
              $0
              <div className="text-[11px] text-muted-foreground">
                {Math.round(s.records_per_sec).toLocaleString()} records/s
              </div>
            </>
          ) : (
            <>
              ${s.cost_usd_30d.toFixed(2)}
              <div className="text-[11px] text-muted-foreground">
                ${s.cost_per_billion.toFixed(2)} per billion records
              </div>
            </>
          )}
        </Stat>
      </div>
    </section>
  );
}

function Stat({
  label,
  testId,
  children,
}: {
  label: string;
  testId: string;
  children: ReactNode;
}) {
  return (
    <div data-testid={testId}>
      <div className="text-[11px] uppercase tracking-wide text-muted-foreground">
        {label}
      </div>
      <div className="mt-0.5 text-sm">{children}</div>
    </div>
  );
}

/** RunsStrip draws one stacked bar per day: succeeded below, failed on top. */
function RunsStrip({ days }: { days: SummaryDayRuns[] }) {
  const max = Math.max(1, ...days.map((d) => d.succeeded + d.failed));
  return (
    <div
      data-testid="workspace-health-runs-strip"
      className="flex h-8 items-end gap-[2px]"
      role="img"
      aria-label="Runs per day, last 14 days"
    >
      {days.map((d) => {
        const total = d.succeeded + d.failed;
        return (
          <div
            key={d.date}
            data-testid="workspace-health-runs-day"
            title={`${d.date}: ${d.succeeded} succeeded, ${d.failed} failed`}
            className="flex h-full flex-1 flex-col-reverse"
          >
            {total === 0 ? (
              <div className="h-px w-full bg-border" />
            ) : (
              <>
                <div
                  className="w-full bg-status-success"
                  style={{ height: `${(d.succeeded / max) * 100}%` }}
                />
                <div
                  className="w-full bg-status-failed"
                  style={{ height: `${(d.failed / max) * 100}%` }}
                />
              </>
            )}
          </div>
        );
      })}
    </div>
  );
}

