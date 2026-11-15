/**
 * WorkspaceHome — the home of one workspace (ADR-032 section 1, page 2):
 * runs per day for 30 days (succeeded and failed stacked), cost per day
 * split by pipeline, then one card per pipeline with its last run, a
 * duration sparkline over its runs and its stale tables. Each pipeline
 * card links to the Pipeline page.
 *
 * Everything comes from GET /api/workspace/summary, the same service call
 * as `clavesa workspace summary --json`. It is "/" inside a workspace of a
 * multi-workspace or hosted server and "/overview" in the local UI, which
 * keeps the Catalog at "/" (lib/runtimeConfig.ts homePath).
 */

import { useMemo, type ReactNode } from "react";
import { Link } from "react-router-dom";
import { ChevronRight } from "lucide-react";

import { useChrome, type PageChrome } from "@/components/PageChrome";
import {
  FAILED_COLOR,
  Legend,
  Sparkline,
  StackedBars,
  SUCCEEDED_COLOR,
} from "@/components/MiniCharts";
import { formatDuration, formatRelative } from "@/lib/format";
import { useWorkspaceSummary, type WorkspaceSummary } from "@/lib/queries";
import { homePath } from "@/lib/runtimeConfig";
import {
  OTHER_COLOR,
  SERIES_COLORS,
  STATUS_DOT,
  STATUS_LABEL,
  cardStatus,
  costPerDayByPipeline,
  formatAge,
  formatUsd,
  freshnessText,
  historyUnread,
  shortDay,
} from "@/lib/summary";
import { cn } from "@/lib/utils";

type PipelineSummary = WorkspaceSummary["pipelines"][number];

export function WorkspaceHome() {
  useChrome(
    useMemo<PageChrome>(
      () => ({ breadcrumbs: [{ label: "Overview", to: homePath }] }),
      [],
    ),
  );
  const { data, error, isLoading } = useWorkspaceSummary();

  return (
    <div
      data-testid="workspace-home"
      className="mx-auto flex w-full max-w-5xl flex-col gap-4 px-4 py-4 md:px-6 md:py-8"
    >
      {isLoading && (
        <div data-testid="workspace-home-skeleton" className="space-y-3">
          <div className="h-6 w-1/2 animate-pulse rounded bg-muted" />
          <div className="h-32 animate-pulse rounded-lg bg-muted/60" />
          <div className="h-32 animate-pulse rounded-lg bg-muted/60" />
        </div>
      )}
      {!isLoading && (error || !data) && (
        <Panel testId="workspace-home-error">
          <p className="text-sm">
            <span className="text-status-failed">Workspace summary unavailable.</span>{" "}
            <span className="break-words text-muted-foreground">
              {error instanceof Error ? error.message : ""}
            </span>
          </p>
        </Panel>
      )}
      {data && <Body s={data} />}
    </div>
  );
}

function Body({ s }: { s: WorkspaceSummary }) {
  const { status, reasons } = cardStatus(s);
  const fresh = freshnessText(s);
  const unread = historyUnread(s);
  const runs = s.runs_per_day_30d;
  const costDays = useMemo(() => costPerDayByPipeline(s), [s]);

  // Pipelines with any cost, most expensive first, get the fixed hues in
  // that order; past eight they fold into "Other".
  const costSeries = useMemo(() => {
    const total = new Map<string, number>();
    for (const d of costDays)
      for (const [p, v] of Object.entries(d.byPipeline))
        total.set(p, (total.get(p) ?? 0) + v);
    const names = [...total.keys()].sort(
      (a, b) => (total.get(b) ?? 0) - (total.get(a) ?? 0) || a.localeCompare(b),
    );
    const shown = names.length > SERIES_COLORS.length ? names.slice(0, SERIES_COLORS.length - 1) : names;
    const series = shown.map((n, i) => ({ key: n, label: n, color: SERIES_COLORS[i] }));
    if (shown.length < names.length) series.push({ key: "__other", label: "Other", color: OTHER_COLOR });
    return series;
  }, [costDays]);
  const shownKeys = new Set(costSeries.map((c) => c.key));

  const pipelines = [...s.pipelines].sort(
    (a, b) =>
      rank(a.status) - rank(b.status) || a.name.localeCompare(b.name),
  );

  return (
    <>
      <div className="flex min-w-0 items-start gap-2.5">
        <span
          data-testid="workspace-home-status"
          data-status={status}
          role="img"
          aria-label={STATUS_LABEL[status]}
          className={cn("mt-2 inline-block size-2.5 shrink-0 rounded-full", STATUS_DOT[status])}
        />
        <div className="min-w-0 flex-1">
          <h1 className="truncate text-xl font-semibold tracking-tight">{s.name}</h1>
          <div className="text-xs text-muted-foreground">
            {s.kind === "local" ? "Local warehouse" : "Cloud"}
            {s.account ? ` · ${s.account}` : ""}
            {s.region ? ` · ${s.region}` : ""} · {STATUS_LABEL[status]} · summary from{" "}
            {formatRelative(s.generated_at)}
          </div>
          {reasons.length > 0 && (
            <ul className="mt-1 space-y-0.5 text-xs text-muted-foreground">
              {reasons.map((r) => (
                <li key={r} className="break-words">
                  {r}
                </li>
              ))}
            </ul>
          )}
          <div
            data-testid="workspace-home-freshness"
            data-state={s.freshness_state}
            className="mt-1 break-words text-xs"
          >
            <span className="text-muted-foreground">Freshness: </span>
            <span className={cn(fresh.tone === "bad" ? "text-status-failed" : "text-muted-foreground")}>
              {fresh.text}
              {fresh.detail ? `, ${fresh.detail}` : ""}
            </span>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
        <Panel testId="workspace-home-runs-chart" title="Runs per day, 30 days">
          <StackedBars
            testId="workspace-home-runs-bars"
            ariaLabel="Runs per day over the last 30 days, succeeded and failed"
            height={72}
            data={runs.map((d) => ({
              key: d.date,
              caption: `${shortDay(d.date)}: ${d.succeeded} succeeded, ${d.failed} failed`,
              segments: [
                { key: "succeeded", value: d.succeeded, color: SUCCEEDED_COLOR },
                { key: "failed", value: d.failed, color: FAILED_COLOR },
              ],
            }))}
            empty={unread ? "Run history unreadable" : "No runs in 30 days"}
            axis={{
              top: `max ${Math.max(0, ...runs.map((d) => d.succeeded + d.failed))} runs`,
              start: runs[0] ? shortDay(runs[0].date) : "",
              end: "today",
            }}
          />
          {runs.some((d) => d.succeeded + d.failed > 0) && (
            <Legend
              items={[
                { key: "s", label: "succeeded", color: SUCCEEDED_COLOR },
                { key: "f", label: "failed", color: FAILED_COLOR },
              ]}
            />
          )}
        </Panel>

        <Panel
          testId="workspace-home-cost-chart"
          title="Cost per day by pipeline, 30 days"
          aside={
            unread ? (
              <span>Unknown</span>
            ) : s.kind === "local" ? (
              <span className="tabular-nums">
                $0 · {Math.round(s.records_per_sec).toLocaleString()} records/s
              </span>
            ) : (
              <span className="tabular-nums">
                {formatUsd(s.cost_usd_30d)} · {formatUsd(s.cost_per_billion)}/billion
              </span>
            )
          }
        >
          <StackedBars
            testId="workspace-home-cost-bars"
            ariaLabel="Compute cost per day over the last 30 days, by pipeline"
            height={72}
            data={costDays.map((d) => {
              let other = 0;
              for (const [p, v] of Object.entries(d.byPipeline))
                if (!shownKeys.has(p)) other += v;
              const segs = costSeries.map((c) => ({
                key: c.key,
                value: c.key === "__other" ? other : d.byPipeline[c.key] ?? 0,
                color: c.color,
              }));
              const total = segs.reduce((a, x) => a + x.value, 0);
              const parts = costSeries
                .map((c, i) => ({ label: c.label, value: segs[i].value }))
                .filter((x) => x.value > 0)
                .map((x) => `${x.label} ${formatUsd(x.value)}`);
              return {
                key: d.date,
                caption: `${shortDay(d.date)}: ${formatUsd(total)}${parts.length > 1 ? ` (${parts.join(", ")})` : ""}`,
                segments: segs,
              };
            })}
            empty={unread ? "Run history unreadable" : "No cost in 30 days"}
            axis={{
              top: `max ${formatUsd(Math.max(0, ...s.cost_per_day_30d.map((d) => d.cost_usd)))}`,
              start: costDays[0] ? shortDay(costDays[0].date) : "",
              end: "today",
            }}
          />
          {costSeries.length > 0 && (
            <Legend items={costSeries} testId="workspace-home-cost-legend" />
          )}
        </Panel>
      </div>

      <h2 className="mt-2 text-sm font-medium text-muted-foreground">
        Pipelines · {pipelines.length}
      </h2>
      {pipelines.length === 0 && (
        <p className="text-sm text-muted-foreground">No pipelines in this workspace.</p>
      )}
      <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
        {pipelines.map((p) => (
          <PipelineCard key={p.dir} p={p} />
        ))}
      </div>
    </>
  );
}

function rank(s: string): number {
  return s === "fail" ? 0 : s === "warn" ? 1 : 2;
}

function PipelineCard({ p }: { p: PipelineSummary }) {
  const to = `/pipelines/dashboard?dir=${encodeURIComponent(p.dir)}`;
  const runs = [...p.runs].reverse(); // oldest first
  const stale = p.freshness.filter((f) => f.stale);
  const last = p.last_run;
  const problemLine = p.problem
    ? `${p.problem.cause}${p.problem.action ? `. ${p.problem.action}` : ""}`
    : "";
  return (
    <section
      data-testid="pipeline-card"
      data-pipeline={p.name}
      className="min-w-0 rounded-lg border border-border bg-card p-4"
    >
      <div className="flex min-w-0 items-start gap-2.5">
        <span
          data-testid="pipeline-card-status"
          data-status={p.status}
          role="img"
          aria-label={STATUS_LABEL[p.status]}
          className={cn("mt-1.5 inline-block size-2.5 shrink-0 rounded-full", STATUS_DOT[p.status])}
        />
        <div className="min-w-0 flex-1">
          <Link
            to={to}
            data-testid="pipeline-card-link"
            className="block truncate font-mono text-base font-semibold hover:underline"
          >
            {p.name}
          </Link>
          <div className="truncate text-xs text-muted-foreground">
            {p.schedule ? p.schedule : "no schedule"}
            {!p.problem && ` · ${p.runs.length} run${p.runs.length === 1 ? "" : "s"} in 30 days`}
          </div>
        </div>
        <Link
          to={to}
          aria-label={`Open ${p.name}`}
          className="-mr-1 shrink-0 rounded p-1 text-muted-foreground hover:text-foreground"
        >
          <ChevronRight className="h-5 w-5" />
        </Link>
      </div>

      {p.problem && (
        <p
          data-testid="pipeline-card-problem"
          data-code={p.problem.code}
          className="mt-2 break-words text-xs text-status-failed"
        >
          {problemLine}
        </p>
      )}
      {p.status_reasons.some((r) => r !== problemLine) && (
        <ul className="mt-1 space-y-0.5 text-xs text-muted-foreground">
          {p.status_reasons.filter((r) => r !== problemLine).map((r) => (
            <li key={r} className="break-words">
              {r}
            </li>
          ))}
        </ul>
      )}

      <div data-testid="pipeline-card-last-run" className="mt-3 text-sm">
        <span className="text-[11px] uppercase tracking-wide text-muted-foreground">Last run </span>
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
            {formatRelative(last.started_at)} · {formatDuration(last.duration_ms)}
            {last.result === "failed" && (last.failed_step || last.error) && (
              <div className="mt-0.5 line-clamp-2 break-words text-xs text-muted-foreground">
                {last.failed_step ? `${last.failed_step}: ` : ""}
                {last.error}
              </div>
            )}
          </>
        ) : (
          <span className="text-muted-foreground">{p.problem ? "unknown" : "no runs yet"}</span>
        )}
      </div>

      {runs.length > 0 && (
        <div className="mt-2">
          <Sparkline
            testId="pipeline-card-duration-sparkline"
            ariaLabel={`Duration of the last ${runs.length} runs of ${p.name}`}
            color={SERIES_COLORS[0]}
            height={32}
            values={runs.map((r) => r.duration_ms / 1000)}
            captions={runs.map(
              (r) =>
                `${shortDay(r.started_at.slice(0, 10))} ${r.started_at.slice(11, 16)}: ${formatDuration(r.duration_ms)}, ${r.result}`,
            )}
            axis={{
              top: `duration per run · max ${formatDuration(Math.max(...runs.map((r) => r.duration_ms)))}`,
              start: `${runs.length} runs ago`,
              end: "latest",
            }}
          />
        </div>
      )}

      <div data-testid="pipeline-card-freshness" data-state={p.freshness_state} className="text-sm">
        {p.freshness_state === "unknown" ? (
          <span className="break-words text-xs text-muted-foreground">
            Freshness unknown
            {/* The problem line above already names the cause. */}
            {p.freshness_reason && p.freshness_reason !== p.problem?.cause
              ? `: ${p.freshness_reason}`
              : ""}
          </span>
        ) : p.freshness_state === "no_sla" ? (
          <span className="text-xs text-muted-foreground">No freshness target set</span>
        ) : stale.length === 0 ? (
          <span className="text-xs text-muted-foreground">
            All {p.freshness.length} table{p.freshness.length === 1 ? "" : "s"} fresh
          </span>
        ) : (
          <ul className="space-y-0.5">
            {stale.map((f) => (
              <li
                key={f.table}
                data-testid="pipeline-card-stale-table"
                className="flex min-w-0 items-baseline justify-between gap-2 text-xs"
              >
                <span className="truncate font-mono">{f.table}</span>
                <span className="shrink-0 text-status-failed">
                  {formatAge(f.age_seconds)} old, limit {formatAge(f.sla_seconds)}
                </span>
              </li>
            ))}
          </ul>
        )}
      </div>
    </section>
  );
}

function Panel({
  testId,
  title,
  aside,
  children,
}: {
  testId: string;
  title?: string;
  aside?: ReactNode;
  children: ReactNode;
}) {
  return (
    <section
      data-testid={testId}
      className="min-w-0 space-y-2 rounded-lg border border-border bg-card p-4"
    >
      {title && (
        <div className="flex flex-wrap items-baseline justify-between gap-x-3 gap-y-0.5">
          <h2 className="text-sm font-medium">{title}</h2>
          {aside && <span className="text-xs text-muted-foreground">{aside}</span>}
        </div>
      )}
      {children}
    </section>
  );
}
