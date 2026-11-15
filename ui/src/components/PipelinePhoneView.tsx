/**
 * PipelinePhoneView — the Pipeline page below md (ADR-032 section 1, page
 * 3), under the PipelineHealthHeader: duration per run as bars coloured by
 * result, cost per billion records per run over time (records per second
 * on a local warehouse, where compute is free), the phase breakdown
 * (RunTimingsCard) and the list of recent runs. A tap on a bar or a run
 * row opens that run.
 *
 * Durations and results come from the runs the dashboard already reads;
 * cost per run comes from the workspace summary (one call, the same
 * numbers as the Workspace page).
 */

import { Suspense, lazy, useMemo } from "react";
import { ChevronRight } from "lucide-react";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { formatDuration, formatRelative } from "@/lib/format";
import { useWorkspaceSummary, type Run } from "@/lib/queries";
import { runIsFailed } from "@/lib/runStatus";
import { SERIES_COLORS, formatUsd, shortDay } from "@/lib/summary";
import { cn } from "@/lib/utils";
import {
  FAILED_COLOR,
  Legend,
  RUNNING_COLOR,
  Sparkline,
  StackedBars,
  SUCCEEDED_COLOR,
} from "./MiniCharts";

const RunTimingsCard = lazy(() => import("@/components/RunTimingsCard"));

const CHART_RUNS = 30;
const LIST_RUNS = 20;

function resultColor(status: string): string {
  if (runIsFailed(status)) return FAILED_COLOR;
  if (status === "SUCCEEDED") return SUCCEEDED_COLOR;
  return RUNNING_COLOR;
}

function stamp(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  const p = (n: number) => String(n).padStart(2, "0");
  return `${d.getDate()}.${d.getMonth() + 1} ${p(d.getHours())}:${p(d.getMinutes())}`;
}

export function PipelinePhoneView({
  dir,
  runs,
  loading,
  isLocal,
  onOpenRun,
}: {
  dir: string;
  runs: Run[];
  loading: boolean;
  isLocal: boolean;
  onOpenRun: (runId: string) => void;
}) {
  const newestFirst = useMemo(
    () => [...runs].sort((a, b) => b.started_at.localeCompare(a.started_at)),
    [runs],
  );
  const chartRuns = useMemo(
    () => newestFirst.slice(0, CHART_RUNS).reverse(),
    [newestFirst],
  );

  const summary = useWorkspaceSummary();
  const costRuns = useMemo(() => {
    const p = summary.data?.pipelines.find((x) => x.dir === dir);
    return [...(p?.runs ?? [])]
      .filter((r) => r.records > 0)
      .reverse()
      .slice(-CHART_RUNS);
  }, [summary.data, dir]);
  const anyCost = costRuns.some((r) => r.cost_usd > 0);
  const showRate = isLocal || !anyCost;
  const costValues = costRuns.map((r) =>
    showRate
      ? r.duration_ms > 0
        ? r.records / (r.duration_ms / 1000)
        : 0
      : (r.cost_usd / r.records) * 1e9,
  );
  const fmtCost = (v: number) =>
    showRate ? `${Math.round(v).toLocaleString()} records/s` : `${formatUsd(v)}/billion`;
  const hasFailed = chartRuns.some((r) => runIsFailed(r.status));
  const hasRunning = chartRuns.some(
    (r) => !runIsFailed(r.status) && r.status !== "SUCCEEDED",
  );

  return (
    <div className="space-y-4" data-testid="pipeline-phone-view">
      <Card data-testid="pipeline-phone-duration-chart">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm">Duration per run</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2 p-4 pt-0">
          {loading && chartRuns.length === 0 ? (
            <Skeleton className="h-16 w-full" />
          ) : chartRuns.length === 0 ? (
            <p className="text-xs text-muted-foreground">No runs yet.</p>
          ) : (
            <>
              <StackedBars
                testId="pipeline-phone-duration-bars"
                ariaLabel={`Duration of the last ${chartRuns.length} runs, coloured by result`}
                height={80}
                maxBarWidth={18}
                onSelect={onOpenRun}
                data={chartRuns.map((r) => ({
                  key: r.run_id,
                  caption: `${stamp(r.started_at)}: ${formatDuration(r.duration_ms)}, ${r.status.toLowerCase()}`,
                  segments: [
                    {
                      key: "d",
                      value: (r.duration_ms ?? 0) / 1000,
                      color: resultColor(r.status),
                    },
                  ],
                }))}
                axis={{
                  top: `max ${formatDuration(Math.max(0, ...chartRuns.map((r) => r.duration_ms ?? 0)))}`,
                  start: stamp(chartRuns[0].started_at),
                  end: "latest",
                }}
              />
              <Legend
                items={[
                  { key: "s", label: "succeeded", color: SUCCEEDED_COLOR },
                  ...(hasFailed ? [{ key: "f", label: "failed", color: FAILED_COLOR }] : []),
                  ...(hasRunning ? [{ key: "r", label: "running", color: RUNNING_COLOR }] : []),
                ]}
              />
            </>
          )}
        </CardContent>
      </Card>

      <Card data-testid="pipeline-phone-cost-chart">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm">
            {showRate ? "Records per second per run" : "Cost per billion records per run"}
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-1 p-4 pt-0">
          {summary.isLoading ? (
            <Skeleton className="h-12 w-full" />
          ) : costRuns.length === 0 ? (
            <p className="text-xs text-muted-foreground">
              {summary.error ? "Workspace summary unavailable." : "No runs with records in 30 days."}
            </p>
          ) : (
            <Sparkline
              testId="pipeline-phone-cost-sparkline"
              ariaLabel={
                showRate
                  ? "Records per second of each run"
                  : "Cost per billion records of each run"
              }
              color={SERIES_COLORS[0]}
              height={48}
              values={costValues}
              captions={costRuns.map(
                (r, i) =>
                  `${shortDay(r.started_at.slice(0, 10))} ${r.started_at.slice(11, 16)}: ${fmtCost(costValues[i])}`,
              )}
              axis={{
                top: `max ${fmtCost(Math.max(0, ...costValues))}`,
                start: stamp(costRuns[0].started_at),
                end: "latest",
              }}
            />
          )}
        </CardContent>
      </Card>

      <Suspense
        fallback={
          <Card data-testid="run-timings-card" className="opacity-50">
            <CardHeader>
              <CardTitle>Where the time goes</CardTitle>
            </CardHeader>
          </Card>
        }
      >
        <RunTimingsCard dir={dir} />
      </Suspense>

      <Card data-testid="pipeline-phone-runs">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm">Recent runs</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          {loading && newestFirst.length === 0 && (
            <div className="space-y-2 p-4">
              <Skeleton className="h-4 w-full" />
              <Skeleton className="h-4 w-2/3" />
            </div>
          )}
          {!loading && newestFirst.length === 0 && (
            <p className="p-4 text-xs text-muted-foreground">No runs yet.</p>
          )}
          <ul className="divide-y divide-border">
            {newestFirst.slice(0, LIST_RUNS).map((r) => (
              <li key={r.run_id}>
                <button
                  type="button"
                  data-testid="pipeline-phone-run-row"
                  data-run-id={r.run_id}
                  onClick={() => onOpenRun(r.run_id)}
                  className="flex w-full min-w-0 items-center gap-3 px-4 py-3 text-left hover:bg-muted/40"
                >
                  <span
                    className="inline-block size-2 shrink-0 rounded-full"
                    style={{ background: resultColor(r.status) }}
                  />
                  <span className="min-w-0 flex-1">
                    <span className="flex items-baseline justify-between gap-2 text-sm">
                      <span className="truncate">{formatRelative(r.started_at)}</span>
                      <span className="shrink-0 tabular-nums text-muted-foreground">
                        {formatDuration(r.duration_ms)}
                      </span>
                    </span>
                    <span
                      className={cn(
                        "block truncate text-xs",
                        runIsFailed(r.status) ? "text-status-failed" : "text-muted-foreground",
                      )}
                    >
                      {r.status.toLowerCase()}
                      {r.trigger ? ` · ${r.trigger}` : ""}
                      {r.failed_step ? ` · ${r.failed_step}` : ""}
                    </span>
                  </span>
                  <ChevronRight className="h-4 w-4 shrink-0 text-muted-foreground" />
                </button>
              </li>
            ))}
          </ul>
        </CardContent>
      </Card>
    </div>
  );
}
