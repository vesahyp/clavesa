/**
 * RunPhoneView — one run below md (ADR-032 section 1, page 4), in the run
 * sheet instead of RunDetailView: the run's result, the node timeline
 * (when each node started and how long it ran, coloured by result), rows
 * in and out per node, and for a failed run the error and the last 50 log
 * lines of each failed node.
 *
 * Same queries as RunDetailView (runs, node_runs, live states, execution
 * logs), so the numbers match the desktop sheet.
 */

import { useMemo } from "react";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { formatCompactCount, formatDuration, formatRelative } from "@/lib/format";
import {
  useExecutionLogs,
  useExecutionStates,
  useNodeRuns,
  useRuns,
  useWarehouse,
  type NodeRun,
} from "@/lib/queries";
import { runIsFailed } from "@/lib/runStatus";
import { useSummaryOnly } from "@/hooks/useSummaryOnly";
import { cn } from "@/lib/utils";
import { FAILED_COLOR, Legend, RUNNING_COLOR, SUCCEEDED_COLOR } from "./MiniCharts";

const LOG_LINES = 50;

function nodeColor(status: string): string {
  if (status === "ok" || status === "succeeded") return SUCCEEDED_COLOR;
  if (status === "failed") return FAILED_COLOR;
  return RUNNING_COLOR;
}

export function RunPhoneView({ dir, runId }: { dir: string; runId: string }) {
  const pipelineName = dir.split("/").pop() ?? dir;
  const wh = useWarehouse();
  const isLocal = wh.data?.warehouse === "local";
  // A hosted local workspace keeps node rows for its newest runs only
  // (ADR-032): say so instead of "none recorded".
  const summaryOnly = useSummaryOnly();
  const noNodes = summaryOnly
    ? "Node timings are kept for the 10 newest runs of a local workspace."
    : "No node runs recorded for this run.";

  const states = useExecutionStates({ dir, run: runId });
  const isRunning = states.data?.status === "RUNNING";
  const settled = !states.isLoading && !isRunning;

  const runs = useRuns(pipelineName, { dir, limit: 200, enabled: settled });
  const run = runs.data?.rows.find((r) => r.run_id === runId);
  const arn = run?.sf_execution_arn || runId;
  const nodeRuns = useNodeRuns(pipelineName, {
    dir,
    arn,
    limit: 200,
    enabled: settled,
  });

  const rows = useMemo(
    () =>
      [...(nodeRuns.data?.rows ?? [])].sort((a, b) =>
        a.started_at.localeCompare(b.started_at),
      ),
    [nodeRuns.data],
  );
  const failedNodes = rows.filter((r) => r.status === "failed");
  const failed = (run && runIsFailed(run.status)) || failedNodes.length > 0;
  const status = run?.status ?? states.data?.status ?? "";

  return (
    <div data-testid="run-phone-view" className="h-full space-y-4 overflow-y-auto p-4 pb-[calc(1rem+env(safe-area-inset-bottom))]">
      <div data-testid="run-phone-header" className="text-sm">
        <div className="flex flex-wrap items-baseline gap-x-2">
          <span
            className={cn(
              "font-medium",
              runIsFailed(status) && "text-status-failed",
              status === "SUCCEEDED" && "text-status-success",
            )}
          >
            {status ? status.toLowerCase() : "loading"}
          </span>
          {run && (
            <span className="text-muted-foreground">
              {formatRelative(run.started_at)} · {formatDuration(run.duration_ms)}
              {run.trigger ? ` · ${run.trigger}` : ""}
            </span>
          )}
        </div>
        <div className="truncate font-mono text-[11px] text-muted-foreground">{runId}</div>
      </div>

      {failed && (
        <Card data-testid="run-phone-error" className="border-status-failed/40">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm text-status-failed">Error</CardTitle>
          </CardHeader>
          <CardContent className="space-y-1 p-4 pt-0 text-xs">
            {run?.failed_step && (
              <div>
                <span className="text-muted-foreground">Step </span>
                <span className="font-mono">{run.failed_step}</span>
              </div>
            )}
            {(run?.error_class || run?.error_msg) && (
              <p className="break-words font-mono">
                {run.error_class}
                {run.error_class && run.error_msg ? ": " : ""}
                {run.error_msg}
              </p>
            )}
            {failedNodes.map((n) =>
              n.error_msg ? (
                <p key={n.node} className="break-words font-mono">
                  <span className="text-muted-foreground">{n.node}: </span>
                  {n.error_class ? `${n.error_class}: ` : ""}
                  {n.error_msg}
                </p>
              ) : null,
            )}
          </CardContent>
        </Card>
      )}

      <Card data-testid="run-phone-timeline">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm">Node timeline</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2 p-4 pt-0">
          {isRunning ? (
            <p className="text-xs text-muted-foreground">
              Run in progress. The timeline appears when it completes.
            </p>
          ) : nodeRuns.isLoading || !settled ? (
            <Skeleton className="h-16 w-full" />
          ) : rows.length === 0 ? (
            <p data-testid="run-phone-no-nodes" className="text-xs text-muted-foreground">{noNodes}</p>
          ) : (
            <>
              <Timeline rows={rows} />
              <Legend
                items={[
                  { key: "ok", label: "succeeded", color: SUCCEEDED_COLOR },
                  ...(failedNodes.length ? [{ key: "f", label: "failed", color: FAILED_COLOR }] : []),
                ]}
              />
            </>
          )}
        </CardContent>
      </Card>

      <Card data-testid="run-phone-node-rows">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm">Rows in and out per node</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          {rows.length === 0 ? (
            <p className="p-4 pt-0 text-xs text-muted-foreground">
              {nodeRuns.isLoading || !settled ? "Loading…" : summaryOnly ? noNodes : "No node runs."}
            </p>
          ) : (
            <ul className="divide-y divide-border">
              {rows.map((r) => (
                <li
                  key={r.node + r.started_at}
                  data-testid="run-phone-node-row"
                  className="flex min-w-0 items-center gap-3 px-4 py-2.5 text-sm"
                >
                  <span
                    className="inline-block size-2 shrink-0 rounded-full"
                    style={{ background: nodeColor(r.status) }}
                  />
                  <span className="min-w-0 flex-1 truncate font-mono text-xs">{r.node}</span>
                  <span className="shrink-0 tabular-nums text-xs text-muted-foreground">
                    {r.input_records != null ? formatCompactCount(r.input_records) : "—"}
                    {" → "}
                    <span className="text-foreground">
                      {r.output_rows != null ? formatCompactCount(r.output_rows) : "—"}
                    </span>
                  </span>
                </li>
              ))}
            </ul>
          )}
        </CardContent>
      </Card>

      {failedNodes.map((n) => (
        <NodeLogTail key={n.node} row={n} isLocal={isLocal} dir={dir} runId={runId} />
      ))}
    </div>
  );
}

/**
 * Timeline: one row per node, its bar placed at its start offset from the
 * run's first node and as long as it ran. The scale is the run's wall time.
 */
function Timeline({ rows }: { rows: NodeRun[] }) {
  const t0 = Math.min(...rows.map((r) => Date.parse(r.started_at)).filter((n) => !Number.isNaN(n)));
  const spans = rows.map((r) => {
    const s = Date.parse(r.started_at) - t0;
    const d = r.duration_ms ?? 0;
    return { r, s: Number.isNaN(s) ? 0 : s, d };
  });
  const total = Math.max(1, ...spans.map((x) => x.s + x.d));
  return (
    <div data-testid="run-phone-timeline-chart" role="img" aria-label="Node timeline">
      <div className="space-y-1.5">
        {spans.map(({ r, s, d }) => (
          <div key={r.node + r.started_at} className="min-w-0">
            <div className="flex items-baseline justify-between gap-2 text-[11px]">
              <span className="truncate font-mono">{r.node}</span>
              <span className="shrink-0 tabular-nums text-muted-foreground">
                +{formatDuration(s)} · {formatDuration(d)}
              </span>
            </div>
            <div className="relative h-2 w-full rounded-sm bg-muted/50">
              <span
                className="absolute inset-y-0 rounded-sm"
                style={{
                  left: `${(s / total) * 100}%`,
                  width: `max(3px, ${(d / total) * 100}%)`,
                  background: nodeColor(r.status),
                }}
              />
            </div>
          </div>
        ))}
      </div>
      <div className="mt-1 flex justify-between text-[10px] tabular-nums text-muted-foreground">
        <span>0s</span>
        <span>{formatDuration(total)}</span>
      </div>
    </div>
  );
}

function NodeLogTail({
  row,
  isLocal,
  dir,
  runId,
}: {
  row: NodeRun;
  isLocal: boolean;
  dir: string;
  runId: string;
}) {
  const logs = useExecutionLogs(
    isLocal
      ? { dir, run: runId, step: row.node }
      : { arn: row.sf_execution_arn, step: row.node },
  );
  const lines = (logs.data?.events ?? []).map((e) => e.message).slice(-LOG_LINES);
  return (
    <Card data-testid="run-phone-logs">
      <CardHeader className="pb-2">
        <CardTitle className="text-sm">
          Last {LOG_LINES} log lines · <span className="font-mono">{row.node}</span>
        </CardTitle>
      </CardHeader>
      <CardContent className="p-4 pt-0">
        {logs.isLoading ? (
          <Skeleton className="h-24 w-full" />
        ) : logs.error ? (
          <p className="text-xs text-muted-foreground">Logs unavailable for this run.</p>
        ) : lines.length === 0 ? (
          <p className="text-xs text-muted-foreground">No log lines recorded for this step.</p>
        ) : (
          <pre className="max-h-96 overflow-auto whitespace-pre-wrap break-all rounded bg-muted/40 p-2 font-mono text-[10px] leading-snug text-muted-foreground">
            {lines.join("\n")}
          </pre>
        )}
      </CardContent>
    </Card>
  );
}
