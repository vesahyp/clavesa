/**
 * NodeDetailDrawer — inline detail for one pipeline node.
 *
 * Opens from the right of the pipeline dashboard's Nodes grid. Shows what
 * the node *is* (inputs, output table + write mode) and what a chosen run
 * actually *did* (rows written, cold start, compute, runner build, error)
 * — substance, not a restatement of the grid. A footer link still opens
 * the full run page for the whole-run DAG.
 */

import { useEffect } from "react";
import { Link } from "react-router-dom";
import { ArrowDown, ArrowUpRight, X } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { formatBytes, formatDuration, formatRelative } from "@/lib/format";
import { nodeVariant } from "@/lib/runStatus";
import { useExecutionLogs, useRightsize } from "@/lib/queries";
import type { NodeRun } from "@/lib/queries";

/** One upstream this node reads, derived from the lineage graph. */
export interface NodeInput {
  /** Producer label — `sources.<name>` or an upstream node id. */
  from: string;
  /** Lineage edge type — "source-registry", "transform", … */
  kind: string;
  /** Catalog table read, when the upstream is a Delta producer. */
  table: string;
}

/** Static node spec, the same for every run. */
export interface NodeSpec {
  language: string;
  /** Delta write mode — replace / append / merge. */
  outputMode: string;
  mergeKeys: string[];
  inputs: NodeInput[];
}

/** One node invocation paired with the pipeline run it belongs to. */
export interface NodeInvocation {
  nodeRun: NodeRun;
  runId: string;
}

export interface NodeDetailDrawerProps {
  node: string;
  tableLabel: string | null;
  tableHref: string | null;
  spec: NodeSpec | null;
  /** The node's invocations, newest first. */
  invocations: NodeInvocation[];
  /** The run currently shown in detail. */
  selectedRunId: string | null;
  onSelectRun: (runId: string) => void;
  /** Local warehouse (ADR-024) — selects the execution-logs addressing mode. */
  isLocal: boolean;
  dir: string;
  /**
   * SQL pipeline name for the rightsizing query (the dir basename, same
   * value used for node-runs). Empty disables the rightsizing card.
   */
  pipelineName: string;
  onClose: () => void;
}

/** Step logs for one node invocation — CloudWatch (cloud) or files (local). */
function StepLogs({
  node,
  inv,
  isLocal,
  dir,
}: {
  node: string;
  inv: NodeInvocation;
  isLocal: boolean;
  dir: string;
}) {
  const logs = useExecutionLogs(
    isLocal
      ? { dir, run: inv.runId, step: node }
      : { arn: inv.nodeRun.sf_execution_arn, step: node },
  );
  if (logs.isLoading) {
    return <div className="text-xs text-muted-foreground">Loading logs…</div>;
  }
  if (logs.error) {
    return (
      <div className="text-xs text-muted-foreground">
        Logs unavailable for this run.
      </div>
    );
  }
  const events = logs.data?.events ?? [];
  if (events.length === 0) {
    return (
      <div className="text-xs text-muted-foreground">
        No log lines recorded for this step.
      </div>
    );
  }
  return (
    <>
      <pre className="max-h-72 overflow-auto rounded bg-muted/40 p-2 font-mono text-[9px] leading-snug text-muted-foreground">
        {events.map((e) => e.message).join("\n")}
      </pre>
      {(logs.data?.log_group || logs.data?.truncated) && (
        <div className="mt-1 text-[10px] text-muted-foreground">
          {logs.data?.truncated && "truncated · "}
          {logs.data?.log_group}
        </div>
      )}
    </>
  );
}

function Section({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div className="border-b border-border px-4 py-3">
      <div className="mb-1.5 text-[10px] font-medium uppercase tracking-wide text-muted-foreground">
        {label}
      </div>
      {children}
    </div>
  );
}

function Field({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex items-baseline justify-between gap-3 py-0.5 text-xs">
      <span className="text-muted-foreground">{label}</span>
      <span className="text-right font-mono text-foreground">{value}</span>
    </div>
  );
}

/** Compact integer formatter (1,234,567) with em-dash fallback. */
function num(n: number | null | undefined): string {
  return n != null ? n.toLocaleString() : "—";
}

/**
 * Per-invocation Spark aggregates. Only rendered when at least one metric is
 * non-null — older runners and skipped/path-mode runs leave them all nil, and
 * an empty card would just be noise.
 */
function SparkMetrics({ nr }: { nr: NodeRun }) {
  const has =
    nr.memory_spilled_bytes != null ||
    nr.disk_spilled_bytes != null ||
    nr.shuffle_read_bytes != null ||
    nr.shuffle_write_bytes != null ||
    nr.jvm_gc_time_ms != null ||
    nr.num_tasks != null ||
    nr.num_failed_tasks != null ||
    nr.num_stages != null ||
    nr.input_records != null ||
    nr.input_bytes != null;
  if (!has) return null;

  const spill =
    (nr.memory_spilled_bytes ?? 0) + (nr.disk_spilled_bytes ?? 0);
  const spillKnown =
    nr.memory_spilled_bytes != null || nr.disk_spilled_bytes != null;

  return (
    <Section label="Spark metrics">
      {spillKnown && (
        <Field
          label="Spill"
          value={
            spill === 0 ? (
              <span className="text-muted-foreground">none</span>
            ) : (
              `${formatBytes(nr.memory_spilled_bytes ?? 0)} mem · ${formatBytes(
                nr.disk_spilled_bytes ?? 0,
              )} disk`
            )
          }
        />
      )}
      {(nr.shuffle_read_bytes != null || nr.shuffle_write_bytes != null) && (
        <Field
          label="Shuffle"
          value={`${formatBytes(nr.shuffle_read_bytes)} read · ${formatBytes(
            nr.shuffle_write_bytes,
          )} write`}
        />
      )}
      {(nr.input_records != null || nr.input_bytes != null) && (
        <Field
          label="Input"
          value={`${num(nr.input_records)} rows · ${formatBytes(
            nr.input_bytes,
          )}`}
        />
      )}
      {(nr.num_tasks != null || nr.num_failed_tasks != null) && (
        <Field
          label="Tasks"
          value={
            <>
              {num(nr.num_tasks)} total
              {nr.num_failed_tasks != null && nr.num_failed_tasks > 0 && (
                <span className="text-status-failed">
                  {" "}
                  · {num(nr.num_failed_tasks)} failed
                </span>
              )}
            </>
          }
        />
      )}
      {nr.num_stages != null && (
        <Field label="Stages" value={num(nr.num_stages)} />
      )}
      {nr.jvm_gc_time_ms != null && (
        <Field label="GC time" value={`${num(nr.jvm_gc_time_ms)} ms`} />
      )}
    </Section>
  );
}

/** Badge color for a rightsize confidence label. */
function confidenceVariant(
  c: string,
): "default" | "secondary" | "outline" {
  if (c === "high") return "default";
  if (c === "medium") return "secondary";
  return "outline";
}

/**
 * Rightsizing recommendation for this node, computed across its recent runs
 * (p95 peak RSS vs allocated memory, factoring spill). Recommend-only — it
 * surfaces advice, nothing here re-deploys. Renders muted when confidence is
 * "n/a" (no allocation on record, or no metric-bearing runs yet) and hides
 * entirely until the query resolves with a row for this node.
 */
function Rightsizing({
  node,
  pipelineName,
  dir,
}: {
  node: string;
  pipelineName: string;
  dir: string;
}) {
  const rs = useRightsize(pipelineName, dir, {
    enabled: Boolean(pipelineName),
  });
  const row = rs.data?.rows.find((r) => r.node === node);
  if (!row) return null;

  const na = row.confidence === "n/a";
  return (
    <Section label="Rightsizing">
      <div className="mb-1.5 flex items-center gap-2">
        <Badge
          variant={confidenceVariant(row.confidence)}
          className="text-[9px] uppercase"
        >
          {row.confidence}
        </Badge>
        <span className="text-[10px] text-muted-foreground">
          {row.samples} sample{row.samples === 1 ? "" : "s"}
        </span>
      </div>
      <Field
        label="Current"
        value={row.current_mb != null ? `${row.current_mb} MB` : "—"}
      />
      <Field
        label="Recommended"
        value={
          row.recommended_mb != null ? (
            <span className={na ? "text-muted-foreground" : "text-foreground"}>
              {row.recommended_mb} MB
            </span>
          ) : (
            "—"
          )
        }
      />
      {row.p95_peak_rss_mb != null && (
        <Field label="p95 peak" value={`${row.p95_peak_rss_mb} MB`} />
      )}
      <div
        className={cn(
          "mt-1.5 text-[11px] leading-snug",
          na ? "text-muted-foreground/70" : "text-muted-foreground",
        )}
      >
        {row.reason}
      </div>
    </Section>
  );
}

/** The chosen run's invocation facts — what the run actually did. */
function RunFacts({ nr, outputMode }: { nr: NodeRun; outputMode: string }) {
  const rowsHint =
    outputMode === "replace" ? "full table" : `${outputMode} delta`;
  return (
    <div>
      <div className="mb-2 flex items-center gap-2">
        <Badge
          variant={nodeVariant(nr.status)}
          className="font-mono text-[10px] uppercase"
        >
          {nr.status}
        </Badge>
        <span className="text-xs text-muted-foreground">
          {nr.started_at ? formatRelative(nr.started_at) : "—"}
        </span>
        <span className="ml-auto font-mono text-xs text-muted-foreground">
          {formatDuration(nr.duration_ms)}
        </span>
      </div>
      <Field
        label="Rows written"
        value={
          nr.output_rows != null ? (
            <>
              {nr.output_rows.toLocaleString()}{" "}
              <span className="text-muted-foreground">· {rowsHint}</span>
            </>
          ) : (
            "—"
          )
        }
      />
      {nr.compute_target && (
        <Field
          label="Compute"
          value={
            nr.memory_mb
              ? `${nr.compute_target} · ${nr.memory_mb} MB`
              : nr.compute_target
          }
        />
      )}
      {nr.peak_rss_mb != null && (
        <Field
          label="Peak memory"
          value={
            nr.memory_mb != null ? (
              <>
                {nr.peak_rss_mb.toLocaleString()} /{" "}
                {nr.memory_mb.toLocaleString()} MB{" "}
                <span className="text-muted-foreground">
                  ({Math.round((nr.peak_rss_mb / nr.memory_mb) * 100)}%)
                </span>
              </>
            ) : (
              `${nr.peak_rss_mb.toLocaleString()} MB`
            )
          }
        />
      )}
      {nr.cold_start != null && (
        <Field label="Cold start" value={nr.cold_start ? "yes" : "no"} />
      )}
      {nr.module_version && (
        <Field label="Module" value={nr.module_version} />
      )}
      {nr.runner_image_digest && (
        <Field
          label="Runner image"
          value={nr.runner_image_digest.replace(/^sha256:/, "").slice(0, 12)}
        />
      )}
      {nr.error_class && (
        <div className="mt-2 text-[11px]">
          <span className="text-status-failed">{nr.error_class}</span>
          {nr.error_msg && (
            <pre className="mt-1 overflow-x-auto whitespace-pre-wrap break-words rounded bg-muted/40 p-2 font-mono text-[11px]">
              {nr.error_msg}
            </pre>
          )}
        </div>
      )}
    </div>
  );
}

export function NodeDetailDrawer({
  node,
  tableLabel,
  tableHref,
  spec,
  invocations,
  selectedRunId,
  onSelectRun,
  isLocal,
  dir,
  pipelineName,
  onClose,
}: NodeDetailDrawerProps) {
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const selected =
    invocations.find((iv) => iv.runId === selectedRunId) ?? invocations[0];
  const openRunId = selected?.runId ?? "";

  return (
    <>
      <div
        className="fixed inset-0 top-14 z-30 bg-background/50"
        onClick={onClose}
        aria-hidden
      />
      <aside className="fixed bottom-0 right-0 top-14 z-40 flex w-[400px] flex-col border-l border-border bg-card shadow-xl">
        <div className="flex items-start justify-between gap-3 border-b border-border px-4 py-3">
          <div className="min-w-0">
            <div className="truncate font-mono text-sm font-semibold">
              {node}
            </div>
            <div className="mt-0.5 flex items-center gap-2">
              {spec?.language && (
                <Badge variant="outline" className="text-[9px] uppercase">
                  {spec.language}
                </Badge>
              )}
            </div>
          </div>
          <button
            onClick={onClose}
            aria-label="Close"
            className="shrink-0 rounded-md p-1 text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        <div className="min-h-0 flex-1 overflow-y-auto">
          {/* Inputs — what the node reads. */}
          <Section label="Inputs">
            {spec && spec.inputs.length > 0 ? (
              <ul className="space-y-1">
                {spec.inputs.map((inp) => (
                  <li key={inp.from} className="text-xs">
                    <span className="font-mono text-foreground">
                      {inp.from}
                    </span>
                    {inp.table && (
                      <span className="ml-1.5 font-mono text-muted-foreground">
                        {inp.table}
                      </span>
                    )}
                  </li>
                ))}
              </ul>
            ) : (
              <span className="text-xs text-muted-foreground">
                No upstream inputs.
              </span>
            )}
          </Section>

          {/* Output — what the node writes, and how. */}
          <Section label="Output">
            <div className="flex items-center gap-1.5 text-xs">
              <ArrowDown className="h-3 w-3 text-muted-foreground" />
              {tableHref && tableLabel ? (
                <Link
                  to={tableHref}
                  className="font-mono text-sky-300 hover:text-sky-200 hover:underline"
                >
                  {tableLabel}
                </Link>
              ) : (
                <span className="font-mono text-muted-foreground">—</span>
              )}
            </div>
            {spec && (
              <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
                <Badge variant="outline" className="text-[9px] uppercase">
                  {spec.outputMode}
                </Badge>
                {spec.mergeKeys.length > 0 && (
                  <span className="font-mono text-[11px] text-muted-foreground">
                    keys: {spec.mergeKeys.join(", ")}
                  </span>
                )}
              </div>
            )}
          </Section>

          {/* The chosen run's facts. */}
          {selected && (
            <Section label="This run">
              <RunFacts
                nr={selected.nodeRun}
                outputMode={spec?.outputMode ?? "replace"}
              />
            </Section>
          )}

          {/* Per-invocation Spark aggregates (renders nothing if all nil). */}
          {selected && <SparkMetrics nr={selected.nodeRun} />}

          {/* Per-node memory recommendation across recent runs (renders
              nothing until the query returns a row for this node). */}
          <Rightsizing node={node} pipelineName={pipelineName} dir={dir} />

          {/* Step logs for the chosen run. */}
          {selected && (
            <Section label="Step logs">
              <StepLogs
                node={node}
                inv={selected}
                isLocal={isLocal}
                dir={dir}
              />
            </Section>
          )}

          {/* Run picker — switch which run "This run" shows. */}
          <Section label={`Run history · ${invocations.length}`}>
            {invocations.length === 0 ? (
              <span className="text-xs text-muted-foreground">
                No runs recorded yet.
              </span>
            ) : (
              <ul className="space-y-0.5">
                {invocations.map(({ nodeRun: nr, runId }) => (
                  <li key={runId + nr.started_at}>
                    <button
                      onClick={() => onSelectRun(runId)}
                      className={cn(
                        "flex w-full items-center gap-2 rounded-md px-2 py-1 text-left text-xs transition-colors hover:bg-muted/60",
                        runId === openRunId && "bg-muted/60",
                      )}
                    >
                      <span
                        className={cn(
                          "h-2 w-2 shrink-0 rounded-sm",
                          nr.status === "ok"
                            ? "bg-status-success"
                            : nr.status === "failed"
                              ? "bg-status-failed"
                              : "bg-muted-foreground/50",
                        )}
                      />
                      <span className="text-muted-foreground">
                        {nr.started_at ? formatRelative(nr.started_at) : "—"}
                      </span>
                      <span className="ml-auto font-mono text-muted-foreground">
                        {formatDuration(nr.duration_ms)}
                      </span>
                    </button>
                  </li>
                ))}
              </ul>
            )}
          </Section>
        </div>

        {openRunId && (
          <div className="border-t border-border p-3">
            <Link
              to={`/pipelines/run?dir=${encodeURIComponent(dir)}&run=${encodeURIComponent(openRunId)}`}
              className="flex items-center justify-center gap-1.5 rounded-md border border-border py-2 text-xs font-medium text-foreground transition-colors hover:bg-muted"
            >
              Open full run
              <ArrowUpRight className="h-3.5 w-3.5" />
            </Link>
          </div>
        )}
      </aside>
    </>
  );
}
