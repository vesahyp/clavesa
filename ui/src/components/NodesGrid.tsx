/**
 * NodesGrid — the pipeline dashboard's "Nodes" view.
 *
 * One row per pipeline node, in topological order. The sticky left panel
 * shows the node's identity and its output table's current state; to the
 * right is a run matrix — one column per run, grouped by day, newest on
 * the right. Each cell is a bar whose height encodes that node's runtime
 * for that run (Airflow-style), so a row reads as the node's duration
 * trend. Clicking a node or a cell opens its detail in a right drawer.
 */

import { useLayoutEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { Play } from "lucide-react";

import { cn } from "@/lib/utils";
import { formatDuration, formatRelative } from "@/lib/format";
import { runVariant, type StatusVariant } from "@/lib/runStatus";
import {
  NodeDetailDrawer,
  type NodeInvocation,
  type NodeSpec,
} from "./NodeDetailDrawer";
import type { NodeRun, Run } from "@/lib/queries";

// Per-cell state. `missing` (no row for a finished run) is drawn distinct
// from `skipped` so absence never reads as failure.
type CellState = "ok" | "failed" | "running" | "skipped" | "pending" | "missing";

// Run status → play-icon tint for the column headers.
const VARIANT_TEXT: Record<StatusVariant, string> = {
  success: "text-status-success",
  failed: "text-status-failed",
  running: "text-status-running animate-pulse",
  outline: "text-muted-foreground",
};

/** Per-node output-table state for the left panel. */
export interface NodeInfo {
  /** Display table name (`__default` suffix already stripped). */
  table: string;
  /** TableDetail link, or null when the table can't be resolved. */
  href: string | null;
  rowCount: number | null;
  snapshotTs: string | null;
}

function nodeCellState(nr: NodeRun): CellState {
  if (nr.status === "ok") return "ok";
  if (nr.status === "failed") return "failed";
  if (nr.status === "running") return "running";
  return "skipped"; // skipped / unknown
}

/** HH:mm for a run column header. */
function runTime(iso: string): string {
  const d = new Date(iso);
  if (!iso || Number.isNaN(d.getTime())) return "—";
  return d.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" });
}

/** Full local timestamp for the run hover tooltip. */
function runWhen(iso: string): string {
  if (!iso) return "unknown time";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
}

/** Stable per-calendar-day key for grouping run columns. */
function dayKey(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "unknown";
  return `${d.getFullYear()}-${d.getMonth()}-${d.getDate()}`;
}

/** Today / Yesterday / "MMM D" label for a day group. */
function dayLabel(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "unknown";
  const start = (x: Date) => new Date(x.getFullYear(), x.getMonth(), x.getDate());
  const diff = Math.round(
    (start(new Date()).getTime() - start(d).getTime()) / 86_400_000,
  );
  if (diff === 0) return "Today";
  if (diff === 1) return "Yesterday";
  return d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

const FULL_BAR = 28;
const MIN_BAR = 4;

/** The bar drawn inside one grid cell. ok/failed encode duration as height. */
function CellBar({
  state,
  durationMs,
  nodeMax,
}: {
  state: CellState;
  durationMs: number | null | undefined;
  nodeMax: number;
}) {
  if (state === "missing") {
    return <div className="h-1 w-5 rounded-sm border border-dashed border-border" />;
  }
  if (state === "pending") {
    return <div className="h-1 w-5 rounded-sm bg-muted-foreground/25" />;
  }
  if (state === "running") {
    return <div className="h-7 w-5 animate-pulse rounded-sm bg-status-running" />;
  }
  if (state === "skipped") {
    return <div className="h-1.5 w-5 rounded-sm bg-muted-foreground/40" />;
  }
  const h =
    durationMs && durationMs > 0 && nodeMax > 0
      ? Math.max(MIN_BAR, Math.round((durationMs / nodeMax) * FULL_BAR))
      : MIN_BAR;
  return (
    <div
      className={cn(
        "w-5 rounded-sm",
        state === "ok" ? "bg-status-success" : "bg-status-failed",
      )}
      style={{ height: `${h}px` }}
    />
  );
}

const LEGEND: { label: string; node: React.ReactNode }[] = [
  { label: "ok", node: <span className="h-3 w-3 rounded-sm bg-status-success" /> },
  { label: "failed", node: <span className="h-3 w-3 rounded-sm bg-status-failed" /> },
  { label: "running", node: <span className="h-3 w-3 rounded-sm bg-status-running" /> },
  { label: "skipped", node: <span className="h-1.5 w-3 rounded-sm bg-muted-foreground/40" /> },
  {
    label: "didn't run",
    node: <span className="h-1 w-3 rounded-sm border border-dashed border-border" />,
  },
];

export interface NodesGridProps {
  runs: Run[];
  nodeRuns: NodeRun[];
  /** Pipeline nodes in topological order — the grid rows. */
  nodeOrder: string[];
  /** Per-node output-table state for the left panel. */
  nodeInfo: Map<string, NodeInfo>;
  /** Per-node static spec (inputs, output mode) for the detail drawer. */
  nodeSpecs: Map<string, NodeSpec>;
  /** Live per-node status for an in-flight run (node id → state). */
  liveStates: Map<string, "running" | "succeeded" | "failed">;
  /** Local pipeline — selects the execution-logs addressing mode. */
  isLocal: boolean;
  dir: string;
}

export function NodesGrid({
  runs,
  nodeRuns,
  nodeOrder,
  nodeInfo,
  nodeSpecs,
  liveStates,
  isLocal,
  dir,
}: NodesGridProps) {
  const navigate = useNavigate();
  const scrollRef = useRef<HTMLDivElement>(null);
  const [selected, setSelected] = useState<{
    node: string;
    runId: string | null;
  } | null>(null);

  // Oldest → newest, left → right (time flows rightward).
  const cols = useMemo(
    () => [...runs].sort((a, b) => a.started_at.localeCompare(b.started_at)),
    [runs],
  );

  // Consecutive runs on the same calendar day, for the grouped header and
  // the day-divider border on the first column of each group.
  const dayGroups = useMemo(() => {
    const groups: { key: string; label: string; count: number }[] = [];
    for (const r of cols) {
      const k = dayKey(r.started_at);
      const last = groups[groups.length - 1];
      if (last && last.key === k) last.count += 1;
      else groups.push({ key: k, label: dayLabel(r.started_at), count: 1 });
    }
    return groups;
  }, [cols]);
  const firstOfDay = useMemo(() => {
    const set = new Set<string>();
    let prev = "";
    for (const r of cols) {
      const k = dayKey(r.started_at);
      if (k !== prev) set.add(r.run_id);
      prev = k;
    }
    return set;
  }, [cols]);

  // Land scrolled to the right edge so the newest runs show without scroll.
  useLayoutEffect(() => {
    const el = scrollRef.current;
    if (el) el.scrollLeft = el.scrollWidth;
  }, [cols.length]);

  // execution arn → node → NodeRun. node_runs join to runs on
  // sf_execution_arn (a node_run's own run_id is per-invocation).
  const matrix = useMemo(() => {
    const m = new Map<string, Map<string, NodeRun>>();
    for (const nr of nodeRuns) {
      const key = nr.sf_execution_arn || nr.run_id;
      const inner = m.get(key) ?? new Map<string, NodeRun>();
      const prev = inner.get(nr.node);
      if (!prev || prev.started_at < nr.started_at) inner.set(nr.node, nr);
      m.set(key, inner);
    }
    return m;
  }, [nodeRuns]);

  // Longest duration per node, for per-row bar normalization.
  const nodeMax = useMemo(() => {
    const m = new Map<string, number>();
    for (const nr of nodeRuns) {
      if (nr.duration_ms && nr.duration_ms > 0) {
        m.set(nr.node, Math.max(m.get(nr.node) ?? 0, nr.duration_ms));
      }
    }
    return m;
  }, [nodeRuns]);

  // Invocations for the open drawer's node, newest first.
  const drawerInvocations = useMemo<NodeInvocation[]>(() => {
    if (!selected) return [];
    const out: NodeInvocation[] = [];
    for (let i = cols.length - 1; i >= 0; i--) {
      const r = cols[i];
      const nr = matrix.get(r.sf_execution_arn || r.run_id)?.get(selected.node);
      if (nr) out.push({ nodeRun: nr, runId: r.run_id });
    }
    return out;
  }, [selected, cols, matrix]);

  function openRun(runId: string) {
    navigate(
      `/pipelines/run?dir=${encodeURIComponent(dir)}&run=${encodeURIComponent(runId)}`,
    );
  }

  if (nodeOrder.length === 0) {
    return (
      <div className="p-6 text-sm text-muted-foreground">
        This pipeline has no transform nodes yet.
      </div>
    );
  }

  return (
    <div className="p-4">
      <div ref={scrollRef} className="overflow-x-auto">
        <table className="border-separate border-spacing-0">
          <thead>
            {/* Day-group band */}
            <tr>
              <th className="sticky left-0 z-10 bg-card pb-1 text-left">
                <span className="text-[10px] font-medium uppercase tracking-wide text-muted-foreground">
                  Node · output table
                </span>
              </th>
              {dayGroups.map((g, gi) => (
                <th
                  key={g.key}
                  colSpan={g.count}
                  className={cn(
                    "pb-1 pl-1.5 text-left",
                    gi > 0 && "border-l border-border",
                  )}
                >
                  <span className="text-[10px] font-medium uppercase tracking-wide text-muted-foreground">
                    {g.label}
                  </span>
                </th>
              ))}
            </tr>
            {/* Per-run header — play icon + time */}
            <tr>
              <th className="sticky left-0 z-10 bg-card" />
              {cols.map((r) => (
                <th
                  key={r.run_id}
                  className={cn(
                    "p-0 align-bottom",
                    firstOfDay.has(r.run_id) && "border-l border-border",
                  )}
                >
                  <button
                    onClick={() => openRun(r.run_id)}
                    title={`${r.status} · ${runWhen(r.started_at)} · ${formatDuration(r.duration_ms)}`}
                    aria-label={`Run ${r.run_id}`}
                    className="flex w-14 flex-col items-center gap-0.5 rounded-md pb-1 pt-1.5 transition-colors hover:bg-muted/60"
                  >
                    <Play
                      className={cn(
                        "h-3 w-3 fill-current",
                        VARIANT_TEXT[runVariant(r.status)],
                      )}
                    />
                    <span className="font-mono text-[10px] leading-none text-foreground">
                      {runTime(r.started_at)}
                    </span>
                    <span className="font-mono text-[9px] leading-none text-muted-foreground">
                      {formatDuration(r.duration_ms)}
                    </span>
                  </button>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {nodeOrder.map((node) => {
              const info = nodeInfo.get(node);
              const max = nodeMax.get(node) ?? 0;
              const isSelectedNode = selected?.node === node;
              return (
                <tr key={node} className="group">
                  <td
                    className={cn(
                      "sticky left-0 z-10 bg-card group-hover:bg-muted/40",
                      isSelectedNode && "bg-muted/50",
                    )}
                  >
                    <div className="flex items-center gap-3 py-1.5 pr-6">
                      <button
                        onClick={() => setSelected({ node, runId: null })}
                        className="w-36 shrink-0 truncate text-left font-mono text-xs font-medium hover:text-primary"
                        title={`${node} — open detail`}
                      >
                        {node}
                      </button>
                      <div className="w-44 min-w-0">
                        {info?.href ? (
                          <span className="block truncate font-mono text-xs text-sky-300">
                            {info.table}
                          </span>
                        ) : (
                          <span className="font-mono text-xs text-muted-foreground">
                            —
                          </span>
                        )}
                        <div className="truncate text-[10px] text-muted-foreground">
                          {info && info.rowCount != null
                            ? `${info.rowCount.toLocaleString()} rows`
                            : "no data yet"}
                          {info?.snapshotTs && (
                            <> · {formatRelative(info.snapshotTs)}</>
                          )}
                        </div>
                      </div>
                    </div>
                  </td>
                  {cols.map((r) => {
                    const nr = matrix
                      .get(r.sf_execution_arn || r.run_id)
                      ?.get(node);
                    let state: CellState;
                    if (nr) {
                      state = nodeCellState(nr);
                    } else if (r.status === "RUNNING") {
                      state =
                        liveStates.get(node) === "running"
                          ? "running"
                          : "pending";
                    } else {
                      state = "missing";
                    }
                    const detail = nr
                      ? `${node} · ${nr.status}` +
                        (nr.duration_ms != null
                          ? ` · ${formatDuration(nr.duration_ms)}`
                          : "") +
                        (nr.output_rows != null
                          ? ` · ${nr.output_rows} rows`
                          : "") +
                        (nr.error_class ? ` · ${nr.error_class}` : "")
                      : `${node} · ${state}`;
                    return (
                      <td
                        key={r.run_id}
                        className={cn(
                          "p-0",
                          firstOfDay.has(r.run_id) && "border-l border-border",
                        )}
                      >
                        <button
                          onClick={() =>
                            setSelected({ node, runId: r.run_id })
                          }
                          title={detail}
                          aria-label={detail}
                          className={cn(
                            "flex h-9 w-14 items-end justify-center pb-1 transition-colors hover:bg-muted/60",
                            selected?.node === node &&
                              selected.runId === r.run_id &&
                              "bg-muted/60",
                          )}
                        >
                          <CellBar
                            state={state}
                            durationMs={nr?.duration_ms}
                            nodeMax={max}
                          />
                        </button>
                      </td>
                    );
                  })}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {cols.length === 0 && (
        <div className="mt-3 text-sm text-muted-foreground">
          No runs yet. Trigger one with <strong>Run pipeline</strong>; each run
          adds a column here.
        </div>
      )}

      <div className="mt-3 flex flex-wrap items-center gap-3 text-[11px] text-muted-foreground">
        {LEGEND.map((l) => (
          <span key={l.label} className="flex items-center gap-1.5">
            {l.node}
            {l.label}
          </span>
        ))}
        <span className="text-muted-foreground/70">· taller bar = longer runtime</span>
      </div>

      {selected && (
        <NodeDetailDrawer
          node={selected.node}
          tableLabel={nodeInfo.get(selected.node)?.table ?? null}
          tableHref={nodeInfo.get(selected.node)?.href ?? null}
          spec={nodeSpecs.get(selected.node) ?? null}
          invocations={drawerInvocations}
          selectedRunId={selected.runId}
          onSelectRun={(runId) =>
            setSelected((s) => (s ? { ...s, runId } : s))
          }
          isLocal={isLocal}
          dir={dir}
          onClose={() => setSelected(null)}
        />
      )}
    </div>
  );
}
