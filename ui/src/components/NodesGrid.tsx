/**
 * NodesGrid — the pipeline dashboard's "Runs" view.
 *
 * One row per pipeline node, in topological order. The sticky left panel
 * shows the node's identity and its output table's current state; to the
 * right is a run matrix — one column per run, grouped by day, newest on
 * the right. Each cell is an Airflow-style status square: solid colored
 * background + status icon overlay (✓ ok, ✗ failed, spinner running,
 * skip-arrow skipped, dashed empty for missing). Clicking a node or a
 * cell opens its detail in a right drawer; clicking a column header
 * opens the run-detail Sheet (handled by the parent).
 */

import {
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { Check, ChevronLeft, ChevronRight, Loader2, SkipForward, X } from "lucide-react";

import { cn } from "@/lib/utils";
import { formatDuration, formatRelative } from "@/lib/format";
import { runVariant, type StatusVariant } from "@/lib/runStatus";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  NodeDetailDrawer,
  type NodeInvocation,
  type NodeSpec,
} from "./NodeDetailDrawer";
import type { NodeRun, Run } from "@/lib/queries";

// Per-cell state. `missing` (no row for a finished run) is drawn distinct
// from `skipped` so absence never reads as failure.
type CellState = "ok" | "failed" | "running" | "skipped" | "pending" | "missing";

// Run status → bar fill for the Airflow-style duration bar in column headers.
const VARIANT_BAR_BG: Record<StatusVariant, string> = {
  success: "bg-status-success",
  failed: "bg-status-failed",
  running: "bg-status-running animate-pulse",
  outline: "bg-muted-foreground/40",
};

// Bar geometry — fits inside the column header without pushing the
// day-group band.
const BAR_MIN_PX = 3;
const BAR_MAX_PX = 32;
// Mid-height for in-flight runs (no duration_ms yet) and any zero-duration
// edge case, so the bar is still visible.
const BAR_MID_PX = 16;

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

/**
 * Visual recipe for one status. Square colour + icon are the only two
 * channels that vary; sharing the recipe keeps the cell, the legend, and
 * any future surface (run header dots, breakdown chips) in lockstep.
 */
const CELL_VARIANTS: Record<
  CellState,
  { bg: string; icon: React.ComponentType<{ className?: string }> | null; label: string }
> = {
  ok: { bg: "bg-status-success", icon: Check, label: "ok" },
  failed: { bg: "bg-status-failed", icon: X, label: "failed" },
  running: { bg: "bg-status-running", icon: Loader2, label: "running" },
  skipped: { bg: "bg-status-skipped", icon: SkipForward, label: "skipped" },
  pending: { bg: "bg-muted-foreground/25", icon: null, label: "queued" },
  missing: { bg: "", icon: null, label: "no record" },
};

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

/** Status square drawn inside one grid cell. */
function CellSquare({ state }: { state: CellState }) {
  if (state === "missing") {
    return (
      <div className="h-6 w-6 rounded-md border border-dashed border-border" />
    );
  }
  const v = CELL_VARIANTS[state];
  const Icon = v.icon;
  return (
    <div
      className={cn(
        "grid h-6 w-6 place-items-center rounded-md",
        v.bg,
        state === "pending" && "animate-pulse",
      )}
    >
      {Icon && (
        <Icon
          className={cn(
            "h-3.5 w-3.5 text-white",
            state === "running" && "animate-spin",
          )}
        />
      )}
    </div>
  );
}

const LEGEND: { state: CellState; label: string }[] = [
  { state: "ok", label: "ok" },
  { state: "failed", label: "failed" },
  { state: "running", label: "running" },
  { state: "skipped", label: "skipped" },
  { state: "missing", label: "no record" },
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
  /** SQL pipeline name, threaded to the drawer's rightsizing query. */
  pipelineName: string;
  /**
   * Click on a run column header. Parent owns URL state; this lets the
   * dashboard open the RunDetail Sheet (and persist `?run=…`) instead of
   * the grid navigating to a separate page.
   */
  onRunSelect: (runId: string) => void;
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
  pipelineName,
  onRunSelect,
}: NodesGridProps) {
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

  // Max duration across the visible runs; used to normalize the
  // Airflow-style runtime bars in the column header. Excludes nulls
  // (in-flight) and non-positives (shouldn't happen but defensive).
  const maxDurationMs = useMemo(() => {
    let m = 0;
    for (const r of cols) {
      if (r.duration_ms != null && r.duration_ms > m) m = r.duration_ms;
    }
    return m;
  }, [cols]);

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

  // Track overflow so the edge fades + scroll buttons appear only when
  // there's actually something hidden. Without this affordance the grid
  // looks like the whole run history fits on screen even when 6+ runs
  // are scrolled off the left edge.
  const [scrollState, setScrollState] = useState({
    canLeft: false,
    canRight: false,
    hiddenLeftCount: 0,
    hiddenRightCount: 0,
  });
  useEffect(() => {
    const el = scrollRef.current;
    if (!el) return;
    const COL_PX = 56; // matches `w-14` on the run-header cells
    const update = () => {
      const slack = 1; // sub-pixel rounding tolerance
      const right = el.scrollWidth - el.clientWidth - el.scrollLeft;
      setScrollState({
        canLeft: el.scrollLeft > slack,
        canRight: right > slack,
        hiddenLeftCount: Math.round(el.scrollLeft / COL_PX),
        hiddenRightCount: Math.round(Math.max(0, right) / COL_PX),
      });
    };
    update();
    el.addEventListener("scroll", update, { passive: true });
    // ResizeObserver catches sidebar collapse / window resize so the
    // affordance disappears the moment the grid actually fits.
    const ro = new ResizeObserver(update);
    ro.observe(el);
    return () => {
      el.removeEventListener("scroll", update);
      ro.disconnect();
    };
  }, [cols.length, nodeOrder.length]);
  const scrollBy = (dx: number) => {
    const el = scrollRef.current;
    if (el) el.scrollBy({ left: dx, behavior: "smooth" });
  };

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

  // Run-header click → parent's URL-state owner opens the Sheet.
  // `dir` stays in props for the per-node drawer below.
  void dir;

  if (nodeOrder.length === 0) {
    return (
      <div className="p-6 text-sm text-muted-foreground">
        This pipeline has no transform nodes yet.
      </div>
    );
  }

  return (
    <TooltipProvider delayDuration={250}>
    <div className="p-4">
      {/* Scroll affordance — only renders when the grid actually
          overflows. Sits in its own row above the table so the
          gradient/chevrons never overlap the sticky node-name column.
          The justify-end placement matches the timeline reading order
          (newest on the right) — older runs scroll back via the left
          chevron + count, newer runs forward via the right chevron. */}
      {(scrollState.canLeft || scrollState.canRight) && (
        <div className="mb-1 flex items-center justify-end gap-2 text-[10px] text-muted-foreground">
          {scrollState.canLeft && (
            <button
              type="button"
              onClick={() => scrollBy(-56 * 5)}
              aria-label={`Scroll back to older runs (${scrollState.hiddenLeftCount} hidden)`}
              className="flex items-center gap-1 rounded-full border border-border px-2 py-0.5 transition-colors hover:bg-muted/60 hover:text-foreground"
            >
              <ChevronLeft className="h-3 w-3" />
              <span>{scrollState.hiddenLeftCount} older</span>
            </button>
          )}
          {scrollState.canRight && (
            <button
              type="button"
              onClick={() => scrollBy(56 * 5)}
              aria-label={`Scroll forward to newer runs (${scrollState.hiddenRightCount} hidden)`}
              className="flex items-center gap-1 rounded-full border border-border px-2 py-0.5 transition-colors hover:bg-muted/60 hover:text-foreground"
            >
              <span>{scrollState.hiddenRightCount} newer</span>
              <ChevronRight className="h-3 w-3" />
            </button>
          )}
        </div>
      )}
      <div className="relative">
        {/* The sticky node-name column carries an OPAQUE background (bg-card,
            matching the card it sits on) plus a right border — the border is
            the "this side is fixed; everything right of it scrolls" affordance.
            Opaque is load-bearing: a translucent fill (the old bg-muted/30) let
            the run cells scroll visibly through the column. The "N older /
            N newer" pills above handle the overflow count. */}
        <div ref={scrollRef} className="overflow-x-auto">
          <table className="border-separate border-spacing-0">
          <thead>
            {/* Day-group band */}
            <tr>
              <th className="sticky left-0 z-20 bg-card border-r border-border pb-1 text-left">
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
            {/* Per-run header — runtime bar + time */}
            <tr>
              <th className="sticky left-0 z-20 bg-card border-r border-border" />
              {cols.map((r) => (
                <th
                  key={r.run_id}
                  className={cn(
                    "p-0 align-bottom",
                    firstOfDay.has(r.run_id) && "border-l border-border",
                  )}
                >
                  <button
                    onClick={() => onRunSelect(r.run_id)}
                    title={`${r.status} · ${runWhen(r.started_at)} · ${formatDuration(r.duration_ms)}`}
                    aria-label={`Run ${r.run_id} · ${r.status} · ${formatDuration(r.duration_ms)}`}
                    className="flex w-14 flex-col items-center gap-1 rounded-md pb-1 pt-1.5 transition-colors hover:bg-muted/60"
                  >
                    {/* Airflow-style runtime bar — height ∝ duration_ms,
                        normalized against the longest run on screen.
                        Sits in a fixed-height shell so all columns align
                        at the bottom (long bars grow up, short ones stay
                        a sliver) and the HH:mm label below stays on the
                        same baseline. */}
                    <div
                      className="flex h-8 w-full items-end justify-center"
                      aria-hidden
                    >
                      <div
                        className={cn(
                          "w-1.5 rounded-sm",
                          VARIANT_BAR_BG[runVariant(r.status)],
                        )}
                        style={{
                          height:
                            r.duration_ms == null || maxDurationMs <= 0
                              ? BAR_MID_PX
                              : Math.max(
                                  BAR_MIN_PX,
                                  Math.round(
                                    (r.duration_ms / maxDurationMs) *
                                      BAR_MAX_PX,
                                  ),
                                ),
                        }}
                      />
                    </div>
                    <span className="font-mono text-[10px] leading-none text-foreground">
                      {runTime(r.started_at)}
                    </span>
                  </button>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {nodeOrder.map((node) => {
              const info = nodeInfo.get(node);
              const isSelectedNode = selected?.node === node;
              return (
                <tr key={node} className="group">
                  <td
                    className={cn(
                      "sticky left-0 z-20 bg-card border-r border-border group-hover:bg-muted",
                      isSelectedNode && "bg-muted",
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
                      // In-flight column: surface every node's live state,
                      // not just "running". Without this, succeeded /
                      // failed nodes that already finished mid-run render
                      // as pending (empty) and the column looks frozen.
                      const live = liveStates.get(node);
                      if (live === "running") state = "running";
                      else if (live === "succeeded") state = "ok";
                      else if (live === "failed") state = "failed";
                      else state = "pending";
                    } else {
                      state = "missing";
                    }
                    const tooltipLines: string[] = [
                      `${node} · ${CELL_VARIANTS[state].label}`,
                    ];
                    if (nr?.duration_ms != null) {
                      tooltipLines.push(formatDuration(nr.duration_ms));
                    }
                    if (nr?.output_rows != null) {
                      tooltipLines.push(`${nr.output_rows} rows`);
                    }
                    if (nr?.error_msg) {
                      tooltipLines.push(nr.error_msg);
                    } else if (nr?.error_class) {
                      tooltipLines.push(nr.error_class);
                    }
                    const ariaLabel = tooltipLines.join(" · ");
                    return (
                      <td
                        key={r.run_id}
                        className={cn(
                          "p-0",
                          firstOfDay.has(r.run_id) && "border-l border-border",
                        )}
                      >
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <button
                              onClick={() =>
                                setSelected({ node, runId: r.run_id })
                              }
                              aria-label={ariaLabel}
                              className={cn(
                                "flex h-9 w-14 items-center justify-center rounded-md transition-colors hover:bg-muted/60",
                                selected?.node === node &&
                                  selected.runId === r.run_id &&
                                  "bg-muted/60",
                              )}
                            >
                              <CellSquare state={state} />
                            </button>
                          </TooltipTrigger>
                          <TooltipContent side="top" className="max-w-xs">
                            <div className="space-y-0.5 text-left">
                              {tooltipLines.map((line, i) => (
                                <div
                                  key={i}
                                  className={cn(
                                    i === 0 ? "font-medium" : "text-muted-foreground",
                                  )}
                                >
                                  {line}
                                </div>
                              ))}
                            </div>
                          </TooltipContent>
                        </Tooltip>
                      </td>
                    );
                  })}
                </tr>
              );
            })}
          </tbody>
        </table>
        </div>
      </div>

      {cols.length === 0 && (
        <div className="mt-3 text-sm text-muted-foreground">
          No runs yet. Trigger one with <strong>Run pipeline</strong>; each run
          adds a column here.
        </div>
      )}

      <div className="mt-3 flex flex-wrap items-center gap-3 text-[11px] text-muted-foreground">
        {LEGEND.map((l) => (
          <span key={l.state} className="flex items-center gap-1.5">
            <CellSquare state={l.state} />
            {l.label}
          </span>
        ))}
        <span className="text-muted-foreground/70">
          · hover any cell for duration + reason
        </span>
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
          pipelineName={pipelineName}
          onClose={() => setSelected(null)}
        />
      )}
    </div>
    </TooltipProvider>
  );
}
