/**
 * PipelineNode — custom React Flow node component.
 *
 * Displays a schema-card style node:
 *   - Icon + type badge in the header
 *   - Node id as the title
 *   - Column list (name + type) below a separator when columns are available
 *   - Target handle (input) on the left for non-source nodes
 *   - Source handle (output) on the right for non-destination nodes
 */

import { useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { Handle, Position } from "@xyflow/react";
import type { NodeProps } from "@xyflow/react";
import { Link } from "react-router-dom";
import { Database, Workflow, Upload, Plus, ChevronLeft } from "lucide-react";

import type { Column } from "../types/pipeline";
import { cn } from "@/lib/utils";

/**
 * The Delta table a transform node writes its default output to.
 * `table` keeps the raw `<node>__default` form; the node footer hides the
 * `__default` suffix for display but links with the full identifier.
 */
export type NodeOutput = {
  catalog: string;
  schema: string;
  table: string;
};

export type PipelineNodeData = {
  label: string;
  nodeType: "source" | "transform" | "destination";
  language?: string;
  /**
   * The transform's `compute` deploy target — `lambda` (default) /
   * `fargate` / `emr-serverless`. Absent on source/destination nodes
   * and on transforms that don't set it (which deploy to lambda).
   */
  compute?: string;
  /**
   * Delta write mode for the `default` output — `replace` (default) /
   * `append` / `merge`. Surfaced as a footer chip next to the output
   * table so authors can see at a glance whether each transform overwrites
   * or accumulates. Absent on source/destination nodes.
   */
  outputMode?: string;
  /**
   * Whether the node participates in runs. `false` means the author has
   * paused it (config `enabled = false`) — the node is skipped in runs but
   * kept in the graph. Rendered dimmed with a "Disabled" pill; still
   * selectable so the user can re-enable it. Defaults to enabled (true /
   * undefined). Always true for synthetic source / external renderings.
   */
  enabled?: boolean;
  /**
   * Source-only metadata surfaced on the synthetic source card so users
   * see what kind of source it is (http / s3), its data format
   * (parquet / csv / json), and a short location label without having to
   * click the card. Set only for `nodeType === "source"` nodes the
   * dashboard built from the workspace source registry.
   */
  sourceKind?: string;
  sourceFormat?: string;
  sourceLocation?: string;
  columns?: Column[];
  /** Delta output table — shown in the node footer (transforms only). */
  output?: NodeOutput;
  loading?: boolean;
  /** Node was part of a completed preview chain */
  previewed?: boolean;
  /**
   * Live SFN run status for this node, derived by polling
   * /pipeline/execution/states during an in-flight execution. Absent
   * when no execution is running, or when the node never entered.
   */
  runStatus?: "running" | "succeeded" | "failed";
  /**
   * Live in-flight Spark progress for a RUNNING node, polled from
   * /pipeline/execution/states. Drives a thin task progress bar under the
   * node header. Absent unless the node is running and the runner has
   * emitted at least one progress tick (tasksTotal > 0).
   */
  progress?: {
    stagesTotal: number;
    stagesCompleted: number;
    tasksTotal: number;
    tasksCompleted: number;
    tasksFailed: number;
  };
  /**
   * Guided (+) authoring affordance on the output handle. Present only in
   * the editor — the read-only dashboard / run-detail DAGs omit it, so the
   * (+) button doesn't render there. `onCreate` / `onConnect` are already
   * bound to this node as the edge's from-node.
   */
  quickAdd?: {
    /** Existing nodes this node can be wired into (ids). */
    targets: string[];
    /** Whether "+ S3 Destination" is offered (false for source nodes —
     *  a source attaches to a transform, never straight to a destination). */
    allowDestination: boolean;
    /** Create a new downstream node of `type` and wire this node into it. */
    onCreate: (type: "transform" | "destination") => void;
    /** Wire this node into an existing node. */
    onConnect: (targetId: string) => void;
  };
};

const TYPE_BADGE_CLS: Record<PipelineNodeData["nodeType"], string> = {
  source: "bg-blue-600",
  transform: "bg-violet-600",
  destination: "bg-emerald-600",
};

const TYPE_ICON: Record<PipelineNodeData["nodeType"], React.ReactNode> = {
  source: <Database className="h-3.5 w-3.5 text-blue-400" />,
  transform: <Workflow className="h-3.5 w-3.5 text-violet-400" />,
  destination: <Upload className="h-3.5 w-3.5 text-emerald-400" />,
};

function LangBadge({ language }: { language: string }) {
  const label = language === "python" ? "PY" : "SQL";
  return (
    <span
      className={cn(
        "rounded-sm px-1.5 py-px text-[9px] font-bold tracking-wider text-white",
        language === "python" ? "bg-amber-600" : "bg-cyan-600"
      )}
    >
      {label}
    </span>
  );
}

/**
 * Short display label per `compute` deploy target — the node card is a
 * fixed 200px, so `emr-serverless` is abbreviated; the full value stays
 * in the chip's tooltip.
 */
const COMPUTE_LABEL: Record<string, string> = {
  lambda: "lambda",
  fargate: "fargate",
  "emr-serverless": "emr",
};

/**
 * ModeBadge — write mode for the `default` output. `replace` overwrites
 * the table on every run; `append` accumulates rows; `merge` upserts on
 * a key. Color-coded so the operational semantics are visible without
 * opening the config drawer: replace is muted, append/merge are tinted
 * because they retain state across runs.
 */
const MODE_CLS: Record<string, string> = {
  replace: "bg-muted text-muted-foreground",
  append: "bg-sky-500/15 text-sky-300",
  merge: "bg-amber-500/15 text-amber-300",
};

/**
 * SourceMetaChip — small chip on the source node card. `kind` (http / s3)
 * uses a tinted variant so the data-shape (`format`) reads as the muted
 * companion; this matches the convention used elsewhere on the card.
 */
function SourceMetaChip({ text, muted }: { text: string; muted?: boolean }) {
  return (
    <span
      className={cn(
        "flex-shrink-0 rounded-sm px-1 py-px text-[9px] font-semibold uppercase tracking-wider",
        muted
          ? "bg-muted text-muted-foreground"
          : "bg-blue-500/15 text-blue-300",
      )}
    >
      {text}
    </span>
  );
}

function ModeBadge({ mode }: { mode: string }) {
  return (
    <span
      title={`write mode: ${mode}`}
      className={cn(
        "flex-shrink-0 rounded-sm px-1 py-px text-[9px] font-semibold uppercase tracking-wider",
        MODE_CLS[mode] ?? "bg-muted text-muted-foreground",
      )}
    >
      {mode}
    </span>
  );
}

/**
 * ComputeBadge — the transform's `compute` deploy target. Understated
 * (muted chip in the node footer): it matters when deploying, not when
 * authoring, and most nodes are the `lambda` default.
 */
function ComputeBadge({ compute }: { compute: string }) {
  return (
    <span
      title={`deploy target: ${compute}`}
      className="flex-shrink-0 rounded-sm bg-muted px-1.5 py-px text-[9px] font-semibold uppercase tracking-wider text-muted-foreground"
    >
      {COMPUTE_LABEL[compute] ?? compute}
    </span>
  );
}

/**
 * QuickAddMenu — the guided (+) affordance on a node's output handle.
 *
 * Replaces drag-to-connect: a drag gesture can't carry the SQL alias an
 * edge needs, so wiring is done through explicit choices instead. The menu
 * has two views — pick a downstream node type to create, or connect to an
 * existing node. Either way the edge's from-node is this node.
 */
function QuickAddMenu({ quickAdd }: { quickAdd: NonNullable<PipelineNodeData["quickAdd"]> }) {
  const [open, setOpen] = useState(false);
  const [view, setView] = useState<"root" | "connect">("root");
  // Anchor rect of the (+) button, captured on open. The menu is rendered
  // in a body portal at a fixed position: a React Flow node sits inside a
  // transformed, clipped container and the next node in the LR layout is
  // always immediately to the right, so an in-node popover gets covered.
  const [anchor, setAnchor] = useState<DOMRect | null>(null);
  const buttonRef = useRef<HTMLButtonElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    function onPointerDown(e: PointerEvent) {
      const t = e.target as Node;
      if (buttonRef.current?.contains(t) || menuRef.current?.contains(t)) return;
      close();
    }
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") {
        e.stopImmediatePropagation();
        close();
      }
    }
    document.addEventListener("pointerdown", onPointerDown);
    document.addEventListener("keydown", onKey, true);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown);
      document.removeEventListener("keydown", onKey, true);
    };
  }, [open]);

  function close() {
    setOpen(false);
    setView("root");
  }

  const itemCls =
    "block w-full rounded px-2 py-1 text-left text-xs text-foreground hover:bg-accent";

  // Place the menu to the right of the button, flipping left if it would
  // overflow the viewport.
  const menuWidth = 176; // w-44
  const placeLeft = anchor !== null && anchor.right + 8 + menuWidth > window.innerWidth;

  return (
    <>
      <button
        ref={buttonRef}
        type="button"
        title="Add downstream node or connection"
        aria-label="Add downstream node or connection"
        onClick={(e) => {
          e.stopPropagation();
          if (open) {
            close();
          } else {
            setAnchor(buttonRef.current?.getBoundingClientRect() ?? null);
            setView("root");
            setOpen(true);
          }
        }}
        className={cn(
          "absolute right-0.5 top-1/2 z-20 flex h-4 w-4 -translate-y-1/2 items-center justify-center rounded-full border text-foreground transition-colors",
          open
            ? "border-primary bg-primary text-primary-foreground"
            : "border-border bg-card hover:border-primary hover:bg-primary hover:text-primary-foreground",
        )}
      >
        <Plus className="h-2.5 w-2.5" />
      </button>

      {open && anchor &&
        createPortal(
          <div
            ref={menuRef}
            className="fixed z-50 w-44 rounded-lg border border-border bg-background p-1 shadow-xl"
            style={{
              top: anchor.top - 4,
              left: placeLeft ? anchor.left - menuWidth - 8 : anchor.right + 8,
            }}
            onClick={(e) => e.stopPropagation()}
          >
          {view === "root" ? (
            <>
              <button
                type="button"
                className={itemCls}
                onClick={() => {
                  quickAdd.onCreate("transform");
                  close();
                }}
              >
                + SQL Transform
              </button>
              {quickAdd.allowDestination && (
                <button
                  type="button"
                  className={itemCls}
                  onClick={() => {
                    quickAdd.onCreate("destination");
                    close();
                  }}
                >
                  + S3 Destination
                </button>
              )}
              <button
                type="button"
                disabled={quickAdd.targets.length === 0}
                className={cn(itemCls, "disabled:opacity-40")}
                onClick={() => setView("connect")}
              >
                Connect to existing node →
              </button>
            </>
          ) : (
            <>
              <button
                type="button"
                className="mb-0.5 flex w-full items-center gap-1 rounded px-2 py-1 text-left text-[11px] font-semibold uppercase tracking-wider text-muted-foreground hover:bg-accent"
                onClick={() => setView("root")}
              >
                <ChevronLeft className="h-3 w-3" />
                Connect to
              </button>
              <div className="max-h-48 overflow-y-auto">
                {quickAdd.targets.map((id) => (
                  <button
                    key={id}
                    type="button"
                    className={cn(itemCls, "font-mono")}
                    onClick={() => {
                      quickAdd.onConnect(id);
                      close();
                    }}
                  >
                    {id}
                  </button>
                ))}
              </div>
            </>
          )}
          </div>,
          document.body,
        )}
    </>
  );
}

/**
 * NodeProgressBar — thin task-progress bar shown under a RUNNING node's
 * header. Fill = tasksCompleted / tasksTotal; the label reads
 * "<done>/<total> tasks · stage <n>/<m>" with a failed-task indicator when
 * tasksFailed > 0. Lightweight by design — it lives inside a 200px node.
 */
function NodeProgressBar({
  progress,
}: {
  progress: NonNullable<PipelineNodeData["progress"]>;
}) {
  const { tasksTotal, tasksCompleted, tasksFailed, stagesTotal, stagesCompleted } =
    progress;
  const pct = tasksTotal > 0 ? Math.min(100, (tasksCompleted / tasksTotal) * 100) : 0;
  return (
    <div className="mt-1.5">
      <div className="h-1 w-full overflow-hidden rounded-full bg-muted">
        <div
          className="h-full rounded-full bg-status-running transition-all"
          style={{ width: `${pct}%` }}
        />
      </div>
      <div className="mt-0.5 flex items-center justify-between gap-2 font-mono text-[9px] text-muted-foreground">
        <span className="truncate">
          {tasksCompleted}/{tasksTotal} tasks
          {stagesTotal > 0 && ` · stage ${stagesCompleted}/${stagesTotal}`}
        </span>
        {tasksFailed > 0 && (
          <span className="flex-shrink-0 font-semibold text-status-failed">
            {tasksFailed} failed
          </span>
        )}
      </div>
    </div>
  );
}

export function PipelineNode({ data, selected }: NodeProps) {
  const nodeData = data as PipelineNodeData;
  const {
    label,
    nodeType,
    language,
    compute,
    outputMode,
    enabled,
    sourceKind,
    sourceFormat,
    sourceLocation,
    columns,
    output,
    loading,
    previewed,
    runStatus,
    progress,
    quickAdd,
  } = nodeData;
  // Only show the in-flight bar while RUNNING and once the runner has
  // reported a non-zero task total — a node that finished before any
  // progress tick (or a fast node) shows no bar rather than an empty one.
  const showProgress =
    runStatus === "running" && !!progress && progress.tasksTotal > 0;
  // compute is a transform-only concept and an explicit cloud override —
  // an absent attribute means "no override, run wherever the env mode
  // dispatches you" (the ADR-014 env-mode shift), so the badge stays
  // hidden. `local` is a legacy value that env mode now owns; skip it
  // too so deprecated `.tf` doesn't render an anachronistic chip.
  const computeTarget =
    nodeType === "transform" && compute && compute !== "local"
      ? compute
      : "";
  // Write mode similarly defaults to `replace` for a transform with no
  // output_definitions override — same fallback the runner uses.
  const writeMode = nodeType === "transform" ? outputMode || "replace" : "";
  const hasColumns = columns && columns.length > 0;
  // A node is disabled only when config explicitly says so. Dim the card and
  // show a "Disabled" pill so a paused node is distinguishable on the canvas;
  // the card stays clickable so the user can re-enable it from the config drawer.
  const isDisabled = enabled === false;

  // Border color precedence: preview-loading > runStatus (live SFN) >
  // previewed (completed preview chain) > selected > idle. The live SFN
  // signal beats stale preview state but yields to the active preview
  // animation so the user sees the immediate UI feedback they triggered.
  const borderClass = loading
    ? "border-status-running shadow-[0_0_12px_rgba(245,158,11,0.4)] animate-pulse"
    : runStatus === "running"
      ? "border-status-running shadow-[0_0_12px_rgba(245,158,11,0.4)] animate-pulse"
      : runStatus === "succeeded"
        ? "border-status-success shadow-[0_0_8px_rgba(34,197,94,0.25)]"
        : runStatus === "failed"
          ? "border-status-failed shadow-[0_0_10px_rgba(239,68,68,0.35)]"
          : previewed
            ? "border-status-success shadow-[0_0_8px_rgba(34,197,94,0.2)]"
            : selected
              ? "border-primary shadow-[0_0_12px_rgba(59,130,246,0.3)]"
              : "border-border";

  return (
    <div
      data-testid="dag-node"
      className={cn(
        "relative min-w-44 rounded-lg border-2 bg-card text-foreground transition-all",
        borderClass,
        // Dim a paused node, but keep it interactive (no pointer-events-none)
        // so the user can click in and re-enable it.
        isDisabled && "opacity-50",
      )}
    >
      {nodeType !== "source" && (
        <Handle
          type="target"
          position={Position.Left}
          id="input"
          style={{ background: "#94a3b8" }}
        />
      )}

      <div className="border-b border-border px-3 pb-2 pt-2">
        <div className="mb-1 flex items-center gap-2">
          {nodeType !== "transform"
            ? TYPE_ICON[nodeType]
            : <LangBadge language={language ?? "sql"} />}
          <span
            className={cn(
              "rounded-sm px-1.5 py-px text-[9px] font-bold uppercase tracking-wider text-white",
              TYPE_BADGE_CLS[nodeType] ?? "bg-muted-foreground"
            )}
          >
            {nodeType}
          </span>
          {isDisabled && (
            <span
              title="This node is paused — skipped in runs until re-enabled"
              className="ml-auto flex-shrink-0 rounded-sm bg-muted px-1.5 py-px text-[9px] font-semibold uppercase tracking-wider text-muted-foreground"
            >
              Disabled
            </span>
          )}
        </div>
        <div className="overflow-hidden truncate font-mono text-sm font-semibold">
          {label}
        </div>
        {showProgress && <NodeProgressBar progress={progress!} />}
      </div>

      {hasColumns && (
        <div className="py-1">
          {columns!.slice(0, 5).map((col) => (
            <div
              key={col.name}
              className="flex items-center justify-between gap-2 px-2.5 py-px font-mono text-[11px]"
            >
              <span className="truncate text-foreground">{col.name}</span>
              <span className="flex-shrink-0 text-sky-300">{col.type}</span>
            </div>
          ))}
          {columns!.length > 5 && (
            <div className="px-2.5 py-px font-mono text-[10px] text-muted-foreground">
              +{columns!.length - 5} more
            </div>
          )}
        </div>
      )}

      {nodeType === "source" && (
        <div className={cn(hasColumns ? "pb-1.5" : "py-2")}>
          {hasColumns && <div className="mb-1 border-t border-border/60" />}
          <div className="relative px-3 pr-7">
            {(sourceKind || sourceFormat || sourceLocation) && (
              <>
                <div className="mb-0.5 flex items-center justify-between gap-2 text-[9px] font-medium uppercase tracking-wider text-muted-foreground">
                  <span>Source</span>
                  <div className="flex items-center gap-1">
                    {sourceKind && <SourceMetaChip text={sourceKind} />}
                    {sourceFormat && <SourceMetaChip text={sourceFormat} muted />}
                  </div>
                </div>
                {sourceLocation && (
                  <div
                    className="truncate font-mono text-[10px] text-muted-foreground"
                    title={sourceLocation}
                  >
                    {sourceLocation}
                  </div>
                )}
              </>
            )}
            <Handle
              type="source"
              position={Position.Right}
              id="output"
              style={{
                background: "#94a3b8",
                right: -8,
                top: "50%",
                transform: "translateY(-50%)",
              }}
            />
            {quickAdd && <QuickAddMenu quickAdd={quickAdd} />}
          </div>
        </div>
      )}

      {nodeType === "transform" && (
        <div className={cn(hasColumns ? "pb-1.5" : "py-2")}>
          {hasColumns && <div className="mb-1 border-t border-border/60" />}
          <div className="relative px-3 pr-7">
            <div className="mb-0.5 flex items-center justify-between gap-2 text-[9px] font-medium uppercase tracking-wider text-muted-foreground">
              <span>Writes</span>
              <div className="flex items-center gap-1">
                {writeMode && <ModeBadge mode={writeMode} />}
                {computeTarget && <ComputeBadge compute={computeTarget} />}
              </div>
            </div>
            <div className="flex items-center font-mono text-[11px]">
              <span className="min-w-0 flex-1 truncate">
                {output ? (
                  <Link
                    to={`/tables/${encodeURIComponent(output.catalog)}/${encodeURIComponent(output.schema)}/${encodeURIComponent(output.table)}`}
                    title={`writes ${output.catalog}.${output.schema}.${output.table}`}
                    onClick={(e) => e.stopPropagation()}
                    className="inline-block max-w-full truncate text-sky-300 hover:text-sky-200 hover:underline"
                  >
                    {output.schema ? `${output.schema}.` : ""}
                    {output.table.replace(/__default$/, "")}
                  </Link>
                ) : (
                  <span className="text-muted-foreground">—</span>
                )}
              </span>
            </div>
            <Handle
              type="source"
              position={Position.Right}
              id="output"
              style={{
                background: "#94a3b8",
                right: -8,
                top: "50%",
                transform: "translateY(-50%)",
              }}
            />
            {quickAdd && <QuickAddMenu quickAdd={quickAdd} />}
          </div>
        </div>
      )}
    </div>
  );
}

export default PipelineNode;
