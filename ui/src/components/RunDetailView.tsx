/**
 * RunDetailView — the body of the per-execution drill-down, decoupled
 * from URL/page chrome so it can render both as a full page and inside a
 * right-side Sheet on the pipeline dashboard.
 *
 * Inputs: dir + runId. Same queries, same sub-components as the old
 * RunDetail page; `embedded` drops the outer max-w-6xl page wrapper and
 * the breadcrumb chrome since the Sheet owns its own header.
 */

import { useEffect, useMemo, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { ReactFlowProvider } from "@xyflow/react";

import "@xyflow/react/dist/style.css";

import { Loader2 } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import {
  PipelineGraph as PipelineGraphView,
  deriveNodeOutputs,
} from "@/components/PipelineGraph";
import { NodeDetailDrawer, type NodeSpec } from "@/components/NodeDetailDrawer";
import { SourceInspectorDrawer } from "@/components/SourceInspectorDrawer";

import {
  useExecutionStates,
  useLineage,
  useNodeRuns,
  usePipelines,
  useRuns,
} from "@/lib/queries";
import type { NodeRun, Run } from "@/lib/queries";
import { formatDuration, formatRelative, formatRowCount } from "@/lib/format";
import {
  liveNodeColor,
  nodeVariant,
  runVariant,
  topoOrder,
  type NodeRunColor,
} from "@/lib/runStatus";
import { getPipeline } from "@/api/pipeline";
import type { PipelineGraph as PipelineGraphType } from "@/types/pipeline";

export interface RunDetailViewProps {
  dir: string;
  runId: string;
  /**
   * When true, drop the page-level wrapper (max-w / px / py). The Sheet
   * provides its own padding; the standalone page keeps the centred
   * 6xl wrapper for readability.
   */
  embedded?: boolean;
}

export function RunDetailView({ dir, runId, embedded }: RunDetailViewProps) {
  const pipelineName = dir.split("/").pop() ?? dir;

  // Local vs cloud — controls how StepLogs addresses CloudWatch / runner
  // log files in the drawer it opens.
  const pipelines = usePipelines();
  const isLocal =
    pipelines.data?.find((p) => p.dir === dir)?.compute === "local";

  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [inspectedSyntheticId, setInspectedSyntheticId] = useState<string | null>(null);

  const [graph, setGraph] = useState<PipelineGraphType | null>(null);
  useEffect(() => {
    if (!dir) return;
    let cancelled = false;
    void getPipeline(dir).then((g) => {
      if (!cancelled) setGraph(g);
    });
    return () => {
      cancelled = true;
    };
  }, [dir]);

  // Live progress channel — see RunDetail history for the reasoning;
  // gates the Spark-backed queries below until the run is settled so
  // they don't race the runner's transform container for Docker memory.
  const states = useExecutionStates({ dir, run: runId });
  const liveStatus = states.data?.status ?? "";
  const isRunning = liveStatus === "RUNNING";
  const settled = !states.isLoading && !isRunning;

  const runs = useRuns(pipelineName, { dir, limit: 200, enabled: settled });
  const runRow: Run | undefined = useMemo(
    () => runs.data?.rows.find((r) => r.run_id === runId),
    [runs.data, runId],
  );

  const arn = runRow?.sf_execution_arn || runId;
  const nodeRuns = useNodeRuns(pipelineName, {
    dir,
    arn,
    limit: 200,
    enabled: settled,
  });

  // Tail-poll until the rollup row + node_runs land — same logic as
  // before, since `recordLocalRun` writes them on a separate runner pass.
  const qc = useQueryClient();
  useEffect(() => {
    if (!settled || runRow) return;
    const t = setInterval(() => {
      void qc.invalidateQueries({ queryKey: ["runs"] });
      void qc.invalidateQueries({ queryKey: ["node-runs"] });
    }, 2000);
    return () => clearInterval(t);
  }, [settled, runRow, qc]);

  const perNode: Map<string, NodeRun> = useMemo(() => {
    const m = new Map<string, NodeRun>();
    for (const r of nodeRuns.data?.rows ?? []) m.set(r.node, r);
    return m;
  }, [nodeRuns.data]);

  const orderedNodeRuns = useMemo(() => {
    const rows = nodeRuns.data?.rows ?? [];
    if (!graph) return rows;
    const order = topoOrder(
      graph.nodes
        .filter((n) => (n.type as string) !== "source")
        .map((n) => n.id),
      graph.edges,
    );
    const idx = new Map(order.map((id, i) => [id, i]));
    return [...rows].sort(
      (a, b) => (idx.get(a.node) ?? 999) - (idx.get(b.node) ?? 999),
    );
  }, [nodeRuns.data, graph]);

  const nodeStatuses = useMemo(() => {
    const m = new Map<string, NodeRunColor>();
    for (const [node, st] of Object.entries(states.data?.states ?? {})) {
      const v = liveNodeColor(st.status);
      if (v) m.set(node, v);
    }
    for (const r of perNode.values()) {
      if (r.status === "ok") m.set(r.node, "succeeded");
      else if (r.status === "failed") m.set(r.node, "failed");
      else m.set(r.node, "running");
    }
    return m;
  }, [states.data, perNode]);

  const lineage = useLineage(dir);
  const nodeOutputs = useMemo(
    () =>
      deriveNodeOutputs(
        graph?.nodes ?? [],
        lineage.data?.catalog ?? "",
        lineage.data?.schema ?? "",
      ),
    [graph, lineage.data],
  );
  const nodeSpecs = useMemo(() => {
    const m = new Map<string, NodeSpec>();
    const inbound = new Map<
      string,
      { from: string; kind: string; table: string }[]
    >();
    for (const e of lineage.data?.edges ?? []) {
      const arr = inbound.get(e.to_node) ?? [];
      arr.push({ from: e.from_node, kind: e.from_type, table: e.via_table });
      inbound.set(e.to_node, arr);
    }
    for (const n of graph?.nodes ?? []) {
      if ((n.type as string) === "source") continue;
      const cfg = (n.config ?? {}) as Record<string, unknown>;
      const defs = cfg.output_definitions as Record<string, unknown> | undefined;
      const def = defs?.default as Record<string, unknown> | undefined;
      const mode = typeof def?.mode === "string" ? def.mode : "";
      const keys = Array.isArray(def?.merge_keys)
        ? (def.merge_keys as unknown[]).filter(
            (k): k is string => typeof k === "string",
          )
        : [];
      m.set(n.id, {
        language: typeof cfg.language === "string" ? cfg.language : "sql",
        outputMode: mode || "replace",
        mergeKeys: keys,
        inputs: inbound.get(n.id) ?? [],
      });
    }
    return m;
  }, [graph, lineage.data]);

  const drawerData = useMemo(() => {
    if (!selectedNodeId) return null;
    const nr = perNode.get(selectedNodeId);
    const tableHref = nodeOutputs.has(selectedNodeId)
      ? (() => {
          const o = nodeOutputs.get(selectedNodeId)!;
          return `/tables/${encodeURIComponent(o.catalog)}/${encodeURIComponent(o.schema)}/${encodeURIComponent(o.table)}`;
        })()
      : null;
    if (!nr) {
      return {
        spec: nodeSpecs.get(selectedNodeId) ?? null,
        invocations: [] as { nodeRun: NodeRun; runId: string }[],
        tableLabel: nodeOutputs.get(selectedNodeId)?.table ?? null,
        tableHref,
      };
    }
    return {
      spec: nodeSpecs.get(selectedNodeId) ?? null,
      invocations: [{ nodeRun: nr, runId }],
      tableLabel: nodeOutputs.get(selectedNodeId)?.table ?? null,
      tableHref,
    };
  }, [selectedNodeId, perNode, nodeSpecs, nodeOutputs, runId]);

  if (!dir || !runId) {
    return (
      <div className={embedded ? "p-6" : "mx-auto w-full max-w-6xl px-6 py-8"}>
        <p className="text-sm text-muted-foreground">
          Missing required <code>dir</code> and <code>run</code> values.
        </p>
      </div>
    );
  }

  const body = (
    <>
      <RunHeader
        run={runRow}
        liveStatus={liveStatus}
        runId={runId}
        pipeline={pipelineName}
      />

      <TriageStrip rows={Array.from(perNode.values())} />

      <Card className="mt-6">
        <CardHeader className="pb-3">
          <CardTitle>DAG</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <div className="h-[360px] w-full">
            {graph ? (
              <ReactFlowProvider>
                <PipelineGraphView
                  graph={graph}
                  dir={dir}
                  onGraphUpdate={() => {
                    /* read-only here */
                  }}
                  onNodeClick={(id) => {
                    if (id.startsWith("source:") || id.startsWith("external:")) {
                      setInspectedSyntheticId((prev) => (prev === id ? null : id));
                      setSelectedNodeId(null);
                      return;
                    }
                    setSelectedNodeId((prev) => (prev === id ? null : id));
                    setInspectedSyntheticId(null);
                  }}
                  nodeStatuses={nodeStatuses}
                  nodeOutputs={nodeOutputs}
                  showSources
                />
              </ReactFlowProvider>
            ) : (
              <div className="space-y-2 p-6">
                <Skeleton className="h-6 w-1/3" />
                <Skeleton className="h-6 w-2/3" />
              </div>
            )}
          </div>
        </CardContent>
      </Card>

      {selectedNodeId && drawerData && (
        <NodeDetailDrawer
          node={selectedNodeId}
          tableLabel={drawerData.tableLabel}
          tableHref={drawerData.tableHref}
          spec={drawerData.spec}
          invocations={drawerData.invocations}
          selectedRunId={runId}
          onSelectRun={() => {
            /* RunDetailView shows exactly one run; selection is a no-op. */
          }}
          isLocal={!!isLocal}
          dir={dir}
          onClose={() => setSelectedNodeId(null)}
        />
      )}
      {inspectedSyntheticId && (
        <SourceInspectorDrawer
          nodeId={inspectedSyntheticId}
          onClose={() => setInspectedSyntheticId(null)}
        />
      )}

      <Card className="mt-6">
        <CardHeader className="flex-row items-center justify-between pb-3">
          <CardTitle>Per-node breakdown</CardTitle>
          <div className="flex items-center gap-3">
            {/* Tail-poll feedback: this view invalidates runs/node-runs
                every 2s until the rollup row + per-node rows have landed.
                Without an indicator the panel looks frozen. */}
            {(nodeRuns.isFetching || runs.isFetching) &&
              !nodeRuns.isLoading && (
                <span className="flex items-center gap-1.5 text-[11px] text-muted-foreground">
                  <Loader2 className="h-3 w-3 animate-spin" />
                  refreshing
                </span>
              )}
            {nodeRuns.data && (
              <span className="text-xs text-muted-foreground">
                {nodeRuns.data.rows.length} invocation
                {nodeRuns.data.rows.length === 1 ? "" : "s"}
              </span>
            )}
          </div>
        </CardHeader>
        <CardContent className="p-0">
          {isRunning && (
            <div className="p-6 text-sm text-muted-foreground">
              Run in progress — per-node detail appears when it completes.
              The DAG above colors live as each node runs.
            </div>
          )}
          {!isRunning && nodeRuns.isLoading && (
            <div className="space-y-2 p-6">
              <Skeleton className="h-4 w-full" />
              <Skeleton className="h-4 w-2/3" />
            </div>
          )}
          {!isRunning && nodeRuns.data && nodeRuns.data.rows.length === 0 && (
            <div className="p-6 text-sm text-muted-foreground">
              No node_runs rows match this execution. The runner may not have
              written them yet, or this run pre-dates the table.
            </div>
          )}
          {nodeRuns.data && nodeRuns.data.rows.length > 0 && (
            <ul className="divide-y divide-border">
              {orderedNodeRuns.map((r) => (
                <PerNodeRow key={r.node + r.started_at} row={r} />
              ))}
            </ul>
          )}
        </CardContent>
      </Card>
    </>
  );

  if (embedded) {
    return <div className="h-full overflow-y-auto p-6">{body}</div>;
  }
  return <div className="mx-auto w-full max-w-6xl px-6 py-8">{body}</div>;
}

// ---------------------------------------------------------------------------
// Run-level header
// ---------------------------------------------------------------------------

function RunHeader({
  run,
  liveStatus,
  runId,
  pipeline,
}: {
  run: Run | undefined;
  liveStatus: string;
  runId: string;
  pipeline: string;
}) {
  const status = run?.status ?? (liveStatus || "—");
  const variant = runVariant(status);
  const trigger = run?.trigger?.trim();
  return (
    <div className="space-y-3">
      <div className="flex items-center gap-3">
        <Badge variant={variant} className="font-mono text-[10px]">
          {status}
        </Badge>
        {trigger && (
          <Badge variant="outline" className="text-[10px] capitalize">
            {trigger}
          </Badge>
        )}
        <h1 className="font-mono text-lg tracking-tight">{runId}</h1>
      </div>
      <p className="text-xs text-muted-foreground">
        <code className="font-mono">{pipeline}</code>
        {run?.started_at && (
          <>
            <span className="mx-2">·</span>
            started {formatRelative(run.started_at)}
          </>
        )}
        {run?.duration_ms != null && (
          <>
            <span className="mx-2">·</span>
            ran for {formatDuration(run.duration_ms)}
          </>
        )}
      </p>
      {run && status !== "SUCCEEDED" && (run.failed_step || run.error_msg) && (
        <Card className="border-status-failed/40">
          <CardContent className="space-y-1 py-3 text-xs">
            {run.failed_step && (
              <div>
                <span className="text-muted-foreground">Failed at:</span>{" "}
                <code className="font-mono text-status-failed">
                  {run.failed_step}
                </code>
              </div>
            )}
            {run.error_class && (
              <div>
                <span className="text-muted-foreground">Error class:</span>{" "}
                <code className="font-mono">{run.error_class}</code>
              </div>
            )}
            {run.error_msg && (
              <pre className="overflow-x-auto whitespace-pre-wrap break-words rounded bg-muted/40 p-2 font-mono text-[11px]">
                {run.error_msg}
              </pre>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Triage strip — image digest + module version, surfaced once at the top.
// ---------------------------------------------------------------------------

function TriageStrip({ rows }: { rows: NodeRun[] }) {
  const digest = rows.find((r) => r.runner_image_digest)?.runner_image_digest;
  const version = rows.find((r) => r.module_version)?.module_version;
  if (!digest && !version) return null;
  return (
    <div className="mt-4 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground">
      {version && (
        <span>
          Module&nbsp;
          <code className="font-mono text-foreground">{version}</code>
        </span>
      )}
      {digest && (
        <span title={digest}>
          Runner&nbsp;
          <code className="font-mono text-foreground">
            {digest.slice(7, 19)}
          </code>
        </span>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Per-node row
// ---------------------------------------------------------------------------

function PerNodeRow({ row }: { row: NodeRun }) {
  const variant = nodeVariant(row.status);
  return (
    <li className="flex flex-col gap-1 px-6 py-3">
      <div className="flex items-center justify-between gap-3">
        <div className="flex min-w-0 items-center gap-3">
          <Badge variant={variant} className="font-mono text-[10px] uppercase">
            {row.status}
          </Badge>
          <code className="truncate font-mono text-sm">{row.node}</code>
        </div>
        <div className="flex items-center gap-3 whitespace-nowrap text-[11px] text-muted-foreground">
          {row.output_rows != null && (
            <span title="Rows written to Delta outputs this run">
              {formatRowCount(row.output_rows)}
            </span>
          )}
          <span>{formatDuration(row.duration_ms)}</span>
          {row.cold_start && (
            <Badge variant="outline" className="text-[10px]">
              cold
            </Badge>
          )}
        </div>
      </div>
      {row.error_class && (
        <div className="text-[11px]">
          <span className="text-status-failed">{row.error_class}</span>
          {row.error_msg && (
            <pre className="mt-1 overflow-x-auto whitespace-pre-wrap break-words rounded bg-muted/40 p-2 font-mono text-[11px]">
              {row.error_msg}
            </pre>
          )}
        </div>
      )}
    </li>
  );
}
