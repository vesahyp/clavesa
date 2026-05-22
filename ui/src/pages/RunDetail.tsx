/**
 * RunDetail — drill-down for one execution.
 *
 * URL: /pipelines/run?dir=<pipelineDir>&run=<runId>
 *
 * The natural target when a user clicks a Run history row on the dashboard.
 * Surfaces every column the v0.14 / v0.14.1 observability slices populate:
 *  - run-level rollup (status, trigger, duration, error) from runs.run_id
 *  - per-node breakdown (status, duration, image digest, module version,
 *    error class/msg) from node_runs filtered by run_id
 *  - the pipeline DAG colored by per-node status so failures jump out
 *
 * Local pipelines source from <pipelineDir>/.clavesa/warehouse/...; cloud
 * pipelines source from the same Iceberg tables via Athena. Same response
 * shapes — the UI cannot tell them apart, per ADR-014.
 */

import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useQueryClient } from "@tanstack/react-query";
import { ReactFlowProvider } from "@xyflow/react";

import "@xyflow/react/dist/style.css";

import { Badge } from "@/components/ui/badge";
import { useChrome, type PageChrome } from "@/components/PageChrome";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { PipelineGraph as PipelineGraphView, deriveNodeOutputs } from "@/components/PipelineGraph";
import {
  NodeDetailDrawer,
  type NodeSpec,
} from "@/components/NodeDetailDrawer";
import { SourceInspectorDrawer } from "@/components/SourceInspectorDrawer";

import { useExecutionStates, useLineage, useNodeRuns, usePipelines, useRuns } from "@/lib/queries";
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

export function RunDetail() {
  const [searchParams] = useSearchParams();
  const dir = searchParams.get("dir") ?? "";
  const runId = searchParams.get("run") ?? "";
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

  // Live progress channel — the source of truth while a (local) run is
  // in flight, before any node_runs / runs table rows are written. For a
  // completed run it reports the final status; for a run started from
  // the dashboard it lets this page render RUNNING the instant it opens.
  // Cheap (reads a state.json), no Spark — safe to poll during a run.
  const states = useExecutionStates({ dir, run: runId });
  const liveStatus = states.data?.status ?? "";
  const isRunning = liveStatus === "RUNNING";

  // `settled` gates the Spark-backed runs / node_runs queries: hold them
  // until the execution-states poll has resolved AND the run is no
  // longer RUNNING. Two reasons — (1) during a run the rows don't exist
  // yet, and (2) the query would spawn the warm Spark worker to compete
  // for Docker memory with the run's own transform container. Once a
  // local run's channel reports a terminal status its node_runs rows
  // are already committed, so a single fetch then is enough.
  const settled = !states.isLoading && !isRunning;

  // Pull this run's rollup row + per-node rows. The rollup narrows to
  // runId via array.find — the runs window is small so a client-side
  // filter is cheap and avoids a `?run=` param on /api/data/runs.
  const runs = useRuns(pipelineName, {
    dir,
    limit: 200,
    enabled: settled,
  });
  const runRow: Run | undefined = useMemo(
    () => runs.data?.rows.find((r) => r.run_id === runId),
    [runs.data, runId],
  );

  // Per-node rows for this exact run — backend filter via
  // sf_execution_arn (the join key), runId as the fallback for local
  // rows that pre-date the threading fix.
  const arn = runRow?.sf_execution_arn || runId;
  const nodeRuns = useNodeRuns(pipelineName, {
    dir,
    arn,
    limit: 200,
    enabled: settled,
  });

  // After a run settles there's a tail: recordLocalRun writes the
  // runs-table rollup row a few seconds later (a separate runner pass),
  // and the node_runs query can race the runner's row commit and come
  // back empty. Poll both the runs and node_runs tables until the
  // rollup row lands — by then node_runs has been refetched with the
  // rows present. Stop once `runRow` is here. No OOM risk: the run's
  // transform container is gone, so these warm-worker queries run
  // alone. Only fires in the settled-but-not-yet-recorded window.
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
    for (const r of nodeRuns.data?.rows ?? []) {
      m.set(r.node, r);
    }
    return m;
  }, [nodeRuns.data]);

  // Per-node rows in topological order (upstream first) so the breakdown
  // reads the way the pipeline runs, not in whatever order the query
  // returned them.
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

  // DAG colors. Two sources, merged: the live progress channel paints
  // the DAG while the run is in flight; node_runs rows take over once
  // they land (authoritative, and they carry the final status). Sources
  // have neither — they stay uncolored, which is correct.
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

  // Lineage feeds the drawer's "Inputs" section: who upstream of the
  // clicked node, and which table they wrote.
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
      const defs = cfg.output_definitions as
        | Record<string, unknown>
        | undefined;
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

  // Build the drawer payload for the currently-selected node, if any.
  const drawerData = useMemo(() => {
    if (!selectedNodeId) return null;
    const nr = perNode.get(selectedNodeId);
    if (!nr) {
      // Selected node has no run row — drawer still renders with spec
      // only; invocations stays empty so "This run" / "Step logs"
      // sections hide.
      return {
        spec: nodeSpecs.get(selectedNodeId) ?? null,
        invocations: [] as { nodeRun: NodeRun; runId: string }[],
        tableLabel: nodeOutputs.get(selectedNodeId)?.table ?? null,
        tableHref: nodeOutputs.has(selectedNodeId)
          ? (() => {
              const o = nodeOutputs.get(selectedNodeId)!;
              return `/tables/${encodeURIComponent(o.catalog)}/${encodeURIComponent(o.schema)}/${encodeURIComponent(o.table)}`;
            })()
          : null,
      };
    }
    return {
      spec: nodeSpecs.get(selectedNodeId) ?? null,
      invocations: [{ nodeRun: nr, runId }],
      tableLabel: nodeOutputs.get(selectedNodeId)?.table ?? null,
      tableHref: nodeOutputs.has(selectedNodeId)
        ? (() => {
            const o = nodeOutputs.get(selectedNodeId)!;
            return `/tables/${encodeURIComponent(o.catalog)}/${encodeURIComponent(o.schema)}/${encodeURIComponent(o.table)}`;
          })()
        : null,
    };
  }, [selectedNodeId, perNode, nodeSpecs, nodeOutputs, runId]);

  useChrome(
    useMemo<PageChrome>(() => {
      const crumbs = [{ label: "Pipelines", to: "/pipelines" }];
      if (dir) {
        crumbs.push({
          label: pipelineName,
          to: `/pipelines/dashboard?dir=${encodeURIComponent(dir)}`,
        });
        if (runId) {
          crumbs.push({
            label: `Run ${runId.slice(0, 8)}`,
            to: `/pipelines/run?dir=${encodeURIComponent(dir)}&run=${encodeURIComponent(runId)}`,
          });
        }
      }
      return { breadcrumbs: crumbs };
    }, [dir, runId, pipelineName]),
  );

  if (!dir || !runId) {
    return (
      <div className="mx-auto w-full max-w-6xl px-6 py-8">
          <p className="text-sm text-muted-foreground">
            Missing required <code>dir</code> and <code>run</code> query
            parameters.
          </p>
      </div>
    );
  }

  return (
    <div className="mx-auto w-full max-w-6xl px-6 py-8">
        <RunHeader
          run={runRow}
          liveStatus={liveStatus}
          runId={runId}
          pipeline={pipelineName}
        />

        {/* Triage strip — image digest + module version. Only renders when
            we have at least one node_runs row that carries them; older runs
            from before v0.14 leave both columns empty. */}
        <TriageStrip rows={Array.from(perNode.values())} />

        {/* DAG */}
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
                      /* read-only on this page */
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

        {/* Drawer — opens on DAG node click. Reuses the dashboard's
            NodeDetailDrawer for transforms; SourceInspectorDrawer for
            synthetic source / external-table nodes. */}
        {selectedNodeId && drawerData && (
          <NodeDetailDrawer
            node={selectedNodeId}
            tableLabel={drawerData.tableLabel}
            tableHref={drawerData.tableHref}
            spec={drawerData.spec}
            invocations={drawerData.invocations}
            selectedRunId={runId}
            onSelectRun={() => {
              /* RunDetail shows exactly one run; selection is a no-op. */
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

        {/* Per-node breakdown */}
        <Card className="mt-6">
          <CardHeader className="flex-row items-center justify-between pb-3">
            <CardTitle>Per-node breakdown</CardTitle>
            {nodeRuns.data && (
              <span className="text-xs text-muted-foreground">
                {nodeRuns.data.rows.length} invocation
                {nodeRuns.data.rows.length === 1 ? "" : "s"}
              </span>
            )}
          </CardHeader>
          <CardContent className="p-0">
            {isRunning && (
              <div className="p-6 text-sm text-muted-foreground">
                Run in progress — per-node detail appears when it
                completes. The DAG above colors live as each node runs.
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
                No node_runs rows match this execution. The runner may not
                have written them yet, or this run pre-dates the table.
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
    </div>
  );
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
  // Status from the live progress channel — used while the run is in
  // flight, before the runs-table rollup row exists.
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
// Triage strip — image digest + module version, surfaced once at the top so
// users don't have to scan every per-node row to learn "which build of the
// runner produced this execution?". Falls back to "unknown" when the columns
// are empty (run pre-dates v0.14 or env wasn't threaded).
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
            <span title="Rows written to Iceberg outputs this run">
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
