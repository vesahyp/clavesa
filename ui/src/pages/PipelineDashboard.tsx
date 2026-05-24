/**
 * PipelineDashboard — read-only per-pipeline view at
 * /pipelines/dashboard?dir=…
 *
 * Closes the loop between the Catalog drill-in (TableDetail) and editor
 * authoring without forcing the user into the editor. Three panes:
 *   - DAG with last-run state overlaid (reuses the live SFN status hook)
 *   - Recent SFN executions (links to console; expandable failures show
 *     CloudWatch logs via the same component the StatusPanel uses)
 *   - Output tables produced by this pipeline (filtered Catalog rows,
 *     each linking back to /tables/:db/:table)
 *
 * Authoring is one click away via the "Open editor" button. Everything on
 * this page is data the user already has — no new backend.
 */

import { useCallback, useEffect, useMemo, useState } from "react";
import { useSearchParams, useNavigate } from "react-router-dom";
import { ArrowUp, ChevronRight, FileWarning, History, Loader2, Play, Settings } from "lucide-react";
import { ReactFlowProvider } from "@xyflow/react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import "@xyflow/react/dist/style.css";

import { useChrome, type PageChrome } from "@/components/PageChrome";
import { formatRelative } from "@/lib/format";
import {
  topoOrder,
  runIsTerminal,
  runIsFailed,
  type Health,
} from "@/lib/runStatus";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Skeleton } from "@/components/ui/skeleton";
import {
  PipelineGraph as PipelineGraphView,
  deriveNodeOutputs,
} from "@/components/PipelineGraph";
import { PipelineHealthHeader } from "@/components/PipelineHealthHeader";
import { NodesGrid, type NodeInfo } from "@/components/NodesGrid";
import type { NodeSpec } from "@/components/NodeDetailDrawer";
import { BackfillStageDialog } from "@/components/BackfillStageDialog";
import { ConfigPanel } from "@/components/ConfigPanel";
import { NodePalette } from "@/components/NodePalette";
import { DataPreview } from "@/components/DataPreview";
import { ValidationBadge } from "@/components/ValidationBadge";
import { PipelineSettings } from "@/components/PipelineSettings";
import { RunDetailSheet } from "@/components/RunDetailSheet";
import { SourceInspectorDrawer } from "@/components/SourceInspectorDrawer";
import type { Column, Node } from "@/types/pipeline";
import {
  useBackfills,
  useCatalogTables,
  useEnvironmentMode,
  useExecutionStates,
  useLineage,
  useNodeRuns,
  usePipelineStatus,
  usePipelines,
  useRuns,
  useTablesState,
  type BackfillRun,
  type ExecutionInfo,
  type Run,
  type TableInfo,
} from "@/lib/queries";
import { getPipeline, runPipeline } from "@/api/pipeline";
import { getPipelineModuleVersion, upgradePipeline } from "@/api/workspace";
import type { PipelineGraph as PipelineGraphType } from "@/types/pipeline";

// incomingTransformEdgeAliases returns the aliases by which a node
// reads its transform-upstream peers. Used to populate the
// "Incremental upstream reads" toggle list in ConfigPanel.
function incomingTransformEdgeAliases(
  g: PipelineGraphType,
  nodeId: string,
): string[] {
  const transformIds = new Set(
    g.nodes.filter((n) => n.type === "transform").map((n) => n.id),
  );
  const aliases: string[] = [];
  for (const e of g.edges) {
    if (e.to_node !== nodeId) continue;
    if (!transformIds.has(e.from_node)) continue;
    const alias = e.to_input === "" || e.to_input === "default"
      ? e.from_node
      : e.to_input;
    aliases.push(alias);
  }
  return aliases.sort();
}

// incomingTransformEdgeMap returns alias → upstream-node-ID for every
// transform→transform edge into nodeId. Feeds TransformInputsSection so
// intra-pipeline node inputs render alongside source and external-table
// inputs.
function incomingTransformEdgeMap(
  g: PipelineGraphType,
  nodeId: string,
): Record<string, string> {
  const transformIds = new Set(
    g.nodes.filter((n) => n.type === "transform").map((n) => n.id),
  );
  const out: Record<string, string> = {};
  for (const e of g.edges) {
    if (e.to_node !== nodeId) continue;
    if (!transformIds.has(e.from_node)) continue;
    const alias =
      e.to_input === "" || e.to_input === "default" ? e.from_node : e.to_input;
    out[alias] = e.from_node;
  }
  return out;
}

export function PipelineDashboard() {
  const [searchParams, setSearchParams] = useSearchParams();
  const navigate = useNavigate();
  const qc = useQueryClient();
  const dir = searchParams.get("dir") ?? "";
  const selectedRunId = searchParams.get("run") ?? null;
  // Open / close the run-detail Sheet via URL — single source of truth so
  // deep links and the back button both work. Replace (not push) so the
  // browser history doesn't grow a sheet-open / sheet-close pair per click.
  const openRunDetail = (runId: string) => {
    setSearchParams(
      (prev) => {
        prev.set("run", runId);
        return prev;
      },
      { replace: true },
    );
  };
  const closeRunDetail = () => {
    setSearchParams(
      (prev) => {
        prev.delete("run");
        return prev;
      },
      { replace: true },
    );
  };

  // Resolve pipeline name from dir via the workspace pipeline list.
  const pipelines = usePipelines();
  const pipelineMeta = useMemo(
    () => pipelines.data?.find((p) => p.dir === dir),
    [pipelines.data, dir],
  );
  const pipelineName = pipelineMeta?.name ?? "";

  // DAG. Loaded into local state because the editable graph mutates it on
  // every node/edge change (fetchPipeline is the canonical resync after).
  const [graph, setGraph] = useState<PipelineGraphType | null>(null);
  const [graphErr, setGraphErr] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const fetchPipeline = useCallback(async () => {
    if (!dir) return;
    try {
      const g = await getPipeline(dir);
      setGraph(g);
      setGraphErr(null);
      setActionError(null);
    } catch (e) {
      setGraphErr(e instanceof Error ? e.message : String(e));
    }
  }, [dir]);
  useEffect(() => {
    fetchPipeline();
  }, [fetchPipeline]);

  // Editor surface state — selected node opens ConfigPanel; preview node
  // opens DataPreview modal; settings open the PipelineSettings modal.
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  // Synthetic source / external-table nodes don't live in graph.nodes and
  // aren't editable via ConfigPanel — they render their own inspector.
  const [inspectedSyntheticId, setInspectedSyntheticId] = useState<string | null>(null);
  const [previewNodeId, setPreviewNodeId] = useState<string | null>(null);
  const [previewKey, setPreviewKey] = useState(0);
  const [previewSql, setPreviewSql] = useState<string | null>(null);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [nodeSchemas, setNodeSchemas] = useState<Map<string, Column[]>>(new Map());

  // Catalog-derived columns for every node whose output Iceberg table
  // already exists. Merged into the schemas passed to the DAG so each
  // node card lists its columns without requiring the user to click
  // Preview first; live-preview entries (richer, possibly from unsaved
  // edits) take precedence on overlap.
  const catalogTables = useCatalogTables();
  const combinedNodeSchemas = useMemo(() => {
    const m = new Map<string, Column[]>();
    for (const t of catalogTables.data?.tables ?? []) {
      if (t.dir !== dir || !t.owning_node) continue;
      if (m.has(t.owning_node)) continue;
      m.set(
        t.owning_node,
        t.columns.map((c) => ({ name: c.name, type: c.type, nullable: true })),
      );
    }
    for (const [id, cols] of nodeSchemas) m.set(id, cols);
    return m;
  }, [catalogTables.data, dir, nodeSchemas]);

  // Node IDs whose Iceberg output table actually exists in the workspace
  // catalog right now. Drives the auto-sample affordance in ConfigPanel —
  // a transform with no committed snapshot has nothing to show, so we
  // skip the sample entirely rather than render an error state.
  const nodesWithExistingOutput = useMemo(() => {
    const s = new Set<string>();
    for (const t of catalogTables.data?.tables ?? []) {
      if (t.dir !== dir || !t.owning_node) continue;
      s.add(t.owning_node);
    }
    return s;
  }, [catalogTables.data, dir]);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewedNodeId, setPreviewedNodeId] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<"graph" | "runs">("graph");

  // Per-node Iceberg output table for the DAG node footers — same
  // derivation the editor uses, so both DAGs name their outputs alike.
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

  // Live status overlay. For local pipelines we don't have an SFN ARN —
  // dispatch by `dir` and let LocalProvider read the filesystem progress
  // channel (ADR-014). Cloud pipelines keep the legacy ARN path so the SFN
  // history continues to back the polling.
  const isLocal = pipelineMeta?.compute === "local";
  // Workspace env mode is the right gate for "is this user running cloud
  // stuff right now?" decisions (Recent executions, Backfills). `isLocal`
  // above is per-pipeline and tells us which runner channel to poll —
  // distinct concept; don't conflate the two. A workspace in local env
  // shouldn't nag about cloud deployments even for pipelines that haven't
  // declared `compute = "local"`.
  const envMode = useEnvironmentMode();
  const isLocalEnv = envMode.data?.mode === "local";
  // A pipeline with zero nodes has no compute target, no warehouse, no
  // runs anywhere — querying observability surfaces produces backend
  // errors (cloud Athena 400, local runner spin-up) that surface as
  // console 500s on first-launch. Gate every observability hook on
  // having at least one node; the "empty pipeline" empty-state below
  // owns the user-facing message.
  const hasNodes = (graph?.nodes.length ?? 0) > 0;
  const queryDir = hasNodes ? dir : "";
  const status = usePipelineStatus(queryDir);
  const trackedExecutionArn = useMemo(() => {
    if (isLocalEnv) return "";
    const execs = status.data?.executions ?? [];
    const running = execs.find((e) => e.status === "RUNNING");
    if (running) return running.execution_arn;
    return execs[0]?.execution_arn ?? "";
  }, [status.data, isLocalEnv]);
  // Live-states dispatch follows the workspace env mode, not the per-
  // pipeline `compute` attr. The local channel (state.json) is written
  // by `clavesa pipeline run` regardless of whether the pipeline
  // declares compute = "local", so a workspace in local env always has
  // a valid filesystem source to poll. Conflating the two surfaces the
  // same kind of bug the cloud-nag fix already had: a pipeline that
  // doesn't declare compute silently falls into the cloud path and
  // never sees its live state.
  const states = useExecutionStates(
    isLocalEnv ? { dir: queryDir } : { arn: trackedExecutionArn },
  );
  const nodeStatuses = useMemo(() => {
    const m = new Map<string, "running" | "succeeded" | "failed">();
    for (const [name, s] of Object.entries(states.data?.states ?? {})) {
      if (s.status === "RUNNING") m.set(name, "running");
      else if (s.status === "SUCCEEDED") m.set(name, "succeeded");
      else if (s.status === "FAILED") m.set(name, "failed");
    }
    return m;
  }, [states.data]);

  // Pipeline names land in node_runs/runs as the literal `pipeline_name`
  // var.tf value — what `pipeline create` writes, dashes preserved. The
  // API validator accepts the hyphenated form (separate from the Glue
  // identifier check). Pass the dir-derived name as-is.
  const queryPipeline = hasNodes ? pipelineName : "";

  const nodeRuns = useNodeRuns(queryPipeline, {
    limit: 200,
    dir: queryDir,
  });

  // Per-execution rollup from the EventBridge-writer-populated runs table.
  // Pairs with /pipeline/status for the live "is anything running now?"
  // signal: status reports current SFN executions (90-day retention),
  // runs is the materialized history that survives beyond retention.
  const runs = useRuns(queryPipeline, {
    limit: 50,
    dir: queryDir,
  });

  // Synthetic in-flight run for the Runs grid. Local runs only get a
  // runs-table row at end-of-run (recordLocalRun), so during the actual
  // execution there's no column for the grid to render the live states
  // against. When the live-state channel reports RUNNING, append a
  // synthetic row sourced from it so cells can paint pending/running
  // *while the run is happening*. Drops itself the instant the real row
  // lands (they share run_id, so the de-dup check on the next refetch
  // wins).
  const gridRuns = useMemo<Run[]>(() => {
    const rows = runs.data?.rows ?? [];
    const live = states.data;
    if (!live || live.status !== "RUNNING" || !live.run_id) return rows;
    if (rows.some((r) => r.run_id === live.run_id)) return rows;
    const synthetic: Run = {
      run_id: live.run_id,
      pipeline: pipelineName,
      sf_execution_arn: live.run_id,
      status: "RUNNING",
      trigger: "manual",
      started_at: live.started_at || new Date().toISOString(),
      ended_at: "",
      duration_ms: null,
      failed_step: "",
      error_class: "",
      error_msg: "",
    };
    return [...rows, synthetic];
  }, [runs.data, states.data, pipelineName]);

  // Backfills (Gate 1). Service layer dispatches on env mode — cloud goes
  // through Lambda + Glue, local replays through the workspace runner
  // (ADR-014 parity). Errors are non-fatal; the card swallows them.
  const backfills = useBackfills(hasNodes ? dir : "");
  const transformNodeIds = useMemo(
    () =>
      (graph?.nodes ?? [])
        .filter((n) => (n.type as string) === "transform")
        .map((n) => n.id),
    [graph],
  );
  const [bfDialogOpen, setBfDialogOpen] = useState(false);

  // Per-output-table snapshot summary from <pipeline>.tables, keyed by the
  // short `<node>__<output_key>` name so the Nodes grid can show each
  // node's row count + freshness next to its run history.
  const tablesState = useTablesState(queryPipeline, {
    limit: 50,
    dir: queryDir,
  });
  const tablesStateByName = useMemo(() => {
    const m = new Map<string, TableInfo>();
    for (const r of tablesState.data?.rows ?? []) {
      m.set(r.table_name, r);
    }
    return m;
  }, [tablesState.data]);

  // Per-node output-table state for the Nodes grid's left panel: the
  // Iceberg table each transform writes, with its current row count and
  // freshness. nodeOutputs (derived from .tf + lineage) gives the table
  // identity; tablesState gives the live counts.
  const nodeInfo = useMemo(() => {
    const m = new Map<string, NodeInfo>();
    for (const [nodeId, out] of nodeOutputs) {
      const st = tablesStateByName.get(out.table);
      m.set(nodeId, {
        table: out.table.replace(/__default$/, ""),
        href: `/tables/${encodeURIComponent(out.catalog)}/${encodeURIComponent(out.schema)}/${encodeURIComponent(out.table)}`,
        rowCount: st?.row_count ?? null,
        snapshotTs: st?.snapshot_ts ?? null,
      });
    }
    return m;
  }, [nodeOutputs, tablesStateByName]);

  // Per-node static spec for the detail drawer: what each node reads
  // (inbound lineage edges), and how it writes (output mode + merge keys
  // from the .tf config). Same for every run — the drawer pairs it with a
  // chosen run's invocation facts.
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

  // Grid rows: non-source nodes in topological order, plus any node seen
  // in node_runs but missing from the current graph (renamed/removed).
  const nodeOrder = useMemo(() => {
    const gNodes = (graph?.nodes ?? []).filter(
      (n) => (n.type as string) !== "source",
    );
    const ordered = topoOrder(
      gNodes.map((n) => n.id),
      graph?.edges ?? [],
    );
    const known = new Set(ordered);
    const extra = [
      ...new Set((nodeRuns.data?.rows ?? []).map((r) => r.node)),
    ]
      .filter((n) => !known.has(n))
      .sort();
    return [...ordered, ...extra];
  }, [graph, nodeRuns.data]);

  // Health verdict for the header. Derived from runs + the live node
  // overlay; never reads `status.deployed` — a local pipeline reports
  // deployed:false and that is not unhealthy (ADR-014).
  const health = useMemo(() => {
    const rows = [...(runs.data?.rows ?? [])].sort((a, b) =>
      b.started_at.localeCompare(a.started_at),
    );
    const anyRunningNode = [...nodeStatuses.values()].includes("running");
    if (runs.error) {
      return {
        state: "unknown" as Health,
        successRate: null as number | null,
        successWindow: 0,
        lastRun: rows[0] ?? null,
      };
    }
    if (rows.length === 0 && !anyRunningNode) {
      return {
        state: "never-run" as Health,
        successRate: null as number | null,
        successWindow: 0,
        lastRun: null as Run | null,
      };
    }
    if (rows.some((r) => r.status === "RUNNING") || anyRunningNode) {
      return {
        state: "running" as Health,
        successRate: null as number | null,
        successWindow: 0,
        lastRun: rows[0] ?? null,
      };
    }
    const terminal = rows.filter((r) => runIsTerminal(r.status));
    const last = terminal[0] ?? null;
    const win = terminal.slice(0, 20);
    const ok = win.filter((r) => r.status === "SUCCEEDED").length;
    return {
      state: (!last
        ? "never-run"
        : runIsFailed(last.status)
          ? "failed"
          : "healthy") as Health,
      successRate: win.length ? ok / win.length : null,
      successWindow: win.length,
      lastRun: rows[0] ?? last,
    };
  }, [runs.data, runs.error, nodeStatuses]);

  useChrome(
    useMemo<PageChrome>(() => {
      if (!dir) {
        return { breadcrumbs: [{ label: "Pipelines", to: "/pipelines" }] };
      }
      const name = pipelineName || dir.split("/").pop() || dir;
      return {
        breadcrumbs: [
          { label: "Pipelines", to: "/pipelines" },
          {
            label: name,
            to: `/pipelines/dashboard?dir=${encodeURIComponent(dir)}`,
          },
        ],
        // Pipeline-specific actions (Run / Settings / validation) live in
        // the page's PipelineHealthHeader, not here. The app header is for
        // workspace-level affordances only.
      };
    }, [dir, pipelineName]),
  );

  // Right-side actions for PipelineHealthHeader — Run, validation, settings.
  const headerActions = (
    <>
      {graph && activeTab === "graph" && (
        <ValidationBadge
          errors={graph.validation.errors}
          warnings={graph.validation.warnings}
        />
      )}
      <RunPipelineButton
        dir={dir}
        disabled={!hasNodes}
        onRunSucceeded={() => {
          // Stay on the dashboard — toast + grid refresh are enough
          // confirmation. The new run column appears via the synthetic
          // in-flight row while running and lands as a real column when
          // the query invalidation refetches. Users open the Sheet
          // explicitly by clicking the column header.
          void qc.invalidateQueries({ queryKey: ["runs"] });
          void qc.invalidateQueries({ queryKey: ["node-runs"] });
          void qc.invalidateQueries({ queryKey: ["catalog"] });
          void qc.invalidateQueries({ queryKey: ["execution-states"] });
        }}
      />
      <Button
        onClick={() => setSettingsOpen(true)}
        variant="outline"
        size="icon"
        className="h-8 w-8"
        title="Pipeline settings"
        aria-label="Pipeline settings"
      >
        <Settings className="h-4 w-4" />
      </Button>
    </>
  );

  // Build the selectedNode object for ConfigPanel — same shape as the
  // legacy editor (App.tsx). Memoised so clicking Preview (which sets
  // previewNodeId) doesn't churn the ConfigPanel's useEffect.
  const selectedNodeRaw: Node | undefined =
    graph?.nodes.find((n) => n.id === selectedNodeId);
  const selectedNode = useMemo(() => {
    if (!selectedNodeRaw) return null;
    const data: Record<string, unknown> = { ...selectedNodeRaw.config };
    if (
      selectedNodeRaw.type === "transform" &&
      !data.sql &&
      selectedNodeRaw.preview_sql
    ) {
      data.sql = selectedNodeRaw.preview_sql;
    }
    return {
      id: selectedNodeRaw.id,
      type: selectedNodeRaw.type as string,
      data,
    };
  }, [selectedNodeRaw]);

  // Clear selection when the selected node disappears (e.g. deleted).
  useEffect(() => {
    if (selectedNodeId && !selectedNodeRaw) {
      setSelectedNodeId(null);
    }
  }, [selectedNodeId, selectedNodeRaw]);

  // Escape closes the read-only source-inspector drawer. The ConfigPanel
  // owns its own Esc handler (collapse-then-close) so it isn't reset
  // here.
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key !== "Escape") return;
      if (previewNodeId || settingsOpen) return;
      setInspectedSyntheticId(null);
    }
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [previewNodeId, settingsOpen]);

  const previewNodeRaw = previewNodeId
    ? graph?.nodes.find((n) => n.id === previewNodeId)
    : null;

  if (!dir) {
    return (
      <div className="mx-auto w-full max-w-6xl px-6 py-8">
          <Card className="border-destructive/40 bg-destructive/5">
            <CardHeader className="flex-row items-start gap-3">
              <FileWarning className="mt-0.5 h-5 w-5 text-destructive" />
              <div>
                <CardTitle className="text-destructive">Missing dir parameter</CardTitle>
                <p className="mt-1 text-xs text-muted-foreground">
                  The dashboard URL needs a <code>?dir=</code> query parameter
                  pointing at a pipeline directory.
                </p>
              </div>
            </CardHeader>
          </Card>
    </div>
    );
  }

  return (
    <div className="mx-auto w-full max-w-6xl px-6 py-8">
      <PipelineHealthHeader
        pipelineName={pipelineName}
        dir={dir}
        nodeCount={graph?.nodes.length ?? 0}
        state={health.state}
        successRate={health.successRate}
        successWindow={health.successWindow}
        lastRun={health.lastRun}
        cloud={pipelineMeta?.cloud}
        moduleSlot={<ModuleVersionChip dir={dir} />}
        actionsSlot={headerActions}
      />

      {graph && !hasNodes && (
        <Card>
          <CardContent className="p-8 text-center">
            <p className="text-sm text-muted-foreground">
              This pipeline has no nodes yet.
            </p>
            <p className="mt-2 text-xs text-muted-foreground">
              Use the + button on the canvas below to add a source,
              transform, or destination.
            </p>
            <div className="relative mt-6 h-[420px] w-full overflow-hidden rounded-md border">
              <ReactFlowProvider>
                <PipelineGraphView
                  graph={graph}
                  dir={dir}
                  onGraphUpdate={fetchPipeline}
                  onError={setActionError}
                  enableQuickAdd
                  nodeStatuses={nodeStatuses}
                  nodeOutputs={nodeOutputs}
                  showSources
                />
              </ReactFlowProvider>
              <NodePalette dir={dir} onGraphUpdate={fetchPipeline} />
            </div>
          </CardContent>
        </Card>
      )}

      {graphErr && !hasNodes && (
        <div className="rounded-md border border-destructive/40 bg-destructive/5 p-4 text-sm text-destructive">
          {graphErr}
        </div>
      )}

      {hasNodes && (
        <Tabs
          value={activeTab}
          onValueChange={(v) => setActiveTab(v as "graph" | "runs")}
        >
          <TabsList>
            <TabsTrigger value="graph">Graph</TabsTrigger>
            <TabsTrigger value="runs">Runs</TabsTrigger>
          </TabsList>

          {/* Runs — node × run matrix: one row per node, columns are recent runs.
              Cloud-only execution / backfill cards sit below it. */}
          <TabsContent value="runs" className="space-y-6">
            <Card>
              <CardHeader className="flex-row items-center justify-between pb-3">
                <CardTitle>Runs</CardTitle>
                <div className="flex items-center gap-3">
                  {/* Background-fetch indicator. `isLoading` is first-load
                      only; once the cache has data, polling refetches set
                      `isFetching` instead — without this label the grid
                      reads as "stuck" while Spark warms or runs land. */}
                  {(runs.isFetching || nodeRuns.isFetching) &&
                    !runs.isLoading &&
                    !nodeRuns.isLoading && (
                      <span className="flex items-center gap-1.5 text-[11px] text-muted-foreground">
                        <Loader2 className="h-3 w-3 animate-spin" />
                        refreshing
                      </span>
                    )}
                  {runs.data && (
                    <span className="text-xs text-muted-foreground">
                      {nodeOrder.length} node
                      {nodeOrder.length === 1 ? "" : "s"} · last{" "}
                      {gridRuns.length}
                      {runs.data.truncated ? "+" : ""} runs
                    </span>
                  )}
                </div>
              </CardHeader>
              <CardContent className="p-0">
                {/* Skeleton only while we genuinely have nothing — gridRuns
                    includes the synthetic in-flight column, so as soon as
                    that exists we render the grid even if node_runs is
                    still loading (it goes slow while the runner container
                    has the Spark worker pinned during a live run). */}
                {gridRuns.length === 0 && (runs.isLoading || nodeRuns.isLoading) && (
                  <div className="space-y-2 p-6">
                    <Skeleton className="h-4 w-full" />
                    <Skeleton className="h-4 w-2/3" />
                  </div>
                )}
                {gridRuns.length === 0 &&
                  !runs.isLoading &&
                  !nodeRuns.isLoading &&
                  Boolean(runs.error) && (
                    <div className="p-6 text-xs text-muted-foreground">
                      No runs recorded for this pipeline yet. Trigger one with{" "}
                      <strong>Run pipeline</strong> — each run adds a column.
                    </div>
                  )}
                {gridRuns.length > 0 && (
                  <NodesGrid
                    runs={gridRuns}
                    nodeRuns={nodeRuns.data?.rows ?? []}
                    nodeOrder={nodeOrder}
                    nodeInfo={nodeInfo}
                    nodeSpecs={nodeSpecs}
                    liveStates={nodeStatuses}
                    isLocal={isLocal}
                    dir={dir}
                    onRunSelect={openRunDetail}
                  />
                )}
              </CardContent>
            </Card>

            {/* Recent executions — cloud-only (SFN), workspace-env gated. */}
            {!isLocalEnv && (
              <Card>
                <CardHeader className="pb-3">
                  <CardTitle>Recent executions</CardTitle>
                </CardHeader>
                <CardContent className="p-0">
                  {status.isLoading && (
                    <div className="space-y-2 p-6">
                      <Skeleton className="h-4 w-full" />
                      <Skeleton className="h-4 w-5/6" />
                      <Skeleton className="h-4 w-2/3" />
                    </div>
                  )}
                  {status.data && !status.data.deployed && (
                    <div className="p-6 text-sm text-muted-foreground">
                      Pipeline is not deployed yet. Run{" "}
                      <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs text-foreground">
                        terraform apply
                      </code>{" "}
                      in <code className="font-mono">{dir}</code> to deploy.
                    </div>
                  )}
                  {status.data &&
                    status.data.deployed &&
                    status.data.executions.length === 0 && (
                      <div className="p-6 text-sm text-muted-foreground">
                        No executions yet.
                      </div>
                    )}
                  {status.data && status.data.executions.length > 0 && (
                    <ul className="divide-y divide-border">
                      {status.data.executions.slice(0, 10).map((e) => (
                        <ExecutionListItem key={e.execution_arn} e={e} />
                      ))}
                    </ul>
                  )}
                </CardContent>
              </Card>
            )}

            {/* Backfills (Gate 1) — local + cloud (ADR-014). */}
            <Card>
              <CardHeader className="flex-row items-center justify-between pb-3">
                <CardTitle className="flex items-center gap-2">
                  <History className="h-4 w-4 text-muted-foreground" />
                  Backfills
                </CardTitle>
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => setBfDialogOpen(true)}
                  disabled={transformNodeIds.length === 0}
                >
                  Stage backfill
                </Button>
              </CardHeader>
              <CardContent className="p-0">
                {backfills.isLoading && (
                  <div className="space-y-2 p-6">
                    <Skeleton className="h-4 w-2/3" />
                    <Skeleton className="h-4 w-1/2" />
                  </div>
                )}
                {Boolean(backfills.error) && (
                  <div className="p-6 text-xs text-muted-foreground">
                    {isLocalEnv
                      ? "Couldn't list backfills for this local pipeline. Run the pipeline at least once so the warehouse has a canonical target."
                      : "Backfills require a deployed pipeline (Lambda + Glue). Apply the pipeline first, then stage a backfill here."}
                  </div>
                )}
                {backfills.data &&
                  backfills.data.backfills.length === 0 && (
                    <div className="p-6 text-sm text-muted-foreground">
                      No open backfills. Stage one to replay a transform
                      over a historical partition window.
                    </div>
                  )}
                {backfills.data && backfills.data.backfills.length > 0 && (
                  <ul className="divide-y divide-border">
                    {backfills.data.backfills.map((b) => (
                      <BackfillRow key={b.run_id} bf={b} dir={dir} />
                    ))}
                  </ul>
                )}
              </CardContent>
            </Card>
          </TabsContent>

          {/* Graph — editable canvas with right-edge ConfigPanel drawer
              and floating NodePalette. Authoring surface lives here. */}
          <TabsContent value="graph">
            <Card>
              <CardContent className="p-0">
                <div className="relative h-[calc(100vh-280px)] min-h-[480px] w-full overflow-hidden">
                  {actionError && (
                    <button
                      type="button"
                      onClick={() => setActionError(null)}
                      className="absolute left-1/2 top-3 z-30 -translate-x-1/2 rounded-md border border-status-failed/40 bg-status-failed/10 px-4 py-2 text-sm text-status-failed"
                      title="Dismiss"
                    >
                      {actionError}
                    </button>
                  )}
                  {graph && (
                    <ReactFlowProvider>
                      <PipelineGraphView
                        graph={graph}
                        dir={dir}
                        onGraphUpdate={fetchPipeline}
                        onError={setActionError}
                        enableQuickAdd
                        onNodeClick={(id) => {
                          if (id.startsWith("source:") || id.startsWith("external:")) {
                            // Synthetic non-editable nodes get the
                            // read-only inspector instead of ConfigPanel.
                            setInspectedSyntheticId((prev) => (prev === id ? null : id));
                            setSelectedNodeId(null);
                            setPreviewNodeId(null);
                            setPreviewSql(null);
                            setPreviewedNodeId(null);
                            return;
                          }
                          setSelectedNodeId((prev) => (prev === id ? null : id));
                          setInspectedSyntheticId(null);
                          setPreviewNodeId(null);
                          setPreviewSql(null);
                          setPreviewedNodeId(null);
                        }}
                        nodeSchemas={combinedNodeSchemas}
                        nodeOutputs={nodeOutputs}
                        loadingNodeId={previewLoading ? previewNodeId : null}
                        previewedNodeId={previewedNodeId}
                        nodeStatuses={nodeStatuses}
                        showSources
                        focusNodeId={selectedNodeId}
                      />
                    </ReactFlowProvider>
                  )}
                  <NodePalette dir={dir} onGraphUpdate={fetchPipeline} />
                </div>
              </CardContent>
            </Card>
          </TabsContent>

        </Tabs>
      )}

      {/* Drawers — fixed at AppShell level (top-14) so they get the full
          app height regardless of the tab content size. Source / external
          synthetic nodes get a read-only inspector; transforms (and
          destinations) get the full ConfigPanel. */}
      {activeTab === "graph" && inspectedSyntheticId && (
        <SourceInspectorDrawer
          nodeId={inspectedSyntheticId}
          onClose={() => setInspectedSyntheticId(null)}
        />
      )}
      {activeTab === "graph" && selectedNode && (
        <ConfigPanel
          dir={dir}
          selectedNode={selectedNode}
          onGraphUpdate={fetchPipeline}
          onClose={() => setSelectedNodeId(null)}
          onPreview={(nodeId, sql) => {
            setPreviewNodeId(nodeId);
            setPreviewSql(sql ?? null);
            setPreviewKey((k) => k + 1);
          }}
          onNodeDeleted={() => {
            setSelectedNodeId(null);
            setPreviewNodeId(null);
          }}
          onNodeRenamed={async (newId) => {
            await fetchPipeline();
            setSelectedNodeId(newId);
            setPreviewNodeId(null);
          }}
          nodeSchemas={combinedNodeSchemas}
          incomingTransformAliases={
            selectedNodeId && graph
              ? incomingTransformEdgeAliases(graph, selectedNodeId)
              : []
          }
          upstreamNodeIds={
            selectedNodeId && graph
              ? graph.nodes
                  .filter(
                    (n) =>
                      n.type === "transform" &&
                      n.id !== selectedNodeId,
                  )
                  .map((n) => n.id)
              : []
          }
          nodeInputs={
            selectedNodeId && graph
              ? incomingTransformEdgeMap(graph, selectedNodeId)
              : {}
          }
          output={
            selectedNodeId && nodesWithExistingOutput.has(selectedNodeId)
              ? nodeOutputs.get(selectedNodeId)
              : undefined
          }
        />
      )}

      {/* Preview — centered modal (Dialog portal). Renders only while a
          preview is open AND the Graph tab is active. */}
      {activeTab === "graph" && previewNodeId && previewNodeRaw && (
        <DataPreview
          key={previewKey}
          dir={dir}
          nodeId={previewNodeId}
          nodeType={previewNodeRaw.type as string}
          sqlOverride={previewSql ?? undefined}
          onClose={() => {
            setPreviewNodeId(null);
            setPreviewSql(null);
            setPreviewLoading(false);
            setPreviewedNodeId(null);
          }}
          onSchema={(schemas) => {
            setNodeSchemas((prev) => {
              const next = new Map(prev);
              for (const [id, cols] of schemas) {
                if (id === "__input__" && graph) {
                  for (const edge of graph.edges) {
                    if (edge.to_node === previewNodeId) {
                      next.set(edge.from_node, cols);
                    }
                  }
                } else {
                  next.set(id, cols);
                }
              }
              return next;
            });
          }}
          onLoadingChange={(loading) => {
            setPreviewLoading(loading);
            if (!loading) setPreviewedNodeId(previewNodeId);
          }}
        />
      )}

      {settingsOpen && (
        <PipelineSettings dir={dir} onClose={() => setSettingsOpen(false)} />
      )}

      <BackfillStageDialog
        open={bfDialogOpen}
        onOpenChange={setBfDialogOpen}
        dir={dir}
        transformNodes={transformNodeIds}
        onStaged={(run) => {
          void qc.invalidateQueries({ queryKey: ["backfills"] });
          void qc.invalidateQueries({ queryKey: ["catalog"] });
          navigate(
            `/backfills?dir=${encodeURIComponent(dir)}&run=${encodeURIComponent(run.run_id)}`,
          );
        }}
      />

      {/* Per-run drill-down opens via `?run=…` from the NodesGrid header. */}
      <RunDetailSheet
        dir={dir}
        runId={selectedRunId}
        onClose={closeRunDetail}
      />
    </div>
  );
}

function BackfillRow({ bf, dir }: { bf: BackfillRun; dir: string }) {
  const navigate = useNavigate();
  const open = () =>
    navigate(
      `/backfills?dir=${encodeURIComponent(dir)}&run=${encodeURIComponent(bf.run_id)}`,
    );
  const window = `${bf.from_cursor.join("/")} → ${bf.to_cursor.join("/")}`;
  return (
    <li
      role="button"
      tabIndex={0}
      onClick={open}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          open();
        }
      }}
      className="flex cursor-pointer items-center gap-3 px-6 py-2.5 transition-colors hover:bg-muted/40"
    >
      <Badge variant="outline" className="font-mono text-[10px] uppercase">
        staging
      </Badge>
      <div className="min-w-0 flex-1">
        <div className="truncate font-mono text-xs">{bf.run_id}</div>
        <div className="text-[11px] text-muted-foreground">
          <code className="font-mono">{bf.node}</code>
          <span className="mx-1.5">·</span>
          <span>{window}</span>
        </div>
      </div>
      <ChevronRight className="h-4 w-4 text-muted-foreground" />
    </li>
  );
}

function ExecutionListItem({ e }: { e: ExecutionInfo }) {
  return (
    <li className="flex items-center gap-3 px-6 py-2.5">
      <Badge
        variant={
          e.status === "SUCCEEDED"
            ? "success"
            : e.status === "FAILED"
              ? "failed"
              : e.status === "RUNNING"
                ? "running"
                : "outline"
        }
        className="font-mono text-[10px]"
      >
        {e.status}
      </Badge>
      <div className="min-w-0 flex-1">
        <div className="truncate font-mono text-xs">{e.name}</div>
        <div className="text-[11px] text-muted-foreground">
          {formatRelative(e.started_at)}
        </div>
      </div>
      {e.console_url && (
        <a
          href={e.console_url}
          target="_blank"
          rel="noopener noreferrer"
          className="text-[11px] text-primary hover:underline"
        >
          Console
        </a>
      )}
    </li>
  );
}

// ---------------------------------------------------------------------------
// RunPipelineButton
// ---------------------------------------------------------------------------

interface RunPipelineButtonProps {
  dir: string;
  disabled?: boolean;
  onRunSucceeded: (result: import("@/api/pipeline").RunPipelineResult) => void;
}

/**
 * Triggers POST /pipeline/run, which backend-routes by compute attr:
 *   - local pipelines block until service.RunPipeline returns (~30-60s for
 *     a single-node pipeline against the warm Spark worker we boot at UI
 *     start; cold Spark in a freshly-started session adds ~15s once).
 *   - cloud pipelines start an SFN execution and return immediately with
 *     execution_arn; observability polling picks the run up from there.
 *
 * Local long-blocking is fine for one user / one pipeline; the spinner +
 * disabled state are the entire UX while it runs. If we ever want
 * streaming progress on local runs the filesystem progress channel under
 * .clavesa/runs/<id>/ is already there — wire to the same polling
 * surface the cloud-side already uses.
 */
function RunPipelineButton({ dir, disabled, onRunSucceeded }: RunPipelineButtonProps) {
  const [running, setRunning] = useState(false);

  async function handleClick() {
    setRunning(true);
    try {
      const result = await runPipeline(dir);
      // Dispatch is async now — the run is starting, not finished.
      toast.success(
        result.execution_arn ? "Execution started" : "Run started",
      );
      onRunSucceeded(result);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : String(err));
    } finally {
      setRunning(false);
    }
  }

  return (
    <Button
      onClick={handleClick}
      disabled={disabled || running}
      size="sm"
      data-testid="run-pipeline"
    >
      {running ? (
        <Loader2 className="h-4 w-4 animate-spin" />
      ) : (
        <Play className="h-4 w-4" />
      )}
      {running ? "Running…" : "Run pipeline"}
    </Button>
  );
}

interface ModuleVersionChipProps {
  dir: string;
}

/**
 * ModuleVersionChip — current vs latest module ref, with a one-click
 * upgrade affordance. The remote ls-remote call is cached by TanStack
 * Query so re-renders of the dashboard don't refire it.
 */
function ModuleVersionChip({ dir }: ModuleVersionChipProps) {
  const qc = useQueryClient();
  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ["module-version", dir],
    queryFn: () => getPipelineModuleVersion(dir),
    enabled: !!dir,
    staleTime: 5 * 60 * 1000, // 5min — git ls-remote isn't free
  });
  const mut = useMutation({
    mutationFn: () => upgradePipeline(dir),
    onSuccess: (resp) => {
      toast.success(
        `Upgraded ${resp.updated} module source${resp.updated === 1 ? "" : "s"}: ${resp.current_ref} → ${resp.target_ref}`,
      );
      void refetch();
      // The graph and run-history shapes can shift on upgrade.
      void qc.invalidateQueries({ queryKey: ["pipelines"] });
    },
    onError: (e) => toast.error(e instanceof Error ? e.message : String(e)),
  });

  if (isLoading) {
    return <Skeleton className="h-6 w-24" />;
  }
  if (error || !data?.current_ref) {
    // No GitHub module sources, or remote unreachable — render nothing
    // rather than a confusing "could not check" state. The PR description
    // covers the affordance; missing-version is a non-event.
    return null;
  }
  const upToDate = data.current_ref === data.latest_ref;
  return (
    <div className="flex items-center gap-1.5">
      <Badge
        variant="outline"
        className="font-mono text-[10px]"
        title={`Module ref in pipeline .tf · latest tag on ${data.repo_url}`}
      >
        Module: {data.current_ref}
        {!upToDate && data.latest_ref && (
          <span className="ml-1 text-muted-foreground">
            → {data.latest_ref}
          </span>
        )}
      </Badge>
      {!upToDate && data.latest_ref && (
        <Button
          size="sm"
          variant="outline"
          className="h-6 gap-1 px-2 text-xs"
          disabled={mut.isPending}
          onClick={() => mut.mutate()}
          data-testid="upgrade-pipeline"
          title={`Rewrite all module ?ref= to ${data.latest_ref} and re-sync orchestration.tf`}
        >
          {mut.isPending ? (
            <Loader2 className="h-3 w-3 animate-spin" />
          ) : (
            <ArrowUp className="h-3 w-3" />
          )}
          Upgrade
        </Button>
      )}
    </div>
  );
}
