/**
 * queries.ts — TanStack Query hooks for the data-first UI.
 *
 * Wraps the API client with React Query so pages get caching,
 * invalidation, polling, and consistent loading/error UX for free.
 * One file for now; split when it gets unwieldy.
 */

import { useQuery } from "@tanstack/react-query";
import { z } from "zod";

import { request, BASE_URL } from "@/api/client";

// ---------------------------------------------------------------------------
// Schemas (boundary-of-trust for API responses)
// ---------------------------------------------------------------------------

const CatalogColumn = z.object({
  name: z.string(),
  type: z.string(),
});

const CatalogTable = z.object({
  database: z.string(),
  name: z.string(),
  owning_pipeline: z.string().optional().default(""),
  owning_node: z.string().optional().default(""),
  output_key: z.string().optional().default(""),
  // Set for local-warehouse tables; threaded into `?dir=` on per-table
  // queries so LocalProvider can route them. Empty for cloud (Glue) tables.
  dir: z.string().optional().default(""),
  // Staleness budget declared in HCL ("4h" → 14400). 0/absent hides the
  // freshness chip on the Catalog tile.
  freshness_sla_seconds: z.number().optional().default(0),
  location: z.string().optional().default(""),
  table_type: z.string().optional().default(""),
  columns: z.array(CatalogColumn).default([]),
  update_time: z.string().nullish(),
});
export type CatalogTable = z.infer<typeof CatalogTable>;

const CatalogResponse = z.object({
  tables: z.array(CatalogTable),
  aws_available: z.boolean(),
});
export type CatalogResponse = z.infer<typeof CatalogResponse>;

const TableQueryColumn = z.object({
  name: z.string(),
  type: z.string(),
});

const TableQueryResult = z.object({
  columns: z.array(TableQueryColumn),
  rows: z.array(z.array(z.string())),
  row_count: z.number(),
  truncated: z.boolean(),
});
export type TableQueryResult = z.infer<typeof TableQueryResult>;

const SnapshotInfo = z.object({
  snapshot_id: z.string(),
  parent_id: z.string().optional().default(""),
  committed_at: z.string(),
  operation: z.string().optional().default(""),
  added_records: z.number().nullish(),
  deleted_records: z.number().nullish(),
  total_records: z.number().nullish(),
  // Provenance from the Delta commitInfo userMetadata — empty for commits
  // written outside clavesa or by a pre-provenance runner image.
  trigger: z.string().optional().default(""),
  writer_run_id: z.string().optional().default(""),
});
export type SnapshotInfo = z.infer<typeof SnapshotInfo>;

const SnapshotsResult = z.object({
  snapshots: z.array(SnapshotInfo),
  latest_record_count: z.number().nullish(),
  truncated: z.boolean(),
});
export type SnapshotsResult = z.infer<typeof SnapshotsResult>;

const PipelineInfo = z.object({
  name: z.string(),
  dir: z.string(),
  node_count: z.number(),
  cloud: z.string().optional().default(""),
  // ADR-014: the UI reads `compute` to choose dir-vs-ARN addressing for
  // observability queries. Local pipelines have no SFN ARN; cloud pipelines
  // do. Treat missing as "lambda" (the transform module default).
  compute: z.string().optional().default(""),
  // ADR-016 schema this pipeline writes into (== the pipeline; one
  // schema, one producing pipeline). Falls back to the sanitized name.
  schema: z.string().optional().default(""),
  // ADR-017 registered sources this pipeline's transforms consume.
  sources: z.array(z.string()).optional().default([]),
});
export type PipelineInfo = z.infer<typeof PipelineInfo>;

// ---------------------------------------------------------------------------
// Hooks
// ---------------------------------------------------------------------------

/** GET /api/workspace/tables — every Delta table in the workspace catalog. */
export function useCatalogTables() {
  return useQuery({
    queryKey: ["catalog", "tables"],
    queryFn: async () => {
      const raw = await request<unknown>("/workspace/tables");
      return CatalogResponse.parse(raw);
    },
  });
}

/**
 * GET /api/data/table — sample rows from one Delta table.
 *
 * Routes through observability.Provider on the server: with `dir` set,
 * local pipelines query their per-pipeline Hadoop catalog via the runner-
 * Spark container; without it, cloud pipelines query Athena over Glue
 * (legacy default). Always pass `dir` for tables with a known owning
 * pipeline so local-only tables stop 500ing on the Sample panel.
 */
export function useTableSample(
  database: string,
  table: string,
  limit = 100,
  opts: { dir?: string; enabled?: boolean } = {},
) {
  const dir = opts.dir ?? "";
  // Default-enabled, but callers that need the catalog metadata first (to
  // resolve `dir` before firing) can pass `enabled: false` until the
  // metadata lands. Without this gate the first render fires a cloud-path
  // request that 500s, then refires with `dir` once available — the user
  // sees an "Query failed" flash.
  const enabled = (opts.enabled ?? true) && Boolean(database && table);
  return useQuery({
    queryKey: ["table", database, table, "sample", limit, dir],
    enabled,
    queryFn: async () => {
      const params = new URLSearchParams({
        catalog_db: database,
        catalog_table: table,
        limit: String(limit),
      });
      if (dir) params.set("dir", dir);
      const res = await fetch(`${BASE_URL}/data/table?${params.toString()}`);
      if (!res.ok) {
        const text = await res.text().catch(() => res.statusText);
        throw new Error(`GET /data/table → ${res.status}: ${text}`);
      }
      return TableQueryResult.parse(await res.json());
    },
  });
}

/**
 * GET /api/data/tables/{database}/{table}/snapshots — Delta commit history
 * for one table. Surfaces freshness ("when did this last commit?") and
 * rowcount from the _delta_log (local) or S3 log scan (cloud).
 *
 * Errors are not fatal for the catalog UI — a per-table 500 (e.g.
 * non-Delta table) should leave the rest of the catalog functional.
 */
export function useTableSnapshots(
  database: string,
  table: string,
  limit = 20,
  opts: { dir?: string } = {},
) {
  const dir = opts.dir ?? "";
  return useQuery({
    queryKey: ["table", database, table, "snapshots", limit, dir],
    enabled: Boolean(database && table),
    // Snapshot queries hit Athena — don't refetch aggressively.
    staleTime: 60_000,
    retry: false,
    queryFn: async () => {
      const params = new URLSearchParams({ limit: String(limit) });
      // Local-pipeline tables route through LocalProvider when `dir` is set;
      // without it the snapshots endpoint defaults to cloud and 500s for
      // tables that only exist in a per-pipeline Hadoop warehouse.
      if (dir) params.set("dir", dir);
      const url = `${BASE_URL}/data/tables/${encodeURIComponent(database)}/${encodeURIComponent(table)}/snapshots?${params.toString()}`;
      const res = await fetch(url);
      if (!res.ok) {
        const text = await res.text().catch(() => res.statusText);
        throw new Error(`GET snapshots → ${res.status}: ${text}`);
      }
      return SnapshotsResult.parse(await res.json());
    },
  });
}

// ---------------------------------------------------------------------------
// column_stats — opt-in per-column profile rendered on TableDetail
// ---------------------------------------------------------------------------

const ColumnStatBucket = z.object({
  value: z.string().nullable().default(""),
  count: z.number(),
});
export type ColumnStatBucket = z.infer<typeof ColumnStatBucket>;

const ColumnStat = z.object({
  column_name: z.string(),
  column_type: z.string().optional().default(""),
  row_count: z.number().nullish(),
  null_count: z.number().nullish(),
  null_pct: z.number().nullish(),
  approx_count_distinct: z.number().nullish(),
  min_value: z.string().optional().default(""),
  max_value: z.string().optional().default(""),
  approx_p50: z.number().nullish(),
  approx_p95: z.number().nullish(),
  top_10: z.array(ColumnStatBucket).optional().default([]),
  snapshot_id: z.string().optional().default(""),
  computed_at: z.string().optional().default(""),
});
export type ColumnStat = z.infer<typeof ColumnStat>;

const ColumnStatsResult = z.object({
  stats: z.array(ColumnStat).default([]),
  snapshot_id: z.string().optional().default(""),
});
export type ColumnStatsResult = z.infer<typeof ColumnStatsResult>;

/**
 * GET /api/data/tables/{database}/{table}/column-stats — opt-in per-column
 * profile for one Delta table. The card on TableDetail only renders
 * when the result has rows; an empty result means the transform's
 * `stats` flag is off (or has never produced a commit yet).
 */
export function useColumnStats(
  database: string,
  table: string,
  opts: { dir?: string } = {},
) {
  const dir = opts.dir ?? "";
  return useQuery({
    queryKey: ["table", database, table, "column-stats", dir],
    enabled: Boolean(database && table),
    staleTime: 60_000,
    retry: false,
    queryFn: async () => {
      const params = new URLSearchParams();
      if (dir) params.set("dir", dir);
      const qs = params.toString();
      const url = `${BASE_URL}/data/tables/${encodeURIComponent(database)}/${encodeURIComponent(table)}/column-stats${qs ? `?${qs}` : ""}`;
      const res = await fetch(url);
      if (!res.ok) {
        const text = await res.text().catch(() => res.statusText);
        throw new Error(`GET column-stats → ${res.status}: ${text}`);
      }
      return ColumnStatsResult.parse(await res.json());
    },
  });
}

// ---------------------------------------------------------------------------
// Lineage — pipeline DAG as edges, derived from .tf
// ---------------------------------------------------------------------------

const LineageEdge = z.object({
  from_node: z.string(),
  from_type: z.string(),
  to_node: z.string(),
  to_type: z.string(),
  // "<database>.<table>" when the upstream is a transform writing a Delta
  // auto-table; empty for source→transform edges (sources stream Parquet,
  // not a catalog table). TableDetail filters by exact "<db>.<t>" match to
  // surface downstream consumers of the table being viewed.
  via_table: z.string().optional().default(""),
  // ADR-016 slice 2 cross-pipeline edges. Set when the producing /
  // consuming node lives in a different pipeline than the one being
  // queried. Empty means same pipeline. UI renders cross-pipeline rows
  // distinctly and labels them with the other pipeline's name.
  from_pipeline: z.string().optional().default(""),
  to_pipeline: z.string().optional().default(""),
  // Consumer's own output table id for downstream cross-pipeline rows.
  // The UI links here for navigation; via_table still names the
  // producer's table (the data-flow handle). Empty for intra-pipeline
  // rows where the UI derives the consumer table from the current DB.
  to_table: z.string().optional().default(""),
});
export type LineageEdge = z.infer<typeof LineageEdge>;

const LineageResponse = z.object({
  edges: z.array(LineageEdge).default([]),
  // The queried pipeline's own ADR-016 namespace — used to label a
  // node's output table without guessing from a cross-pipeline via_table.
  catalog: z.string().optional().default(""),
  schema: z.string().optional().default(""),
});
export type LineageResponse = z.infer<typeof LineageResponse>;

/**
 * GET /api/pipeline/lineage?dir=… — directed table-to-table edges for a
 * pipeline. Pure function of the workspace .tf, so the same response shape
 * works for cloud and local pipelines (ADR-014). Cached: lineage doesn't
 * change between .tf edits, so the page transition is instant.
 */
export function useLineage(dir: string) {
  return useQuery({
    queryKey: ["pipeline", "lineage", dir],
    enabled: Boolean(dir),
    staleTime: 60_000,
    retry: false,
    queryFn: async () => {
      const url = `${BASE_URL}/pipeline/lineage?dir=${encodeURIComponent(dir)}`;
      const res = await fetch(url);
      if (!res.ok) {
        const text = await res.text().catch(() => res.statusText);
        throw new Error(`GET /pipeline/lineage → ${res.status}: ${text}`);
      }
      return LineageResponse.parse(await res.json());
    },
  });
}

// ---------------------------------------------------------------------------
// Pipeline deployment status / SFN executions
// ---------------------------------------------------------------------------

const ExecutionInfo = z.object({
  name: z.string(),
  status: z.string(),
  started_at: z.string(),
  stopped_at: z.string().optional().default(""),
  console_url: z.string().optional().default(""),
  execution_arn: z.string(),
});
export type ExecutionInfo = z.infer<typeof ExecutionInfo>;

const PipelineStatusResponse = z.object({
  deployed: z.boolean(),
  cloud: z.string().optional().default(""),
  state_machine_arn: z.string().optional().default(""),
  executions: z.array(ExecutionInfo).default([]),
});
export type PipelineStatusResponse = z.infer<typeof PipelineStatusResponse>;

/**
 * GET /api/pipeline/status?dir=… — pipeline deployment + recent execution
 * list. Polled at 5s while the editor is on a deployed pipeline so the
 * editor learns about new in-flight executions promptly.
 */
export function usePipelineStatus(dir: string) {
  return useQuery({
    queryKey: ["pipeline-status", dir],
    enabled: Boolean(dir),
    retry: false,
    refetchInterval: 5_000,
    queryFn: async () => {
      const url = `${BASE_URL}/pipeline/status?dir=${encodeURIComponent(dir)}`;
      const res = await fetch(url);
      if (!res.ok) {
        const text = await res.text().catch(() => res.statusText);
        throw new Error(`GET status → ${res.status}: ${text}`);
      }
      return PipelineStatusResponse.parse(await res.json());
    },
  });
}

// ---------------------------------------------------------------------------
// SFN execution state polling
// ---------------------------------------------------------------------------

const StateStatus = z.object({
  status: z.string(),
  entered_at: z.string().optional().default(""),
});
export type StateStatus = z.infer<typeof StateStatus>;

const ExecutionStatesResponse = z.object({
  status: z.string(),
  states: z.record(z.string(), StateStatus),
  // Lets the dashboard render an in-flight run as a synthetic Runs-grid
  // column before the runs-table row exists (locally that row only
  // lands at end-of-run via recordLocalRun).
  run_id: z.string().optional().default(""),
  started_at: z.string().optional().default(""),
});
export type ExecutionStatesResponse = z.infer<typeof ExecutionStatesResponse>;

/**
 * GET /api/pipeline/execution/states — per-step status for one execution.
 * Polled every 2s while RUNNING; the editor uses it to overlay live DAG
 * colors. Two addressing modes (ADR-014):
 *   - { arn }                        — cloud pipelines (legacy SFN path).
 *   - { dir, run? }                  — local pipelines (filesystem channel).
 * Pass `{}` (or empty values) to disable the query.
 */
export function useExecutionStates(
  ref: { arn?: string; dir?: string; run?: string },
) {
  const arn = ref.arn ?? "";
  const dir = ref.dir ?? "";
  const run = ref.run ?? "";
  const enabled = Boolean(arn || dir);
  return useQuery({
    queryKey: ["execution-states", arn, dir, run],
    enabled,
    retry: false,
    // refetchInterval fires only after a successful response — the
    // pre-data branch was dead (G P2-8, 2026-05-24). Mirror the clean
    // form `useRuntimeWorkers` already has.
    refetchInterval: (query) =>
      (query.state.data as ExecutionStatesResponse | undefined)?.status === "RUNNING"
        ? 2_000
        : false,
    queryFn: async () => {
      const params = new URLSearchParams();
      if (dir) {
        params.set("dir", dir);
        if (run) params.set("run", run);
      } else if (arn) {
        params.set("arn", arn);
      }
      const url = `${BASE_URL}/pipeline/execution/states?${params.toString()}`;
      const res = await fetch(url);
      if (!res.ok) {
        const text = await res.text().catch(() => res.statusText);
        throw new Error(`GET execution states → ${res.status}: ${text}`);
      }
      return ExecutionStatesResponse.parse(await res.json());
    },
  });
}

// ---------------------------------------------------------------------------
// node_runs — runner-populated invocation history
// ---------------------------------------------------------------------------

const NodeRun = z.object({
  run_id: z.string(),
  pipeline: z.string(),
  // Join key against `runs.sf_execution_arn` — groups a run's node
  // invocations. Empty for runner rows that predate the column.
  sf_execution_arn: z.string().optional().default(""),
  node: z.string(),
  started_at: z.string(),
  ended_at: z.string().optional().default(""),
  duration_ms: z.number().nullish(),
  status: z.string(),
  compute_target: z.string().optional().default(""),
  memory_mb: z.number().nullish(),
  cold_start: z.boolean().nullish(),
  lambda_request_id: z.string().optional().default(""),
  error_class: z.string().optional().default(""),
  error_msg: z.string().optional().default(""),
  // v0.14.0 triage columns. Empty string when the producing runner
  // pre-dated the slice — keeps clients schema-stable across upgrades.
  runner_image_digest: z.string().optional().default(""),
  module_version: z.string().optional().default(""),
  // Sum of added-records across this run's Delta outputs. Null when
  // the run had no Delta outputs (path-mode-only, skipped) or when
  // the producing runner pre-dated this column.
  output_rows: z.number().nullish(),
});
export type NodeRun = z.infer<typeof NodeRun>;

const NodeRunsResult = z.object({
  rows: z.array(NodeRun),
  truncated: z.boolean(),
});
export type NodeRunsResult = z.infer<typeof NodeRunsResult>;

/**
 * GET /api/data/node-runs?pipeline=…[&dir=…&node=…&limit=…] — recent runner
 * invocations from clavesa_<pipeline>.node_runs. When `dir` is supplied,
 * the backend dispatches to the local provider (compute = "local" pipelines
 * read from the local Hadoop catalog via the runner image, ADR-014). Errors
 * are non-fatal for the dashboard — a fresh pipeline whose table doesn't
 * exist yet returns empty rows.
 */
export function useNodeRuns(
  pipeline: string,
  opts: {
    dir?: string;
    node?: string;
    arn?: string;
    limit?: number;
    // Callers can gate the query off — e.g. RunDetail skips it while a
    // local run is still in flight (no node_runs rows exist yet, and
    // querying would spawn the warm Spark worker mid-run).
    enabled?: boolean;
  } = {},
) {
  const limit = opts.limit ?? 50;
  const node = opts.node ?? "";
  const dir = opts.dir ?? "";
  const arn = opts.arn ?? "";
  return useQuery({
    queryKey: ["node-runs", pipeline, dir, node, arn, limit],
    enabled: Boolean(pipeline) && (opts.enabled ?? true),
    retry: false,
    staleTime: 30_000,
    queryFn: async () => {
      const params = new URLSearchParams({ pipeline, limit: String(limit) });
      if (node) params.set("node", node);
      if (dir) params.set("dir", dir);
      // Run-detail page narrows to a single execution by sf_execution_arn —
      // the join key against runs. Cloud passes the SFN ARN, local passes
      // the pipeline-run uuid (same column on both sides).
      if (arn) params.set("arn", arn);
      const url = `${BASE_URL}/data/node-runs?${params.toString()}`;
      const res = await fetch(url);
      if (!res.ok) {
        const text = await res.text().catch(() => res.statusText);
        throw new Error(`GET node-runs → ${res.status}: ${text}`);
      }
      return NodeRunsResult.parse(await res.json());
    },
  });
}

// ---------------------------------------------------------------------------
// runs — EventBridge-writer-populated per-execution rollup
// ---------------------------------------------------------------------------

const Run = z.object({
  run_id: z.string(),
  pipeline: z.string(),
  sf_execution_arn: z.string().optional().default(""),
  status: z.string(),
  trigger: z.string().optional().default(""),
  started_at: z.string().optional().default(""),
  ended_at: z.string().optional().default(""),
  duration_ms: z.number().nullish(),
  failed_step: z.string().optional().default(""),
  error_class: z.string().optional().default(""),
  error_msg: z.string().optional().default(""),
});
export type Run = z.infer<typeof Run>;

const RunsResult = z.object({
  rows: z.array(Run),
  truncated: z.boolean(),
});
export type RunsResult = z.infer<typeof RunsResult>;

/**
 * GET /api/data/runs?pipeline=…[&dir=…] — recent executions from the per-
 * pipeline runs table. Cloud-deployed pipelines source rows from the
 * EventBridge-writer-populated Delta table; local pipelines source from
 * the same table written via runner-Spark (ADR-014). Same graceful
 * degradation: a fresh pipeline returns empty rows.
 */
export function useRuns(
  pipeline: string,
  opts: { dir?: string; limit?: number; enabled?: boolean } = {},
) {
  const limit = opts.limit ?? 50;
  const dir = opts.dir ?? "";
  return useQuery({
    queryKey: ["runs", pipeline, dir, limit],
    enabled: Boolean(pipeline) && (opts.enabled ?? true),
    retry: false,
    staleTime: 30_000,
    queryFn: async () => {
      const params = new URLSearchParams({ pipeline, limit: String(limit) });
      if (dir) params.set("dir", dir);
      const url = `${BASE_URL}/data/runs?${params.toString()}`;
      const res = await fetch(url);
      if (!res.ok) {
        const text = await res.text().catch(() => res.statusText);
        throw new Error(`GET runs → ${res.status}: ${text}`);
      }
      return RunsResult.parse(await res.json());
    },
  });
}

// ---------------------------------------------------------------------------
// Tables — current-state-per-table from clavesa_<pipeline>.tables
// ---------------------------------------------------------------------------

const TableInfo = z.object({
  pipeline: z.string(),
  node: z.string(),
  output_key: z.string(),
  table_name: z.string(),
  table_id: z.string(),
  snapshot_id: z.string().optional().default(""),
  snapshot_ts: z.string().optional().default(""),
  row_count: z.number().nullish(),
  file_count: z.number().nullish(),
  total_bytes: z.number().nullish(),
  last_writer_run_id: z.string().optional().default(""),
});
export type TableInfo = z.infer<typeof TableInfo>;

const TablesResult = z.object({
  rows: z.array(TableInfo),
  truncated: z.boolean(),
});
export type TablesResult = z.infer<typeof TablesResult>;

/**
 * GET /api/data/tables-state?pipeline=…[&dir=…] — current-state-per-table.
 * One row per Delta-output the pipeline produces, projecting the latest
 * commit's row count + file count + bytes + refresh time. Distinct from
 * /data/tables/{db}/{table}/snapshots, which lists commit history for one
 * specific table.
 */
export function useTablesState(
  pipeline: string,
  opts: { dir?: string; limit?: number } = {},
) {
  const limit = opts.limit ?? 50;
  const dir = opts.dir ?? "";
  return useQuery({
    queryKey: ["tables-state", pipeline, dir, limit],
    enabled: Boolean(pipeline),
    retry: false,
    staleTime: 30_000,
    queryFn: async () => {
      const params = new URLSearchParams({ pipeline, limit: String(limit) });
      if (dir) params.set("dir", dir);
      const url = `${BASE_URL}/data/tables-state?${params.toString()}`;
      const res = await fetch(url);
      if (!res.ok) {
        const text = await res.text().catch(() => res.statusText);
        throw new Error(`GET tables-state → ${res.status}: ${text}`);
      }
      return TablesResult.parse(await res.json());
    },
  });
}

// ---------------------------------------------------------------------------
// CloudWatch Logs for one failed step
// ---------------------------------------------------------------------------

const LogEvent = z.object({
  timestamp: z.string(),
  message: z.string(),
});
export type LogEvent = z.infer<typeof LogEvent>;

const ExecutionLogsResponse = z.object({
  // Discriminator for backend-specific UI affordances. Older servers may
  // not emit this field, so allow absent and default to "" — the UI
  // treats unknown values as "show the generic empty-state hint".
  source: z.enum(["cloudwatch", "local"]).or(z.literal("")).optional().default(""),
  log_group: z.string(),
  function_name: z.string(),
  events: z.array(LogEvent),
  truncated: z.boolean(),
});
export type ExecutionLogsResponse = z.infer<typeof ExecutionLogsResponse>;

/**
 * GET /api/pipeline/execution/logs — log lines for one step within one
 * execution. Cloud sources from CloudWatch FilterLogEvents; local sources
 * from <pipelineDir>/.clavesa/runs/<runID>/logs (ADR-014). Two
 * addressing modes:
 *   - { arn, step }            — cloud pipelines.
 *   - { dir, run?, step }      — local pipelines.
 * Lazy / pull-based; only the StatusPanel fires this on demand.
 */
export function useExecutionLogs(
  ref: { arn?: string; dir?: string; run?: string; step?: string },
) {
  const arn = ref.arn ?? "";
  const dir = ref.dir ?? "";
  const run = ref.run ?? "";
  const step = ref.step ?? "";
  const enabled = Boolean(step && (arn || dir));
  return useQuery({
    queryKey: ["execution-logs", arn, dir, run, step],
    enabled,
    retry: false,
    staleTime: 30_000,
    queryFn: async () => {
      const params = new URLSearchParams({ step });
      if (dir) {
        params.set("dir", dir);
        if (run) params.set("run", run);
      } else if (arn) {
        params.set("arn", arn);
      }
      const url = `${BASE_URL}/pipeline/execution/logs?${params.toString()}`;
      const res = await fetch(url);
      if (!res.ok) {
        const text = await res.text().catch(() => res.statusText);
        throw new Error(`GET execution logs → ${res.status}: ${text}`);
      }
      return ExecutionLogsResponse.parse(await res.json());
    },
  });
}

/** GET /api/pipelines — workspace's pipeline list. */
export function usePipelines() {
  return useQuery({
    queryKey: ["pipelines"],
    queryFn: async () => {
      const raw = await request<unknown>("/pipelines");
      // Backend returns either a JSON array or null when no pipelines exist.
      const arr = Array.isArray(raw) ? raw : [];
      return z.array(PipelineInfo).parse(arr);
    },
  });
}

// ---------------------------------------------------------------------------
// Dashboards — saved SQL widgets over the catalog
// ---------------------------------------------------------------------------

const DashboardSummary = z.object({
  slug: z.string(),
  title: z.string(),
});
export type DashboardSummary = z.infer<typeof DashboardSummary>;

const DashboardsListResponse = z.object({
  dashboards: z.array(DashboardSummary).default([]),
});

const DashboardWidgetLayout = z.object({
  x: z.number(),
  y: z.number(),
  w: z.number(),
  h: z.number(),
});

// DashboardDataset is a named, reusable SQL query. Each carries its own
// pipeline dir, so one dashboard can blend tables from multiple pipelines
// and mix local + cloud. Widgets bind to a dataset by name.
const DashboardDataset = z.object({
  name: z.string(),
  dir: z.string(),
  sql: z.string(),
});
export type DashboardDataset = z.infer<typeof DashboardDataset>;

const DashboardWidget = z.object({
  id: z.string(),
  // big_number | line | bar | table — the UI tolerates unknown types
  // (renders as a blank card) so adding a widget type doesn't break old
  // builds.
  type: z.string(),
  title: z.string(),
  // dataset references a DashboardDataset by name; the widget renders
  // that query's result.
  dataset: z.string().optional().default(""),
  value_field: z.string().optional().default(""),
  x_field: z.string().optional().default(""),
  y_field: z.string().optional().default(""),
  // series_fields are the value columns a stacked_bar stacks per x;
  // line_field is the line series of a bar_line combo.
  series_fields: z.array(z.string()).optional().default([]),
  line_field: z.string().optional().default(""),
  // world_map-only: ISO 3166-1 alpha-2 or alpha-3 country code column,
  // and an optional column shown in the hover tooltip.
  region_field: z.string().optional().default(""),
  tooltip_field: z.string().optional().default(""),
  layout: DashboardWidgetLayout,
});
export type DashboardWidget = z.infer<typeof DashboardWidget>;

// DashboardControl is a dashboard-level filter substituted into dataset
// SQL at render time. `time_range` writes `{{<name>.start}}` and
// `{{<name>.end}}`; `select` writes `{{<name>}}`. Backend tolerates
// unknown control types (renders nothing) so future control kinds don't
// break old builds.
const DashboardControl = z.object({
  name: z.string(),
  type: z.string(),
  label: z.string().optional().default(""),
  default: z.string().optional().default(""),
  dir: z.string().optional().default(""),
  sql: z.string().optional().default(""),
  options: z.array(z.string()).optional().default([]),
});
export type DashboardControl = z.infer<typeof DashboardControl>;

const Dashboard = z.object({
  slug: z.string(),
  title: z.string(),
  datasets: z.array(DashboardDataset).default([]),
  widgets: z.array(DashboardWidget).default([]),
  controls: z.array(DashboardControl).default([]),
  updated_at: z.string().optional().default(""),
});
export type Dashboard = z.infer<typeof Dashboard>;

/**
 * resolveControlDefaults — synthesize a params map from a dashboard's
 * declared control defaults. Mirrors the Go resolveControlDefaults
 * helper. Used by the editor when probing dataset columns — without
 * substituted values, a dataset referencing `{{name}}` would 400 on
 * the column-fetch call before the editor can even render its field
 * pickers.
 */
export function resolveControlDefaults(
  controls: DashboardControl[],
): Record<string, string> {
  const out: Record<string, string> = {};
  const now = new Date();
  for (const c of controls) {
    if (c.type === "time_range") {
      const { start, end } = resolveTimePreset(c.default || "last_30d", now);
      out[`${c.name}.start`] = start;
      out[`${c.name}.end`] = end;
    } else if (c.type === "select") {
      if (c.default) {
        out[c.name] = c.default;
      } else if (c.options.length > 0) {
        out[c.name] = c.options[0];
      }
    }
  }
  return out;
}

function resolveTimePreset(
  preset: string,
  now: Date,
): { start: string; end: string } {
  const end = now.toISOString();
  let ms = 30 * 24 * 60 * 60 * 1000;
  switch (preset) {
    case "last_24h":
      ms = 24 * 60 * 60 * 1000;
      break;
    case "last_7d":
      ms = 7 * 24 * 60 * 60 * 1000;
      break;
    case "last_90d":
      ms = 90 * 24 * 60 * 60 * 1000;
      break;
    case "last_30d":
    default:
      ms = 30 * 24 * 60 * 60 * 1000;
  }
  return { start: new Date(now.getTime() - ms).toISOString(), end };
}

const DashboardQueryColumn = z.object({
  name: z.string(),
  type: z.string().optional().default(""),
});

const DashboardQueryResult = z.object({
  columns: z.array(DashboardQueryColumn).default([]),
  rows: z.array(z.array(z.string())).default([]),
  row_count: z.number(),
  truncated: z.boolean(),
});
export type DashboardQueryResult = z.infer<typeof DashboardQueryResult>;

// ---------------------------------------------------------------------------
// Sources — workspace input source registry (ADR-017 slice 1)
// ---------------------------------------------------------------------------

const SourceSpec = z.object({
  name: z.string(),
  kind: z.string(),
  url: z.string().optional().default(""),
  bucket: z.string().optional().default(""),
  prefix: z.string().optional().default(""),
  format: z.string().optional().default(""),
  credentials: z.string().optional().default(""),
  partitions: z.array(z.string()).optional().default([]),
  start_from: z.string().optional().default(""),
  manage_bucket_notifications: z.boolean().optional().default(false),
});
export type SourceSpec = z.infer<typeof SourceSpec>;

// ---------------------------------------------------------------------------
// Credentials — workspace credentials registry (ADR-017 slice 2)
// ---------------------------------------------------------------------------

const CredentialSpec = z.object({
  name: z.string(),
  kind: z.string(),
  header_name: z.string().optional().default(""),
  value_prefix: z.string().optional().default(""),
  secret: z.string(),
  backend: z.string().optional().default(""),
});
export type CredentialSpec = z.infer<typeof CredentialSpec>;

const CredentialsListResponse = z.object({
  credentials: z.array(CredentialSpec).default([]),
});

/** GET /api/credentials — registered credentials in this workspace. */
export function useCredentials() {
  return useQuery({
    queryKey: ["credentials"],
    queryFn: async () => {
      const raw = await request<unknown>("/credentials");
      return CredentialsListResponse.parse(raw);
    },
  });
}

/** POST /api/credentials — register a new credential. */
export async function registerCredential(spec: {
  name: string;
  kind?: string;
  header_name?: string;
  value_prefix?: string;
  secret: string;
}): Promise<CredentialSpec> {
  const res = await fetch(`${BASE_URL}/credentials`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ kind: spec.kind ?? "header", ...spec }),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`POST /credentials → ${res.status}: ${text}`);
  }
  return CredentialSpec.parse(await res.json());
}

/** DELETE /api/credentials/{name}. */
export async function deleteCredential(
  name: string,
  opts: { force?: boolean } = {},
): Promise<{ usages?: { source_name: string }[] } | null> {
  const params = opts.force ? "?force=1" : "";
  const res = await fetch(
    `${BASE_URL}/credentials/${encodeURIComponent(name)}${params}`,
    { method: "DELETE" },
  );
  if (res.status === 204) return null;
  if (res.status === 409) return await res.json();
  const text = await res.text().catch(() => res.statusText);
  throw new Error(`DELETE /credentials/${name} → ${res.status}: ${text}`);
}

const SourcesListResponse = z.object({
  sources: z.array(SourceSpec).default([]),
});

/** GET /api/sources — registered sources in this workspace. */
export function useSources() {
  return useQuery({
    queryKey: ["sources"],
    queryFn: async () => {
      const raw = await request<unknown>("/sources");
      return SourcesListResponse.parse(raw);
    },
  });
}

/** POST /api/sources — register a new source. */
export async function registerSource(spec: {
  name: string;
  kind?: string;
  url?: string;
  bucket?: string;
  prefix?: string;
  format?: string;
  credentials?: string;
  partitions?: string[];
  start_from?: string;
  manage_bucket_notifications?: boolean;
}): Promise<SourceSpec> {
  const res = await fetch(`${BASE_URL}/sources`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    // kind omitted on purpose — the server sniffs `s3://` vs `https://`
    // out of the URL field. Caller can still pass kind explicitly when
    // it knows.
    body: JSON.stringify(spec),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`POST /sources → ${res.status}: ${text}`);
  }
  return SourceSpec.parse(await res.json());
}

/**
 * PUT /api/sources/{name} — overwrite an existing source's spec. The
 * name is the fixed registry key; renaming is a delete + re-register.
 */
export async function updateSource(
  name: string,
  spec: {
    name: string;
    kind?: string;
    url?: string;
    bucket?: string;
    prefix?: string;
    format?: string;
    credentials?: string;
    partitions?: string[];
    start_from?: string;
    manage_bucket_notifications?: boolean;
  },
): Promise<SourceSpec> {
  const res = await fetch(`${BASE_URL}/sources/${encodeURIComponent(name)}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(spec),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`PUT /sources/${name} → ${res.status}: ${text}`);
  }
  return SourceSpec.parse(await res.json());
}

/**
 * POST /api/sources/{name}/attach — wire a registered source into a
 * transform's `inputs` map as `inputs[alias] = "sources.<name>"`. Returns
 * void (204 on success); body of the throw includes the server error
 * message on failure.
 */
export async function attachSource(
  name: string,
  body: { dir: string; to: string; alias?: string },
): Promise<void> {
  const res = await fetch(
    `${BASE_URL}/sources/${encodeURIComponent(name)}/attach`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    },
  );
  if (res.status === 204) return;
  const text = await res.text().catch(() => res.statusText);
  throw new Error(`POST /sources/${name}/attach → ${res.status}: ${text}`);
}

/**
 * POST /api/pipeline/external-table/attach — wire a cross-pipeline or
 * external Glue table into a transform's `inputs` map as
 * `inputs[alias] = "<schema>.<table>"` (ADR-016 slice 2). HTTP twin of
 * the CLI `node connect --from-table` command. Returns the updated
 * graph on success.
 */
export async function attachExternalTable(body: {
  dir: string;
  ref: string;
  to: string;
  alias?: string;
}): Promise<void> {
  const res = await fetch(`${BASE_URL}/pipeline/external-table/attach`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (res.ok) return;
  const text = await res.text().catch(() => res.statusText);
  throw new Error(`POST /pipeline/external-table/attach → ${res.status}: ${text}`);
}

/** DELETE /api/sources/{name}. Returns null on success, usage list on 409. */
export async function deleteSource(
  name: string,
  opts: { force?: boolean } = {},
): Promise<{ usages?: { pipeline_dir: string; node_ids: string[] }[] } | null> {
  const params = opts.force ? "?force=1" : "";
  const res = await fetch(
    `${BASE_URL}/sources/${encodeURIComponent(name)}${params}`,
    { method: "DELETE" },
  );
  if (res.status === 204) return null;
  if (res.status === 409) {
    return await res.json();
  }
  const text = await res.text().catch(() => res.statusText);
  throw new Error(`DELETE /sources/${name} → ${res.status}: ${text}`);
}

// ---------------------------------------------------------------------------
// Backfills — Gate 1 stage / list / diff / promote / discard
// ---------------------------------------------------------------------------

const BackfillRun = z.object({
  run_id: z.string(),
  pipeline: z.string().optional().default(""),
  node: z.string().optional().default(""),
  output_key: z.string().optional().default(""),
  from_cursor: z.array(z.string()).default([]),
  to_cursor: z.array(z.string()).default([]),
  direct: z.boolean().optional().default(false),
  target_table: z.string().optional().default(""),
  canonical_table: z.string().optional().default(""),
  started_at: z.string().optional().default(""),
  stopped_at: z.string().optional().default(""),
  status: z.string().optional().default(""),
  rows_written: z.number().nullish(),
  error_msg: z.string().optional().default(""),
});
export type BackfillRun = z.infer<typeof BackfillRun>;

const BackfillsListResponse = z.object({
  backfills: z.array(BackfillRun).default([]),
});

const BackfillColumnInfo = z.object({
  name: z.string(),
  type: z.string().optional().default(""),
});
export type BackfillColumnInfo = z.infer<typeof BackfillColumnInfo>;

const BackfillDiff = z.object({
  run_id: z.string(),
  staging_table: z.string().optional().default(""),
  canonical_table: z.string().optional().default(""),
  staging_rows: z.number().optional().default(0),
  canonical_rows: z.number().optional().default(0),
  schema_matches: z.boolean().optional().default(false),
  schema_diff: z.string().optional().default(""),
  output_mode: z.string().optional().default(""),
  merge_keys: z.array(z.string()).optional().default([]),
  matching_key_rows: z.number().optional().default(0),
  new_key_rows: z.number().optional().default(0),
  staging_columns: z.array(BackfillColumnInfo).optional().default([]),
});
export type BackfillDiff = z.infer<typeof BackfillDiff>;

const BackfillDedupCheckResult = z.object({
  matching_rows: z.number(),
  new_rows: z.number(),
});
export type BackfillDedupCheckResult = z.infer<typeof BackfillDedupCheckResult>;

/**
 * GET /api/backfills?dir=… — open (un-promoted/un-discarded) staging
 * tables for the pipeline. Cloud backend scans Glue tags; local backend
 * scans the workspace warehouse for staging-table sidecar files
 * (ADR-014). Same response shape. Errors are non-fatal — the dashboard
 * card swallows them and renders the empty state so an undeployed
 * pipeline doesn't break the page.
 */
export function useBackfills(dir: string) {
  return useQuery({
    queryKey: ["backfills", dir],
    enabled: Boolean(dir),
    retry: false,
    staleTime: 30_000,
    queryFn: async () => {
      const url = `${BASE_URL}/backfills?dir=${encodeURIComponent(dir)}`;
      const res = await fetch(url);
      if (!res.ok) {
        const text = await res.text().catch(() => res.statusText);
        throw new Error(`GET /backfills → ${res.status}: ${text}`);
      }
      return BackfillsListResponse.parse(await res.json());
    },
  });
}

/** GET /api/backfills/{run_id}/diff?dir=…. */
export function useBackfillDiff(dir: string, runID: string) {
  return useQuery({
    queryKey: ["backfills", "diff", dir, runID],
    enabled: Boolean(dir && runID),
    retry: false,
    staleTime: 30_000,
    queryFn: async () => {
      const url = `${BASE_URL}/backfills/${encodeURIComponent(runID)}/diff?dir=${encodeURIComponent(dir)}`;
      const res = await fetch(url);
      if (!res.ok) {
        const text = await res.text().catch(() => res.statusText);
        throw new Error(`GET /backfills/${runID}/diff → ${res.status}: ${text}`);
      }
      return BackfillDiff.parse(await res.json());
    },
  });
}

/**
 * GET /api/backfills/{run_id}/dedup-check?dir=…&col=… — preview the
 * matching/new-key counts a `--force-dedup <col>` promote would produce.
 * Two Athena queries; ~2-5s. The append-mode promote UI fires this live
 * as the user picks a column so they can see consequences before
 * pressing Promote.
 */
export function useBackfillDedupCheck(dir: string, runID: string, col: string) {
  return useQuery({
    queryKey: ["backfills", "dedup-check", dir, runID, col],
    enabled: Boolean(dir && runID && col),
    retry: false,
    staleTime: 60_000,
    queryFn: async () => {
      const url = `${BASE_URL}/backfills/${encodeURIComponent(runID)}/dedup-check?dir=${encodeURIComponent(dir)}&col=${encodeURIComponent(col)}`;
      const res = await fetch(url);
      if (!res.ok) {
        const text = await res.text().catch(() => res.statusText);
        throw new Error(`GET /backfills/${runID}/dedup-check → ${res.status}: ${text}`);
      }
      return BackfillDedupCheckResult.parse(await res.json());
    },
  });
}

/** POST /api/backfills/stage — stage a new backfill window. */
export async function stageBackfill(body: {
  dir: string;
  node: string;
  from: string[];
  to: string[];
  direct?: boolean;
}): Promise<BackfillRun> {
  const res = await fetch(`${BASE_URL}/backfills/stage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  // 502 with a JSON body carries the partial run + error_msg — surface
  // both to the caller so the dialog can show the Lambda's complaint
  // inline rather than a generic alert.
  const text = await res.text();
  let parsed: unknown = null;
  try {
    parsed = text ? JSON.parse(text) : null;
  } catch {
    /* ignore — non-JSON error body falls through to the text path */
  }
  if (!res.ok) {
    if (parsed && typeof parsed === "object" && "error_msg" in (parsed as object)) {
      const run = BackfillRun.parse(parsed);
      throw new Error(run.error_msg || `POST /backfills/stage → ${res.status}`);
    }
    throw new Error(`POST /backfills/stage → ${res.status}: ${text || res.statusText}`);
  }
  return BackfillRun.parse(parsed);
}

const BackfillPromoteResult = z.object({
  columns_added: z.array(z.string()),
});
export type BackfillPromoteResult = z.infer<typeof BackfillPromoteResult>;

/**
 * POST /api/backfills/{run_id}/promote.
 *
 * Returns `columns_added` so the UI can surface schema evolution that
 * happened during the promote — the runner ALTER-TABLE-ADD-COLUMNs any
 * staging-only columns into the target before the MERGE so they don't
 * get silently dropped (Delta schema evolution via mergeSchema).
 */
export async function promoteBackfill(
  runID: string,
  body: { dir: string; force_dedup?: string; allow_duplicates?: boolean },
): Promise<BackfillPromoteResult> {
  const res = await fetch(
    `${BASE_URL}/backfills/${encodeURIComponent(runID)}/promote`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    },
  );
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`POST /backfills/${runID}/promote → ${res.status}: ${text}`);
  }
  return BackfillPromoteResult.parse(await res.json());
}

/** POST /api/backfills/{run_id}/discard. */
export async function discardBackfill(
  runID: string,
  body: { dir: string },
): Promise<void> {
  const res = await fetch(
    `${BASE_URL}/backfills/${encodeURIComponent(runID)}/discard`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    },
  );
  if (res.status === 204) return;
  const text = await res.text().catch(() => res.statusText);
  throw new Error(`POST /backfills/${runID}/discard → ${res.status}: ${text}`);
}

/** GET /api/dashboards — workspace's dashboard list. */
export function useDashboards() {
  return useQuery({
    queryKey: ["dashboards"],
    queryFn: async () => {
      const raw = await request<unknown>("/dashboards");
      return DashboardsListResponse.parse(raw);
    },
  });
}

/** GET /api/dashboards/:slug — full dashboard spec. */
export function useDashboard(slug: string) {
  return useQuery({
    queryKey: ["dashboards", slug],
    enabled: Boolean(slug),
    queryFn: async () => {
      const url = `${BASE_URL}/dashboards/${encodeURIComponent(slug)}`;
      const res = await fetch(url);
      if (!res.ok) {
        const text = await res.text().catch(() => res.statusText);
        throw new Error(`GET /dashboards/${slug} → ${res.status}: ${text}`);
      }
      return Dashboard.parse(await res.json());
    },
  });
}

/**
 * POST /api/dashboards/query — run one widget's SQL.
 *
 * `dir` scopes the Provider dispatch (cloud vs local) and is required by
 * the backend regardless of which provider serves the request — both
 * fail the same way (ADR-014 parity) when it's absent. The hook
 * gates `enabled` on `dir` for the same reason: no point firing a
 * request that will 400.
 *
 * Stale time bumped to 5 minutes so flipping between widgets / tabs
 * doesn't re-hit Athena every time. Refresh on demand by invalidating
 * the query.
 */
export function useDashboardQuery(
  sql: string,
  dir: string,
  params?: Record<string, string>,
) {
  // Cache key includes a stable string of the params so distinct
  // control selections cache distinctly. Object identity would defeat
  // the cache; sorted JSON gives a deterministic key. Empty/undefined
  // params collapse to "" so dashboards without controls keep the same
  // cache key shape they had before.
  const paramsKey = paramsCacheKey(params);
  return useQuery({
    queryKey: ["dashboards", "query", dir, sql, paramsKey],
    enabled: Boolean(sql) && Boolean(dir),
    staleTime: 5 * 60_000,
    retry: false,
    queryFn: () => runDashboardQuery(dir, sql, params),
  });
}

function paramsCacheKey(
  params: Record<string, string> | undefined,
): string {
  if (!params) return "";
  const keys = Object.keys(params).sort();
  if (keys.length === 0) return "";
  return keys.map((k) => `${k}=${params[k]}`).join("&");
}

/**
 * runDashboardQuery — imperative form of the widget-SQL query.
 *
 * Same request as `useDashboardQuery`'s queryFn, factored out so
 * button-driven callers (the dataset SQL preview) and `useQueries`-based
 * callers can share it. Reuse the `["dashboards","query",dir,sql,params]`
 * query key when caching so all three paths hit one cache entry.
 */
export async function runDashboardQuery(
  dir: string,
  sql: string,
  params?: Record<string, string>,
): Promise<DashboardQueryResult> {
  const body: { dir: string; sql: string; params?: Record<string, string> } = {
    dir,
    sql,
  };
  if (params && Object.keys(params).length > 0) {
    body.params = params;
  }
  const res = await fetch(`${BASE_URL}/dashboards/query`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`POST /dashboards/query → ${res.status}: ${text}`);
  }
  return DashboardQueryResult.parse(await res.json());
}

// DashboardInput is the body of a create/replace — the spec without the
// server-stamped `updated_at`.
export interface DashboardInput {
  slug: string;
  title: string;
  datasets: DashboardDataset[];
  widgets: DashboardWidget[];
  controls: DashboardControl[];
}

/** POST /api/dashboards — create a dashboard (409 if the slug is taken). */
export async function createDashboard(d: DashboardInput): Promise<Dashboard> {
  const res = await fetch(`${BASE_URL}/dashboards`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(d),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`POST /dashboards → ${res.status}: ${text}`);
  }
  return Dashboard.parse(await res.json());
}

/** PUT /api/dashboards/:slug — create or replace a dashboard. */
export async function saveDashboard(
  slug: string,
  d: DashboardInput,
): Promise<Dashboard> {
  const res = await fetch(`${BASE_URL}/dashboards/${encodeURIComponent(slug)}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(d),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`PUT /dashboards/${slug} → ${res.status}: ${text}`);
  }
  return Dashboard.parse(await res.json());
}

/** DELETE /api/dashboards/:slug. */
export async function deleteDashboard(slug: string): Promise<void> {
  const res = await fetch(`${BASE_URL}/dashboards/${encodeURIComponent(slug)}`, {
    method: "DELETE",
  });
  if (!res.ok && res.status !== 204) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`DELETE /dashboards/${slug} → ${res.status}: ${text}`);
  }
}

// ---------------------------------------------------------------------------
// Workspace environment mode
// ---------------------------------------------------------------------------

const EnvironmentMode = z.object({
  mode: z.enum(["local", "cloud"]),
});
export type EnvironmentMode = z.infer<typeof EnvironmentMode>;

/**
 * GET /api/workspace/environment — the workspace environment mode that
 * drives local-vs-cloud dispatch. Absent → "local".
 */
export function useEnvironmentMode() {
  return useQuery({
    queryKey: ["environment"],
    queryFn: async () =>
      EnvironmentMode.parse(await request("/workspace/environment")),
  });
}

/**
 * PUT /api/workspace/environment — persist the workspace environment
 * mode. The CLI twin is `clavesa workspace use --env` (ADR-015).
 */
export async function setEnvironmentMode(
  mode: "local" | "cloud",
): Promise<EnvironmentMode> {
  return EnvironmentMode.parse(
    await request("/workspace/environment", {
      method: "PUT",
      body: JSON.stringify({ mode }),
    }),
  );
}

// ---------------------------------------------------------------------------
// Runtime status — warm-Spark worker state
// ---------------------------------------------------------------------------

const RuntimeWorker = z.object({
  warehouse: z.string(),
  // "spawning" while the container boots, "ready" once it serves
  // queries. Kept as a plain string (not an enum) so an unrecognized
  // future state can't break the whole header.
  state: z.string(),
  age_ms: z.number(),
});

const RuntimeWorkers = z.object({
  workers: z.array(RuntimeWorker),
});
export type RuntimeWorkers = z.infer<typeof RuntimeWorkers>;

/**
 * GET /api/runtime/workers — warm-Spark worker spawn state, polled by the
 * header runtime indicator. The endpoint is an in-memory map read, so
 * polling is cheap: 750ms while a worker is spawning (the indicator is
 * live then), 3s otherwise so a freshly-started spawn still shows within
 * a few seconds. Paused while the tab is backgrounded.
 */
export function useRuntimeWorkers() {
  return useQuery({
    queryKey: ["runtime", "workers"],
    queryFn: async () =>
      RuntimeWorkers.parse(await request("/runtime/workers")),
    refetchInterval: (query) => {
      const data = query.state.data as RuntimeWorkers | undefined;
      const spawning = data?.workers.some((w) => w.state === "spawning");
      return spawning ? 750 : 3000;
    },
    refetchIntervalInBackground: false,
    retry: false,
  });
}

const RuntimeIdentity = z.object({
  available: z.boolean(),
  account_id: z.string().optional().default(""),
  arn: z.string().optional().default(""),
  profile: z.string().optional().default(""),
});
export type RuntimeIdentity = z.infer<typeof RuntimeIdentity>;

/**
 * GET /api/runtime/identity — the UI server's effective AWS identity
 * (account / profile), resolved once at startup. Static, so this is
 * fetched once and never refetched — the header chip uses it to answer
 * "which account am I operating as?" at a glance.
 */
export function useRuntimeIdentity() {
  return useQuery({
    queryKey: ["runtime", "identity"],
    queryFn: async () =>
      RuntimeIdentity.parse(await request("/runtime/identity")),
    staleTime: Infinity,
    retry: false,
  });
}

// ---------------------------------------------------------------------------
// Workspace AWS profile
// ---------------------------------------------------------------------------

const AWSProfileInfo = z.object({
  profile: z.string().optional().default(""),
  profiles: z.array(z.string()).optional().default([]),
  restarting: z.boolean().optional().default(false),
});
export type AWSProfileInfo = z.infer<typeof AWSProfileInfo>;

/**
 * GET /api/workspace/aws-profile — the persisted per-workspace AWS
 * profile plus the profiles available in ~/.aws (the switcher's
 * choices).
 */
export function useAWSProfile() {
  return useQuery({
    queryKey: ["workspace", "aws-profile"],
    queryFn: async () =>
      AWSProfileInfo.parse(await request("/workspace/aws-profile")),
  });
}

/**
 * PUT /api/workspace/aws-profile — persist the AWS profile. The server
 * re-execs itself to apply it (the AWS SDK clients are built once at
 * startup), so the caller should wait the server out and reload.
 */
export async function setAWSProfile(profile: string): Promise<AWSProfileInfo> {
  const res = await fetch(`${BASE_URL}/workspace/aws-profile`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ profile }),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`PUT /workspace/aws-profile → ${res.status}: ${text}`);
  }
  return AWSProfileInfo.parse(await res.json());
}

/**
 * Poll the server until it answers again — used after a profile-change
 * self-restart. The initial delay lets the old process actually go down
 * first, so we don't match it still serving. Rejects after timeoutMs.
 */
export async function waitForServerReady(timeoutMs = 30_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  await new Promise((r) => setTimeout(r, 800));
  for (;;) {
    try {
      const res = await fetch(`${BASE_URL}/workspace`, { cache: "no-store" });
      if (res.ok) return;
    } catch {
      // server still down mid-restart — keep polling
    }
    if (Date.now() > deadline) throw new Error("server did not come back");
    await new Promise((r) => setTimeout(r, 500));
  }
}

// ---------------------------------------------------------------------------
// Notebooks — workspace .ipynb registry + cell execution (Slice 1)
// ---------------------------------------------------------------------------

const NotebookSummary = z.object({
  name: z.string(),
  cell_count: z.number(),
  updated_at: z.string(),
});
export type NotebookSummary = z.infer<typeof NotebookSummary>;

const NotebookCellClavesaMeta = z.object({
  last_run_at: z.string().optional().default(""),
  last_duration_ms: z.number().optional().default(0),
  last_status: z.string().optional().default(""),
});

const NotebookCellMetadata = z.object({
  clavesa: NotebookCellClavesaMeta.optional(),
});

const NotebookOutput = z.object({
  output_type: z.string(),
  name: z.string().optional(),
  text: z.array(z.string()).optional(),
  ename: z.string().optional(),
  evalue: z.string().optional(),
  traceback: z.array(z.string()).optional(),
  execution_count: z.number().nullable().optional(),
  data: z.record(z.string(), z.unknown()).optional(),
  metadata: z.record(z.string(), z.unknown()).optional(),
});
export type NotebookOutput = z.infer<typeof NotebookOutput>;

const NotebookCell = z.object({
  cell_type: z.enum(["code", "markdown"]),
  id: z.string(),
  source: z.array(z.string()),
  metadata: NotebookCellMetadata.optional().default({}),
  execution_count: z.number().nullable().optional(),
  outputs: z.array(NotebookOutput).optional().default([]),
});
export type NotebookCell = z.infer<typeof NotebookCell>;

const Notebook = z.object({
  // nbformat-required scalars
  nbformat: z.number(),
  nbformat_minor: z.number(),
  metadata: z.object({
    kernelspec: z.object({ name: z.string(), display_name: z.string() }),
    language_info: z.object({ name: z.string() }),
    clavesa: z.object({ format_version: z.number() }),
  }),
  cells: z.array(NotebookCell),
  // clavesa convenience — service layer surfaces the filename name here
  // even though nbformat itself doesn't carry it. Optional for safety on
  // older notebooks.
  name: z.string().optional().default(""),
});
export type Notebook = z.infer<typeof Notebook>;

const NotebooksListResponse = z.object({
  notebooks: z.array(NotebookSummary).default([]),
});

const CellDisplay = z.object({
  type: z.enum(["table", "text", "none"]),
  columns: z.array(z.string()).optional(),
  column_types: z.array(z.string()).optional(),
  rows: z.array(z.array(z.unknown())).optional(),
  truncated: z.boolean().optional(),
  text_repr: z.string(),
});

const CellError = z.object({
  ename: z.string(),
  evalue: z.string(),
  traceback: z.array(z.string()),
});

const CellResult = z.object({
  status: z.enum(["ok", "error", "cancelled"]),
  duration_ms: z.number(),
  stdout: z.string(),
  stderr: z.string(),
  display: CellDisplay.optional(),
  error: CellError.optional(),
});
export type CellResult = z.infer<typeof CellResult>;

const CellRunResult = z.object({
  cell: NotebookCell,
  result: CellResult,
});
export type CellRunResult = z.infer<typeof CellRunResult>;

/** GET /api/notebooks — lightweight summaries of every workspace notebook. */
export function useNotebooks() {
  return useQuery({
    queryKey: ["notebooks"],
    queryFn: async () => {
      const raw = await request<unknown>("/notebooks");
      return NotebooksListResponse.parse(raw);
    },
  });
}

/** GET /api/notebooks/{name} — full notebook (cells + outputs). */
export function useNotebook(name: string | null | undefined) {
  return useQuery({
    queryKey: ["notebook", name ?? ""],
    enabled: !!name,
    queryFn: async () => {
      const raw = await request<unknown>(`/notebooks/${encodeURIComponent(name!)}`);
      return Notebook.parse(raw);
    },
  });
}

/** POST /api/notebooks — create empty notebook. */
export async function createNotebook(name: string): Promise<Notebook> {
  const res = await fetch(`${BASE_URL}/notebooks`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name }),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`POST /notebooks → ${res.status}: ${text}`);
  }
  return Notebook.parse(await res.json());
}

/** PATCH /api/notebooks/{name} — save full notebook (cells + metadata). */
export async function saveNotebook(name: string, nb: Notebook): Promise<Notebook> {
  const res = await fetch(`${BASE_URL}/notebooks/${encodeURIComponent(name)}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(nb),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`PATCH /notebooks/${name} → ${res.status}: ${text}`);
  }
  return Notebook.parse(await res.json());
}

/** DELETE /api/notebooks/{name}. */
export async function deleteNotebook(name: string): Promise<void> {
  const res = await fetch(`${BASE_URL}/notebooks/${encodeURIComponent(name)}`, {
    method: "DELETE",
  });
  if (res.status === 204) return;
  const text = await res.text().catch(() => res.statusText);
  throw new Error(`DELETE /notebooks/${name} → ${res.status}: ${text}`);
}

/** POST /api/notebooks/{name}/clear-outputs. */
export async function clearNotebookOutputs(name: string): Promise<Notebook> {
  const res = await fetch(
    `${BASE_URL}/notebooks/${encodeURIComponent(name)}/clear-outputs`,
    { method: "POST" },
  );
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`POST /notebooks/${name}/clear-outputs → ${res.status}: ${text}`);
  }
  return Notebook.parse(await res.json());
}

/** POST /api/notebooks/{name}/cells/{cellId}/run — blocks until cell finishes. */
export async function runNotebookCell(
  name: string,
  cellId: string,
): Promise<CellRunResult> {
  const res = await fetch(
    `${BASE_URL}/notebooks/${encodeURIComponent(name)}/cells/${encodeURIComponent(cellId)}/run`,
    { method: "POST" },
  );
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`POST run → ${res.status}: ${text}`);
  }
  return CellRunResult.parse(await res.json());
}

/** POST /api/notebooks/{name}/cells/{cellRunId}/cancel. */
export async function cancelNotebookCell(
  name: string,
  cellRunId: string,
): Promise<void> {
  await fetch(
    `${BASE_URL}/notebooks/${encodeURIComponent(name)}/cells/${encodeURIComponent(cellRunId)}/cancel`,
    { method: "POST" },
  );
}

/** DELETE /api/notebooks/{name}/session — kill the REPL subprocess. */
export async function stopNotebookSession(name: string): Promise<void> {
  await fetch(
    `${BASE_URL}/notebooks/${encodeURIComponent(name)}/session`,
    { method: "DELETE" },
  );
}

// ---------------------------------------------------------------------------
// Ad-hoc query — POST /data/query (workspace-level SQL editor)
// ---------------------------------------------------------------------------

const AdhocQueryResult = z.object({
  columns: z.array(
    z.object({
      name: z.string(),
      type: z.string().optional(),
      nullable: z.boolean().optional(),
    }),
  ),
  rows: z.array(z.array(z.string())),
  row_count: z.number().optional(),
  truncated: z.boolean().optional(),
});
export type AdhocQueryResult = z.infer<typeof AdhocQueryResult>;

/**
 * POST /api/data/query?dir=<pipeline-dir> — run free-form SparkSQL against
 * the workspace catalog. `dir` only scopes the provider dispatch (local vs
 * cloud); any pipeline dir in the workspace routes to the same warehouse.
 * The caller (the UI's /query page) picks the first pipeline dir it sees.
 */
export async function runAdhocQuery(
  sql: string,
  dir: string,
): Promise<AdhocQueryResult> {
  const params = dir ? `?dir=${encodeURIComponent(dir)}` : "";
  const res = await fetch(`${BASE_URL}/data/query${params}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sql }),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`POST /data/query → ${res.status}: ${text}`);
  }
  return AdhocQueryResult.parse(await res.json());
}

/** POST /api/notebooks/{name}/cells/{cellId}/graduate — turn cell into transform. */
export async function graduateNotebookCell(
  name: string,
  cellId: string,
  body: { pipeline: string; transform_name: string },
): Promise<void> {
  const res = await fetch(
    `${BASE_URL}/notebooks/${encodeURIComponent(name)}/cells/${encodeURIComponent(cellId)}/graduate`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    },
  );
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`POST graduate → ${res.status}: ${text}`);
  }
}
