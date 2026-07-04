/**
 * catalog.ts — catalog + per-table data hooks: the workspace table list,
 * sample rows, Delta commit history, column stats, tables-state, and the
 * ad-hoc workspace SQL runner.
 */

import { useQuery } from "@tanstack/react-query";
import { z } from "zod";

import { request } from "@/api/client";
import { ServedInfo, isQueryableIdentifier, requestParsed } from "./core";

// ---------------------------------------------------------------------------
// Schemas (boundary-of-trust for API responses)
// ---------------------------------------------------------------------------

const CatalogColumn = z.object({
  name: z.string(),
  type: z.string(),
});

const CatalogTable = z.object({
  database: z.string(),
  // ADR-020: three-piece namespace surfaced separately so the UI can render
  // <catalog>.<schema>.<table> without splitting `database` on `__`. Slice 8
  // migrates consumers; for this slice they're additive and optional.
  catalog: z.string().optional().default(""),
  schema: z.string().optional().default(""),
  table: z.string().optional().default(""),
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
  served: ServedInfo.optional(),
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
  // Full commit count, independent of the (limit-truncated) snapshots slice.
  total: z.number().optional().default(0),
});
export type SnapshotsResult = z.infer<typeof SnapshotsResult>;

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
  const enabled =
    (opts.enabled ?? true) &&
    Boolean(database && table) &&
    isQueryableIdentifier(database, table);
  return useQuery({
    queryKey: ["table", database, table, "sample", limit, dir],
    enabled,
    meta: { spark: true },
    queryFn: () => {
      const params = new URLSearchParams({
        catalog_db: database,
        catalog_table: table,
        limit: String(limit),
      });
      if (dir) params.set("dir", dir);
      return requestParsed(`/data/table?${params.toString()}`, TableQueryResult, {
        errorLabel: "GET /data/table",
      });
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
    enabled: Boolean(database && table) && isQueryableIdentifier(database, table),
    meta: { spark: true },
    // Snapshot queries hit Athena — don't refetch aggressively.
    staleTime: 60_000,
    retry: false,
    queryFn: () => {
      const params = new URLSearchParams({ limit: String(limit) });
      // Local-pipeline tables route through LocalProvider when `dir` is set;
      // without it the snapshots endpoint defaults to cloud and 500s for
      // tables that only exist in a per-pipeline Hadoop warehouse.
      if (dir) params.set("dir", dir);
      return requestParsed(
        `/data/tables/${encodeURIComponent(database)}/${encodeURIComponent(table)}/snapshots?${params.toString()}`,
        SnapshotsResult,
        { errorLabel: "GET snapshots" },
      );
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
    enabled: Boolean(database && table) && isQueryableIdentifier(database, table),
    meta: { spark: true },
    staleTime: 60_000,
    retry: false,
    queryFn: () => {
      const params = new URLSearchParams();
      if (dir) params.set("dir", dir);
      const qs = params.toString();
      return requestParsed(
        `/data/tables/${encodeURIComponent(database)}/${encodeURIComponent(table)}/column-stats${qs ? `?${qs}` : ""}`,
        ColumnStatsResult,
        { errorLabel: "GET column-stats" },
      );
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
  opts: { dir?: string; limit?: number; enabled?: boolean } = {},
) {
  const limit = opts.limit ?? 50;
  const dir = opts.dir ?? "";
  const enabled = (opts.enabled ?? true) && Boolean(pipeline);
  return useQuery({
    queryKey: ["tables-state", pipeline, dir, limit],
    enabled,
    retry: false,
    staleTime: 30_000,
    queryFn: () => {
      const params = new URLSearchParams({ pipeline, limit: String(limit) });
      if (dir) params.set("dir", dir);
      return requestParsed(`/data/tables-state?${params.toString()}`, TablesResult, {
        errorLabel: "GET tables-state",
      });
    },
  });
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
  served: ServedInfo.optional(),
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
  return requestParsed(`/data/query${params}`, AdhocQueryResult, {
    method: "POST",
    body: JSON.stringify({ sql }),
    errorLabel: "POST /data/query",
  });
}
