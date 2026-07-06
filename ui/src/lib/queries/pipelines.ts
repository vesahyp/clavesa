/**
 * pipelines.ts — pipeline-level hooks: the workspace pipeline list,
 * lineage, deployment status / SFN executions, execution states + logs,
 * pipeline reset, and Delta maintenance (optimize).
 */

import { useQuery } from "@tanstack/react-query";
import { z } from "zod";

import { request } from "@/api/client";
import { requestParsed } from "./core";

const PipelineInfo = z.object({
  name: z.string(),
  dir: z.string(),
  node_count: z.number(),
  cloud: z.string().optional().default(""),
  // `compute` is the transforms' deploy target (lambda/fargate/…). Display
  // only — observability addressing follows the workspace warehouse
  // (ADR-024, useWarehouse), never this field. Missing reads as "lambda"
  // (the transform module default).
  compute: z.string().optional().default(""),
  // ADR-016 schema this pipeline writes into (== the pipeline; one
  // schema, one producing pipeline). Falls back to the sanitized name.
  schema: z.string().optional().default(""),
  // ADR-017 registered sources this pipeline's transforms consume.
  sources: z.array(z.string()).optional().default([]),
});
export type PipelineInfo = z.infer<typeof PipelineInfo>;

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
    queryFn: () =>
      requestParsed(
        `/pipeline/lineage?dir=${encodeURIComponent(dir)}`,
        LineageResponse,
        { errorLabel: "GET /pipeline/lineage" },
      ),
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
    queryFn: () =>
      requestParsed(
        `/pipeline/status?dir=${encodeURIComponent(dir)}`,
        PipelineStatusResponse,
        { errorLabel: "GET status" },
      ),
  });
}

// ---------------------------------------------------------------------------
// SFN execution state polling
// ---------------------------------------------------------------------------

const StateStatus = z.object({
  status: z.string(),
  entered_at: z.string().optional().default(""),
  // In-flight Spark progress for a RUNNING node (local pipelines today;
  // cloud fills these in a later slice). Nullish: absent until the first
  // progress tick and once the node reaches a terminal state.
  stages_total: z.number().nullish(),
  stages_completed: z.number().nullish(),
  tasks_total: z.number().nullish(),
  tasks_completed: z.number().nullish(),
  tasks_failed: z.number().nullish(),
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
 *   - { arn }      — a single exec-ref token: an SFN execution ARN, or the
 *                    `local:<dir>#<runID>` value /pipeline/status emits in
 *                    `execution_arn` (GH #78 — one encoding, both accepted).
 *   - { dir, run? } — explicit params; the server routes by the workspace
 *                    warehouse (ADR-024).
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
    queryFn: () => {
      const params = new URLSearchParams();
      if (dir) {
        params.set("dir", dir);
        if (run) params.set("run", run);
      } else if (arn) {
        params.set("arn", arn);
      }
      return requestParsed(
        `/pipeline/execution/states?${params.toString()}`,
        ExecutionStatesResponse,
        { errorLabel: "GET execution states" },
      );
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
 * GET /api/pipeline/execution/logs — log lines for one execution. Cloud
 * sources from CloudWatch FilterLogEvents windowed to `step`; local sources
 * the run's `_bundle.log` at <pipelineDir>/.clavesa/runs/<runID>/ — per-run
 * output (the bundle runner shares one container across nodes), labeled
 * per-run (ADR-014, GH #64). Addressing mirrors useExecutionStates:
 *   - { arn, step }            — exec-ref token (SFN ARN or local ref).
 *   - { dir, run?, step }      — explicit params.
 * Lazy / pull-based; only the NodeDetailDrawer fires this on demand.
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
    queryFn: () => {
      const params = new URLSearchParams({ step });
      if (dir) {
        params.set("dir", dir);
        if (run) params.set("run", run);
      } else if (arn) {
        params.set("arn", arn);
      }
      return requestParsed(
        `/pipeline/execution/logs?${params.toString()}`,
        ExecutionLogsResponse,
        { errorLabel: "GET execution logs" },
      );
    },
  });
}

// ---------------------------------------------------------------------------
// Pipeline reset — drop canonical output tables (+ optionally watermarks)
// ---------------------------------------------------------------------------

const PipelineResetTarget = z.object({
  node: z.string(),
  output_key: z.string().optional().default(""),
  table: z.string(),
  glue_db: z.string().optional().default(""),
  location: z.string().optional().default(""),
});
export type PipelineResetTarget = z.infer<typeof PipelineResetTarget>;

const PipelineResetWatermark = z.object({
  consumer: z.string(),
  alias: z.string(),
  path: z.string().optional().default(""),
});
export type PipelineResetWatermark = z.infer<typeof PipelineResetWatermark>;

const PipelineResetResult = z.object({
  pipeline: z.string().optional().default(""),
  mode: z.string().optional().default(""),
  tables_dropped: z.array(PipelineResetTarget).default([]),
  watermarks_cleared: z.array(PipelineResetWatermark).default([]),
});
export type PipelineResetResult = z.infer<typeof PipelineResetResult>;

/**
 * POST /api/pipeline/reset/plan — dry resolve of what a reset would
 * delete (the confirm modal's list). Same body as the execute call;
 * nothing is deleted.
 */
export async function planPipelineReset(body: {
  dir: string;
  node?: string;
  include_watermarks?: boolean;
}): Promise<PipelineResetResult> {
  return requestParsed("/pipeline/reset/plan", PipelineResetResult, {
    method: "POST",
    body: JSON.stringify(body),
    errorLabel: "POST /pipeline/reset/plan",
  });
}

/**
 * POST /api/pipeline/reset — execute the reset. The receipt lists only
 * what was actually deleted (planned targets that didn't exist are
 * omitted). Deployed infra is never touched.
 */
export async function resetPipeline(body: {
  dir: string;
  node?: string;
  include_watermarks?: boolean;
}): Promise<PipelineResetResult> {
  return requestParsed("/pipeline/reset", PipelineResetResult, {
    method: "POST",
    body: JSON.stringify(body),
    errorLabel: "POST /pipeline/reset",
  });
}

// Optimize — Delta table maintenance (compact / re-cluster / vacuum)
const OptimizeTableResult = z.object({
  table: z.string(),
  node: z.string(),
  output_key: z.string(),
  operation: z.string(),
  vacuumed: z.boolean().default(false),
  status: z.string(),
  error: z.string().optional().default(""),
});
export type OptimizeTableResult = z.infer<typeof OptimizeTableResult>;

const OptimizeResponse = z.object({
  results: z.array(OptimizeTableResult).default([]),
});

/**
 * POST /api/pipelines/optimize — run Delta maintenance over a pipeline's
 * output tables. With no `node` it sweeps every transform output; `recluster`
 * migrates pre-clustering tables to liquid clustering before compacting;
 * `vacuum` additionally prunes tombstoned files. The per-table results come
 * back even when individual tables failed (status === "failed").
 */
export async function optimizePipeline(body: {
  dir: string;
  node?: string;
  recluster?: boolean;
  vacuum?: boolean;
}): Promise<OptimizeTableResult[]> {
  const resp = await requestParsed("/pipelines/optimize", OptimizeResponse, {
    method: "POST",
    body: JSON.stringify(body),
    errorLabel: "POST /pipelines/optimize",
  });
  return resp.results;
}
