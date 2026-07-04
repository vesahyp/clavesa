/**
 * runs.ts — run-observability hooks: node_runs, per-execution runs,
 * the north-star cost rollup, and rightsize recommendations.
 */

import { useQuery } from "@tanstack/react-query";
import { z } from "zod";

import { requestParsed } from "./core";

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
  // Spark-observability metrics (v0.14.x). peak_rss_mb is a process-lifetime
  // high-water mark; the rest are per-invocation Spark aggregates. Null on
  // older runners and skipped/path-mode runs.
  peak_rss_mb: z.number().nullish(),
  peak_execution_memory_mb: z.number().nullish(),
  memory_spilled_bytes: z.number().nullish(),
  disk_spilled_bytes: z.number().nullish(),
  shuffle_read_bytes: z.number().nullish(),
  shuffle_write_bytes: z.number().nullish(),
  input_bytes: z.number().nullish(),
  input_records: z.number().nullish(),
  num_stages: z.number().nullish(),
  num_tasks: z.number().nullish(),
  num_failed_tasks: z.number().nullish(),
  jvm_gc_time_ms: z.number().nullish(),
  executor_cpu_time_ms: z.number().nullish(),
  executor_run_time_ms: z.number().nullish(),
  max_task_duration_ms: z.number().nullish(),
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
    queryFn: () => {
      const params = new URLSearchParams({ pipeline, limit: String(limit) });
      if (node) params.set("node", node);
      if (dir) params.set("dir", dir);
      // Run-detail page narrows to a single execution by sf_execution_arn —
      // the join key against runs. Cloud passes the SFN ARN, local passes
      // the pipeline-run uuid (same column on both sides).
      if (arn) params.set("arn", arn);
      return requestParsed(`/data/node-runs?${params.toString()}`, NodeRunsResult, {
        errorLabel: "GET node-runs",
      });
    },
  });
}

// ---------------------------------------------------------------------------
// pipeline-cost — north-star "cost per billion records" rollup
// ---------------------------------------------------------------------------

const NodeCostSchema = z.object({
  node: z.string(),
  computeTarget: z.string(),
  runs: z.number(),
  records: z.number(),
  billedSeconds: z.number(),
  costUsd: z.number(),
  recordsPerSec: z.number(),
  costPerBillion: z.number(),
});
export type NodeCost = z.infer<typeof NodeCostSchema>;

const PipelineCostSchema = z.object({
  pipeline: z.string(),
  totalRecords: z.number(),
  totalCostUsd: z.number(),
  costPerBillion: z.number(),
  recordsPerSec: z.number(),
  perNode: z.array(NodeCostSchema),
  priceBasis: z.string(),
});
export type PipelineCost = z.infer<typeof PipelineCostSchema>;

/**
 * GET /api/data/pipeline-cost?dir=… — the north-star "cost per billion
 * records processed" rollup, aggregated from the pipeline's recent runner
 * invocations. Local pipelines report $0 compute (the throughput half of
 * the metric stays meaningful pre-deploy). Errors are non-fatal for the
 * dashboard — a fresh pipeline with no runs returns an empty rollup.
 */
export function usePipelineCost(dir: string) {
  return useQuery({
    queryKey: ["pipeline-cost", dir],
    enabled: Boolean(dir),
    retry: false,
    staleTime: 30_000,
    queryFn: () => {
      const params = new URLSearchParams({ dir });
      return requestParsed(`/data/pipeline-cost?${params.toString()}`, PipelineCostSchema, {
        errorLabel: "GET pipeline-cost",
      });
    },
  });
}

// ---------------------------------------------------------------------------
// rightsize — recommend-only per-node memory recommendation
// ---------------------------------------------------------------------------

const NodeRightsize = z.object({
  node: z.string(),
  // Allocated memory_mb of the newest run; null for local rows (no
  // allocation on record).
  current_mb: z.number().nullish(),
  // Recommended memory_mb; null when confidence is "n/a".
  recommended_mb: z.number().nullish(),
  p95_peak_rss_mb: z.number().nullish(),
  samples: z.number(),
  spill_rate: z.number(),
  reason: z.string(),
  // "high" | "medium" | "low" | "n/a".
  confidence: z.string(),
});
export type NodeRightsize = z.infer<typeof NodeRightsize>;

const RightsizeResult = z.object({
  rows: z.array(NodeRightsize),
});
export type RightsizeResult = z.infer<typeof RightsizeResult>;

/**
 * GET /api/data/rightsize?pipeline=…[&dir=…&last=…] — per-node memory
 * recommendations from the pipeline's recent runner invocations. Recommend-
 * only: the backend reads node_runs and returns advice, never mutating the
 * pipeline. Local and cloud return the same shape (ADR-014); a fresh
 * pipeline with no metric-bearing runs returns empty rows.
 */
export function useRightsize(
  pipeline: string,
  dir: string,
  opts: { last?: number; enabled?: boolean } = {},
) {
  const last = opts.last ?? 50;
  return useQuery({
    queryKey: ["rightsize", pipeline, dir, last],
    enabled: Boolean(pipeline) && (opts.enabled ?? true),
    retry: false,
    staleTime: 30_000,
    queryFn: () => {
      const params = new URLSearchParams({ pipeline, last: String(last) });
      if (dir) params.set("dir", dir);
      return requestParsed(`/data/rightsize?${params.toString()}`, RightsizeResult, {
        errorLabel: "GET rightsize",
      });
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
    queryFn: () => {
      const params = new URLSearchParams({ pipeline, limit: String(limit) });
      if (dir) params.set("dir", dir);
      return requestParsed(`/data/runs?${params.toString()}`, RunsResult, {
        errorLabel: "GET runs",
      });
    },
  });
}
