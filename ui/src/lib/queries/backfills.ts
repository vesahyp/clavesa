/**
 * backfills.ts — Gate 1 backfill lifecycle: stage / list / diff /
 * dedup-check / promote / discard.
 */

import { useQuery } from "@tanstack/react-query";
import { z } from "zod";

import { BASE_URL } from "@/api/client";
import { requestParsed, requestVoid } from "./core";

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
  // Which machine ran the staging compute. Absent on old servers →
  // the Lambda path (the only one that existed before ADR-024 slice 6).
  compute: z.enum(["lambda", "local"]).optional(),
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
    queryFn: () =>
      requestParsed(`/backfills?dir=${encodeURIComponent(dir)}`, BackfillsListResponse, {
        errorLabel: "GET /backfills",
      }),
  });
}

/** GET /api/backfills/{run_id}/diff?dir=…. */
export function useBackfillDiff(dir: string, runID: string) {
  return useQuery({
    queryKey: ["backfills", "diff", dir, runID],
    enabled: Boolean(dir && runID),
    retry: false,
    staleTime: 30_000,
    queryFn: () =>
      requestParsed(
        `/backfills/${encodeURIComponent(runID)}/diff?dir=${encodeURIComponent(dir)}`,
        BackfillDiff,
        { errorLabel: `GET /backfills/${runID}/diff` },
      ),
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
    queryFn: () =>
      requestParsed(
        `/backfills/${encodeURIComponent(runID)}/dedup-check?dir=${encodeURIComponent(dir)}&col=${encodeURIComponent(col)}`,
        BackfillDedupCheckResult,
        { errorLabel: `GET /backfills/${runID}/dedup-check` },
      ),
  });
}

/** POST /api/backfills/stage — stage a new backfill window.
 *
 * Stays hand-rolled (not requestParsed): a 502 with a JSON body carries
 * the partial run + error_msg, which the dialog surfaces inline rather
 * than as a generic transport error.
 */
export async function stageBackfill(body: {
  dir: string;
  node: string;
  from: string[];
  to: string[];
  direct?: boolean;
  /** Omit for the default Lambda path; "local" runs the heavy Spark
   * work in a local docker container against the cloud warehouse. */
  compute?: "local";
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
  body: {
    dir: string;
    force_dedup?: string;
    allow_duplicates?: boolean;
    /** Omit for the default Lambda path; "local" runs the MERGE in a
     * local docker container against the cloud warehouse. */
    compute?: "local";
  },
): Promise<BackfillPromoteResult> {
  return requestParsed(
    `/backfills/${encodeURIComponent(runID)}/promote`,
    BackfillPromoteResult,
    {
      method: "POST",
      body: JSON.stringify(body),
      errorLabel: `POST /backfills/${runID}/promote`,
    },
  );
}

/** POST /api/backfills/{run_id}/discard. */
export async function discardBackfill(
  runID: string,
  body: {
    dir: string;
    /** Omit for the default Lambda path; "local" runs the staging
     * cleanup in a local docker container against the cloud warehouse. */
    compute?: "local";
  },
): Promise<void> {
  return requestVoid(`/backfills/${encodeURIComponent(runID)}/discard`, {
    method: "POST",
    body: JSON.stringify(body),
    errorLabel: `POST /backfills/${runID}/discard`,
    require204: true,
  });
}
