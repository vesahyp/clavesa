/**
 * data.ts — typed API client for the Clavesa preview endpoints.
 */

import type { Column } from "../types/pipeline";
import { request } from "./client";

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

export interface PreviewResult {
  items: Record<string, unknown>[];
  schema: Column[];
  total: number;
  truncated: boolean;
}

export interface TransformPair {
  input: Record<string, unknown>;
  output: Record<string, unknown>[];
}

export interface TransformPreviewResult {
  pairs: TransformPair[];
  sql: string;
}

// ---------------------------------------------------------------------------
// Exported API functions
// ---------------------------------------------------------------------------

/**
 * GET /preview/source — fetch a page of items from an S3 source node.
 */
export function getSourcePreview(
  dir: string,
  nodeId: string,
  offset = 0,
  limit = 50
): Promise<PreviewResult> {
  return request<PreviewResult>(
    `/preview/source?dir=${encodeURIComponent(dir)}&node_id=${encodeURIComponent(nodeId)}&offset=${offset}&limit=${limit}`
  );
}

/**
 * GET /sources/{name}/preview — sample a workspace-registered source's
 * raw data standalone, without attaching it to a pipeline.
 */
export function getRegistrySourcePreview(
  name: string,
  offset = 0,
  limit = 50,
): Promise<PreviewResult> {
  return request<PreviewResult>(
    `/sources/${encodeURIComponent(name)}/preview?offset=${offset}&limit=${limit}`,
  );
}

/**
 * GET /preview/transform — execute a transform node's SQL via DuckDB and return aligned pairs.
 * Pass `sql` to preview unsaved edits without saving to disk first.
 */
export function getTransformPreview(
  dir: string,
  nodeId: string,
  rows = 15,
  sql?: string
): Promise<TransformPreviewResult> {
  const params = new URLSearchParams({ dir, node_id: nodeId, rows: String(rows) });
  if (sql) params.set('sql', sql);
  return request<TransformPreviewResult>(`/preview/transform?${params.toString()}`);
}

/**
 * GET /preview/destination — preview rows that would be written to a destination node.
 * Traces upstream automatically; returns output rows as a PreviewResult.
 */
export function getDestinationPreview(
  dir: string,
  nodeId: string,
  rows = 15,
): Promise<PreviewResult> {
  const params = new URLSearchParams({ dir, node_id: nodeId, rows: String(rows) });
  return request<PreviewResult>(`/preview/destination?${params.toString()}`);
}
