/**
 * pipeline.ts — typed API client for the Clavesa Pipeline API.
 *
 * All mutations return a refreshed PipelineGraph so the caller can replace
 * its local state without a second round-trip.
 */

import type { PipelineGraph, ValidationResult } from "../types/pipeline";
import { BASE_URL, request } from "./client";

// ---------------------------------------------------------------------------
// Exported API functions
// ---------------------------------------------------------------------------

/** GET /pipeline?dir= */
export function getPipeline(dir: string): Promise<PipelineGraph> {
  return request<PipelineGraph>(
    `/pipeline?dir=${encodeURIComponent(dir)}`
  );
}

/** POST /pipeline/nodes — add a new node of the given type (raw-attribute path; prefer addTypedNode for new code). */
export function addNode(
  dir: string,
  file: string,
  block_name: string,
  attributes: Record<string, unknown>
): Promise<PipelineGraph> {
  return request<PipelineGraph>("/pipeline/nodes", {
    method: "POST",
    body: JSON.stringify({ dir, file, block_name, attributes }),
  });
}

/**
 * POST /pipeline/typed-nodes — add a node by type. Server threads
 * pipeline_name, bucket, catalog/schema, runner_image and pins `?ref=` to
 * the current ModuleVersion (matches `clavesa node add` CLI). Pass
 * name = "" to let the server auto-generate `<type><N>`.
 *
 * `type` must be "transform" or "destination"; sources are not authored
 * here (see /sources route + ADR-017 workspace source registry).
 */
export function addTypedNode(
  dir: string,
  type: "transform" | "destination",
  name = ""
): Promise<PipelineGraph> {
  return request<PipelineGraph>("/pipeline/typed-nodes", {
    method: "POST",
    body: JSON.stringify({ dir, type, name }),
  });
}

/** PUT /pipeline/nodes/:id — update an existing node's config */
export function updateNode(
  dir: string,
  node_id: string,
  config: Record<string, unknown>
): Promise<PipelineGraph> {
  return request<PipelineGraph>(`/pipeline/nodes/${encodeURIComponent(node_id)}`, {
    method: "PUT",
    body: JSON.stringify({ dir, attributes: config }),
  });
}

/**
 * POST /pipeline/nodes/:id/rename — rename a node. Moves the module block,
 * every downstream edge reference, and the transform's script files. The
 * node id is also the stem of its Delta output table, so a rename
 * changes that table's name.
 */
export function renameNode(
  dir: string,
  node_id: string,
  new_id: string,
): Promise<PipelineGraph> {
  return request<PipelineGraph>(
    `/pipeline/nodes/${encodeURIComponent(node_id)}/rename`,
    {
      method: "POST",
      body: JSON.stringify({ dir, new_id }),
    },
  );
}

/** DELETE /pipeline/nodes/:id */
export function deleteNode(dir: string, node_id: string): Promise<PipelineGraph> {
  return request<PipelineGraph>(`/pipeline/nodes/${encodeURIComponent(node_id)}`, {
    method: "DELETE",
    body: JSON.stringify({ dir }),
  });
}

/** POST /pipeline/edges
 *
 * `to_input` sets the SQL table alias the edge is read by. Omit it and
 * the server defaults the alias to `from_node` — matching the graph's
 * guided (+) menu and the CLI's `node connect` without `--input`. The
 * ConfigPanel inputs form passes an explicit alias so the SQL can read
 * naturally. */
export function addEdge(
  dir: string,
  from_node: string,
  to_node: string,
  to_input?: string,
): Promise<PipelineGraph> {
  return request<PipelineGraph>("/pipeline/edges", {
    method: "POST",
    body: JSON.stringify({
      dir,
      from_node,
      from_output: "default",
      to_node,
      ...(to_input ? { to_input } : {}),
    }),
  });
}

/** DELETE /pipeline/edges/:id */
export function deleteEdge(dir: string, edge_id: string): Promise<PipelineGraph> {
  return request<PipelineGraph>(`/pipeline/edges/${encodeURIComponent(edge_id)}`, {
    method: "DELETE",
    body: JSON.stringify({ dir }),
  });
}

/** POST /pipeline/inputs/detach — remove an aliased input from a transform.
 * Covers all three kinds (transform→transform edge, registry source,
 * external `<schema>.<table>` reference) under one call. */
export function detachInput(
  dir: string,
  to: string,
  alias: string,
): Promise<PipelineGraph> {
  return request<PipelineGraph>("/pipeline/inputs/detach", {
    method: "POST",
    body: JSON.stringify({ dir, to, alias }),
  });
}

/** GET /pipeline/validate?dir= */
export function validatePipeline(dir: string): Promise<ValidationResult> {
  return request<ValidationResult>(
    `/pipeline/validate?dir=${encodeURIComponent(dir)}`
  );
}

/** GET /pipeline/script?dir=&path= — returns raw script text */
export async function getScript(dir: string, path: string): Promise<string> {
  const res = await fetch(
    `${BASE_URL}/pipeline/script?dir=${encodeURIComponent(dir)}&path=${encodeURIComponent(path)}`
  );
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`API GET /pipeline/script → ${res.status}: ${text}`);
  }
  return res.text();
}

/** PUT /pipeline/script — writes raw script text */
export async function putScript(dir: string, path: string, content: string): Promise<void> {
  const res = await fetch(`${BASE_URL}/pipeline/script`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ dir, path, content }),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`API PUT /pipeline/script → ${res.status}: ${text}`);
  }
}

/**
 * Result of POST /pipeline/run. Backend dispatches by compute attr (ADR-014):
 *   - local pipeline: returns `{ run_id, nodes: [...] }` from service.RunPipeline.
 *   - cloud pipeline: returns `{ execution_arn }` from SFN StartExecution.
 * Either shape is fine — the UI uses run_id to navigate to RunDetail when present.
 */
export interface RunPipelineResult {
  run_id?: string;
  execution_arn?: string;
  nodes?: Array<{
    node_id: string;
    type: string;
    status: "ok" | "skipped" | "failed";
    output?: string;
    note?: string;
  }>;
}

/**
 * POST /pipeline/run — trigger a pipeline run (local or cloud).
 *
 * Local runs dispatch asynchronously: the call returns a `run_id` as
 * soon as the run is prepared, not after it finishes. A 409 means the
 * pipeline already has a run in flight — surfaced as its clean message.
 */
export async function runPipeline(dir: string): Promise<RunPipelineResult> {
  const res = await fetch(`${BASE_URL}/pipeline/run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ dir }),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    // The backend errors as {"error": "..."}; surface that message.
    let message = text;
    try {
      const parsed = JSON.parse(text);
      if (parsed && typeof parsed.error === "string") message = parsed.error;
    } catch {
      /* not JSON — keep the raw text */
    }
    throw new Error(message);
  }
  return (await res.json()) as RunPipelineResult;
}
