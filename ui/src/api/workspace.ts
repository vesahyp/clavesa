/**
 * workspace.ts — API client for workspace-level endpoints.
 */

import { BASE_URL, request } from "./client";

// PipelineInfo + listPipelines were removed in C24 (2026-05-24 review) —
// the live versions live in lib/queries.ts and carry the full ADR-016
// shape (compute, schema, sources). The api/workspace.ts copies were a
// stale duplicate with no callers.

/** POST /pipelines — create a new pipeline directory.
 * `schema` is the optional ADR-016 schema identifier; empty falls back to
 * the sanitized pipeline name on the server side. */
export function createPipeline(name: string, schema = ""): Promise<{ dir: string }> {
  return request<{ dir: string }>("/pipelines", {
    method: "POST",
    body: JSON.stringify({ name, schema }),
  });
}

/** DELETE /pipelines?dir=<dir> — permanently remove a pipeline directory.
 * The confirm dialog at the call site is the UI equivalent of the CLI's
 * mandatory `--force` flag. Does NOT tear down deployed AWS resources;
 * use `clavesa pipeline destroy` for that.
 *
 * Raw fetch rather than request<T> because the server replies 204 No
 * Content on success — request<T> would crash trying to parse JSON. */
export async function deletePipeline(dir: string): Promise<void> {
  const res = await fetch(
    `${BASE_URL}/pipelines?dir=${encodeURIComponent(dir)}`,
    { method: "DELETE" },
  );
  if (res.status === 204) return;
  const text = await res.text().catch(() => res.statusText);
  throw new Error(`DELETE /pipelines?dir=${dir} → ${res.status}: ${text}`);
}

export interface WorkspaceInfo {
  root: string;
  /** false when the server's root directory has no clavesa.json yet. */
  exists: boolean;
  name?: string;
  catalog?: string;
}

/** GET /workspace — root path + whether a workspace exists there. */
export function getWorkspace(): Promise<WorkspaceInfo> {
  return request<WorkspaceInfo>("/workspace");
}

/** POST /workspace/init — create a workspace in the server's root dir.
 * Synchronous: includes the local runner Docker image build, which can
 * take minutes on a cold machine. */
export function initWorkspace(
  name: string,
  catalog = "",
): Promise<WorkspaceInfo> {
  return request<WorkspaceInfo>("/workspace/init", {
    method: "POST",
    body: JSON.stringify({ name, ...(catalog ? { catalog } : {}) }),
  });
}

export interface ModuleVersionInfo {
  current_ref: string;
  latest_ref: string;
  repo_url: string;
}

/** GET /pipeline/module-version?dir= — current + latest module ref. */
export function getPipelineModuleVersion(dir: string): Promise<ModuleVersionInfo> {
  return request<ModuleVersionInfo>(
    `/pipeline/module-version?dir=${encodeURIComponent(dir)}`,
  );
}

/** POST /pipeline/upgrade?dir=&version= — rewrite ?ref= across the
 * pipeline's .tf files and re-sync orchestration.tf. Empty version
 * picks the latest remote tag. */
export function upgradePipeline(
  dir: string,
  version = "",
): Promise<{ current_ref: string; target_ref: string; updated: number }> {
  const params = new URLSearchParams({ dir });
  if (version) params.set("version", version);
  return request(`/pipeline/upgrade?${params.toString()}`, { method: "POST" });
}

/** GET /pipeline/vars?dir= — read terraform.tfvars as a flat key→value map */
export function getVars(dir: string): Promise<Record<string, string>> {
  return request<Record<string, string>>(`/pipeline/vars?dir=${encodeURIComponent(dir)}`);
}
