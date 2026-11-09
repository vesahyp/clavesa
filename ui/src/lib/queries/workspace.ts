/**
 * workspace.ts — workspace-level state: the warehouse (local | cloud),
 * warm-Spark runtime status, AWS profile, and the runner-image
 * requirements editor.
 */

import { useQuery } from "@tanstack/react-query";
import { z } from "zod";

import { BASE_URL, request } from "@/api/client";
import { requestParsed } from "./core";

// ---------------------------------------------------------------------------
// Workspace warehouse (local | cloud)
// ---------------------------------------------------------------------------

// The wire shape carries `warehouse` plus a deprecated `mode` alias with
// the same value; older servers send only `mode`. Prefer `warehouse`,
// fall back to `mode`, default "local" (absent → local).
const Warehouse = z
  .object({
    warehouse: z.enum(["local", "cloud"]).optional(),
    mode: z.enum(["local", "cloud"]).optional(),
  })
  .transform(({ warehouse, mode }) => ({
    warehouse: warehouse ?? mode ?? ("local" as const),
  }));
export type Warehouse = z.infer<typeof Warehouse>;

/**
 * GET /api/workspace/environment — the workspace warehouse that drives
 * local-vs-cloud dispatch for what every page reads and writes.
 */
export function useWarehouse() {
  return useQuery({
    queryKey: ["warehouse"],
    queryFn: async () =>
      Warehouse.parse(await request("/workspace/environment")),
  });
}

/**
 * PUT /api/workspace/environment — persist the workspace warehouse.
 * The CLI twin is `clavesa workspace use` (ADR-015).
 */
export async function setWarehouse(
  warehouse: "local" | "cloud",
): Promise<Warehouse> {
  return Warehouse.parse(
    await request("/workspace/environment", {
      method: "PUT",
      body: JSON.stringify({ warehouse }),
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
  return requestParsed("/workspace/aws-profile", AWSProfileInfo, {
    method: "PUT",
    body: JSON.stringify({ profile }),
    errorLabel: "PUT /workspace/aws-profile",
  });
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
// Terraform backend (ADR-025) — the workspace's remote Terraform state
// ---------------------------------------------------------------------------

const BackendInfo = z.object({
  configured: z.boolean(),
  type: z.string().optional().default(""),
  bucket: z.string().optional().default(""),
  region: z.string().optional().default(""),
  key_prefix: z.string().optional().default(""),
});
export type BackendInfo = z.infer<typeof BackendInfo>;

/**
 * GET /api/workspace/backend — the workspace's configured remote
 * Terraform backend, or `configured: false` for local state (ADR-025).
 */
export function useBackend() {
  return useQuery({
    queryKey: ["workspace", "backend"],
    queryFn: async () =>
      BackendInfo.parse(await request("/workspace/backend")),
  });
}

export interface SetBackendInput {
  bucket: string;
  region: string;
  keyPrefix?: string;
}

/**
 * PUT /api/workspace/backend — configure the remote backend. The CLI
 * twin is `clavesa workspace set-backend`; both write clavesa.json only
 * — moving state is the separate migrateBackend call (ADR-015).
 */
export async function setBackend(input: SetBackendInput): Promise<BackendInfo> {
  return requestParsed("/workspace/backend", BackendInfo, {
    method: "PUT",
    body: JSON.stringify({
      type: "s3",
      bucket: input.bucket,
      region: input.region,
      key_prefix: input.keyPrefix || undefined,
    }),
    errorLabel: "PUT /workspace/backend",
  });
}

/**
 * PUT /api/workspace/backend with a `null` body — clear the configured
 * backend. Refused (409) once any stack has already migrated; the CLI's
 * `workspace set-backend --clear` hits the identical refusal.
 */
export async function clearBackend(): Promise<BackendInfo> {
  return requestParsed("/workspace/backend", BackendInfo, {
    method: "PUT",
    body: "null",
    errorLabel: "PUT /workspace/backend (clear)",
  });
}

const MigrateStackResult = z.object({
  dir: z.string(),
  key: z.string(),
  status: z.enum([
    "migrated",
    "already-migrated",
    "skipped-no-state",
    "failed",
  ]),
  plan: z.string().optional().default(""),
  err: z.string().optional().default(""),
});
export type MigrateStackResult = z.infer<typeof MigrateStackResult>;

const MigrateResult = z.object({
  // A run that fails a precondition has no rows; accept null as empty.
  stacks: z.array(MigrateStackResult).nullish().transform((v) => v ?? []),
  error: z.string().optional().default(""),
});
export type MigrateResult = z.infer<typeof MigrateResult>;

/**
 * POST /api/workspace/backend/migrate — move the workspace's Terraform
 * state onto the configured backend (ADR-025 "Migration, not
 * recreate"). The CLI twin is `clavesa workspace migrate-state`; both
 * call Service.MigrateState. Parses the body on every status (not just
 * 2xx) because a failed run still carries the per-stack rows the caller
 * renders — `result.error` is what signals the overall outcome, not the
 * HTTP status.
 */
export async function migrateBackend(): Promise<MigrateResult> {
  const res = await fetch(`${BASE_URL}/workspace/backend/migrate`, {
    method: "POST",
  });
  const body: unknown = await res.json().catch(() => ({}));
  return MigrateResult.parse(body);
}

// ---------------------------------------------------------------------------
// Databricks connection (ADR-027) — the Unity Catalog publish target
// ---------------------------------------------------------------------------

const DatabricksCheck = z.object({
  id: z.string(),
  title: z.string(),
  status: z.enum(["pass", "fail", "unknown", "skipped"]),
  detail: z.string().optional().default(""),
  fix: z.string().optional().default(""),
});
export type DatabricksCheck = z.infer<typeof DatabricksCheck>;

const DatabricksStatus = z.object({
  connected: z.boolean(),
  saved: z.boolean().optional().default(false),
  /** Terraform addresses a disconnect released: objects that stay, unmanaged. */
  released: z.array(z.string()).optional().default([]),
  client_id: z.string().optional().default(""),
  connection: z
    .object({
      host: z.string(),
      catalog: z.string(),
      credential: z.string(),
      external_location: z.string().optional().default(""),
      create_catalog: z.boolean().optional().default(false),
      publish_tables: z.boolean(),
      push_lineage: z.boolean(),
      readers: z.array(z.string()).nullish().transform((v) => v ?? []),
    })
    .nullish(),
  checks: z.array(DatabricksCheck).nullish().transform((v) => v ?? []),
});
export type DatabricksStatus = z.infer<typeof DatabricksStatus>;

export interface DatabricksConnectInput {
  host: string;
  catalog: string;
  credential: string;
  external_location?: string;
  create_catalog: boolean;
  publish_tables: boolean;
  push_lineage: boolean;
  /** CLI twin: --reader. Principals that get SELECT on every table clavesa registers. */
  readers?: string[];
}

/** GET /api/workspace/databricks — the recorded connection; no Databricks call. */
export function useDatabricks() {
  return useQuery({
    queryKey: ["workspace", "databricks"],
    queryFn: async () =>
      DatabricksStatus.parse(await request("/workspace/databricks")),
  });
}

/**
 * Runs `fetch` and parses a DatabricksStatus. A failed check is still a
 * 200 (with `saved: false`); any other status throws with the server's
 * error message.
 */
async function databricksCall(
  path: string,
  init: RequestInit,
): Promise<DatabricksStatus> {
  const res = await fetch(`${BASE_URL}${path}`, init);
  const body: unknown = await res.json().catch(() => ({}));
  if (res.ok) return DatabricksStatus.parse(body);
  const msg =
    body && typeof body === "object" && "error" in body
      ? String((body as { error: unknown }).error)
      : `${init.method} ${path}: HTTP ${res.status}`;
  throw new Error(msg);
}

/**
 * PUT /api/workspace/databricks — run the admin checks and record the
 * connection when none fails. CLI twin: `clavesa workspace connect
 * databricks`. `saved` in the answer says whether it was recorded.
 */
export function connectDatabricks(
  input: DatabricksConnectInput,
): Promise<DatabricksStatus> {
  return databricksCall("/workspace/databricks", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input),
  });
}

/** POST /api/workspace/databricks/check — CLI twin: `workspace databricks --check`. */
export function checkDatabricks(): Promise<DatabricksStatus> {
  return databricksCall("/workspace/databricks/check", { method: "POST" });
}

const DatabricksTable = z.object({
  full_name: z.string(),
  type: z.string(),
  location: z.string().optional().default(""),
  owner: z.string().optional().default(""),
  by_clavesa: z.boolean(),
  url: z.string(),
});
export type DatabricksTable = z.infer<typeof DatabricksTable>;

const DatabricksPipelineTables = z.object({
  pipeline: z.string(),
  dir: z.string(),
  schema: z.string(),
  schema_url: z.string(),
  schema_error: z.string().optional().default(""),
  tables: z.array(DatabricksTable).nullish().transform((v) => v ?? []),
  jobs: z.array(z.number()).nullish().transform((v) => v ?? []),
});
export type DatabricksPipelineTables = z.infer<typeof DatabricksPipelineTables>;

/**
 * GET /api/workspace/databricks/tables — each pipeline's tables in Unity
 * Catalog, read from Databricks. CLI twin: `workspace databricks tables`.
 */
export function useDatabricksTables(enabled: boolean) {
  return useQuery({
    queryKey: ["workspace", "databricks", "tables"],
    enabled,
    queryFn: async () =>
      z
        .object({ pipelines: z.array(DatabricksPipelineTables) })
        .parse(await request("/workspace/databricks/tables")),
  });
}

/**
 * PUT /api/pipeline/databricks-jobs — the jobs a pipeline starts after a
 * successful run. CLI twin: `pipeline databricks-jobs <p> --set …`.
 */
export async function setDatabricksJobs(
  pipeline: string,
  jobs: number[],
): Promise<{ pipeline: string; jobs: number[] }> {
  const res = await fetch(`${BASE_URL}/pipeline/databricks-jobs`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ pipeline, jobs }),
  });
  const body: unknown = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(
      body && typeof body === "object" && "error" in body
        ? String((body as { error: unknown }).error)
        : `PUT /pipeline/databricks-jobs: HTTP ${res.status}`,
    );
  }
  return z
    .object({ pipeline: z.string(), jobs: z.array(z.number()).nullish().transform((v) => v ?? []) })
    .parse(body);
}

const DatabricksCompareResult = z.object({
  pass: z.boolean(),
  existing: z.string(),
  shadow: z.string(),
  partition: z.string(),
  where: z.string().optional().default(""),
  partitions: z
    .array(
      z.object({
        partition: z.string(),
        status: z.string(),
        rows: z.tuple([z.number().nullable(), z.number().nullable()]),
        sums: z.record(z.string(), z.tuple([z.unknown(), z.unknown()])).nullish().transform((v) => v ?? {}),
      }),
    )
    .nullish()
    .transform((v) => v ?? []),
  schema_diff: z.array(z.string()).nullish().transform((v) => v ?? []),
});
export type DatabricksCompareResult = z.infer<typeof DatabricksCompareResult>;

export interface DatabricksCompareInput {
  pipeline: string;
  existing: string;
  shadow: string;
  partition: string;
  sums: string[];
  where?: string;
}

/**
 * POST /api/pipeline/databricks-compare — the shadow-mode check. CLI twin:
 * `pipeline databricks-compare <p> --existing … --shadow … --partition …`.
 */
export async function compareDatabricksTables(input: DatabricksCompareInput): Promise<DatabricksCompareResult> {
  const res = await fetch(`${BASE_URL}/pipeline/databricks-compare`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input),
  });
  const body: unknown = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(
      body && typeof body === "object" && "error" in body
        ? String((body as { error: unknown }).error)
        : `POST /pipeline/databricks-compare: HTTP ${res.status}`,
    );
  }
  return DatabricksCompareResult.parse(body);
}

const PreflightResult = z.object({
  ready: z.boolean(),
  checks: z.array(DatabricksCheck).nullish().transform((v) => v ?? []),
});
export type PreflightResult = z.infer<typeof PreflightResult>;

/**
 * POST /api/databricks/preflight — read-only checks for a step to replace.
 * CLI twin: `clavesa workspace databricks preflight`.
 */
export async function runPreflight(input: {
  host: string;
  profile: string;
  principal?: string;
  table?: string;
  shadow_location?: string;
  job?: number;
  inputs?: string[];
  readers?: string[];
  /** CLI twin: --compute (default fargate). */
  compute?: string;
  subnets?: string[];
}): Promise<PreflightResult> {
  const res = await fetch(`${BASE_URL}/databricks/preflight`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input),
  });
  const body: unknown = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(
      body && typeof body === "object" && "error" in body
        ? String((body as { error: unknown }).error)
        : `POST /databricks/preflight: HTTP ${res.status}`,
    );
  }
  return PreflightResult.parse(body);
}

const DatabricksRun = z.object({
  found: z.boolean(),
  status: z.string().optional().default(""),
  tables: z
    .array(
      z.object({
        full_name: z.string().optional().default(""),
        action: z.string().optional().default(""),
        url: z.string().optional().default(""),
        lineage: z.string().optional().default(""),
      }),
    )
    .nullish()
    .transform((v) => v ?? []),
  jobs: z
    .array(
      z.object({
        job_id: z.number(),
        run_id: z.number().optional(),
        run_page_url: z.string().optional().default(""),
        error: z.string().optional().default(""),
      }),
    )
    .nullish()
    .transform((v) => v ?? []),
});
export type DatabricksRun = z.infer<typeof DatabricksRun>;

/**
 * GET /api/pipeline/databricks-run — the Unity Catalog tables a run wrote
 * and the Databricks jobs it started. CLI twin: `pipeline databricks-run`.
 */
export function useDatabricksRun(
  dir: string,
  runId: string | undefined,
  enabled: boolean,
) {
  return useQuery({
    queryKey: ["pipeline", "databricks-run", dir, runId],
    enabled: enabled && !!runId,
    queryFn: async () =>
      DatabricksRun.parse(
        await request(
          `/pipeline/databricks-run?dir=${encodeURIComponent(dir)}&run=${encodeURIComponent(runId ?? "")}`,
        ),
      ),
  });
}

/** DELETE /api/workspace/databricks — CLI twin: `workspace disconnect databricks`. */
export function disconnectDatabricks(): Promise<DatabricksStatus> {
  return databricksCall("/workspace/databricks", { method: "DELETE" });
}

// ---------------------------------------------------------------------------
// Runner requirements — extra Python pip deps baked into the runner image
// ---------------------------------------------------------------------------

const RunnerRequirementsResponse = z.object({
  content: z.string(),
  requirements: z.array(z.string()).default([]),
});
export type RunnerRequirementsResponse = z.infer<
  typeof RunnerRequirementsResponse
>;

/** GET /api/runner/requirements — the workspace's runner requirements.txt. */
export function useRunnerRequirements() {
  return useQuery({
    queryKey: ["runner-requirements"],
    queryFn: async () => {
      const raw = await request<unknown>("/runner/requirements");
      return RunnerRequirementsResponse.parse(raw);
    },
  });
}

/** PUT /api/runner/requirements — save the raw requirements.txt content. */
export async function setRunnerRequirements(
  content: string,
): Promise<RunnerRequirementsResponse> {
  const raw = await request<unknown>("/runner/requirements", {
    method: "PUT",
    body: JSON.stringify({ content }),
  });
  return RunnerRequirementsResponse.parse(raw);
}
