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
