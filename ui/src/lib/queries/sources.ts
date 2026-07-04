/**
 * sources.ts — the workspace source + credential registries (ADR-017
 * slices 1 and 2): list hooks, register/update mutations, attach, and the
 * guarded deletes.
 */

import { useQuery } from "@tanstack/react-query";
import { z } from "zod";

import { request } from "@/api/client";
import { requestDelete, requestParsed, requestVoid } from "./core";

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
  return requestParsed("/credentials", CredentialSpec, {
    method: "POST",
    body: JSON.stringify({ kind: spec.kind ?? "header", ...spec }),
    errorLabel: "POST /credentials",
  });
}

/** DELETE /api/credentials/{name}. */
export async function deleteCredential(
  name: string,
  opts: { force?: boolean } = {},
): Promise<{ usages?: { source_name: string }[] } | null> {
  const params = opts.force ? "?force=1" : "";
  return requestDelete(
    `/credentials/${encodeURIComponent(name)}${params}`,
    `DELETE /credentials/${name}`,
  );
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
  // kind omitted on purpose — the server sniffs `s3://` vs `https://`
  // out of the URL field. Caller can still pass kind explicitly when
  // it knows.
  return requestParsed("/sources", SourceSpec, {
    method: "POST",
    body: JSON.stringify(spec),
    errorLabel: "POST /sources",
  });
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
  return requestParsed(`/sources/${encodeURIComponent(name)}`, SourceSpec, {
    method: "PUT",
    body: JSON.stringify(spec),
    errorLabel: `PUT /sources/${name}`,
  });
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
  return requestVoid(`/sources/${encodeURIComponent(name)}/attach`, {
    method: "POST",
    body: JSON.stringify(body),
    errorLabel: `POST /sources/${name}/attach`,
    require204: true,
  });
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
  return requestVoid("/pipeline/external-table/attach", {
    method: "POST",
    body: JSON.stringify(body),
    errorLabel: "POST /pipeline/external-table/attach",
  });
}

/** DELETE /api/sources/{name}. Returns null on success, usage list on 409. */
export async function deleteSource(
  name: string,
  opts: { force?: boolean } = {},
): Promise<{ usages?: { pipeline_dir: string; node_ids: string[] }[] } | null> {
  const params = opts.force ? "?force=1" : "";
  return requestDelete(
    `/sources/${encodeURIComponent(name)}${params}`,
    `DELETE /sources/${name}`,
  );
}
