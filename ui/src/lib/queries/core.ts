/**
 * core.ts — shared plumbing for the query modules under lib/queries/.
 *
 * Holds the fetch+Zod helpers every domain module converges on
 * (G P2-1 requestParsed sweep) plus the cross-domain schemas.
 */

import { z } from "zod";

import { BASE_URL } from "@/api/client";

// isQueryableIdentifier mirrors the server's database/table validation
// (`[A-Za-z_][A-Za-z0-9_]*`). The Delta/Spark per-table endpoints
// (snapshots, column-stats, sample) reject anything else with a 400, so a
// stray Glue/warehouse table with a dot or dash in its name (e.g. a manual
// `foo.backup_20260530`) would otherwise spew console errors from every
// catalog row that probes it. Gate those queries off when the name can't be
// addressed rather than firing a request the server is guaranteed to reject.
const IDENTIFIER_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;
export function isQueryableIdentifier(database: string, table: string): boolean {
  return IDENTIFIER_RE.test(database) && IDENTIFIER_RE.test(table);
}

// ---------------------------------------------------------------------------
// Cross-domain schemas
// ---------------------------------------------------------------------------

// ADR-024: engine identity is per-response metadata. Every SQL-running
// endpoint may stamp `served` describing which engine answered and against
// which warehouse. Optional — absent on old servers and on endpoints that
// haven't adopted it yet; the EngineBadge renders nothing in that case.
// `engine`/`warehouse` stay open strings so a new engine value degrades to
// a badge with an unfamiliar label instead of failing the whole response
// parse.
export const ServedInfo = z.object({
  engine: z.string(),
  warehouse: z.string(),
  transpiled: z.boolean().optional(),
});
export type ServedInfo = z.infer<typeof ServedInfo>;

// ---------------------------------------------------------------------------
// Shared fetch helpers (G P2-1 requestParsed sweep)
// ---------------------------------------------------------------------------

// jsonInit sets the JSON content-type only when the request carries a body —
// matching what the hand-rolled fetch sites always did (GETs sent no
// content-type; JSON-bodied writes set it explicitly).
function jsonInit(init: RequestInit): RequestInit {
  if (init.body == null) return init;
  return {
    ...init,
    headers: { "Content-Type": "application/json", ...init.headers },
  };
}

/**
 * requestParsed — fetch + Zod-parse for the JSON endpoints in the query
 * modules. Prepends BASE_URL and parses the response body through `schema`
 * at the trust boundary.
 *
 * `errorLabel` preserves each call site's historical error prefix — the
 * thrown message is `<errorLabel> → <status>: <body>`, exactly what the
 * hand-rolled fetch sites threw before the sweep, so error panels, toasts,
 * and the verify-readme assertions render unchanged.
 */
export async function requestParsed<S extends z.ZodTypeAny>(
  path: string,
  schema: S,
  options: RequestInit & { errorLabel: string },
): Promise<z.infer<S>> {
  const { errorLabel, ...init } = options;
  const res = await fetch(`${BASE_URL}${path}`, jsonInit(init));
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`${errorLabel} → ${res.status}: ${text}`);
  }
  return schema.parse(await res.json()) as z.infer<S>;
}

/**
 * requestVoid — fetch for endpoints whose success carries no body.
 * `require204` mirrors the strict call sites that treat only 204 as
 * success (the registry attach/delete contract).
 */
export async function requestVoid(
  path: string,
  options: RequestInit & { errorLabel: string; require204?: boolean },
): Promise<void> {
  const { errorLabel, require204, ...init } = options;
  const res = await fetch(`${BASE_URL}${path}`, jsonInit(init));
  if (require204 ? res.status === 204 : res.ok) return;
  const text = await res.text().catch(() => res.statusText);
  throw new Error(`${errorLabel} → ${res.status}: ${text}`);
}

/**
 * requestDelete — DELETE with the registry deletion-guard contract:
 * 204 → null (deleted), 409 → parsed conflict body (the usage list the
 * caller surfaces in its confirm dialog).
 */
export async function requestDelete<T>(
  path: string,
  errorLabel: string,
): Promise<T | null> {
  const res = await fetch(`${BASE_URL}${path}`, { method: "DELETE" });
  if (res.status === 204) return null;
  if (res.status === 409) return (await res.json()) as T;
  const text = await res.text().catch(() => res.statusText);
  throw new Error(`${errorLabel} → ${res.status}: ${text}`);
}
