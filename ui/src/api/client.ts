/**
 * client.ts — shared fetch client for all Clavesa API modules.
 *
 * The base URL is the runtime config's apiBase (ADR-032): "/api" in the
 * local UI, "/w/<name>/api" inside one workspace of a multi-workspace or
 * hosted server. Never hardcode host, port or "/api" in component code;
 * call request / apiFetch with a path that starts at the API root.
 *
 * In hosted mode apiFetch adds `Authorization: Bearer <id token>`
 * (hosted/auth.ts) and a 401 signs the page out.
 */

import { runtimeConfig } from "@/lib/runtimeConfig";

export const BASE_URL: string = runtimeConfig.apiBase;

/**
 * apiFetch — fetch against the API base. For the callers that need the
 * raw Response (204 bodies, 409 conflict payloads, plain-text bodies).
 * `base` overrides the page's API base: the Workspaces page reads each
 * workspace's summary from that workspace's own /w/<name>/api.
 */
export async function apiFetch(
  path: string,
  init?: RequestInit,
  base: string = BASE_URL,
): Promise<Response> {
  if (runtimeConfig.mode !== "hosted") return fetch(`${base}${path}`, init);
  // Hosted (ADR-032): every call carries the signed-in user's id token.
  // The auth module is loaded on demand, so the local UI never ships it.
  const auth = await import("@/hosted/auth");
  const headers = new Headers(init?.headers);
  for (const [k, v] of Object.entries(await auth.authHeaders())) headers.set(k, v);
  const res = await fetch(`${base}${path}`, { ...init, headers });
  if (res.status === 401) auth.sessionLost();
  return res;
}

/**
 * Typed fetch wrapper. Sends JSON content-type, throws on non-2xx.
 */
export async function request<T>(
  path: string,
  options: RequestInit = {}
): Promise<T> {
  const res = await apiFetch(path, {
    headers: { "Content-Type": "application/json", ...options.headers },
    ...options,
  });
  if (!res.ok) {
    throw new Error(`API ${options.method ?? "GET"} ${path} → ${res.status}: ${await errorMessage(res)}`);
  }
  return res.json() as Promise<T>;
}

/**
 * errorMessage reads a failed response's body. Clavesa error responses are
 * `{"error":"…"}`; unwrap so toasts show the message itself, not escaped
 * JSON. Non-JSON bodies pass through.
 */
export async function errorMessage(res: Response): Promise<string> {
  const text = await res.text().catch(() => res.statusText);
  try {
    const parsed = JSON.parse(text);
    if (parsed && typeof parsed.error === "string") return parsed.error;
  } catch {
    // not JSON — keep the raw text
  }
  return text;
}
