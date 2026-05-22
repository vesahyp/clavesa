/**
 * client.ts — shared fetch client for all Clavesa API modules.
 *
 * Base URL is read from VITE_API_BASE_URL at build time, falling back to the
 * local dev server address. Never hardcode host/port in component code.
 */

export const BASE_URL: string =
  (import.meta as unknown as { env: Record<string, string> }).env
    .VITE_API_BASE_URL ?? "/api";

/**
 * Typed fetch wrapper. Sends JSON content-type, throws on non-2xx.
 */
export async function request<T>(
  path: string,
  options: RequestInit = {}
): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, {
    headers: { "Content-Type": "application/json", ...options.headers },
    ...options,
  });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    // Clavesa error responses are `{"error":"…"}`; unwrap so toasts show
    // the message itself, not escaped JSON. Non-JSON bodies pass through.
    let message = text;
    try {
      const parsed = JSON.parse(text);
      if (parsed && typeof parsed.error === "string") message = parsed.error;
    } catch {
      // not JSON — keep the raw text
    }
    throw new Error(`API ${options.method ?? "GET"} ${path} → ${res.status}: ${message}`);
  }
  return res.json() as Promise<T>;
}
