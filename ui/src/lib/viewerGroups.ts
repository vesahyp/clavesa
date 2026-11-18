/**
 * viewerGroups — the signed-in hosted user's Cognito groups (ADR-035),
 * read from the id token's payload that the hosted sign-in keeps in
 * sessionStorage. The server enforces the groups; the UI only uses them
 * to leave out what the user cannot open (Site usage is for "all").
 * Kept apart from src/hosted so the local UI never loads the sign-in code.
 */

const SESSION_KEY = "clavesa.hosted.session"; // src/hosted/auth.ts

export function viewerGroups(): string[] {
  try {
    const raw = window.sessionStorage.getItem(SESSION_KEY);
    if (!raw) return [];
    const tok = (JSON.parse(raw) as { idToken?: string }).idToken ?? "";
    const part = tok.split(".")[1];
    if (!part) return [];
    const json = atob(part.replace(/-/g, "+").replace(/_/g, "/").padEnd(Math.ceil(part.length / 4) * 4, "="));
    const groups = (JSON.parse(json) as Record<string, unknown>)["cognito:groups"];
    return Array.isArray(groups) ? groups.map(String) : [];
  } catch {
    return [];
  }
}

/** seesAll: the user is in "all", which sees every workspace and Site usage. */
export function seesAll(): boolean {
  return viewerGroups().includes("all");
}
