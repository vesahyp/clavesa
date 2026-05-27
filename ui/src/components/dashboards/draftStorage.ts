/**
 * draftStorage — autosave + restore for the dashboard editor.
 *
 * Stores each in-progress edit to `localStorage` under
 * `dashboard-draft:<slug>` with a millisecond stamp. The editor
 * autosaves on every state change (debounced); on mount it reads
 * any existing draft and, if it's newer than what the server has,
 * surfaces a restore banner. Save clears the draft. Same shape
 * the explicit "publish" button writes, so a recovered draft
 * round-trips identically.
 *
 * The risk this addresses: a tab close mid-edit, a browser crash,
 * or a refresh after a long auto-preview session would otherwise
 * lose every dataset rewrite. Autosave is the safety net; explicit
 * Save remains the publish moment so the dashboards system table
 * stays curated.
 *
 * Storage is per-slug. A draft for `pipeline-runs-demo` doesn't
 * collide with one for `revenue` — multiple editor tabs can each
 * carry their own work-in-progress.
 */

import type { Dashboard } from "@/lib/queries";

const KEY_PREFIX = "dashboard-draft:";

export interface StoredDraft {
  spec: Dashboard;
  /** ms since epoch when the draft was last autosaved. */
  savedAt: number;
}

/** loadDraft returns the stored draft for a slug, or null. */
export function loadDraft(slug: string): StoredDraft | null {
  try {
    const raw = window.localStorage.getItem(KEY_PREFIX + slug);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as StoredDraft;
    if (!parsed || typeof parsed.savedAt !== "number" || !parsed.spec) {
      return null;
    }
    return parsed;
  } catch {
    // localStorage may be disabled (private browsing, full quota).
    // Treat as "no draft" — autosave is best-effort.
    return null;
  }
}

/** saveDraft writes a draft (caller is expected to debounce). */
export function saveDraft(slug: string, spec: Dashboard): void {
  try {
    const payload: StoredDraft = { spec, savedAt: Date.now() };
    window.localStorage.setItem(KEY_PREFIX + slug, JSON.stringify(payload));
  } catch {
    // Quota exceeded or storage unavailable — silently skip. We don't
    // want a localStorage write failure to bubble into the editor's
    // toast surface; the user will see the explicit Save button still
    // works.
  }
}

/** clearDraft removes the stored draft for a slug (Save / Discard). */
export function clearDraft(slug: string): void {
  try {
    window.localStorage.removeItem(KEY_PREFIX + slug);
  } catch {
    // best-effort
  }
}

/**
 * shouldOfferRestore — comparator the editor uses on mount. Restore
 * is worth surfacing only when the draft is newer than the server's
 * `updated_at` (parsed as ISO; an unparseable / empty value is
 * treated as "no server copy", which means any draft is fresher).
 * A draft older than the server is stale work the user has since
 * superseded — silently discard.
 */
export function shouldOfferRestore(
  draft: StoredDraft,
  serverUpdatedAt: string,
): boolean {
  if (!serverUpdatedAt) return true;
  const serverMs = Date.parse(serverUpdatedAt);
  if (!Number.isFinite(serverMs)) return true;
  return draft.savedAt > serverMs;
}
