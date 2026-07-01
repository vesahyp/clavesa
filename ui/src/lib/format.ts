// Shared formatters used by Catalog tiles, the dashboard, and the run-detail
// page. Duplicating these per-page risks drift in how "5 minutes ago" or
// "1.2K rows" actually renders, so they live here.

import { formatDistanceToNow } from "date-fns";

export function formatRelative(iso: string): string {
  if (!iso) return "—";
  try {
    const d = new Date(iso);
    if (isNaN(d.getTime())) return "—";
    return formatDistanceToNow(d, { addSuffix: true });
  } catch {
    return "—";
  }
}

export function formatDuration(ms: number | null | undefined): string {
  if (ms == null) return "—";
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  const m = Math.floor(ms / 60_000);
  const s = Math.round((ms % 60_000) / 1000);
  return s === 0 ? `${m}m` : `${m}m ${s}s`;
}

// formatRowCount renders a Delta table row count compactly. Same suffix rules
// across surfaces — keeps the Catalog row badges and the dashboard's
// Output tables panel speaking the same shorthand.
export function formatRowCount(n: number): string {
  if (n < 1000) return `${n} row${n === 1 ? "" : "s"}`;
  if (n < 1_000_000) return `${(n / 1000).toFixed(n < 10_000 ? 1 : 0)}K rows`;
  if (n < 1_000_000_000)
    return `${(n / 1_000_000).toFixed(n < 10_000_000 ? 1 : 0)}M rows`;
  return `${(n / 1_000_000_000).toFixed(1)}B rows`;
}

// displayTableName drops the `__<output_key>` wire suffix for the common
// single-output case. A transform's default output keys to "default", so
// `revenue_by_payment__default` is just noise to a human. Multi-output
// nodes keep the key — surfaced as a sub-label, not glued onto the name.
// The full `<node>__<key>` identifier still lives in `name` for URLs.
// Shared by the Catalog rows, TableDetail, and the dashboard's Output
// tables panel so all three render the same name.
export function displayTableName(t: {
  owning_node?: string;
  name: string;
}): string {
  return t.owning_node || t.name;
}

// displayTablePath renders the three-level logical identifier
// (ADR-020) — `<catalog>.<schema>.<table>` with the `__default` suffix
// stripped via displayTableName. Pure display: the engine still accepts
// the flat-encoded wire form `<catalog>__<schema>.<table>`, so SQL
// surfaces compose their own string. Use this for the TableDetail
// header chip, lineage labels, dashboard output chips, and anywhere
// else the goal is to read out the table's identity to a human.
// Falls back gracefully when catalog/schema are blank (pre-Slice-7
// API responses): drops empty segments so `database`-only payloads
// still render something sensible.
export function displayTablePath(t: {
  catalog?: string;
  schema?: string;
  owning_node?: string;
  name: string;
}): string {
  const parts = [t.catalog ?? "", t.schema ?? "", displayTableName(t)].filter(
    (s) => s !== "",
  );
  return parts.join(".");
}

// showOutputKey is true only for genuine multi-output nodes — "default" is
// the implicit single-output key and renders as noise next to the name.
export function showOutputKey(t: {
  owning_node?: string;
  output_key?: string;
}): boolean {
  return !!t.owning_node && !!t.output_key && t.output_key !== "default";
}

// formatBytes is sized to dashboard chips — IEC-ish, not strict.
export function formatBytes(n: number | null | undefined): string {
  if (n == null) return "—";
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 * 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(1)} MB`;
  return `${(n / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

// avgFileBytes is the mean size of a table's Delta data files —
// total_bytes / file_count. Null when either metric is missing or zero
// (a table with no files, or a snapshot the provider couldn't size), so
// callers render "—" rather than NaN / Infinity.
export function avgFileBytes(
  fileCount: number | null | undefined,
  totalBytes: number | null | undefined,
): number | null {
  if (fileCount == null || totalBytes == null) return null;
  if (fileCount <= 0 || totalBytes <= 0) return null;
  return totalBytes / fileCount;
}

// The small-file threshold: a table earns the "small files" warning only
// when it has at least 16 data files AND averages under 16 MB per file.
// Both bounds are deliberately conservative. Delta's default target file
// size is 128 MB and the read-request cost that motivates GH #26 (one S3
// GET per file per scan) only bites once a table has fanned out into many
// small files — a 1-2 file demo table that happens to be tiny is not a
// problem worth alarming on, so the file-count floor suppresses it. 16 MB
// is a full order of magnitude under the 128 MB target, so a table has to
// be genuinely fragmented (not just slightly-below-target) to trip it.
const SMALL_FILE_MIN_COUNT = 16;
const SMALL_FILE_AVG_BYTES = 16 * 1024 * 1024; // 16 MB

export function fileHealth(
  fileCount: number | null | undefined,
  totalBytes: number | null | undefined,
): "ok" | "small" | "unknown" {
  const avg = avgFileBytes(fileCount, totalBytes);
  if (fileCount == null || totalBytes == null || avg == null) return "unknown";
  if (fileCount >= SMALL_FILE_MIN_COUNT && avg < SMALL_FILE_AVG_BYTES) {
    return "small";
  }
  return "ok";
}
