/**
 * runStatus — shared status helpers for the run surfaces (PipelineDashboard
 * runs grid + health header, RunDetail).
 *
 * Two status vocabularies coexist: the `runs` table carries UPPERCASE
 * Step-Functions-style statuses (SUCCEEDED / FAILED / …); the `node_runs`
 * table carries lowercase runner statuses (ok / failed / skipped). One
 * mapper each, plus the topological node order both surfaces sort by.
 */

export type StatusVariant = "success" | "failed" | "running" | "outline";

/** Pipeline health verdict shown in the dashboard header. */
export type Health = "healthy" | "failed" | "running" | "never-run" | "unknown";

const RUN_TERMINAL = new Set(["SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"]);
const RUN_FAILED = new Set(["FAILED", "TIMED_OUT", "ABORTED"]);

/** A runs-table status (UPPERCASE) → Badge variant. */
export function runVariant(status: string): StatusVariant {
  if (status === "SUCCEEDED") return "success";
  if (RUN_FAILED.has(status)) return "failed";
  if (status === "RUNNING") return "running";
  return "outline";
}

/** A node_runs-table status (lowercase) → Badge variant. */
export function nodeVariant(status: string): StatusVariant {
  if (status === "ok") return "success";
  if (status === "failed") return "failed";
  if (status === "running") return "running";
  return "outline"; // skipped / unknown
}

/** The colors a PipelineGraph node can carry. */
export type NodeRunColor = "running" | "succeeded" | "failed";

/**
 * A live execution-channel per-node status (UPPERCASE: RUNNING / PENDING
 * / SUCCEEDED / OK / FAILED) → DAG node color. null for PENDING / unknown
 * — the node renders uncolored.
 */
export function liveNodeColor(status: string): NodeRunColor | null {
  if (status === "RUNNING") return "running";
  if (status === "SUCCEEDED" || status === "OK") return "succeeded";
  if (status === "FAILED") return "failed";
  return null;
}

export function runIsTerminal(status: string): boolean {
  return RUN_TERMINAL.has(status);
}

export function runIsFailed(status: string): boolean {
  return RUN_FAILED.has(status);
}

/**
 * topoOrder — pipeline node ids in dependency order (upstream first) via
 * Kahn's algorithm. Branching DAGs have many valid orders, so the ready
 * set is drained in declaration order for a deterministic result; nodes
 * left over (a cycle) are appended in declaration order.
 */
export function topoOrder(
  nodeIds: string[],
  edges: ReadonlyArray<{ from_node: string; to_node: string }>,
): string[] {
  const idx = new Map(nodeIds.map((id, i) => [id, i]));
  const indeg = new Map(nodeIds.map((id) => [id, 0]));
  const adj = new Map<string, string[]>();
  for (const e of edges) {
    if (!idx.has(e.from_node) || !idx.has(e.to_node)) continue;
    adj.set(e.from_node, [...(adj.get(e.from_node) ?? []), e.to_node]);
    indeg.set(e.to_node, (indeg.get(e.to_node) ?? 0) + 1);
  }
  const ready = nodeIds.filter((id) => (indeg.get(id) ?? 0) === 0);
  const out: string[] = [];
  const seen = new Set<string>();
  while (ready.length > 0) {
    ready.sort((a, b) => (idx.get(a) ?? 0) - (idx.get(b) ?? 0));
    const n = ready.shift()!;
    if (seen.has(n)) continue;
    seen.add(n);
    out.push(n);
    for (const m of adj.get(n) ?? []) {
      indeg.set(m, (indeg.get(m) ?? 0) - 1);
      if ((indeg.get(m) ?? 0) === 0) ready.push(m);
    }
  }
  for (const id of nodeIds) if (!seen.has(id)) out.push(id);
  return out;
}
