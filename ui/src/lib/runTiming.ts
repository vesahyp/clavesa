/**
 * runTiming.ts — pure mapping from a run-timings record to the stacked
 * series the "Where the time goes" chart draws. Series order and colours
 * are fixed; a colour follows the series name, never its position.
 */

export interface NodeTiming {
  phases: Record<string, number>;
  inputs?: Record<string, number>;
  outputs?: Record<string, number>;
  spark?: {
    write?: {
      num_stages?: number;
      num_tasks?: number;
      input_bytes?: number;
      shuffle_read_bytes?: number;
      shuffle_write_bytes?: number;
    };
  };
}

export interface TimingNode {
  node: string;
  status?: string;
  duration_ms?: number | null;
  timing?: unknown;
}

export interface TimingRecord {
  run_id: string;
  status: string;
  started_at: string;
  total_ms: number;
  phases: Record<string, number>;
  nodes: TimingNode[];
}

export const SERIES = [
  { key: "startup", color: "#2a78d6" },
  { key: "metastore", color: "#eb6834" },
  { key: "spark", color: "#1baf7a" },
  { key: "reads", color: "#eda100" },
  { key: "transform", color: "#e87ba4" },
  { key: "writes", color: "#008300" },
  { key: "stats", color: "#4a3aa7" },
  { key: "nodes (no phase data yet)", color: "#5c6168" },
  { key: "bookkeeping", color: "#8a8f98" },
] as const;

export type SeriesKey = (typeof SERIES)[number]["key"];

const num = (v: unknown): number =>
  typeof v === "number" && Number.isFinite(v) ? v : 0;

/** Narrow an unknown node timing value (object or JSON text) to NodeTiming. */
export function parseNodeTiming(raw: unknown): NodeTiming | null {
  let v: unknown = raw;
  if (typeof v === "string") {
    if (!v.trim()) return null;
    try {
      v = JSON.parse(v);
    } catch {
      return null;
    }
  }
  if (!v || typeof v !== "object") return null;
  const o = v as Record<string, unknown>;
  if (!o.phases || typeof o.phases !== "object") return null;
  return o as unknown as NodeTiming;
}

/** Series values in ms for one run. All keys present, all >= 0. */
export function runSeries(rec: TimingRecord): Record<SeriesKey, number> {
  const p = rec.phases ?? {};
  const timings = (rec.nodes ?? [])
    .map((n) => parseNodeTiming(n.timing))
    .filter((t): t is NodeTiming => t !== null);
  const sum = (k: string) => timings.reduce((a, t) => a + num(t.phases[k]), 0);

  const startup =
    num(p.startup) + num(p.prepare) + num(p.lock) + num(p.image) + num(p.container) + num(p.boot);
  const metastore = num(p.metastore);
  const out: Record<SeriesKey, number> = {
    startup,
    metastore,
    spark: 0,
    reads: num(p.inputs),
    transform: 0,
    writes: 0,
    stats: 0,
    "nodes (no phase data yet)": 0,
    bookkeeping: 0,
  };
  if (timings.length > 0) {
    out.spark = sum("spark_session") + sum("catalog");
    out.reads += sum("inputs");
    out.transform = sum("transform");
    out.writes = sum("write");
    out.stats = sum("stats");
  } else {
    out["nodes (no phase data yet)"] = num(p.nodes);
  }
  const used =
    out.startup + out.metastore + out.spark + out.reads + out.transform +
    out.writes + out.stats + out["nodes (no phase data yet)"];
  out.bookkeeping = Math.max(0, num(rec.total_ms) - used);
  return out;
}

/** Phases of one node for the thin bar: inputs, transform, write, stats, other. */
export function nodePhaseSegments(
  t: NodeTiming,
  durationMs: number | null | undefined,
): { key: string; ms: number; color: string }[] {
  const inputs = num(t.phases.inputs);
  const transform = num(t.phases.transform);
  const write = num(t.phases.write);
  const stats = num(t.phases.stats);
  const other = Math.max(0, num(durationMs) - inputs - transform - write - stats);
  return [
    { key: "inputs", ms: inputs, color: "#eda100" },
    { key: "transform", ms: transform, color: "#e87ba4" },
    { key: "write", ms: write, color: "#008300" },
    { key: "stats", ms: stats, color: "#4a3aa7" },
    { key: "other", ms: other, color: "#8a8f98" },
  ];
}
