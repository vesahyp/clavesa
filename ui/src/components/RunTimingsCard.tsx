/**
 * RunTimingsCard — "Where the time goes": one stacked bar per run (last 20,
 * oldest left), minutes per phase series. Lazy-loaded by PipelineDashboard so
 * recharts stays out of the eager bundle.
 */

import { useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { useRunTimings } from "@/lib/queries";
import { runSeries, SERIES, type SeriesKey } from "@/lib/runTiming";

const MIN = 60_000;
const CARD_BG = "var(--card)";

function stamp(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  const p = (n: number) => String(n).padStart(2, "0");
  return `${p(d.getDate())}.${p(d.getMonth() + 1)} ${p(d.getHours())}:${p(d.getMinutes())}`;
}

interface Row {
  label: string;
  run_id: string;
  status: string;
  failed: boolean;
  totalMin: number;
  [k: string]: string | number | boolean;
}

// Row field for a series. recharts spreads each row onto the bar's <path>,
// so a bare "transform" key would become the SVG transform attribute; the
// prefix keeps every series value out of the DOM.
function dk(key: string): string {
  return "v_" + key;
}

function fmtMin(v: number): string {
  return v.toFixed(1);
}

// Draws the segment, plus a red triangle above the bar when the run failed.
// Attached to the top series (bookkeeping) so the marker sits on the stack.
function TopShape(props: {
  x?: number;
  y?: number;
  width?: number;
  height?: number;
  fill?: string;
  payload?: Row;
}) {
  const { x = 0, y = 0, width = 0, height = 0, fill, payload } = props;
  const cx = x + width / 2;
  return (
    <g>
      <rect x={x} y={y} width={width} height={height} fill={fill} stroke={CARD_BG} strokeWidth={2} />
      {payload?.failed && (
        <polygon
          data-testid="run-timings-failed-marker"
          points={`${cx - 4},${y - 10} ${cx + 4},${y - 10} ${cx},${y - 3}`}
          fill="var(--status-failed)"
        />
      )}
    </g>
  );
}

function ChartTooltip({
  active,
  payload,
}: {
  active?: boolean;
  payload?: { payload: Row }[];
}) {
  if (!active || !payload?.length) return null;
  const row = payload[0].payload;
  const segs = SERIES.filter((s) => Number(row[dk(s.key)]) > 0);
  return (
    <div className="rounded-md border border-border bg-popover px-3 py-2 text-xs shadow-md">
      <div className="mb-1 flex items-center gap-2">
        <span className="font-medium">{row.label}</span>
        {row.failed && (
          <Badge variant="destructive" className="text-[10px]">
            {row.status}
          </Badge>
        )}
        {!row.failed && <span className="text-muted-foreground">{row.status}</span>}
      </div>
      <div className="mb-1 text-muted-foreground">total {fmtMin(row.totalMin)} min</div>
      {segs.map((s) => {
        const v = Number(row[dk(s.key)]);
        const pct = row.totalMin > 0 ? (v / row.totalMin) * 100 : 0;
        return (
          <div key={s.key} className="flex items-center justify-between gap-4 tabular-nums">
            <span className="flex items-center gap-1.5">
              <span className="inline-block h-2 w-2 rounded-sm" style={{ background: s.color }} />
              {s.key}
            </span>
            <span>
              {fmtMin(v)} min · {pct.toFixed(0)}%
            </span>
          </div>
        );
      })}
    </div>
  );
}

export default function RunTimingsCard({ dir }: { dir: string }) {
  const q = useRunTimings(dir, 20);
  const [asTable, setAsTable] = useState(false);

  const rows: Row[] = useMemo(
    () =>
      (q.data?.runs ?? []).map((r) => {
        const s = runSeries(r);
        const row: Row = {
          label: stamp(r.started_at),
          run_id: r.run_id,
          status: r.status,
          failed: r.status === "FAILED",
          totalMin: r.total_ms / MIN,
        };
        for (const k of Object.keys(s) as SeriesKey[]) row[dk(k)] = s[k] / MIN;
        return row;
      }),
    [q.data],
  );

  // Hide the "no phase data" series from the legend when no run uses it.
  const series = SERIES.filter(
    (s) => s.key !== "nodes (no phase data yet)" || rows.some((r) => Number(r[dk(s.key)]) > 0),
  );
  const topKey = series[series.length - 1].key;

  return (
    <Card data-testid="run-timings-card">
      <CardHeader className="flex-row items-center justify-between pb-3">
        <CardTitle>Where the time goes</CardTitle>
        <Button
          type="button"
          variant="outline"
          size="sm"
          data-testid="run-timings-toggle"
          onClick={() => setAsTable((v) => !v)}
        >
          {asTable ? "Chart" : "Table"}
        </Button>
      </CardHeader>
      <CardContent className={`p-6 pt-0 ${q.isLoading ? "opacity-50" : ""}`}>
        {q.isError ? (
          <div className="text-xs text-muted-foreground">
            Couldn't load run timings for this pipeline.
          </div>
        ) : !q.isLoading && rows.length === 0 ? (
          <div className="text-xs text-muted-foreground" data-testid="run-timings-empty">
            No runs yet.
          </div>
        ) : asTable ? (
          <div className="overflow-x-auto rounded-md border border-border" data-testid="run-timings-table">
            <table className="w-full text-sm">
              <thead className="bg-muted/50 text-xs text-muted-foreground">
                <tr>
                  <th className="px-3 py-2 text-left font-medium">Started</th>
                  <th className="px-3 py-2 text-left font-medium">Status</th>
                  <th className="px-3 py-2 text-right font-medium">Total (min)</th>
                  {series.map((s) => (
                    <th key={s.key} className="px-3 py-2 text-right font-medium">
                      {s.key}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {[...rows].reverse().map((r) => (
                  <tr key={r.run_id}>
                    <td className="px-3 py-2 tabular-nums">{r.label}</td>
                    <td className="px-3 py-2">{r.status}</td>
                    <td className="px-3 py-2 text-right tabular-nums">{fmtMin(r.totalMin)}</td>
                    {series.map((s) => (
                      <td key={s.key} className="px-3 py-2 text-right tabular-nums">
                        {fmtMin(Number(r[dk(s.key)]))}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="h-72 w-full text-muted-foreground" data-testid="run-timings-chart" data-point-count={rows.length}>
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={rows} margin={{ top: 14, right: 8, bottom: 0, left: 0 }} barCategoryGap="30%">
                <CartesianGrid vertical={false} stroke="currentColor" strokeOpacity={0.12} strokeWidth={0.5} />
                <XAxis dataKey="label" tick={{ fontSize: 10, fill: "currentColor" }} tickLine={false} axisLine={false} />
                <YAxis
                  tick={{ fontSize: 10, fill: "currentColor" }}
                  tickLine={false}
                  axisLine={false}
                  width={44}
                  unit=" min"
                />
                <Tooltip content={<ChartTooltip />} cursor={{ fill: "currentColor", fillOpacity: 0.06 }} />
                {/* itemSorter=null keeps the fixed series order; recharts 3 sorts the
                    legend alphabetically by default. */}
                <Legend wrapperStyle={{ fontSize: 11 }} iconType="square" itemSorter={null} />
                {series.map((s) => (
                  <Bar
                    key={s.key}
                    dataKey={dk(s.key)}
                    name={s.key}
                    stackId="t"
                    fill={s.color}
                    stroke={CARD_BG}
                    strokeWidth={2}
                    maxBarSize={18}
                    isAnimationActive={false}
                    shape={s.key === topKey ? (TopShape as never) : undefined}
                  />
                ))}
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
