/**
 * Widget — dispatch one widget spec to its renderer.
 *
 * Owns the data fetch (`useDashboardQuery`) and routes the result to a
 * type-specific renderer (BigNumber / Line / Bar / Table). Unknown
 * widget types render as an empty shell with a friendly hint, so adding
 * new widget types in JSON doesn't break old UI builds.
 */

import { useMemo } from "react";
import {
  BarChart,
  Bar,
  CartesianGrid,
  Cell,
  ComposedChart,
  Legend,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Tooltip as UITooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  useDashboardQuery,
  type DashboardWidget,
  type DashboardQueryResult,
} from "@/lib/queries";
import { formatRowCount } from "@/lib/format";

import { WidgetShell } from "./WidgetShell";
import { WorldMap } from "./WorldMap";

interface WidgetProps {
  widget: DashboardWidget;
  /**
   * SQL and pipeline dir resolved from the widget's bound dataset. Both
   * empty when `widget.dataset` doesn't resolve to a dataset on the
   * dashboard — the widget renders a "dataset not found" hint. The dir
   * scopes the Provider dispatch (ADR-014 parity); two widgets bound to
   * the same dataset share one query execution (the hook's cache key is
   * `[dir, sql, params]`).
   */
  sql: string;
  dir: string;
  /**
   * Current dashboard control values, substituted into `{{name}}`
   * placeholders in the dataset SQL on the backend. Undefined for
   * dashboards without controls — keeps the cache key shape unchanged.
   */
  params?: Record<string, string>;
  /** Rendered inside the editor's drag/resize grid — fills the cell. */
  inGrid?: boolean;
}

// Widget types we actually know how to render. Unknown types skip the
// query entirely — no point hitting Athena for a widget that won't draw.
const KNOWN_TYPES = new Set([
  "big_number",
  "line",
  "bar",
  "stacked_bar",
  "bar_line",
  "pie",
  "donut",
  "table",
  "world_map",
]);

export function Widget({ widget, sql, dir, params, inGrid = false }: WidgetProps) {
  const known = KNOWN_TYPES.has(widget.type);
  // A widget whose `dataset` doesn't resolve gets no SQL — surface that
  // as its own hint rather than firing an empty query.
  const datasetMissing = known && !sql;
  // useDashboardQuery's `enabled` gate keeps the request from firing for
  // unknown types / unresolved datasets. The hook still returns
  // `data === undefined` so the shell renders the hint.
  const query = useDashboardQuery(known && sql ? sql : "", dir, params);
  const data = query.data;
  const isEmpty = !!data && data.rows.length === 0;
  // For unknown types we skip the loading/empty states and go straight
  // to the hint — otherwise the shell would forever show the empty
  // state (no query → no rows).
  if (!known) {
    return (
      <WidgetShell
        title={widget.title}
        layout={widget.layout}
        isLoading={false}
        error={null}
        isEmpty={false}
        inGrid={inGrid}
      >
        <UnknownTypeHint type={widget.type} />
      </WidgetShell>
    );
  }

  if (datasetMissing) {
    return (
      <WidgetShell
        title={widget.title}
        layout={widget.layout}
        isLoading={false}
        error={null}
        isEmpty={false}
        inGrid={inGrid}
      >
        <DatasetMissingHint dataset={widget.dataset} />
      </WidgetShell>
    );
  }

  const headerExtra =
    data && data.rows.length > 0
      ? `${formatRowCount(data.rows.length)}${data.truncated ? "+" : ""}`
      : null;

  return (
    <WidgetShell
      title={widget.title}
      layout={widget.layout}
      isLoading={query.isLoading}
      error={query.error}
      isEmpty={isEmpty}
      headerExtra={headerExtra}
      inGrid={inGrid}
    >
      {data && data.rows.length > 0 && (
        <WidgetBody widget={widget} data={data} />
      )}
    </WidgetShell>
  );
}

interface WidgetBodyProps {
  widget: DashboardWidget;
  data: DashboardQueryResult;
}

function WidgetBody({ widget, data }: WidgetBodyProps) {
  switch (widget.type) {
    case "big_number":
      return <BigNumberBody widget={widget} data={data} />;
    case "line":
      return <LineBody widget={widget} data={data} />;
    case "bar":
      return <BarBody widget={widget} data={data} />;
    case "stacked_bar":
      return <StackedBarBody widget={widget} data={data} />;
    case "bar_line":
      return <BarLineBody widget={widget} data={data} />;
    case "pie":
      return <PieBody widget={widget} data={data} variant="pie" />;
    case "donut":
      return <PieBody widget={widget} data={data} variant="donut" />;
    case "table":
      return <TableBody2 data={data} />;
    case "world_map":
      return (
        <WorldMap
          data={data}
          regionField={widget.region_field ?? ""}
          valueField={widget.value_field ?? ""}
          tooltipField={widget.tooltip_field ?? ""}
        />
      );
    default:
      return <UnknownTypeHint type={widget.type} />;
  }
}

function UnknownTypeHint({ type }: { type: string }) {
  return (
    <div className="flex flex-1 items-center justify-center p-4 text-xs text-muted-foreground">
      Unknown widget type:{" "}
      <code className="ml-1 font-mono">{type}</code>
    </div>
  );
}

function DatasetMissingHint({ dataset }: { dataset: string }) {
  return (
    <div className="flex flex-1 items-center justify-center p-4 text-center text-xs text-muted-foreground">
      {dataset ? (
        <>
          No dataset named{" "}
          <code className="mx-1 font-mono">{dataset}</code> on this dashboard
        </>
      ) : (
        "Widget is not bound to a dataset"
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// big_number — single SUM/COUNT result rendered as a large stat
// ---------------------------------------------------------------------------

function BigNumberBody({ widget, data }: WidgetBodyProps) {
  const valueIdx = pickColumn(data, widget.value_field, 0);
  if (valueIdx < 0) return <NoFieldsHint />;
  const raw = data.rows[0]?.[valueIdx] ?? "";
  if (raw === "") {
    return (
      <div className="flex flex-1 items-center justify-center p-6">
        <span className="font-mono text-4xl font-semibold text-muted-foreground">
          —
        </span>
      </div>
    );
  }
  // Display compact (79.46M); reveal the exact, separator-grouped value
  // on hover — the same axis/tooltip split the charts use.
  const display = compactNumber(raw);
  const exact = formatNumber(raw);
  const stat = (
    <span className="font-mono text-4xl font-semibold tabular-nums tracking-tight">
      {display}
    </span>
  );
  return (
    <div className="flex flex-1 items-center justify-center p-6">
      {display === exact ? (
        stat
      ) : (
        <UITooltip>
          <TooltipTrigger asChild>
            <span className="cursor-default">{stat}</span>
          </TooltipTrigger>
          <TooltipContent className="font-mono tabular-nums">
            {exact}
          </TooltipContent>
        </UITooltip>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// line — time-series-ish; x is whatever the user picks (timestamp, ordinal)
// ---------------------------------------------------------------------------

function LineBody({ widget, data }: WidgetBodyProps) {
  const primary = usePrimaryColor();
  const xIdx = pickColumn(data, widget.x_field, 0);
  const yIdx = pickColumn(data, widget.y_field, 1);
  if (xIdx < 0 || yIdx < 0) return <NoFieldsHint />;

  // Recharts wants `[{ x, y }]` with numeric y. Coerce strings to numbers
  // here; non-numeric rows render as gaps, no error. `name` on the series
  // is the real column name, so the tooltip reads `trips: 42` not `y: 42`.
  const yName = data.columns[yIdx]?.name || "value";
  const xName = data.columns[xIdx]?.name || "x";
  const points = data.rows.map((row) => ({
    x: row[xIdx],
    y: Number(row[yIdx]),
  }));
  return (
    <ChartFrame>
      <LineChart data={points} margin={{ top: 8, right: 12, left: 4, bottom: 4 }}>
        <CartesianGrid stroke="rgb(255 255 255 / 0.06)" vertical={false} />
        <XAxis
          dataKey="x"
          tick={{ fill: "currentColor", fontSize: 10 }}
          stroke="currentColor"
          tickLine={false}
          axisLine={false}
          minTickGap={24}
        />
        <YAxis
          tick={{ fill: "currentColor", fontSize: 10 }}
          stroke="currentColor"
          tickLine={false}
          axisLine={false}
          width={48}
          tickFormatter={compactNumber}
        />
        <Tooltip
          {...tooltipBase}
          cursor={{ stroke: "rgb(255 255 255 / 0.25)" }}
          formatter={(v) => formatNumber(v)}
          labelFormatter={(label) => `${xName}: ${label}`}
        />
        <Line
          type="monotone"
          dataKey="y"
          name={yName}
          stroke={primary}
          strokeWidth={1.5}
          dot={{ r: 2, fill: primary }}
          isAnimationActive={false}
        />
      </LineChart>
    </ChartFrame>
  );
}

// ---------------------------------------------------------------------------
// bar — categorical x, numeric y
// ---------------------------------------------------------------------------

function BarBody({ widget, data }: WidgetBodyProps) {
  const primary = usePrimaryColor();
  const xIdx = pickColumn(data, widget.x_field, 0);
  const yIdx = pickColumn(data, widget.y_field, 1);
  if (xIdx < 0 || yIdx < 0) return <NoFieldsHint />;

  const yName = data.columns[yIdx]?.name || "value";
  const xName = data.columns[xIdx]?.name || "x";
  const bars = data.rows.map((row) => ({
    x: row[xIdx],
    y: Number(row[yIdx]),
  }));
  return (
    <ChartFrame>
      <BarChart data={bars} margin={{ top: 8, right: 12, left: 4, bottom: 4 }}>
        <CartesianGrid stroke="rgb(255 255 255 / 0.06)" vertical={false} />
        <XAxis
          dataKey="x"
          tick={{ fill: "currentColor", fontSize: 10 }}
          stroke="currentColor"
          tickLine={false}
          axisLine={false}
        />
        <YAxis
          tick={{ fill: "currentColor", fontSize: 10 }}
          stroke="currentColor"
          tickLine={false}
          axisLine={false}
          width={48}
          tickFormatter={compactNumber}
        />
        <Tooltip
          {...tooltipBase}
          cursor={{ fill: "rgb(255 255 255 / 0.06)" }}
          formatter={(v) => formatNumber(v)}
          labelFormatter={(label) => `${xName}: ${label}`}
        />
        <Bar dataKey="y" name={yName} fill={primary} isAnimationActive={false} />
      </BarChart>
    </ChartFrame>
  );
}

// ---------------------------------------------------------------------------
// stacked_bar — several value columns stacked per x
// ---------------------------------------------------------------------------

// SERIES_PALETTE colors the segments of a stacked bar / the line of a
// combo. Recharts needs literal colors (it writes SVG fill/stroke
// attributes, which don't resolve CSS var()), so these are fixed and
// chosen to stay distinct on the dark theme.
const SERIES_PALETTE = [
  "#3b82f6", // blue
  "#f59e0b", // amber
  "#10b981", // emerald
  "#ef4444", // red
  "#a855f7", // purple
  "#06b6d4", // cyan
  "#ec4899", // pink
  "#84cc16", // lime
];

// StackedBarBody stacks the chosen value columns into one bar per x.
// Wide format: the SQL returns one row per x with a column per series
// (`SELECT cat, SUM(a), SUM(b), SUM(c) GROUP BY cat`). With no series
// columns picked it stacks every column except x.
function StackedBarBody({ widget, data }: WidgetBodyProps) {
  const xIdx = pickColumn(data, widget.x_field, 0);
  if (xIdx < 0) return <NoFieldsHint />;

  const chosen = widget.series_fields.filter((s) =>
    data.columns.some((c) => c.name === s),
  );
  const seriesNames =
    chosen.length > 0
      ? chosen
      : data.columns.map((c) => c.name).filter((_, i) => i !== xIdx);
  if (seriesNames.length === 0) return <NoFieldsHint />;

  const xName = data.columns[xIdx]?.name || "x";
  const points = data.rows.map((row) => {
    const rec: Record<string, string | number> = { __x: row[xIdx] };
    for (const s of seriesNames) {
      const ci = data.columns.findIndex((c) => c.name === s);
      rec[s] = Number(row[ci]);
    }
    return rec;
  });

  return (
    <ChartFrame>
      <BarChart data={points} margin={{ top: 8, right: 12, left: 4, bottom: 4 }}>
        <CartesianGrid stroke="rgb(255 255 255 / 0.06)" vertical={false} />
        <XAxis
          dataKey="__x"
          tick={{ fill: "currentColor", fontSize: 10 }}
          stroke="currentColor"
          tickLine={false}
          axisLine={false}
        />
        <YAxis
          tick={{ fill: "currentColor", fontSize: 10 }}
          stroke="currentColor"
          tickLine={false}
          axisLine={false}
          width={48}
          tickFormatter={compactNumber}
        />
        <Tooltip
          {...tooltipBase}
          cursor={{ fill: "rgb(255 255 255 / 0.06)" }}
          formatter={(v) => formatNumber(v)}
          labelFormatter={(label) => `${xName}: ${label}`}
        />
        <Legend wrapperStyle={{ fontSize: 11 }} />
        {seriesNames.map((s, i) => (
          <Bar
            key={s}
            dataKey={s}
            name={s}
            stackId="a"
            fill={SERIES_PALETTE[i % SERIES_PALETTE.length]}
            isAnimationActive={false}
          />
        ))}
      </BarChart>
    </ChartFrame>
  );
}

// ---------------------------------------------------------------------------
// pie / donut — categorical share-of-total
// ---------------------------------------------------------------------------

// PIE_MAX_SLICES caps how many distinct categories render as their own
// slice; the rest collapse into a single "Other" so a long-tail pie
// stays legible. Picked at 7 so an 8th category triggers the rollup —
// any more than that is unreadable anyway.
const PIE_MAX_SLICES = 7;

interface PieBodyProps extends WidgetBodyProps {
  variant: "pie" | "donut";
}

function PieBody({ widget, data, variant }: PieBodyProps) {
  const nameIdx = pickColumn(data, widget.x_field, 0);
  const valueIdx = pickColumn(data, widget.value_field, 1);
  if (nameIdx < 0 || valueIdx < 0) {
    return (
      <div className="flex flex-1 items-center justify-center p-4 text-xs text-muted-foreground">
        Pie needs `x_field` (category) and `value_field` (numeric) to render
      </div>
    );
  }

  // Numeric-coerce values; drop rows whose value isn't a number (Recharts
  // would draw zero-sized slices for them).
  const all = data.rows
    .map((row) => ({
      name: String(row[nameIdx] ?? ""),
      value: Number(row[valueIdx]),
    }))
    .filter((p) => Number.isFinite(p.value) && p.value > 0);
  if (all.length === 0) {
    return (
      <div className="flex flex-1 items-center justify-center p-4 text-xs text-muted-foreground">
        No positive values to chart
      </div>
    );
  }

  // Top-N by value; collapse the long tail into "Other" so the chart
  // stays legible. The "Other" slice is rendered in muted gray so the
  // eye treats it as a residual, not another category.
  const sorted = [...all].sort((a, b) => b.value - a.value);
  let slices = sorted;
  if (sorted.length > PIE_MAX_SLICES) {
    const top = sorted.slice(0, PIE_MAX_SLICES);
    const otherValue = sorted
      .slice(PIE_MAX_SLICES)
      .reduce((s, p) => s + p.value, 0);
    slices = [...top, { name: "Other", value: otherValue }];
  }

  const innerRadius = variant === "donut" ? "55%" : 0;
  const outerRadius = "80%";
  const OTHER_COLOR = "rgb(120 120 120)";

  return (
    <ChartFrame>
      <PieChart>
        <Pie
          data={slices}
          dataKey="value"
          nameKey="name"
          innerRadius={innerRadius}
          outerRadius={outerRadius}
          isAnimationActive={false}
          stroke="rgb(20 20 22)"
          strokeWidth={1}
        >
          {slices.map((s, i) => (
            <Cell
              key={`${s.name}-${i}`}
              fill={
                s.name === "Other"
                  ? OTHER_COLOR
                  : SERIES_PALETTE[i % SERIES_PALETTE.length]
              }
            />
          ))}
        </Pie>
        <Tooltip
          {...tooltipBase}
          formatter={(v: unknown) => formatNumber(v)}
        />
        <Legend wrapperStyle={{ fontSize: 11 }} />
      </PieChart>
    </ChartFrame>
  );
}

// ---------------------------------------------------------------------------
// bar_line — a bar metric and a line metric sharing one x, dual y-axis
// ---------------------------------------------------------------------------

function BarLineBody({ widget, data }: WidgetBodyProps) {
  const primary = usePrimaryColor();
  const xIdx = pickColumn(data, widget.x_field, 0);
  const barIdx = pickColumn(data, widget.y_field, 1);
  const lineIdx = pickColumn(data, widget.line_field, 2);
  if (xIdx < 0 || barIdx < 0 || lineIdx < 0) return <NoFieldsHint />;

  const barName = data.columns[barIdx]?.name || "bar";
  const lineName = data.columns[lineIdx]?.name || "line";
  const xName = data.columns[xIdx]?.name || "x";
  const lineColor = SERIES_PALETTE[1];
  const points = data.rows.map((row) => ({
    x: row[xIdx],
    bar: Number(row[barIdx]),
    line: Number(row[lineIdx]),
  }));

  return (
    <ChartFrame>
      <ComposedChart
        data={points}
        margin={{ top: 8, right: 8, left: 4, bottom: 4 }}
      >
        <CartesianGrid stroke="rgb(255 255 255 / 0.06)" vertical={false} />
        <XAxis
          dataKey="x"
          tick={{ fill: "currentColor", fontSize: 10 }}
          stroke="currentColor"
          tickLine={false}
          axisLine={false}
          minTickGap={24}
        />
        <YAxis
          yAxisId="bar"
          tick={{ fill: "currentColor", fontSize: 10 }}
          stroke="currentColor"
          tickLine={false}
          axisLine={false}
          width={44}
          tickFormatter={compactNumber}
        />
        <YAxis
          yAxisId="line"
          orientation="right"
          tick={{ fill: lineColor, fontSize: 10 }}
          stroke={lineColor}
          tickLine={false}
          axisLine={false}
          width={44}
          tickFormatter={compactNumber}
        />
        <Tooltip
          {...tooltipBase}
          cursor={{ fill: "rgb(255 255 255 / 0.06)" }}
          formatter={(v) => formatNumber(v)}
          labelFormatter={(label) => `${xName}: ${label}`}
        />
        <Legend wrapperStyle={{ fontSize: 11 }} />
        <Bar
          yAxisId="bar"
          dataKey="bar"
          name={barName}
          fill={primary}
          isAnimationActive={false}
        />
        <Line
          yAxisId="line"
          type="monotone"
          dataKey="line"
          name={lineName}
          stroke={lineColor}
          strokeWidth={1.5}
          dot={{ r: 2, fill: lineColor }}
          isAnimationActive={false}
        />
      </ComposedChart>
    </ChartFrame>
  );
}

// ---------------------------------------------------------------------------
// table — display every column the SQL returned
// ---------------------------------------------------------------------------

interface TableBodyProps {
  data: DashboardQueryResult;
}

function TableBody2({ data }: TableBodyProps) {
  return (
    <div className="flex-1 overflow-auto">
      <Table>
        <TableHeader className="sticky top-0 z-10 bg-card">
          <TableRow className="hover:bg-transparent">
            {data.columns.map((c) => (
              <TableHead key={c.name} className="whitespace-nowrap font-mono text-xs">
                {c.name}
              </TableHead>
            ))}
          </TableRow>
        </TableHeader>
        <TableBody>
          {data.rows.map((row, i) => (
            <TableRow key={i} data-testid="recent-runs-row">
              {row.map((cell, j) => (
                <TableCell key={j} className="whitespace-nowrap font-mono text-xs">
                  {cell === "" || cell == null ? (
                    <span className="text-muted-foreground/50">—</span>
                  ) : (
                    cell
                  )}
                </TableCell>
              ))}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Shared bits
// ---------------------------------------------------------------------------

// usePrimaryColor resolves the theme's `--primary` token to a literal
// color string. Recharts writes `fill` / `stroke` as SVG presentation
// attributes, and those do not resolve CSS `var()` — passing
// `hsl(var(--primary))` (or any `var()`) leaves a bar default-black and a
// line with no stroke at all. getComputedStyle gives the resolved value
// (an `oklch(...)` literal here), which is a valid SVG attribute color.
function usePrimaryColor(): string {
  return useMemo(() => {
    if (typeof document === "undefined") return "#2563eb"; // blue-600 fallback
    const v = getComputedStyle(document.documentElement)
      .getPropertyValue("--primary")
      .trim();
    return v || "#2563eb";
  }, []);
}

// compactNumber formats an axis tick so a wide value fits a narrow axis:
// 60000000 → "60M", 19600 → "19.6K". Non-numeric ticks pass through.
const compactFmt = new Intl.NumberFormat("en", {
  notation: "compact",
  maximumFractionDigits: 2,
});
function compactNumber(v: unknown): string {
  const n = Number(v);
  return Number.isFinite(n) ? compactFmt.format(n) : String(v ?? "");
}

// formatNumber renders a tooltip value human-readable — thousands
// separators, at most two decimals (65533599.31 → "65,533,599.31").
// Non-numeric or blank values pass through unchanged.
function formatNumber(v: unknown): string {
  const s = String(v ?? "");
  const n = Number(v);
  return s.trim() !== "" && Number.isFinite(n)
    ? n.toLocaleString("en-US", { maximumFractionDigits: 2 })
    : s;
}

// tooltipBase styles the recharts tooltip for the dark theme. The default
// tooltip box is white with black text; contentStyle + labelStyle fix
// that. The hover `cursor` (white by default — a bright block behind the
// hovered bar) is set per chart by the caller.
const tooltipBase = {
  contentStyle: {
    background: "rgb(20 20 22)",
    border: "1px solid rgb(255 255 255 / 0.1)",
    borderRadius: 6,
    fontSize: 12,
  },
  labelStyle: { color: "rgb(255 255 255 / 0.55)" },
};

function ChartFrame({ children }: { children: React.ReactElement }) {
  return (
    <div className="flex-1 px-2 pb-2 text-muted-foreground">
      <ResponsiveContainer width="100%" height="100%">
        {children}
      </ResponsiveContainer>
    </div>
  );
}

function NoFieldsHint() {
  return (
    <div className="flex flex-1 items-center justify-center p-4 text-xs text-muted-foreground">
      Widget needs `x_field` / `y_field` (or `value_field`) to render
    </div>
  );
}

// pickColumn returns the column index for a name-or-fallback lookup.
// Caller passes the user-declared field name (e.g. `widget.value_field`)
// and a default ordinal (e.g. 0 for the first column). Empty/missing
// names fall back to the ordinal; missing-by-name returns -1 so the
// widget can render a hint instead of guessing wrong.
function pickColumn(
  data: DashboardQueryResult,
  name: string,
  fallback: number,
): number {
  if (name) {
    const idx = data.columns.findIndex((c) => c.name === name);
    return idx;
  }
  if (fallback < data.columns.length) return fallback;
  return -1;
}
