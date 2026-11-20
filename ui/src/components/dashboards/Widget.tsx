/**
 * Widget — dispatch one widget spec to its renderer.
 *
 * Owns the data fetch (`useDashboardQuery`) and routes the result to a
 * type-specific renderer (BigNumber / Line / Bar / Table). Unknown
 * widget types render as an empty shell with a friendly hint, so adding
 * new widget types in JSON doesn't break old UI builds.
 */

import { Fragment, useCallback, useId, useLayoutEffect, useMemo, useRef, useState } from "react";
import {
  Area,
  AreaChart,
  BarChart,
  Bar,
  CartesianGrid,
  Cell,
  ComposedChart,
  Line,
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
import { useIsNarrow } from "@/hooks/useIsPhone";
import { cn } from "@/lib/utils";

import {
  ChartTooltip,
  CURSOR_FILL,
  CURSOR_STROKE,
  gridProps,
  OTHER_COLOR,
  SERIES_PALETTE,
  SURFACE,
  InViewContext,
  inViewport,
  ToggleLegend,
  useChartMotion,
  useHiddenSeries,
  xAxisStyle,
  yAxisStyle,
  type LegendItem,
} from "./chartKit";

import { isIdLike, pieSlices, xFormat } from "./chartLogic";
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
  /** Place in reading order: the cards rise one after another. */
  order?: number;
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

export function Widget({ widget, sql, dir, params, inGrid = false, order = 0 }: WidgetProps) {
  // The card, so a chart can ask whether it is on screen (useChartMotion).
  const cardRef = useRef<HTMLDivElement>(null);
  const inView = useCallback(() => inViewport(cardRef.current), []);
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
        type={widget.type}
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
        type={widget.type}
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

  // A stat tile shows one value: its "1 row" says nothing and pushes the
  // title onto three lines in half a phone row.
  const headerExtra =
    data && data.rows.length > 0 && widget.type !== "big_number"
      ? `${formatRowCount(data.rows.length)}${data.truncated ? "+" : ""}`
      : null;

  return (
    <WidgetShell
      title={widget.title}
      type={widget.type}
      layout={widget.layout}
      isLoading={query.isLoading}
      error={query.error}
      isEmpty={isEmpty}
      headerExtra={headerExtra}
      inGrid={inGrid}
      order={order}
      cardRef={cardRef}
    >
      {data && data.rows.length > 0 && (
        // The previous result, shown while a control change runs: dimmed.
        <div
          className={cn(
            "flex min-h-0 flex-1 flex-col transition-opacity duration-300 motion-reduce:transition-none",
            query.isPlaceholderData && "opacity-50",
          )}
          aria-busy={query.isPlaceholderData || undefined}
          data-stale={query.isPlaceholderData ? "" : undefined}
          data-testid="widget-body"
        >
          <InViewContext.Provider value={inView}>
            <WidgetBody widget={widget} data={data} sql={sql} />
          </InViewContext.Provider>
        </div>
      )}
    </WidgetShell>
  );
}

interface WidgetBodyProps {
  widget: DashboardWidget;
  data: DashboardQueryResult;
  /** The dataset SQL, read only to name a series called y, x or value. */
  sql?: string;
}

function WidgetBody({ widget, data, sql }: WidgetBodyProps) {
  switch (widget.type) {
    case "big_number":
      return <BigNumberBody widget={widget} data={data} />;
    case "line":
      return <LineBody widget={widget} data={data} sql={sql} />;
    case "bar":
      return <BarBody widget={widget} data={data} sql={sql} />;
    case "stacked_bar":
      return <StackedBarBody widget={widget} data={data} sql={sql} />;
    case "bar_line":
      return <BarLineBody widget={widget} data={data} sql={sql} />;
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
      <div className="flex flex-1 items-end px-4 pb-4">
        <span className="text-3xl font-semibold text-muted-foreground">—</span>
      </div>
    );
  }
  // Display compact (79.46M); reveal the exact, separator-grouped value
  // on hover — the same axis/tooltip split the charts use.
  // "%" when the title or the field says percent, as on the chart axes.
  const pct = isPercent(widget.title, data.columns[valueIdx]?.name) ? "%" : "";
  const display = compactNumber(raw) + pct;
  const exact = formatNumber(raw) + pct;
  const stat = (
    <span
      className="text-[1.75rem] font-semibold leading-none tracking-tight text-foreground tabular-nums md:text-4xl"
      data-testid="stat-value"
    >
      {display}
    </span>
  );
  // Left-aligned at the foot of the tile, under its label: a row of tiles
  // reads as one line of numbers.
  return (
    <div className="flex flex-1 items-end px-4 pb-4 pt-1">
      {display === exact ? (
        stat
      ) : (
        <UITooltip>
          <TooltipTrigger asChild>
            <span className="cursor-default">{stat}</span>
          </TooltipTrigger>
          <TooltipContent className="tabular-nums">{exact}</TooltipContent>
        </UITooltip>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// line — time-series-ish; x is whatever the user picks (timestamp, ordinal)
// ---------------------------------------------------------------------------

// A line with more points than this draws no dot per point (a row of
// dots on 90 days is noise); the hovered point still gets one.
const DOTS_UP_TO = 31;

function LineBody({ widget, data, sql }: WidgetBodyProps) {
  const gradientId = `area-${useId().replace(/:/g, "")}`;
  const motion = useChartMotion(data);
  const xIdx = pickColumn(data, widget.x_field, 0);
  const yIdx = pickColumn(data, widget.y_field, 1);
  if (xIdx < 0 || yIdx < 0) return <NoFieldsHint />;
  // A line through a few points (3 days of key events) suggests values
  // between them that were never measured; under 8 points, bars.
  if (data.rows.length < SPARSE_POINTS) return <BarBody widget={widget} data={data} sql={sql} />;

  // Recharts wants `[{ x, y }]` with numeric y. Coerce strings to numbers
  // here; non-numeric rows render as gaps, no error. `name` on the series
  // is the real column name, so the tooltip reads `trips: 42` not `y: 42`.
  const yName = seriesLabel(data.columns[yIdx]?.name ?? "", sql, widget.title);
  const color = SERIES_PALETTE[0];
  const xf = xFormat(data.rows.map((row) => row[xIdx]));
  const points = data.rows.map((row) => ({
    x: row[xIdx],
    y: Number(row[yIdx]),
  }));
  // One series: no legend (the title names it), nothing to turn off.
  return (
    <ChartFrame pointCount={points.length} seriesTotal={1} seriesVisible={1}>
      <AreaChart data={points} margin={{ top: 8, right: 36, left: 0, bottom: 0 }}>
        <defs>
          <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity={0.28} />
            <stop offset="100%" stopColor={color} stopOpacity={0.02} />
          </linearGradient>
        </defs>
        <CartesianGrid {...gridProps} />
        <XAxis dataKey="x" {...xAxisStyle} tickFormatter={xf.tick} />
        <YAxis
          {...yAxisStyle}
          {...yAxisProps(points.map((p) => p.y), isPercent(widget.title, yName))}
        />
        <Tooltip
          cursor={{ stroke: CURSOR_STROKE, strokeDasharray: "3 3" }}
          content={(p) => <ChartTooltip {...p} format={formatNumber} labelFormat={xf.heading} />}
        />
        <Area
          type="linear"
          dataKey="y"
          name={yName}
          stroke={color}
          strokeWidth={2}
          fill={`url(#${gradientId})`}
          dot={points.length <= DOTS_UP_TO ? { r: 2.5, fill: color, strokeWidth: 0 } : false}
          activeDot={{ r: 4.5, fill: color, stroke: SURFACE, strokeWidth: 2 }}
          {...motion.line}
        />
      </AreaChart>
    </ChartFrame>
  );
}

// ---------------------------------------------------------------------------
// bar — categorical x, numeric y
// ---------------------------------------------------------------------------

function BarBody({ widget, data, sql }: WidgetBodyProps) {
  const motion = useChartMotion(data);
  const xIdx = pickColumn(data, widget.x_field, 0);
  const yIdx = pickColumn(data, widget.y_field, 1);
  if (xIdx < 0 || yIdx < 0) return <NoFieldsHint />;

  const yName = seriesLabel(data.columns[yIdx]?.name ?? "", sql, widget.title);
  const xf = xFormat(data.rows.map((row) => row[xIdx]));
  const bars = data.rows.map((row) => ({
    x: row[xIdx],
    y: Number(row[yIdx]),
  }));
  return (
    <ChartFrame pointCount={bars.length} seriesTotal={1} seriesVisible={1}>
      <BarChart data={bars} margin={{ top: 8, right: 36, left: 0, bottom: 0 }}>
        <CartesianGrid {...gridProps} />
        <XAxis dataKey="x" {...xAxisStyle} tickFormatter={xf.tick} />
        <YAxis
          {...yAxisStyle}
          {...yAxisProps(bars.map((b) => b.y), isPercent(widget.title, yName))}
        />
        <Tooltip
          cursor={{ fill: CURSOR_FILL }}
          content={(p) => <ChartTooltip {...p} format={formatNumber} labelFormat={xf.heading} />}
        />
        <Bar
          dataKey="y"
          name={yName}
          fill={SERIES_PALETTE[0]}
          radius={[4, 4, 0, 0]}
          maxBarSize={56}
          {...motion.bar}
        />
      </BarChart>
    </ChartFrame>
  );
}

// ---------------------------------------------------------------------------
// stacked_bar — several value columns stacked per x
// ---------------------------------------------------------------------------

// StackedBarBody stacks the chosen value columns into one bar per x.
// Wide format: the SQL returns one row per x with a column per series
// (`SELECT cat, SUM(a), SUM(b), SUM(c) GROUP BY cat`). With no series
// columns picked it stacks every column except x. The legend turns a
// series off; the others keep their colours.
function StackedBarBody({ widget, data, sql }: WidgetBodyProps) {
  const xIdx = pickColumn(data, widget.x_field, 0);
  const chosen = widget.series_fields.filter((s) =>
    data.columns.some((c) => c.name === s),
  );
  const seriesNames =
    xIdx < 0
      ? []
      : chosen.length > 0
        ? chosen
        : data.columns.map((c) => c.name).filter((_, i) => i !== xIdx);
  const { hidden, toggle } = useHiddenSeries(seriesNames);
  const motion = useChartMotion(data, hidden);
  if (xIdx < 0 || seriesNames.length === 0) return <NoFieldsHint />;

  const xf = xFormat(data.rows.map((row) => row[xIdx]));
  // A series turned off stays in the stack at zero, so its bars shrink
  // away and the ones above settle down (and grow back on), animated.
  const points = data.rows.map((row) => {
    const rec: Record<string, string | number> = { __x: row[xIdx] };
    for (const s of seriesNames) {
      const ci = data.columns.findIndex((c) => c.name === s);
      rec[s] = hidden.has(s) ? 0 : Number(row[ci]);
    }
    return rec;
  });
  const shown = seriesNames.filter((s) => !hidden.has(s));
  const top = shown[shown.length - 1];
  const items: LegendItem[] = seriesNames.map((s, i) => ({
    key: s,
    label: seriesLabel(s, sql, s),
    color: SERIES_PALETTE[i % SERIES_PALETTE.length],
  }));

  return (
    <>
      <ChartFrame
        pointCount={points.length}
        seriesTotal={seriesNames.length}
        seriesVisible={shown.length}
      >
        <BarChart data={points} margin={{ top: 8, right: 36, left: 0, bottom: 0 }}>
          <CartesianGrid {...gridProps} />
          <XAxis dataKey="__x" {...xAxisStyle} tickFormatter={xf.tick} />
          <YAxis
            {...yAxisStyle}
            {...yAxisProps(
              points.map((p) => shown.reduce((sum, n) => sum + (Number(p[n]) || 0), 0)),
              isPercent(widget.title, seriesNames.length === 1 ? seriesNames[0] : ""),
            )}
          />
          <Tooltip
            cursor={{ fill: CURSOR_FILL }}
            content={(p) => (
              <ChartTooltip {...p} hidden={hidden} format={formatNumber} labelFormat={xf.heading} />
            )}
          />
          {seriesNames.map((s, i) => (
            <Bar
              key={s}
              dataKey={s}
              name={items[i].label}
              stackId="a"
              fill={items[i].color}
              stroke={SURFACE}
              strokeWidth={1}
              radius={s === top ? [4, 4, 0, 0] : 0}
              maxBarSize={56}
              {...motion.bar}
            />
          ))}
        </BarChart>
      </ChartFrame>
      <ToggleLegend items={items} hidden={hidden} onToggle={toggle} />
    </>
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

  // Numeric-coerce values; drop rows whose value isn't a number (Recharts
  // would draw zero-sized slices for them). Top-N by value; the long tail
  // folds into "Other", in grey so the eye reads it as the rest. Each
  // slice's colour is fixed here, before any is turned off, so a slice
  // keeps its colour when another one goes.
  const slices = useMemo(() => {
    if (nameIdx < 0 || valueIdx < 0) return [];
    // Same-name rows summed, the biggest first, the tail folded into a
    // grey "Other". Keys are names and colours come from the names'
    // alphabetical order (chartLogic.pieSlices), so a slice keeps its
    // colour, and stays hidden, when the values change order.
    return pieSlices(
      data.rows.map((row) => ({
        name: String(row[nameIdx] ?? ""),
        value: Number(row[valueIdx]),
      })),
      PIE_MAX_SLICES,
    ).map((s) => ({
      ...s,
      color: s.slot < 0 ? OTHER_COLOR : SERIES_PALETTE[s.slot % SERIES_PALETTE.length],
    }));
  }, [data, nameIdx, valueIdx]);
  const { hidden, toggle } = useHiddenSeries(slices.map((s) => s.key));
  const motion = useChartMotion(data, hidden);

  if (nameIdx < 0 || valueIdx < 0) {
    return (
      <div className="flex flex-1 items-center justify-center p-4 text-xs text-muted-foreground">
        Pie needs `x_field` (category) and `value_field` (numeric) to render
      </div>
    );
  }
  if (slices.length === 0) {
    return (
      <div className="flex flex-1 items-center justify-center p-4 text-xs text-muted-foreground">
        No positive values to chart
      </div>
    );
  }

  const shown = slices.filter((s) => !hidden.has(s.key));
  const total = shown.reduce((sum, s) => sum + s.value, 0);
  const share = (v: number) => {
    const p = total > 0 ? (v / total) * 100 : 0;
    return `${p >= 10 || p === 0 ? Math.round(p) : p.toFixed(1)}%`;
  };
  const items: LegendItem[] = slices.map((s) => ({
    key: s.key,
    label: s.name || "(blank)",
    color: s.color,
    detail: hidden.has(s.key) ? undefined : share(s.value),
  }));
  const donut = variant === "donut";
  const pct = isPercent(widget.title, data.columns[valueIdx]?.name) ? "%" : "";

  return (
    <>
      <ChartFrame seriesTotal={slices.length} seriesVisible={shown.length}>
        <PieChart>
          {/* Every slice stays in the pie; one turned off goes to zero, so
              the others widen into its room and it grows back, animated. */}
          <Pie
            data={slices.map((s) => (hidden.has(s.key) ? { ...s, value: 0 } : s))}
            dataKey="value"
            nameKey="name"
            innerRadius={donut ? "62%" : 0}
            outerRadius="88%"
            paddingAngle={donut && shown.length > 1 ? 1.5 : 0}
            cornerRadius={donut ? 3 : 0}
            {...motion.pie}
            stroke={SURFACE}
            strokeWidth={donut ? 0 : 2}
          >
            {slices.map((s) => (
              <Cell key={s.key} fill={s.color} />
            ))}
          </Pie>
          <Tooltip
            content={(p) => (
              <ChartTooltip
                {...p}
                label={null}
                format={formatNumber}
                extra={(e) => share(Number(e.value))}
              />
            )}
          />
        </PieChart>
        {donut && (
          <div className="pointer-events-none absolute inset-0 flex flex-col items-center justify-center">
            <span className="text-lg font-semibold leading-tight tabular-nums text-foreground md:text-xl">
              {compactNumber(total)}
              {pct}
            </span>
            <span className="text-[11px] text-muted-foreground">
              {hidden.size > 0 ? "shown" : "total"}
            </span>
          </div>
        )}
      </ChartFrame>
      <ToggleLegend items={items} hidden={hidden} onToggle={toggle} />
    </>
  );
}

// ---------------------------------------------------------------------------
// bar_line — a bar metric and a line metric sharing one x, dual y-axis
// ---------------------------------------------------------------------------

function BarLineBody({ widget, data, sql }: WidgetBodyProps) {
  const xIdx = pickColumn(data, widget.x_field, 0);
  const barIdx = pickColumn(data, widget.y_field, 1);
  const lineIdx = pickColumn(data, widget.line_field, 2);
  const { hidden, toggle } = useHiddenSeries(["bar", "line"]);
  const motion = useChartMotion(data, hidden);
  // The line comes back drawn from the left, as on first show: a new key
  // each time it is turned on mounts it fresh.
  const lineShows = useRef({ off: false, n: 0 });
  if (lineShows.current.off && !hidden.has("line")) lineShows.current.n++;
  lineShows.current.off = hidden.has("line");
  if (xIdx < 0 || barIdx < 0 || lineIdx < 0) return <NoFieldsHint />;

  const barName = seriesLabel(data.columns[barIdx]?.name ?? "", sql, "bars");
  const lineName = seriesLabel(data.columns[lineIdx]?.name ?? "", sql, "line");
  const barColor = SERIES_PALETTE[0];
  const lineColor = SERIES_PALETTE[1];
  const xf = xFormat(data.rows.map((row) => row[xIdx]));
  const barOff = hidden.has("bar");
  const lineOff = hidden.has("line");
  // Bars turned off go to zero (they shrink into the axis and grow back);
  // the line turned off fades out where it is.
  const points = data.rows.map((row) => ({
    x: row[xIdx],
    bar: barOff ? 0 : Number(row[barIdx]),
    line: Number(row[lineIdx]),
  }));
  const barValues = data.rows.map((row) => Number(row[barIdx]));
  const items: LegendItem[] = [
    { key: "bar", label: barName, color: barColor },
    { key: "line", label: lineName, color: lineColor },
  ];

  return (
    <>
      <ChartFrame
        pointCount={points.length}
        seriesTotal={2}
        seriesVisible={2 - hidden.size}
      >
        <ComposedChart data={points} margin={{ top: 8, right: 4, left: 0, bottom: 0 }}>
          <CartesianGrid {...gridProps} />
          <XAxis dataKey="x" {...xAxisStyle} tickFormatter={xf.tick} />
          <YAxis
            yAxisId="bar"
            hide={barOff}
            {...yAxisStyle}
            // Each axis in its series' colour, so it is clear which is which.
            tick={{ fill: barColor, fontSize: 11 }}
            {...yAxisProps(barValues, isPercent(widget.title, barName))}
          />
          <YAxis
            yAxisId="line"
            orientation="right"
            hide={lineOff}
            {...yAxisStyle}
            tick={{ fill: lineColor, fontSize: 11 }}
            {...yAxisProps(points.map((p) => p.line), isPercent("", lineName))}
          />
          <Tooltip
            cursor={{ fill: CURSOR_FILL }}
            content={(p) => (
              <ChartTooltip {...p} hidden={hidden} format={formatNumber} labelFormat={xf.heading} />
            )}
          />
          <Bar
            yAxisId="bar"
            dataKey="bar"
            name={barName}
            fill={barColor}
            fillOpacity={0.9}
            radius={[4, 4, 0, 0]}
            maxBarSize={48}
            {...motion.bar}
          />
          <Line
            key={lineShows.current.n}
            yAxisId="line"
            className={cn("chart-series", lineOff && "chart-series-off")}
            type="linear"
            dataKey="line"
            name={lineName}
            stroke={lineColor}
            strokeWidth={2}
            dot={points.length <= DOTS_UP_TO ? { r: 2.5, fill: lineColor, strokeWidth: 0 } : false}
            activeDot={lineOff ? false : { r: 4.5, fill: lineColor, stroke: SURFACE, strokeWidth: 2 }}
            {...motion.line}
          />
        </ComposedChart>
      </ChartFrame>
      <ToggleLegend items={items} hidden={hidden} onToggle={toggle} />
    </>
  );
}

// ---------------------------------------------------------------------------
// table — display every column the SQL returned
// ---------------------------------------------------------------------------

interface TableBodyProps {
  data: DashboardQueryResult;
}

const isNumberCell = (cell: unknown) =>
  String(cell ?? "").trim() !== "" && Number.isFinite(Number(cell));

// TableBody2 draws the rows as a table. On a phone (below 640 px), a
// table that does not fit its widget even with its text wrapped turns
// into a list (Vesa, 2026-11-19: the smoke2 "Recent runs" table scrolled
// sideways): each row's first column is its title, the other columns sit
// under it as small "name value" pairs. Nothing is cut or scrolled.
function TableBody2({ data }: TableBodyProps) {
  const narrow = useIsNarrow();
  const boxRef = useRef<HTMLDivElement>(null);
  // The data the table was found too wide for; a new result is measured
  // again.
  const [stackFor, setStackFor] = useState<DashboardQueryResult | null>(null);
  const stack = narrow && stackFor === data;
  useLayoutEffect(() => {
    if (!narrow || stack) return;
    // The table primitive scrolls inside its own wrapper, so compare the
    // table's width with the box, not the box's scroll width.
    const el = boxRef.current;
    const table = el?.querySelector("table");
    if (el && table && table.scrollWidth > el.clientWidth + 1) setStackFor(data);
  });
  if (stack) return <StackedRows data={data} />;
  // A column whose filled cells are all numbers is right-aligned, header
  // included, so its digits line up.
  const numeric = data.columns.map((_, j) => {
    let seen = false;
    for (const row of data.rows) {
      const c = row[j];
      if (c === "" || c == null) continue;
      if (!isNumberCell(c)) return false;
      seen = true;
    }
    return seen;
  });
  return (
    <div ref={boxRef} className="flex-1 overflow-auto px-1 pb-1">
      <Table>
        <TableHeader className="sticky top-0 z-10 bg-card">
          <TableRow className="border-white/[0.06] hover:bg-transparent">
            {data.columns.map((c, j) => (
              <TableHead
                key={c.name}
                className={cn(
                  "h-9 whitespace-normal text-xs font-medium text-muted-foreground",
                  numeric[j] && "text-right",
                )}
              >
                {breakable(c.name)}
              </TableHead>
            ))}
          </TableRow>
        </TableHeader>
        <TableBody>
          {data.rows.map((row, i) => (
            <TableRow
              key={i}
              data-testid="recent-runs-row"
              className="border-white/[0.05] hover:bg-white/[0.03]"
            >
              {row.map((cell, j) => (
                // Numbers stay on one line; text wraps, so on a phone the
                // table fits its row and no number is hidden off the edge.
                <TableCell
                  key={j}
                  className={
                    isNumberCell(cell)
                      ? cn("whitespace-nowrap py-2 text-xs tabular-nums text-foreground", numeric[j] && "text-right")
                      : "whitespace-normal py-2 text-xs text-foreground/85"
                  }
                >
                  {cell === "" || cell == null ? (
                    <span className="text-muted-foreground/50">—</span>
                  ) : (
                    breakable(String(cell))
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

function StackedRows({ data }: TableBodyProps) {
  const rest = data.columns.slice(1);
  return (
    <ul
      className="flex-1 divide-y divide-white/[0.06] overflow-y-auto px-4 pb-2"
      data-testid="table-stacked"
    >
      {data.rows.map((row, i) => {
        const head = row[0] === "" || row[0] == null ? "—" : String(row[0]);
        return (
          <li key={i} className="min-w-0 py-2.5" data-testid="recent-runs-row">
            {/* One line: a long title ends in an ellipsis, the whole value
                is in the title attribute (hover, long press). */}
            <div
              className={cn(
                "truncate text-xs font-medium text-foreground",
                isIdLike(head) && "font-mono tracking-tight",
                isNumberCell(head) && "tabular-nums",
              )}
              title={head}
              data-testid="stacked-title"
            >
              {head}
            </div>
            {rest.length > 0 && (
              <dl className="mt-1 flex flex-wrap gap-x-4 gap-y-0.5 text-[11px] leading-4">
                {rest.map((c, k) => {
                  const cell = row[k + 1];
                  const empty = cell === "" || cell == null;
                  return (
                    <div key={c.name} className="flex min-w-0 gap-1.5">
                      <dt className="text-muted-foreground">{c.name}</dt>
                      <dd
                        className={cn(
                          "min-w-0 text-foreground/85 [overflow-wrap:anywhere]",
                          !empty && isNumberCell(cell) && "tabular-nums",
                          !empty && isIdLike(cell) && "font-mono",
                        )}
                      >
                        {empty ? "—" : String(cell)}
                      </dd>
                    </div>
                  );
                })}
              </dl>
            )}
          </li>
        );
      })}
    </ul>
  );
}

// ---------------------------------------------------------------------------
// Shared bits
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Series names (2026-11-18): a legend never says "y". A column called y, x,
// value, line or bar is a placeholder alias; its label comes from the
// expression behind it in the dataset SQL (SUM(p.page_views) AS y → "page
// views"), else from the fallback (the widget title for a single series).
// ---------------------------------------------------------------------------

/** Fewer points than this draw a line widget as bars. */
const SPARSE_POINTS = 8;

const PLACEHOLDER = /^(x|y|value|val|line|bar|bars)$/i;

/** The legend and tooltip name of a series. */
export function seriesLabel(field: string, sql: string | undefined, fallback: string): string {
  if (field && !PLACEHOLDER.test(field)) return field;
  const expr = sql ? selectExpression(sql, field) : null;
  const named = expr ? expressionName(expr) : null;
  return named ?? (fallback || field || "value");
}

/** The expression the first SELECT list aliases to `alias`, or null. */
function selectExpression(sql: string, alias: string): string | null {
  const start = sql.search(/\bselect\b/i);
  if (start < 0 || !alias) return null;
  const items: string[] = [];
  let depth = 0;
  let cur = "";
  const rest = sql.slice(start + 6);
  for (let i = 0; i < rest.length; i++) {
    const ch = rest[i];
    if (ch === "(") depth++;
    else if (ch === ")") depth--;
    if (depth === 0 && /^from\b/i.test(rest.slice(i)) && /\s/.test(rest[i - 1] ?? " ")) break;
    if (depth === 0 && ch === ",") {
      items.push(cur);
      cur = "";
    } else cur += ch;
  }
  items.push(cur);
  for (const item of items) {
    const m = /^([\s\S]*?)\s+as\s+["`]?(\w+)["`]?\s*$/i.exec(item.trim());
    if (m && m[2].toLowerCase() === alias.toLowerCase()) return m[1].trim();
  }
  return null;
}

/** "SUM(p.page_views)" → "page views", "COUNT(*)" → "count"; null for anything else. */
function expressionName(expr: string): string | null {
  if (/^count\s*\(\s*\*\s*\)$/i.test(expr)) return "count";
  const m =
    /^\w+\s*\(\s*(?:distinct\s+)?(?:\w+\.)?(\w+)\s*\)$/i.exec(expr) ?? /^(?:\w+\.)?(\w+)$/.exec(expr);
  return m ? m[1].replace(/_/g, " ") : null;
}

// ---------------------------------------------------------------------------
// y axis ticks: readable on a phone (2026-11-18). Integers when every value
// is an integer (a count never shows 0.5), short units (1.2k, 3.4M), at
// most two decimals, "%" when the widget title or the field says percent,
// and an axis just wide enough for its longest label so none is cut off.
// ---------------------------------------------------------------------------

const PERCENT_FIELD = /(^|_)(pct|percent|percentage)($|_)/i;

/** True when the widget's title or the field name says the value is a percent. */
export function isPercent(title: string | undefined, field: string | undefined): boolean {
  return /%/.test(title ?? "") || PERCENT_FIELD.test(field ?? "");
}

/** One y tick: 0, 35, 1.5, 0.25, 1.2k, 3.4M, 12%. */
export function axisNumber(v: unknown, percent = false): string {
  const n = Number(v);
  if (!Number.isFinite(n)) return String(v ?? "");
  const a = Math.abs(n);
  const trim = (x: number, d: number) => String(Number(x.toFixed(d)));
  let out: string;
  if (a >= 1e9) out = `${trim(n / 1e9, 1)}B`;
  else if (a >= 1e6) out = `${trim(n / 1e6, 1)}M`;
  else if (a >= 1e3) out = `${trim(n / 1e3, a >= 1e4 ? 0 : 1)}k`;
  else if (a === 0 || a >= 0.01) out = trim(n, 2);
  else out = String(Number(n.toPrecision(2)));
  if (/e/i.test(out)) out = n.toFixed(4).replace(/\.?0+$/, "");
  return percent ? `${out}%` : out;
}

/**
 * The props every chart's YAxis takes from the values it draws. A ratio
 * (all values within 0..1) on a percent axis is shown times 100.
 */
export function yAxisProps(values: number[], percent: boolean) {
  const finite = values.filter(Number.isFinite);
  const integers = finite.length > 0 && finite.every(Number.isInteger);
  const ratio = percent && finite.length > 0 && !integers && finite.every((v) => v >= 0 && v <= 1);
  const fmt = (v: unknown) => axisNumber(ratio ? Number(v) * 100 : v, percent);
  const max = finite.reduce((m, v) => Math.max(m, Math.abs(v)), 0);
  // Recharts rounds the top tick up; size for a label a little past max.
  const longest = Math.max(fmt(max).length, fmt(max * 1.25).length, fmt(-max).length * (finite.some((v) => v < 0) ? 1 : 0), 1);
  return {
    allowDecimals: !integers,
    tickFormatter: fmt,
    width: Math.min(68, Math.max(30, Math.ceil(longest * 6.8 + 10))),
  };
}

// breakable lets a table cell wrap after "/", "-", "_" and ".", and
// inside a token longer than 12 characters (a hash or an ID), so paths and
// IDs fit a phone row while words such as SUCCEEDED stay whole. A table
// that still does not fit scrolls sideways in its widget.
function breakable(text: string): React.ReactNode {
  const parts = text.split(/(?<=[/\-_.])/);
  if (parts.length === 1 && text.length <= 12) return text;
  return parts.map((p, i) => (
    <Fragment key={i}>
      {p.length > 12 ? <span className="[overflow-wrap:anywhere]">{p}</span> : p}
      {i < parts.length - 1 && <wbr />}
    </Fragment>
  ));
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

// `pointCount` stamps the container with data-point-count so automated
// walkthroughs (verify-readme.sh) can assert "chart has N points" without
// counting recharts SVG internals, which are brittle. data-series-total and
// data-series-visible say how many series the chart has and how many are
// on, for the legend checks in dashboard-phone-shots.
function ChartFrame({
  children,
  pointCount,
  seriesTotal,
  seriesVisible,
}: {
  children: React.ReactNode;
  pointCount?: number;
  seriesTotal?: number;
  seriesVisible?: number;
}) {
  const [chart, ...overlay] = Array.isArray(children) ? children : [children];
  return (
    <div
      className="relative min-h-0 flex-1 px-2 pb-2 text-muted-foreground [font-variant-numeric:tabular-nums]"
      data-testid="widget-chart"
      data-point-count={pointCount}
      data-series-total={seriesTotal}
      data-series-visible={seriesVisible}
    >
      <ResponsiveContainer width="100%" height="100%">
        {chart as React.ReactElement}
      </ResponsiveContainer>
      {overlay}
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
