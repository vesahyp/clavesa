/**
 * chartKit — the parts every dashboard chart shares: the series palette,
 * the axis and grid styling, the tooltip, and the legend whose entries turn
 * a series off and on.
 *
 * Vesa, 2026-11-19: "Clavesa dashboard graphs dont allow deselecting
 * things". The recharts <Legend> only draws names; nothing listened to a
 * click. ToggleLegend is a row of real buttons (aria-pressed) under the
 * chart: a tap turns a series off (hollow dot, struck-through name), a
 * second tap brings it back, and the last series left on cannot be turned
 * off, so a chart never goes blank. On a phone each button is 44 px tall.
 */

import { useCallback, useMemo, useState } from "react";

import { cn } from "@/lib/utils";

// SERIES_PALETTE: the categorical colours, in a fixed order, for the dark
// card surface. The order is part of the colour-blind safety (every
// adjacent pair is at least ΔE 8.4 apart under protan/deutan/tritan
// simulation, 19.3 for normal vision, all >= 3:1 on the card), so a series
// keeps its slot when another one is turned off. Recharts writes fill and
// stroke as SVG attributes, which do not resolve CSS var(), so these are
// literals.
export const SERIES_PALETTE = [
  "#3987e5", // blue
  "#d95926", // orange
  "#199e70", // aqua
  "#c98500", // yellow
  "#d55181", // magenta
  "#0ca30c", // green
  "#9085e9", // violet
  "#e66767", // red
];

/** The colour of the residual "Other" slice: grey, read as the rest. */
export const OTHER_COLOR = "#64748b";
/** The card surface, for the gap drawn between touching fills. */
export const SURFACE = "#1b2232";
export const GRID_STROKE = "rgb(255 255 255 / 0.07)";
export const CURSOR_FILL = "rgb(255 255 255 / 0.05)";
export const CURSOR_STROKE = "rgb(255 255 255 / 0.22)";

/** Recharts props for a quiet x axis. */
export const xAxisStyle = {
  tick: { fill: "currentColor", fontSize: 11 },
  stroke: "currentColor",
  tickLine: false,
  axisLine: false,
  tickMargin: 8,
} as const;

/** Recharts props for a quiet y axis (the caller adds yAxisProps). */
export const yAxisStyle = {
  tick: { fill: "currentColor", fontSize: 11 },
  stroke: "currentColor",
  tickLine: false,
  axisLine: false,
  tickMargin: 4,
} as const;

/** Horizontal hairlines only, dashed. */
export const gridProps = {
  stroke: GRID_STROKE,
  strokeDasharray: "3 3",
  vertical: false,
} as const;

// ---------------------------------------------------------------------------
// Series on and off
// ---------------------------------------------------------------------------

/**
 * The series a viewer turned off. toggle() refuses to turn off the last
 * series that is on. Keys that are no longer in `keys` (the data changed)
 * do not count.
 */
export function useHiddenSeries(keys: string[]) {
  const [hidden, setHidden] = useState<Set<string>>(() => new Set());
  const keyList = keys.join("\u0000");
  const live = useMemo(() => {
    const ks = new Set(keyList.split("\u0000"));
    return new Set([...hidden].filter((k) => ks.has(k)));
  }, [hidden, keyList]);
  const toggle = useCallback(
    (key: string) => {
      setHidden((prev) => {
        const next = new Set(prev);
        if (next.has(key)) {
          next.delete(key);
          return next;
        }
        const ks = keyList.split("\u0000");
        const on = ks.filter((k) => !next.has(k)).length;
        if (on <= 1) return prev;
        next.add(key);
        return next;
      });
    },
    [keyList],
  );
  return { hidden: live, toggle };
}

export interface LegendItem {
  key: string;
  label: string;
  color: string;
  /** A short figure after the name, e.g. a pie slice's share. */
  detail?: string;
}

export function ToggleLegend({
  items,
  hidden,
  onToggle,
}: {
  items: LegendItem[];
  hidden: Set<string>;
  onToggle: (key: string) => void;
}) {
  const shown = items.filter((i) => !hidden.has(i.key)).length;
  return (
    <div
      role="group"
      aria-label="Series shown in the chart"
      data-testid="chart-legend"
      className="flex flex-wrap gap-x-1.5 px-3 pb-2 md:gap-y-1.5 md:pb-3"
    >
      {items.map((it) => {
        const off = hidden.has(it.key);
        const last = !off && shown === 1;
        return (
          <button
            key={it.key}
            type="button"
            data-testid="legend-item"
            data-off={off ? "" : undefined}
            aria-pressed={!off}
            aria-disabled={last || undefined}
            title={
              last
                ? "At least one series stays on"
                : off
                  ? `Show ${it.label}`
                  : `Hide ${it.label}`
            }
            onClick={() => {
              if (!last) onToggle(it.key);
            }}
            className={cn(
              "group flex min-h-11 max-w-full items-center outline-none md:min-h-0",
              last ? "cursor-default" : "cursor-pointer",
            )}
          >
            <span
              className={cn(
                "flex min-w-0 items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs leading-4 transition-colors",
                "group-focus-visible:ring-2 group-focus-visible:ring-ring",
                off
                  ? "border-dashed border-white/15 text-muted-foreground/70"
                  : "border-white/10 bg-white/[0.04] text-foreground/90",
                !off && !last && "group-hover:bg-white/[0.09]",
                off && "group-hover:text-muted-foreground",
              )}
            >
              <span
                aria-hidden
                className="h-2 w-2 flex-shrink-0 rounded-full"
                style={
                  off
                    ? { boxShadow: `inset 0 0 0 1.5px ${it.color}` }
                    : { background: it.color }
                }
              />
              <span className={cn("truncate", off && "line-through")}>
                {it.label}
              </span>
              {it.detail && !off && (
                <span className="tabular-nums text-muted-foreground">
                  {it.detail}
                </span>
              )}
            </span>
          </button>
        );
      })}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Tooltip
// ---------------------------------------------------------------------------

interface TooltipEntry {
  name?: string | number;
  value?: unknown;
  color?: string;
  dataKey?: string | number | ((o: unknown) => unknown);
  payload?: Record<string, unknown>;
}

interface ChartTooltipProps {
  active?: boolean;
  payload?: ReadonlyArray<TooltipEntry>;
  label?: unknown;
  /** Formats each value. */
  format: (v: unknown) => string;
  /** Shown under the value, e.g. a pie slice's share. */
  extra?: (e: TooltipEntry) => string | undefined;
}

/**
 * The hover card: the x value on top, then one row per series with its
 * colour dot, its name and its value. Values are in text colour; the dot
 * carries the series identity.
 */
export function ChartTooltip({
  active,
  payload,
  label,
  format,
  extra,
}: ChartTooltipProps) {
  if (!active || !payload || payload.length === 0) return null;
  const heading = label == null || label === "" ? null : String(label);
  return (
    <div
      className="min-w-[9rem] max-w-[16rem] rounded-lg border border-white/10 bg-popover/95 px-3 py-2 text-xs shadow-xl shadow-black/40 backdrop-blur-sm"
      data-testid="chart-tooltip"
    >
      {heading && (
        <div className="mb-1.5 truncate font-medium text-muted-foreground">
          {heading}
        </div>
      )}
      <div className="space-y-1">
        {payload.map((p, i) => {
          const color =
            (p.payload?.color as string | undefined) ?? p.color ?? "currentColor";
          const more = extra?.(p);
          return (
            <div
              key={`${String(p.dataKey ?? p.name)}-${i}`}
              className="flex items-center justify-between gap-4"
            >
              <span className="flex min-w-0 items-center gap-1.5 text-muted-foreground">
                <span
                  aria-hidden
                  className="h-2 w-2 flex-shrink-0 rounded-full"
                  style={{ background: color }}
                />
                <span className="truncate">{String(p.name ?? "")}</span>
              </span>
              <span className="whitespace-nowrap font-medium tabular-nums text-foreground">
                {format(p.value)}
                {more && (
                  <span className="ml-1.5 font-normal text-muted-foreground">
                    {more}
                  </span>
                )}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

const SKELETON_BARS = [38, 52, 44, 66, 58, 72, 61, 80, 70, 86, 77, 92];

/** A loading placeholder in the shape of what is coming. */
export function WidgetSkeleton({ type }: { type?: string }) {
  if (type === "big_number") {
    return (
      <div className="flex flex-1 items-end px-4 pb-4">
        <div className="h-9 w-28 animate-pulse rounded-md bg-muted/80" />
      </div>
    );
  }
  if (type === "table") {
    return (
      <div className="flex-1 space-y-2.5 px-4 pb-4 pt-1">
        {[100, 92, 96, 88, 94].map((w, i) => (
          <div
            key={i}
            className="h-3.5 animate-pulse rounded bg-muted/70"
            style={{ width: `${w}%` }}
          />
        ))}
      </div>
    );
  }
  if (type === "pie" || type === "donut" || type === "world_map") {
    return (
      <div className="flex flex-1 items-center justify-center p-4">
        <div className="aspect-square h-full max-h-40 min-h-24 animate-pulse rounded-full bg-muted/70" />
      </div>
    );
  }
  return (
    <div className="flex min-h-32 flex-1 items-end gap-1.5 px-4 pb-4 pt-2">
      {SKELETON_BARS.map((h, i) => (
        <div
          key={i}
          className="flex-1 animate-pulse rounded-t-[4px] bg-muted/70"
          style={{ height: `${h}%`, animationDelay: `${i * 60}ms` }}
        />
      ))}
    </div>
  );
}
