/**
 * MiniCharts — the small charts of the phone pages (ADR-032 section 1):
 * a stacked bar strip (runs per day, cost per day by pipeline, duration
 * per run) and a sparkline (cost per day, cost per billion per run).
 *
 * Plain divs and inline SVG on purpose: recharts is lazy-loaded on the
 * dashboard routes only, and these sit on eager pages. Every chart takes
 * a tap (or hover) on a bar or point and names its value in a caption
 * line under the plot, since a phone has no hover.
 */

import { useState, type ReactNode } from "react";

import { cn } from "@/lib/utils";

export interface BarSegment {
  key: string;
  value: number;
  color: string;
}

export interface BarDatum {
  key: string;
  /** Caption text for this bar when it is tapped. */
  caption: string;
  segments: BarSegment[];
}

interface AxisProps {
  /** Text under the left and right ends of the plot. */
  start?: string;
  end?: string;
  /** Text at the top left: the value the full height stands for. */
  top?: string;
}

function Axis({ start, end }: AxisProps) {
  if (!start && !end) return null;
  return (
    <div className="mt-1 flex justify-between text-[10px] tabular-nums text-muted-foreground">
      <span>{start}</span>
      <span>{end}</span>
    </div>
  );
}

function Caption({ text }: { text: string | null }) {
  return (
    <div
      className="mt-0.5 min-h-4 truncate text-[11px] tabular-nums text-muted-foreground"
      aria-live="polite"
    >
      {text ?? ""}
    </div>
  );
}

/**
 * EmptyChart is what a chart draws when it has nothing to plot: one line
 * of text in the chart's place, not an empty axis or a flat line.
 */
function EmptyChart({ testId, text }: { testId: string; text: string }) {
  return (
    <div
      data-testid={testId}
      data-point-count={0}
      data-empty="true"
      className="py-1 text-xs text-muted-foreground"
    >
      {text}
    </div>
  );
}

/**
 * StackedBars draws one bar per datum, its segments stacked bottom-up in
 * the order given. A datum with no value draws a 1px baseline tick, so a
 * quiet day still reads as a day. onSelect, when set, is called with the
 * tapped datum's key (the run list of a pipeline opens a run this way).
 * `empty`, when set, replaces the plot with that line when every value
 * is zero.
 */
export function StackedBars({
  data,
  height = 40,
  testId,
  ariaLabel,
  axis,
  onSelect,
  maxBarWidth,
  empty,
}: {
  data: BarDatum[];
  height?: number;
  testId: string;
  ariaLabel: string;
  axis?: AxisProps;
  onSelect?: (key: string) => void;
  maxBarWidth?: number;
  empty?: string;
}) {
  const [sel, setSel] = useState<number | null>(null);
  const totals = data.map((d) => d.segments.reduce((a, s) => a + s.value, 0));
  const max = Math.max(0, ...totals);
  if (empty && max === 0) return <EmptyChart testId={testId} text={empty} />;
  return (
    <div data-testid={testId} data-point-count={data.length}>
      {axis?.top && (
        <div className="mb-0.5 text-[10px] tabular-nums text-muted-foreground">
          {axis.top}
        </div>
      )}
      <div
        role="img"
        aria-label={ariaLabel}
        className="flex items-end gap-[2px] border-b border-border"
        style={{ height }}
      >
        {data.map((d, i) => {
          const total = totals[i];
          return (
            <button
              type="button"
              key={d.key}
              title={d.caption}
              aria-label={d.caption}
              onMouseEnter={() => setSel(i)}
              onFocus={() => setSel(i)}
              onClick={() => {
                setSel(i);
                onSelect?.(d.key);
              }}
              className={cn(
                "flex h-full min-w-0 flex-1 cursor-pointer flex-col-reverse rounded-t-[3px] outline-none",
                sel === i && "bg-foreground/[0.06]",
              )}
              style={maxBarWidth ? { maxWidth: maxBarWidth } : undefined}
            >
              {total === 0 || max === 0 ? (
                <span className="block h-px w-full bg-border" />
              ) : (
                d.segments
                  .filter((s) => s.value > 0)
                  .map((s, j, arr) => (
                    <span
                      key={s.key}
                      className={cn(
                        "block w-full",
                        j === arr.length - 1 && "rounded-t-[3px]",
                        j > 0 && "border-b border-card",
                      )}
                      style={{
                        height: `${Math.max(2, (s.value / max) * height)}px`,
                        background: s.color,
                      }}
                    />
                  ))
              )}
            </button>
          );
        })}
      </div>
      <Axis {...axis} />
      <Caption text={sel !== null ? data[sel]?.caption ?? null : null} />
    </div>
  );
}

/**
 * Sparkline draws one series as a line with a faint area under it, scaled
 * from zero. Points are evenly spaced (one per day, or one per run).
 * `empty`, when set, replaces the plot with that line when every value
 * is zero.
 */
export function Sparkline({
  values,
  captions,
  color = "#3987e5",
  height = 36,
  testId,
  ariaLabel,
  axis,
  empty,
}: {
  values: number[];
  captions: string[];
  color?: string;
  height?: number;
  testId: string;
  ariaLabel: string;
  axis?: AxisProps;
  empty?: string;
}) {
  const [sel, setSel] = useState<number | null>(null);
  const n = values.length;
  const max = Math.max(0, ...values);
  if (empty && max === 0) return <EmptyChart testId={testId} text={empty} />;
  const x = (i: number) => (n <= 1 ? 50 : (i / (n - 1)) * 100);
  const y = (v: number) => (max === 0 ? 98 : 98 - (v / max) * 94);
  const line = values.map((v, i) => `${i === 0 ? "M" : "L"}${x(i)},${y(v)}`).join(" ");
  const area = n > 0 ? `${line} L${x(n - 1)},100 L${x(0)},100 Z` : "";
  const dotI = sel ?? n - 1;
  return (
    <div data-testid={testId} data-point-count={n}>
      {axis?.top && (
        <div className="mb-0.5 text-[10px] tabular-nums text-muted-foreground">
          {axis.top}
        </div>
      )}
      <div className="relative" style={{ height }} role="img" aria-label={ariaLabel}>
        <svg
          viewBox="0 0 100 100"
          preserveAspectRatio="none"
          className="absolute inset-0 h-full w-full overflow-visible"
        >
          <line
            x1="0"
            x2="100"
            y1="100"
            y2="100"
            stroke="var(--border)"
            vectorEffect="non-scaling-stroke"
          />
          {n > 1 && <path d={area} fill={color} fillOpacity={0.14} stroke="none" />}
          {n > 1 && (
            <path
              d={line}
              fill="none"
              stroke={color}
              strokeWidth={2}
              strokeLinejoin="round"
              vectorEffect="non-scaling-stroke"
            />
          )}
        </svg>
        {n > 0 && (
          <span
            className="pointer-events-none absolute size-2 -translate-x-1/2 -translate-y-1/2 rounded-full ring-2 ring-card"
            style={{
              left: `${x(dotI)}%`,
              top: `${y(values[dotI])}%`,
              background: color,
            }}
          />
        )}
        {/* Hit targets: one column per point, wider than the mark. */}
        <div className="absolute inset-0 flex">
          {values.map((_, i) => (
            <button
              type="button"
              key={i}
              aria-label={captions[i]}
              title={captions[i]}
              className="h-full min-w-0 flex-1 cursor-pointer outline-none"
              onMouseEnter={() => setSel(i)}
              onFocus={() => setSel(i)}
              onClick={() => setSel(i)}
            />
          ))}
        </div>
      </div>
      <Axis {...axis} />
      <Caption text={sel !== null ? captions[sel] ?? null : null} />
    </div>
  );
}

/** Legend: a swatch and a name per series, in the series order. */
export function Legend({
  items,
  testId,
}: {
  items: { key: string; label: ReactNode; color: string }[];
  testId?: string;
}) {
  return (
    <div
      data-testid={testId}
      className="flex flex-wrap gap-x-3 gap-y-1 text-[11px] text-muted-foreground"
    >
      {items.map((it) => (
        <span key={it.key} className="flex min-w-0 items-center gap-1.5">
          <span
            className="inline-block size-2 shrink-0 rounded-sm"
            style={{ background: it.color }}
          />
          <span className="truncate">{it.label}</span>
        </span>
      ))}
    </div>
  );
}

/** Status colours as CSS values, for chart marks. */
export const SUCCEEDED_COLOR = "var(--status-success)";
export const FAILED_COLOR = "var(--status-failed)";
export const RUNNING_COLOR = "var(--status-running)";
