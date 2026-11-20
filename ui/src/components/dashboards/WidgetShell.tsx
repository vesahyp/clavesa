/**
 * WidgetShell — common card frame for every dashboard widget.
 *
 * Owns the loading skeleton (shaped like the widget: bars, a ring, rows or
 * a number), the error state and the empty state, so individual widget
 * components can focus on their viz. The shell sets the grid
 * placement from the widget's layout (1-indexed CSS-grid lines, since
 * the JSON layout uses 0-indexed coordinates this component translates).
 */

import { ReactNode } from "react";
import { useIsNarrow, useIsPhone } from "@/hooks/useIsPhone";
import { Inbox, TriangleAlert } from "lucide-react";

import { cn } from "@/lib/utils";
import { WidgetSkeleton } from "./chartKit";

import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";

interface WidgetShellProps {
  title: string;
  layout: { x: number; y: number; w: number; h: number };
  isLoading: boolean;
  error: unknown;
  isEmpty: boolean;
  children: ReactNode;
  /** Optional right-aligned summary text in the card header (row counts, etc.). */
  headerExtra?: ReactNode;
  /**
   * When true the shell is placed inside an editor grid cell (react-grid-
   * layout positions the cell wrapper, not the card), so it fills the
   * cell instead of setting its own CSS-grid lines from `layout`.
   */
  inGrid?: boolean;
  /** The widget type: a stat tile (big_number) may share a narrow row. */
  type?: string;
  /**
   * Place in reading order. The cards rise 60 ms apart, as ecarbrowser's
   * stat tiles do; from the ninth on they rise together.
   */
  order?: number;
  /** The card element, for the in-view check of chart motion. */
  cardRef?: React.Ref<HTMLDivElement>;
}

export function WidgetShell({
  title,
  layout,
  isLoading,
  error,
  isEmpty,
  children,
  headerExtra,
  inGrid = false,
  type,
  order = 0,
  cardRef,
}: WidgetShellProps) {
  // CSS grid is 1-indexed; the spec uses 0-indexed positions for ease of
  // hand-editing the JSON. `gridColumn: "span N / span N"` when we don't
  // care about the absolute column would be simpler, but absolute lets
  // widgets compose neatly without overlap on a hand-authored dashboard.
  // Inside an editor grid cell the wrapper owns placement, so skip this.
  //
  // Phone (below 768px): no absolute placement. A widget up to 6 of the
  // 12 columns wide takes half the row, a wider one the full row, in the
  // reading order the Dashboard page sorts them into (y, then x), so the
  // page never scrolls sideways at iPhone width.
  // Below 640px (useIsNarrow) every widget takes the full row, whatever
  // the JSON layout says (Vesa, 2026-11-18: "strange to have 2 widgets
  // when there clearly is not enough room"); only stat tiles up to 6
  // columns wide stay two per row, their numbers fit in half a phone.
  const phone = useIsPhone();
  const narrow = useIsNarrow();
  const phoneSpan = narrow
    ? type === "big_number" && layout.w <= 6
      ? 6
      : 12
    : layout.w <= 6
      ? 6
      : 12;
  const style = inGrid
    ? undefined
    : phone
      ? {
          gridColumn: `span ${phoneSpan}`,
          gridRow: `span ${layout.h}`,
        }
      : {
          gridColumn: `${layout.x + 1} / span ${layout.w}`,
          gridRow: `${layout.y + 1} / span ${layout.h}`,
        };
  const tile = type === "big_number";
  return (
    <Card
      ref={cardRef}
      style={{ ...style, animationDelay: `${Math.min(order, 8) * 60}ms` }}
      className={cn(
        "widget-rise rounded-xl border-white/[0.08] shadow-none",
        "bg-[linear-gradient(180deg,rgb(255_255_255/0.025),transparent_40%)] bg-card",
        inGrid ? "flex h-full w-full flex-col" : "flex min-w-0 flex-col",
      )}
      data-testid="dashboard-widget"
      data-widget-title={title}
      data-widget-type={type}
    >
      <CardHeader
        className={cn(
          "flex flex-row items-baseline justify-between gap-3 space-y-0 px-4 pt-3.5",
          tile ? "pb-1" : "pb-2",
        )}
      >
        <CardTitle
          className={cn(
            "min-w-0 font-medium leading-snug tracking-normal",
            tile ? "text-xs text-muted-foreground" : "text-[13px] text-foreground/90",
          )}
        >
          {title}
        </CardTitle>
        {headerExtra && (
          <div className="flex-shrink-0 whitespace-nowrap text-[11px] tabular-nums text-muted-foreground/70">
            {headerExtra}
          </div>
        )}
      </CardHeader>
      <CardContent className="flex min-h-0 flex-1 flex-col p-0">
        {isLoading && <WidgetSkeleton type={type} />}
        {error != null && (
          <div className="mx-4 mb-4 flex items-start gap-2.5 rounded-lg border border-destructive/30 bg-destructive/[0.07] p-3 text-xs">
            <TriangleAlert className="mt-0.5 h-4 w-4 flex-shrink-0 text-destructive" />
            <div className="min-w-0">
              <div className="font-medium text-destructive">Query failed</div>
              <p className="mt-1 line-clamp-6 break-words text-muted-foreground [overflow-wrap:anywhere]">
                {error instanceof Error ? error.message : String(error)}
              </p>
            </div>
          </div>
        )}
        {!isLoading && !error && isEmpty && (
          <div
            className={cn(
              "flex flex-1 items-center gap-2.5 text-muted-foreground",
              tile ? "px-4 pb-4" : "flex-col justify-center p-6 text-center",
            )}
            data-testid="widget-empty"
          >
            <span className="flex h-8 w-8 items-center justify-center rounded-full bg-white/[0.05]">
              <Inbox className="h-4 w-4" />
            </span>
            <div>
              <div className="text-xs font-medium text-foreground/80">No rows</div>
              {!tile && (
                <div className="mt-0.5 text-[11px]">Nothing in this range yet</div>
              )}
            </div>
          </div>
        )}
        {!isLoading && !error && !isEmpty && (
          <div className="flex min-h-0 flex-1 flex-col">{children}</div>
        )}
      </CardContent>
    </Card>
  );
}
