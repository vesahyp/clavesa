/**
 * WidgetShell — common card frame for every dashboard widget.
 *
 * Owns the loading skeleton, error state, and empty state so individual
 * widget components can focus on their viz. The shell sets the grid
 * placement from the widget's layout (1-indexed CSS-grid lines, since
 * the JSON layout uses 0-indexed coordinates this component translates).
 */

import { ReactNode } from "react";
import { FileWarning } from "lucide-react";

import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";

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
}: WidgetShellProps) {
  // CSS grid is 1-indexed; the spec uses 0-indexed positions for ease of
  // hand-editing the JSON. `gridColumn: "span N / span N"` when we don't
  // care about the absolute column would be simpler, but absolute lets
  // widgets compose neatly without overlap on a hand-authored dashboard.
  // Inside an editor grid cell the wrapper owns placement, so skip this.
  const style = inGrid
    ? undefined
    : {
        gridColumn: `${layout.x + 1} / span ${layout.w}`,
        gridRow: `${layout.y + 1} / span ${layout.h}`,
      };
  return (
    <Card
      style={style}
      className={inGrid ? "flex h-full w-full flex-col" : "flex flex-col"}
    >
      <CardHeader className="flex flex-row items-center justify-between gap-3 pb-3">
        <CardTitle className="text-sm font-medium text-foreground">
          {title}
        </CardTitle>
        {headerExtra && (
          <div className="text-xs text-muted-foreground">{headerExtra}</div>
        )}
      </CardHeader>
      <CardContent className="flex flex-1 flex-col p-0">
        {isLoading && (
          <div className="space-y-2 p-6">
            <Skeleton className="h-4 w-full" />
            <Skeleton className="h-4 w-3/4" />
            <Skeleton className="h-4 w-2/3" />
          </div>
        )}
        {error != null && (
          <div className="m-4 flex items-start gap-2 rounded-md border border-destructive/40 bg-destructive/5 p-3 text-xs">
            <FileWarning className="mt-0.5 h-3.5 w-3.5 flex-shrink-0 text-destructive" />
            <div className="min-w-0">
              <div className="font-medium text-destructive">Query failed</div>
              <p className="mt-0.5 break-words text-muted-foreground">
                {error instanceof Error ? error.message : String(error)}
              </p>
            </div>
          </div>
        )}
        {!isLoading && !error && isEmpty && (
          <div className="flex flex-1 items-center justify-center p-6 text-xs text-muted-foreground">
            No rows
          </div>
        )}
        {!isLoading && !error && !isEmpty && (
          <div className="flex flex-1 flex-col">{children}</div>
        )}
      </CardContent>
    </Card>
  );
}
