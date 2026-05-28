/**
 * EditorGrid — interactive drag/resize layout for the dashboard editor.
 *
 * Replaces the old read-only preview grid: every widget is a live render
 * that can be dragged (by its handle) and resized. Position and size fold
 * straight back into `widgets[].layout`, so Save persists exactly what the
 * author arranged.
 *
 * `compactType={null}` + `preventCollision={false}` keep absolute, hand-
 * placed positions — vertical compaction would silently rewrite every
 * widget's `y` and produce a spurious diff on Save.
 */

import { useEffect, useMemo, useRef } from "react";
import GridLayout, { WidthProvider, type Layout } from "react-grid-layout";
import { GripHorizontal } from "lucide-react";

import type { DashboardDataset, DashboardWidget } from "@/lib/queries";
import { cn } from "@/lib/utils";

import { Widget } from "./Widget";

import "react-grid-layout/css/styles.css";
import "react-resizable/css/styles.css";

const Grid = WidthProvider(GridLayout);

interface EditorGridProps {
  widgets: DashboardWidget[];
  datasets: DashboardDataset[];
  onChange: (widgets: DashboardWidget[]) => void;
  /** When set, the matching widget cell scrolls into view (freshly added). */
  scrollToId?: string;
  /** Currently-selected widget id; renders a ring on its cell. */
  selectedId?: string | null;
  /** Cell-body click handler — opens the chart-first drawer. */
  onSelect?: (id: string) => void;
  /**
   * Resolved `{{name}}` values forwarded to every widget query. Without
   * these, datasets referencing a control placeholder 400 on the column
   * fetch the moment the editor mounts — same shape the viewer passes.
   */
  params?: Record<string, string>;
}

export function EditorGrid({
  widgets,
  datasets,
  onChange,
  scrollToId,
  selectedId,
  onSelect,
  params,
}: EditorGridProps) {
  const datasetMap = useMemo(() => {
    const m = new Map<string, DashboardDataset>();
    for (const d of datasets) m.set(d.name, d);
    return m;
  }, [datasets]);

  const layout: Layout[] = widgets.map((w) => ({
    i: w.id,
    x: w.layout.x,
    y: w.layout.y,
    w: w.layout.w,
    h: w.layout.h,
    minW: 1,
    minH: 1,
  }));

  function handleLayoutChange(next: Layout[]) {
    const byId = new Map(next.map((l) => [l.i, l]));
    let changed = false;
    const updated = widgets.map((w) => {
      const l = byId.get(w.id);
      if (!l) return w;
      if (
        l.x === w.layout.x &&
        l.y === w.layout.y &&
        l.w === w.layout.w &&
        l.h === w.layout.h
      ) {
        return w;
      }
      changed = true;
      return { ...w, layout: { x: l.x, y: l.y, w: l.w, h: l.h } };
    });
    if (changed) onChange(updated);
  }

  const cellRefs = useRef(new Map<string, HTMLDivElement>());
  useEffect(() => {
    if (!scrollToId) return;
    cellRefs.current
      .get(scrollToId)
      ?.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }, [scrollToId]);

  return (
    <Grid
      className="layout"
      layout={layout}
      cols={12}
      rowHeight={80}
      margin={[16, 16]}
      compactType={null}
      preventCollision={false}
      draggableHandle=".widget-drag-handle"
      onLayoutChange={handleLayoutChange}
    >
      {widgets.map((w) => {
        const ds = datasetMap.get(w.dataset);
        const selected = selectedId === w.id;
        return (
          <div
            key={w.id}
            ref={(el) => {
              if (el) cellRefs.current.set(w.id, el);
              else cellRefs.current.delete(w.id);
            }}
            className={cn(
              "flex flex-col rounded-md transition-shadow",
              selected && "ring-2 ring-primary ring-offset-1 ring-offset-background",
            )}
          >
            <div className="widget-drag-handle flex cursor-move items-center justify-center rounded-t-md border border-b-0 border-border bg-muted/40 py-0.5 text-muted-foreground hover:bg-muted">
              <GripHorizontal className="h-3.5 w-3.5" />
            </div>
            <div
              className={cn("min-h-0 flex-1", onSelect && "cursor-pointer")}
              onClick={(e) => {
                // Don't fire on clicks bubbling from interactive widget
                // controls (Recharts tooltips, in-cell links). The handle
                // is a sibling, not a descendant, so dragging is
                // unaffected. The check walks from the click target up to
                // — but NOT including — the wrapper itself, so the
                // wrapper's own role doesn't self-match and swallow every
                // click.
                if (!onSelect) return;
                const wrapper = e.currentTarget;
                let n: HTMLElement | null = e.target as HTMLElement;
                while (n && n !== wrapper) {
                  if (n.matches("button, a, input, [role='button']")) return;
                  n = n.parentElement;
                }
                onSelect(w.id);
              }}
              tabIndex={onSelect ? 0 : undefined}
              onKeyDown={(e) => {
                if (!onSelect) return;
                if (e.key === "Enter" || e.key === " ") {
                  e.preventDefault();
                  onSelect(w.id);
                }
              }}
              aria-label={onSelect ? `Edit widget ${w.title}` : undefined}
            >
              <Widget
                widget={w}
                sql={ds?.sql ?? ""}
                dir={ds?.dir ?? ""}
                params={params}
                inGrid
              />
            </div>
          </div>
        );
      })}
    </Grid>
  );
}
