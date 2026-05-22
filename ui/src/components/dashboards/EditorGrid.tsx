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
}

export function EditorGrid({
  widgets,
  datasets,
  onChange,
  scrollToId,
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
        return (
          <div
            key={w.id}
            ref={(el) => {
              if (el) cellRefs.current.set(w.id, el);
              else cellRefs.current.delete(w.id);
            }}
            className="flex flex-col"
          >
            <div className="widget-drag-handle flex cursor-move items-center justify-center rounded-t-md border border-b-0 border-border bg-muted/40 py-0.5 text-muted-foreground hover:bg-muted">
              <GripHorizontal className="h-3.5 w-3.5" />
            </div>
            <div className="min-h-0 flex-1">
              <Widget
                widget={w}
                sql={ds?.sql ?? ""}
                dir={ds?.dir ?? ""}
                inGrid
              />
            </div>
          </div>
        );
      })}
    </Grid>
  );
}
