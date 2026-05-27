/**
 * DatasetsSidebar — left rail of the chart-first dashboard editor.
 *
 * Lists user-named (shared) datasets with the number of widgets bound
 * to each, plus an "Add widget" entry point. Inline-anonymous datasets
 * (`__widget_<id>` prefix) are intentionally filtered out by the parent
 * before they reach here — they're owned by a single widget and edited
 * through that widget's drawer.
 *
 * Clicking a dataset selects the first widget bound to it (or creates
 * one if none exists yet), opening the drawer in Shared mode pointing
 * at that dataset. That's the path for editing a dataset's SQL after it
 * already has consumers.
 */

import { Database, Plus } from "lucide-react";

import { Button } from "@/components/ui/button";
import type { DashboardDataset, DashboardWidget } from "@/lib/queries";

interface DatasetsSidebarProps {
  datasets: DashboardDataset[];
  widgets: DashboardWidget[];
  onAddWidget: () => void;
  onPickDataset: (name: string) => void;
}

export function DatasetsSidebar({
  datasets,
  widgets,
  onAddWidget,
  onPickDataset,
}: DatasetsSidebarProps) {
  const widgetsByDataset = new Map<string, number>();
  for (const w of widgets) {
    widgetsByDataset.set(w.dataset, (widgetsByDataset.get(w.dataset) ?? 0) + 1);
  }
  return (
    <aside className="space-y-3">
      <Button
        size="sm"
        className="w-full"
        onClick={onAddWidget}
      >
        <Plus className="mr-1 h-3.5 w-3.5" /> Add widget
      </Button>

      <div className="space-y-1">
        <p className="font-mono text-xs font-semibold uppercase tracking-wide text-muted-foreground">
          Shared datasets
        </p>
        {datasets.length === 0 ? (
          <p className="text-xs text-muted-foreground">
            None yet. Promote a widget's inline query to share it.
          </p>
        ) : (
          <ul className="space-y-1">
            {datasets.map((d) => {
              const count = widgetsByDataset.get(d.name) ?? 0;
              return (
                <li key={d.name}>
                  <button
                    type="button"
                    onClick={() => onPickDataset(d.name)}
                    className="flex w-full items-center gap-2 rounded-md border border-transparent px-2 py-1.5 text-left text-xs hover:border-border hover:bg-muted/40"
                    title={`${d.name} · ${d.dir || "(no pipeline)"}`}
                  >
                    <Database className="h-3.5 w-3.5 flex-shrink-0 text-muted-foreground" />
                    <span className="min-w-0 flex-1 truncate font-mono">
                      {d.name}
                    </span>
                    <span className="flex-shrink-0 text-muted-foreground">
                      {count}
                    </span>
                  </button>
                </li>
              );
            })}
          </ul>
        )}
      </div>
    </aside>
  );
}
