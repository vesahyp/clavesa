/**
 * DatasetPanel — edit a dashboard's named datasets.
 *
 * Each dataset is a reusable SQL query with its own pipeline dir, so one
 * dashboard can blend tables from multiple pipelines (and mix local +
 * cloud). Widgets bind to a dataset by name. The catalog browser beside
 * each SQL editor lists the pipeline's tables and columns so authoring
 * isn't blind guessing of identifiers.
 */

import { useRef } from "react";
import type { EditorView } from "@codemirror/view";
import { Plus, Trash2 } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { DashboardDataset, PipelineInfo } from "@/lib/queries";

import { CodeEditor } from "@/components/CodeEditor";
import { CatalogBrowser } from "./CatalogBrowser";
import { SqlPreview } from "./SqlPreview";

interface DatasetPanelProps {
  datasets: DashboardDataset[];
  pipelines: PipelineInfo[];
  onChange: (datasets: DashboardDataset[]) => void;
}

export function DatasetPanel({ datasets, pipelines, onChange }: DatasetPanelProps) {
  function update(i: number, patch: Partial<DashboardDataset>) {
    onChange(datasets.map((d, idx) => (idx === i ? { ...d, ...patch } : d)));
  }
  function remove(i: number) {
    onChange(datasets.filter((_, idx) => idx !== i));
  }
  function add() {
    const name = uniqueName("dataset", datasets.map((d) => d.name));
    onChange([
      ...datasets,
      { name, dir: pipelines[0]?.dir ?? "", sql: "SELECT 1 AS n" },
    ]);
  }

  return (
    <section className="space-y-3">
      <div className="flex items-center justify-between">
        <h2 className="font-mono text-sm font-semibold uppercase tracking-wide text-muted-foreground">
          Datasets
        </h2>
        <Button size="sm" variant="outline" onClick={add}>
          <Plus className="mr-1 h-3.5 w-3.5" /> Add dataset
        </Button>
      </div>

      {datasets.length === 0 && (
        <p className="text-sm text-muted-foreground">
          No datasets yet. A widget needs a dataset to query — add one.
        </p>
      )}

      {datasets.map((ds, i) => (
        <DatasetCard
          key={i}
          dataset={ds}
          pipelines={pipelines}
          onChange={(patch) => update(i, patch)}
          onRemove={() => remove(i)}
        />
      ))}
    </section>
  );
}

interface DatasetCardProps {
  dataset: DashboardDataset;
  pipelines: PipelineInfo[];
  onChange: (patch: Partial<DashboardDataset>) => void;
  onRemove: () => void;
}

function DatasetCard({ dataset: ds, pipelines, onChange, onRemove }: DatasetCardProps) {
  // CM6 view, captured via onReady. CatalogBrowser uses it to insert an
  // identifier at the cursor.
  const viewRef = useRef<EditorView | null>(null);

  function insert(text: string) {
    const view = viewRef.current;
    if (!view) return;
    const { from, to } = view.state.selection.main;
    view.dispatch({
      changes: { from, to, insert: text },
      selection: { anchor: from + text.length },
    });
    view.focus();
  }

  return (
    <div className="space-y-3 rounded-md border border-border bg-card p-4">
      <div className="flex items-end gap-3">
        <div className="flex-1 space-y-1">
          <Label className="text-xs">Name</Label>
          <Input
            value={ds.name}
            onChange={(e) => onChange({ name: e.target.value })}
            placeholder="revenue"
            className="font-mono"
          />
        </div>
        <div className="flex-1 space-y-1">
          <Label className="text-xs">Pipeline</Label>
          {pipelines.length > 0 ? (
            <Select value={ds.dir} onValueChange={(v) => onChange({ dir: v })}>
              <SelectTrigger>
                <SelectValue placeholder="pick a pipeline" />
              </SelectTrigger>
              <SelectContent>
                {pipelines.map((p) => (
                  <SelectItem key={p.dir} value={p.dir}>
                    {p.dir}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          ) : (
            <Input
              value={ds.dir}
              onChange={(e) => onChange({ dir: e.target.value })}
              placeholder="pipeline dir"
              className="font-mono"
            />
          )}
        </div>
        <Button
          size="icon"
          variant="ghost"
          onClick={onRemove}
          aria-label={`Remove dataset ${ds.name}`}
        >
          <Trash2 className="h-4 w-4 text-muted-foreground" />
        </Button>
      </div>
      <div className="space-y-1">
        <Label className="text-xs">SQL</Label>
        <div className="flex gap-3">
          <div className="min-w-0 flex-1 overflow-hidden rounded-md border border-border">
            <CodeEditor
              value={ds.sql}
              onValueChange={(v) => onChange({ sql: v })}
              language="sql"
              height={200}
              lineNumbers={false}
              wordWrap
              onReady={(view) => {
                viewRef.current = view;
              }}
            />
          </div>
          <CatalogBrowser dir={ds.dir} onInsert={insert} />
        </div>
      </div>
      <SqlPreview dir={ds.dir} sql={ds.sql} />
    </div>
  );
}

// uniqueName returns base, base2, base3, … so a freshly-added dataset
// never collides with an existing name (the save would 400).
export function uniqueName(base: string, taken: string[]): string {
  if (!taken.includes(base)) return base;
  for (let n = 2; ; n++) {
    const candidate = `${base}${n}`;
    if (!taken.includes(candidate)) return candidate;
  }
}
