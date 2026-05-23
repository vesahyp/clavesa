/**
 * WidgetEditor — edit a dashboard's widgets.
 *
 * Each widget binds to a dataset by name and picks which result columns
 * the renderer reads. Position and size are set by dragging in the editor
 * grid below, not here.
 */

import { useState } from "react";
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
import type { DashboardDataset, DashboardWidget } from "@/lib/queries";
import type { DatasetColumns } from "@/hooks/useDatasetColumns";
import { cn } from "@/lib/utils";

import { uniqueName } from "./DatasetPanel";
import { WidgetTypePicker, type WidgetType } from "./WidgetTypePicker";

const WIDGET_TYPES = [
  "big_number",
  "line",
  "bar",
  "stacked_bar",
  "bar_line",
  "pie",
  "donut",
  "table",
] as const;

// Sensible default size per widget type on the 12-column grid.
const DEFAULT_SIZE: Record<WidgetType, { w: number; h: number }> = {
  big_number: { w: 3, h: 2 },
  line: { w: 6, h: 4 },
  bar: { w: 6, h: 4 },
  stacked_bar: { w: 6, h: 4 },
  bar_line: { w: 6, h: 4 },
  pie: { w: 4, h: 4 },
  donut: { w: 4, h: 4 },
  table: { w: 6, h: 5 },
};

interface WidgetEditorProps {
  widgets: DashboardWidget[];
  datasets: DashboardDataset[];
  /** Result columns per dataset name, for the field pickers. */
  columnsByDataset: Map<string, DatasetColumns>;
  onChange: (widgets: DashboardWidget[]) => void;
  /** Called with a freshly-added widget's id so the grid can reveal it. */
  onWidgetAdded?: (id: string) => void;
}

export function WidgetEditor({
  widgets,
  datasets,
  columnsByDataset,
  onChange,
  onWidgetAdded,
}: WidgetEditorProps) {
  const [pickerOpen, setPickerOpen] = useState(false);

  function update(i: number, patch: Partial<DashboardWidget>) {
    onChange(widgets.map((w, idx) => (idx === i ? { ...w, ...patch } : w)));
  }
  function remove(i: number) {
    onChange(widgets.filter((_, idx) => idx !== i));
  }
  function add(type: WidgetType) {
    setPickerOpen(false);
    const id = uniqueName("widget", widgets.map((w) => w.id));
    // Stack new widgets down the grid so they don't overlap on add.
    const y = widgets.reduce((m, w) => Math.max(m, w.layout.y + w.layout.h), 0);
    const dataset = datasets[0]?.name ?? "";
    // Pre-fill field mappings from the dataset's columns when known.
    const cols = columnsByDataset.get(dataset)?.columns ?? [];
    const size = DEFAULT_SIZE[type];
    // Pre-fill the type's fields from the first columns the dataset returns.
    const charty = type === "line" || type === "bar" || type === "bar_line";
    const pieLike = type === "pie" || type === "donut";
    onChange([
      ...widgets,
      {
        id,
        type,
        title: "New widget",
        dataset,
        value_field:
          type === "big_number" || pieLike ? (cols[pieLike ? 1 : 0] ?? "") : "",
        x_field:
          charty || type === "stacked_bar" || pieLike ? (cols[0] ?? "") : "",
        y_field: charty ? (cols[1] ?? "") : "",
        series_fields: type === "stacked_bar" ? cols.slice(1) : [],
        line_field: type === "bar_line" ? (cols[2] ?? "") : "",
        layout: { x: 0, y, w: size.w, h: size.h },
      },
    ]);
    onWidgetAdded?.(id);
  }

  return (
    <section className="space-y-3">
      <div className="flex items-center justify-between">
        <h2 className="font-mono text-sm font-semibold uppercase tracking-wide text-muted-foreground">
          Widgets
        </h2>
        <Button
          size="sm"
          variant="outline"
          onClick={() => setPickerOpen(true)}
        >
          <Plus className="mr-1 h-3.5 w-3.5" /> Add widget
        </Button>
        <WidgetTypePicker
          open={pickerOpen}
          onOpenChange={setPickerOpen}
          onPick={add}
        />
      </div>

      {widgets.length === 0 && (
        <p className="text-sm text-muted-foreground">No widgets yet.</p>
      )}

      {widgets.map((w, i) => (
        <div
          key={w.id}
          className="space-y-3 rounded-md border border-border bg-card p-4"
        >
          <div className="flex items-end gap-3">
            <div className="flex-1 space-y-1">
              <Label className="text-xs">Title</Label>
              <Input
                value={w.title}
                onChange={(e) => update(i, { title: e.target.value })}
              />
            </div>
            <div className="w-36 space-y-1">
              <Label className="text-xs">Type</Label>
              <Select value={w.type} onValueChange={(v) => update(i, { type: v })}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {WIDGET_TYPES.map((t) => (
                    <SelectItem key={t} value={t}>
                      {t}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="w-44 space-y-1">
              <Label className="text-xs">Dataset</Label>
              <Select
                value={w.dataset}
                onValueChange={(v) => update(i, { dataset: v })}
              >
                <SelectTrigger>
                  <SelectValue placeholder="pick a dataset" />
                </SelectTrigger>
                <SelectContent>
                  {datasets.map((d) => (
                    <SelectItem key={d.name} value={d.name}>
                      {d.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <Button
              size="icon"
              variant="ghost"
              onClick={() => remove(i)}
              aria-label={`Remove widget ${w.title}`}
            >
              <Trash2 className="h-4 w-4 text-muted-foreground" />
            </Button>
          </div>

          {/* Type-specific column mappings. */}
          {w.type !== "table" && (
            <div className="flex gap-3">
              {w.type === "big_number" && (
                <ColumnSelect
                  label="Value field"
                  value={w.value_field}
                  columns={columnsByDataset.get(w.dataset)}
                  onChange={(v) => update(i, { value_field: v })}
                />
              )}
              {(w.type === "line" || w.type === "bar") && (
                <>
                  <ColumnSelect
                    label="X field"
                    value={w.x_field}
                    columns={columnsByDataset.get(w.dataset)}
                    onChange={(v) => update(i, { x_field: v })}
                  />
                  <ColumnSelect
                    label="Y field"
                    value={w.y_field}
                    columns={columnsByDataset.get(w.dataset)}
                    onChange={(v) => update(i, { y_field: v })}
                  />
                </>
              )}
              {w.type === "stacked_bar" && (
                <>
                  <ColumnSelect
                    label="X field"
                    value={w.x_field}
                    columns={columnsByDataset.get(w.dataset)}
                    onChange={(v) => update(i, { x_field: v })}
                  />
                  <ColumnMultiSelect
                    label="Stacked value fields"
                    value={w.series_fields}
                    columns={columnsByDataset.get(w.dataset)}
                    exclude={w.x_field}
                    onChange={(v) => update(i, { series_fields: v })}
                  />
                </>
              )}
              {w.type === "bar_line" && (
                <>
                  <ColumnSelect
                    label="X field"
                    value={w.x_field}
                    columns={columnsByDataset.get(w.dataset)}
                    onChange={(v) => update(i, { x_field: v })}
                  />
                  <ColumnSelect
                    label="Bar field"
                    value={w.y_field}
                    columns={columnsByDataset.get(w.dataset)}
                    onChange={(v) => update(i, { y_field: v })}
                  />
                  <ColumnSelect
                    label="Line field"
                    value={w.line_field}
                    columns={columnsByDataset.get(w.dataset)}
                    onChange={(v) => update(i, { line_field: v })}
                  />
                </>
              )}
              {(w.type === "pie" || w.type === "donut") && (
                <>
                  <ColumnSelect
                    label="Category field"
                    value={w.x_field}
                    columns={columnsByDataset.get(w.dataset)}
                    onChange={(v) => update(i, { x_field: v })}
                  />
                  <ColumnSelect
                    label="Value field"
                    value={w.value_field}
                    columns={columnsByDataset.get(w.dataset)}
                    onChange={(v) => update(i, { value_field: v })}
                  />
                </>
              )}
            </div>
          )}
        </div>
      ))}
    </section>
  );
}

/**
 * ColumnSelect — pick a result column for a chart field.
 *
 * Prefers a dropdown of the bound dataset's actual columns. Falls back to
 * a free-text input when columns aren't available (no dataset bound, SQL
 * errored, or query not run yet) so authoring is never blocked. A saved
 * value that isn't in the current column list is kept and flagged rather
 * than dropped — silently clearing it would corrupt the saved spec.
 */
function ColumnSelect({
  label,
  value,
  columns,
  onChange,
}: {
  label: string;
  value: string;
  columns: DatasetColumns | undefined;
  onChange: (v: string) => void;
}) {
  if (columns?.isLoading) {
    return (
      <div className="flex-1 space-y-1">
        <Label className="text-xs">{label}</Label>
        <Select disabled value={undefined}>
          <SelectTrigger>
            <SelectValue placeholder="loading columns…" />
          </SelectTrigger>
          <SelectContent />
        </Select>
      </div>
    );
  }

  const cols = columns?.columns ?? [];
  if (cols.length === 0) {
    // No columns to offer — free-text so the user can still author.
    return (
      <div className="flex-1 space-y-1">
        <Label className="text-xs">{label}</Label>
        <Input
          value={value}
          onChange={(e) => onChange(e.target.value)}
          placeholder="result column name"
          className="font-mono"
        />
        <p className="text-xs text-muted-foreground">
          columns unavailable — run the SQL to populate
        </p>
      </div>
    );
  }

  // Keep a saved value that the current result no longer returns.
  const stale = value !== "" && !cols.includes(value);
  const options = stale ? [...cols, value] : cols;

  return (
    <div className="flex-1 space-y-1">
      <Label className="text-xs">{label}</Label>
      <Select value={value || undefined} onValueChange={onChange}>
        <SelectTrigger>
          <SelectValue placeholder="pick a column" />
        </SelectTrigger>
        <SelectContent>
          {options.map((c) => (
            <SelectItem key={c} value={c} className="font-mono">
              {c}
              {c === value && stale && (
                <span className="ml-1 text-muted-foreground">
                  (not in result)
                </span>
              )}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
}

/**
 * ColumnMultiSelect — pick several value columns for a stacked bar.
 *
 * Each chosen column becomes one stacked segment. Toggle chips when the
 * dataset's columns are known; a comma-separated text field otherwise so
 * authoring is never blocked.
 */
function ColumnMultiSelect({
  label,
  value,
  columns,
  exclude,
  onChange,
}: {
  label: string;
  value: string[];
  columns: DatasetColumns | undefined;
  /** Column to leave out of the choices (the x axis). */
  exclude?: string;
  onChange: (v: string[]) => void;
}) {
  if (columns?.isLoading) {
    return (
      <div className="flex-1 space-y-1">
        <Label className="text-xs">{label}</Label>
        <p className="text-xs text-muted-foreground">loading columns…</p>
      </div>
    );
  }

  const cols = (columns?.columns ?? []).filter((c) => c !== exclude);
  if (cols.length === 0) {
    return (
      <div className="flex-1 space-y-1">
        <Label className="text-xs">{label}</Label>
        <Input
          value={value.join(", ")}
          onChange={(e) =>
            onChange(
              e.target.value
                .split(",")
                .map((s) => s.trim())
                .filter(Boolean),
            )
          }
          placeholder="comma-separated column names"
          className="font-mono"
        />
        <p className="text-xs text-muted-foreground">
          columns unavailable — run the SQL to populate
        </p>
      </div>
    );
  }

  function toggle(c: string) {
    onChange(value.includes(c) ? value.filter((v) => v !== c) : [...value, c]);
  }

  return (
    <div className="flex-1 space-y-1">
      <Label className="text-xs">{label}</Label>
      <div className="flex flex-wrap gap-1.5 rounded-md border border-border bg-background p-2">
        {cols.map((c) => (
          <button
            key={c}
            type="button"
            onClick={() => toggle(c)}
            className={cn(
              "rounded px-2 py-0.5 font-mono text-xs transition-colors",
              value.includes(c)
                ? "bg-primary text-primary-foreground"
                : "bg-muted text-muted-foreground hover:bg-muted/70",
            )}
          >
            {c}
          </button>
        ))}
      </div>
      {value.length === 0 && (
        <p className="text-xs text-muted-foreground">
          none picked — every column except x is stacked
        </p>
      )}
    </div>
  );
}

