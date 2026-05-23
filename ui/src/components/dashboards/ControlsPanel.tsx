/**
 * ControlsPanel — edit a dashboard's dashboard-level controls.
 *
 * A `time_range` control writes two placeholders (`<name>.start` and
 * `<name>.end`); a `select` control writes one (`<name>`). The author
 * references them in dataset SQL as `{{name.start}}` etc. — the
 * Available-placeholders hint at the top of this panel surfaces the
 * current set so authoring isn't blind.
 */

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
import { CodeEditor } from "@/components/CodeEditor";
import type { DashboardControl, PipelineInfo } from "@/lib/queries";

import { uniqueName } from "./DatasetPanel";

interface ControlsPanelProps {
  controls: DashboardControl[];
  pipelines: PipelineInfo[];
  onChange: (controls: DashboardControl[]) => void;
}

const TIME_PRESET_OPTIONS = [
  { value: "last_24h", label: "Last 24 hours" },
  { value: "last_7d", label: "Last 7 days" },
  { value: "last_30d", label: "Last 30 days" },
  { value: "last_90d", label: "Last 90 days" },
];

export function ControlsPanel({
  controls,
  pipelines,
  onChange,
}: ControlsPanelProps) {
  function update(i: number, patch: Partial<DashboardControl>) {
    onChange(controls.map((c, idx) => (idx === i ? { ...c, ...patch } : c)));
  }
  function remove(i: number) {
    onChange(controls.filter((_, idx) => idx !== i));
  }
  function addTimeRange() {
    const name = uniqueName("tr", controls.map((c) => c.name));
    onChange([
      ...controls,
      {
        name,
        type: "time_range",
        label: "Time range",
        default: "last_30d",
        dir: "",
        sql: "",
        options: [],
      },
    ]);
  }
  function addSelect() {
    const name = uniqueName("filter", controls.map((c) => c.name));
    onChange([
      ...controls,
      {
        name,
        type: "select",
        label: "Filter",
        default: "",
        dir: pipelines[0]?.dir ?? "",
        sql: "",
        options: [],
      },
    ]);
  }

  return (
    <section className="space-y-3">
      <div className="flex items-center justify-between">
        <h2 className="font-mono text-sm font-semibold uppercase tracking-wide text-muted-foreground">
          Controls
        </h2>
        <div className="flex gap-2">
          <Button size="sm" variant="outline" onClick={addTimeRange}>
            <Plus className="mr-1 h-3.5 w-3.5" /> Time range
          </Button>
          <Button size="sm" variant="outline" onClick={addSelect}>
            <Plus className="mr-1 h-3.5 w-3.5" /> Select
          </Button>
        </div>
      </div>

      {controls.length === 0 ? (
        <p className="text-sm text-muted-foreground">
          No controls yet. Add a time range or a dropdown to let viewers filter
          this dashboard. Reference values from dataset SQL with{" "}
          <code className="font-mono">{`{{name}}`}</code> (or{" "}
          <code className="font-mono">{`{{name.start}}`}</code> /{" "}
          <code className="font-mono">{`{{name.end}}`}</code> for a time range).
        </p>
      ) : (
        <PlaceholderHint controls={controls} />
      )}

      {controls.map((c, i) => (
        <div
          key={i}
          className="space-y-3 rounded-md border border-border bg-card p-4"
        >
          <div className="flex items-end gap-3">
            <div className="flex-1 space-y-1">
              <Label className="text-xs">Name</Label>
              <Input
                value={c.name}
                onChange={(e) => update(i, { name: e.target.value })}
                placeholder="filter"
                className="font-mono"
              />
              <p className="text-xs text-muted-foreground">
                placeholder:{" "}
                <code className="font-mono">
                  {c.type === "time_range"
                    ? `{{${c.name || "name"}.start}}, {{${c.name || "name"}.end}}`
                    : `{{${c.name || "name"}}}`}
                </code>
              </p>
            </div>
            <div className="w-44 space-y-1">
              <Label className="text-xs">Label</Label>
              <Input
                value={c.label}
                onChange={(e) => update(i, { label: e.target.value })}
                placeholder={c.type === "time_range" ? "Time range" : "Filter"}
              />
            </div>
            <div className="w-36 space-y-1">
              <Label className="text-xs">Type</Label>
              <Input value={c.type} disabled className="font-mono" />
            </div>
            <Button
              size="icon"
              variant="ghost"
              onClick={() => remove(i)}
              aria-label={`Remove control ${c.name}`}
            >
              <Trash2 className="h-4 w-4 text-muted-foreground" />
            </Button>
          </div>

          {c.type === "time_range" && (
            <div className="w-48 space-y-1">
              <Label className="text-xs">Default preset</Label>
              <Select
                value={c.default || "last_30d"}
                onValueChange={(v) => update(i, { default: v })}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {TIME_PRESET_OPTIONS.map((p) => (
                    <SelectItem key={p.value} value={p.value}>
                      {p.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          )}

          {c.type === "select" && (
            <div className="space-y-3">
              <div className="flex items-end gap-3">
                <div className="flex-1 space-y-1">
                  <Label className="text-xs">Default value</Label>
                  <Input
                    value={c.default}
                    onChange={(e) => update(i, { default: e.target.value })}
                    placeholder="(first option if blank)"
                    className="font-mono"
                  />
                </div>
                <div className="flex-1 space-y-1">
                  <Label className="text-xs">Options pipeline</Label>
                  {pipelines.length > 0 ? (
                    <Select
                      value={c.dir || undefined}
                      onValueChange={(v) => update(i, { dir: v })}
                    >
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
                      value={c.dir}
                      onChange={(e) => update(i, { dir: e.target.value })}
                      placeholder="pipeline dir"
                      className="font-mono"
                    />
                  )}
                </div>
              </div>
              <div className="space-y-1">
                <Label className="text-xs">
                  Options SQL (first column populates the dropdown)
                </Label>
                <div className="overflow-hidden rounded-md border border-border">
                  <CodeEditor
                    value={c.sql}
                    onValueChange={(v) => update(i, { sql: v })}
                    language="sql"
                    height={120}
                    lineNumbers={false}
                    wordWrap
                  />
                </div>
                <p className="text-xs text-muted-foreground">
                  e.g.{" "}
                  <code className="font-mono">
                    SELECT DISTINCT site FROM events ORDER BY site
                  </code>
                </p>
              </div>
              <div className="space-y-1">
                <Label className="text-xs">
                  Static options (fallback; comma-separated)
                </Label>
                <Input
                  value={c.options.join(", ")}
                  onChange={(e) =>
                    update(i, {
                      options: e.target.value
                        .split(",")
                        .map((s) => s.trim())
                        .filter(Boolean),
                    })
                  }
                  placeholder="acme, globex, initech"
                  className="font-mono"
                />
              </div>
            </div>
          )}
        </div>
      ))}
    </section>
  );
}

function PlaceholderHint({ controls }: { controls: DashboardControl[] }) {
  const placeholders = controls.flatMap((c) =>
    c.type === "time_range"
      ? [`{{${c.name}.start}}`, `{{${c.name}.end}}`]
      : [`{{${c.name}}}`],
  );
  return (
    <p className="text-xs text-muted-foreground">
      Available in dataset SQL:{" "}
      {placeholders.map((p, i) => (
        <span key={p}>
          {i > 0 && ", "}
          <code className="font-mono">{p}</code>
        </span>
      ))}
    </p>
  );
}
