/**
 * ControlStrip — dashboard-level filter controls rendered above the
 * widget grid.
 *
 * Two control kinds today: `time_range` (preset picker + custom dates)
 * and `select` (dropdown populated by a SQL query or a static options
 * list). Values flow into the URL so a filtered view is shareable; the
 * resolved `params` map is passed back to the page via `onChange` and
 * gets handed down to every Widget, which substitutes `{{name}}` tokens
 * server-side at query time.
 */

import { useMemo } from "react";
import { useSearchParams } from "react-router-dom";

import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  useDashboardQuery,
  type DashboardControl,
} from "@/lib/queries";

/**
 * useDashboardParams — resolve URL search params + declared control
 * defaults into the param map a Dashboard hands down to its widgets.
 *
 * Lives here so the parent page and the strip share one source of
 * truth — the parent calls this on first render (no state, no effect),
 * so widget queries fire with the right values immediately and don't
 * 400 on a missing placeholder before a `useEffect` fills the map.
 */
export function useDashboardParams(
  controls: DashboardControl[],
): Record<string, string> {
  const [searchParams] = useSearchParams();
  return useMemo(() => {
    const out: Record<string, string> = {};
    for (const c of controls) {
      if (c.type === "time_range") {
        const startKey = `${c.name}.start`;
        const endKey = `${c.name}.end`;
        const start = searchParams.get(startKey);
        const end = searchParams.get(endKey);
        if (start && end) {
          out[startKey] = start;
          out[endKey] = end;
        } else {
          const { start: s, end: e } = resolveTimeRange(c.default || "last_30d");
          out[startKey] = s;
          out[endKey] = e;
        }
      } else if (c.type === "select") {
        const v = searchParams.get(c.name);
        if (v != null) {
          out[c.name] = v;
        } else if (c.default) {
          out[c.name] = c.default;
        } else if (c.options.length > 0) {
          out[c.name] = c.options[0];
        }
      }
    }
    return out;
  }, [controls, searchParams]);
}

interface ControlStripProps {
  controls: DashboardControl[];
  /**
   * Resolved params from the parent (via useDashboardParams). The
   * strip is presentational — it surfaces the values to the user and
   * pushes URL changes on edit; the parent re-resolves from the URL
   * via the same hook and re-renders the widgets.
   */
  params: Record<string, string>;
}

// TIME_PRESETS labels the short, ordered set the user picks from. The
// keys match resolveTimePreset in the Go service so the same preset
// chosen here renders the same window the CLI would pick.
const TIME_PRESETS: { key: string; label: string }[] = [
  { key: "last_24h", label: "Last 24 hours" },
  { key: "last_7d", label: "Last 7 days" },
  { key: "last_30d", label: "Last 30 days" },
  { key: "last_90d", label: "Last 90 days" },
  { key: "custom", label: "Custom range" },
];

export function ControlStrip({ controls, params }: ControlStripProps) {
  const [searchParams, setSearchParams] = useSearchParams();

  function update(next: Record<string, string | null>) {
    const sp = new URLSearchParams(searchParams);
    for (const [k, v] of Object.entries(next)) {
      if (v == null) sp.delete(k);
      else sp.set(k, v);
    }
    setSearchParams(sp, { replace: true });
  }

  if (controls.length === 0) return null;

  return (
    <div className="mb-4 flex flex-wrap items-end gap-3 rounded-md border border-border bg-card p-3">
      {controls.map((c) => {
        if (c.type === "time_range") {
          const startKey = `${c.name}.start`;
          const endKey = `${c.name}.end`;
          return (
            <TimeRangeControl
              key={c.name}
              control={c}
              startValue={params[startKey] ?? ""}
              endValue={params[endKey] ?? ""}
              onChange={(start, end) =>
                update({ [startKey]: start, [endKey]: end })
              }
            />
          );
        }
        if (c.type === "select") {
          return (
            <SelectControl
              key={c.name}
              control={c}
              value={params[c.name] ?? ""}
              onChange={(v) => update({ [c.name]: v })}
            />
          );
        }
        return null;
      })}
    </div>
  );
}

function TimeRangeControl({
  control,
  startValue,
  endValue,
  onChange,
}: {
  control: DashboardControl;
  startValue: string;
  endValue: string;
  onChange: (start: string, end: string) => void;
}) {
  // Infer which preset the current {start,end} pair matches; if none
  // does, the picker shows "Custom range" with the explicit inputs.
  const preset = matchPreset(startValue, endValue);
  const isCustom = preset === "custom";

  function onPresetPick(key: string) {
    if (key === "custom") {
      // Keep the current values, just reveal the inputs.
      onChange(startValue, endValue);
      return;
    }
    const { start, end } = resolveTimeRange(key);
    onChange(start, end);
  }

  return (
    <div className="flex items-end gap-2">
      <div className="space-y-1">
        <Label className="text-xs">{control.label || control.name}</Label>
        <Select value={preset} onValueChange={onPresetPick}>
          <SelectTrigger className="w-44">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {TIME_PRESETS.map((p) => (
              <SelectItem key={p.key} value={p.key}>
                {p.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
      {isCustom && (
        <>
          <div className="space-y-1">
            <Label className="text-xs">Start</Label>
            <Input
              type="datetime-local"
              value={isoToLocal(startValue)}
              onChange={(e) =>
                onChange(localToISO(e.target.value), endValue)
              }
              className="w-52 font-mono text-xs"
            />
          </div>
          <div className="space-y-1">
            <Label className="text-xs">End</Label>
            <Input
              type="datetime-local"
              value={isoToLocal(endValue)}
              onChange={(e) =>
                onChange(startValue, localToISO(e.target.value))
              }
              className="w-52 font-mono text-xs"
            />
          </div>
        </>
      )}
    </div>
  );
}

function SelectControl({
  control,
  value,
  onChange,
}: {
  control: DashboardControl;
  value: string;
  onChange: (v: string) => void;
}) {
  // SQL-backed dropdown reuses the dashboard query hook; the result's
  // first column drives the options. Static `options` are the fallback
  // when no SQL is set.
  const query = useDashboardQuery(control.sql || "", control.dir || "");
  const dynamicOptions = useMemo(() => {
    if (!query.data) return [] as string[];
    return query.data.rows
      .map((row) => row[0] ?? "")
      .filter((v) => v !== "")
      // De-dup defensively — most authors use DISTINCT but a careless
      // SELECT shouldn't render twenty identical entries.
      .filter((v, i, arr) => arr.indexOf(v) === i);
  }, [query.data]);
  const options =
    control.sql && dynamicOptions.length > 0 ? dynamicOptions : control.options;

  // Keep a saved value the current option set no longer contains so
  // shared URLs don't silently change selection.
  const stale = value !== "" && !options.includes(value);
  const display = stale ? [...options, value] : options;

  return (
    <div className="space-y-1">
      <Label className="text-xs">{control.label || control.name}</Label>
      <Select value={value || undefined} onValueChange={onChange}>
        <SelectTrigger className="w-48">
          <SelectValue
            placeholder={query.isLoading ? "loading…" : "pick a value"}
          />
        </SelectTrigger>
        <SelectContent>
          {display.map((o) => (
            <SelectItem key={o} value={o} className="font-mono text-xs">
              {o}
              {o === value && stale && (
                <span className="ml-1 text-muted-foreground">
                  (not in results)
                </span>
              )}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
}

// resolveTimeRange mirrors the Go resolveTimePreset helper. Same preset
// keys, same window math; computed at "now" so a custom-range stays
// stable but a preset re-evaluates each load.
function resolveTimeRange(preset: string): { start: string; end: string } {
  const now = new Date();
  const end = now.toISOString();
  let ms = 30 * 24 * 60 * 60 * 1000;
  switch (preset) {
    case "last_24h":
      ms = 24 * 60 * 60 * 1000;
      break;
    case "last_7d":
      ms = 7 * 24 * 60 * 60 * 1000;
      break;
    case "last_30d":
      ms = 30 * 24 * 60 * 60 * 1000;
      break;
    case "last_90d":
      ms = 90 * 24 * 60 * 60 * 1000;
      break;
  }
  return { start: new Date(now.getTime() - ms).toISOString(), end };
}

// matchPreset returns the preset key whose window matches the current
// {start, end} pair (within ±2 minutes — preset windows are computed at
// load time so they drift second-by-second). Falls back to "custom".
function matchPreset(start: string, end: string): string {
  if (!start || !end) return "last_30d";
  const s = new Date(start).getTime();
  const e = new Date(end).getTime();
  if (!Number.isFinite(s) || !Number.isFinite(e)) return "custom";
  const span = e - s;
  const candidates: { key: string; ms: number }[] = [
    { key: "last_24h", ms: 24 * 60 * 60 * 1000 },
    { key: "last_7d", ms: 7 * 24 * 60 * 60 * 1000 },
    { key: "last_30d", ms: 30 * 24 * 60 * 60 * 1000 },
    { key: "last_90d", ms: 90 * 24 * 60 * 60 * 1000 },
  ];
  // Preset windows end at "now"; allow a small slack so a 30-second
  // page-load gap doesn't flip the dropdown to Custom.
  const now = Date.now();
  if (Math.abs(e - now) > 5 * 60 * 1000) return "custom";
  for (const c of candidates) {
    if (Math.abs(span - c.ms) < 5 * 60 * 1000) return c.key;
  }
  return "custom";
}

// isoToLocal converts an ISO timestamp into the `YYYY-MM-DDTHH:MM`
// shape the native datetime-local input expects (no seconds, no zone).
function isoToLocal(iso: string): string {
  if (!iso) return "";
  const d = new Date(iso);
  if (!Number.isFinite(d.getTime())) return "";
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

function localToISO(local: string): string {
  if (!local) return "";
  const d = new Date(local);
  if (!Number.isFinite(d.getTime())) return "";
  return d.toISOString();
}

