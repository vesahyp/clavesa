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

import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useQueryClient } from "@tanstack/react-query";

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
import {
  normaliseExpr,
  parseRelative,
  resolveTimeRange as resolveTimeRangeShared,
  TIME_RANGE_PRESETS,
} from "@/lib/timeRange";

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
        const relKey = `${c.name}.rel`;
        // Resolution priority:
        //   1. Explicit absolute pair on the URL → freeze the window.
        //      The right shape for shareable point-in-time links.
        //   2. Relative expression on the URL → re-evaluate every render.
        //      The default the picker emits; reload shows fresh `now-1h`.
        //   3. Declared control default → same back-compat path as before.
        const start = searchParams.get(startKey);
        const end = searchParams.get(endKey);
        const rel = searchParams.get(relKey);
        if (start && end) {
          out[startKey] = start;
          out[endKey] = end;
        } else if (rel) {
          const { start: s, end: e } = resolveTimeRangeShared(rel);
          out[startKey] = s;
          out[endKey] = e;
        } else {
          const { start: s, end: e } = resolveTimeRangeShared(
            c.default || "now-30d",
          );
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

// Preset options for the dropdown — sourced from the shared
// `TIME_RANGE_PRESETS` table so the Go side, the picker, and the
// default-value editor in `ControlsPanel` all agree on what's offered.
// "Custom range" is the explicit absolute / relative escape hatch.
const TIME_PRESET_OPTIONS: { value: string; label: string }[] = [
  ...TIME_RANGE_PRESETS.map((p) => ({ value: p.expr, label: p.label })),
  { value: "custom", label: "Custom range" },
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
      <RefreshControl />
      {controls.map((c) => {
        if (c.type === "time_range") {
          const startKey = `${c.name}.start`;
          const endKey = `${c.name}.end`;
          const relKey = `${c.name}.rel`;
          // The picker needs to know what's *on the URL*, not what
          // `useDashboardParams` resolved — the latter always fills
          // start/end, so checking it would make every state look
          // "absolute" and the picker would stick on Custom forever.
          const urlStart = searchParams.get(startKey) ?? "";
          const urlEnd = searchParams.get(endKey) ?? "";
          const urlRel = searchParams.get(relKey) ?? "";
          return (
            <TimeRangeControl
              key={c.name}
              control={c}
              startValue={urlStart}
              endValue={urlEnd}
              relValue={urlRel}
              onChange={(updates) => {
                const next: Record<string, string | null> = {};
                for (const [k, v] of Object.entries(updates)) {
                  const key =
                    k === "start" ? startKey : k === "end" ? endKey : relKey;
                  next[key] = v;
                }
                update(next);
              }}
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
  relValue,
  onChange,
}: {
  control: DashboardControl;
  startValue: string;
  endValue: string;
  relValue: string;
  onChange: (updates: { start?: string | null; end?: string | null; rel?: string | null }) => void;
}) {
  // What's the picker showing?
  //   - rel set + matches a preset → that preset.
  //   - rel set + custom relative   → "custom" (reveals the relative field).
  //   - absolute start/end set      → "custom" (reveals absolute fields).
  //   - nothing set                 → falls back to the declared default.
  const isAbsolute = !!startValue && !!endValue;
  const presetExpr = isAbsolute
    ? "custom"
    : pickPresetExpr(relValue, control.default);
  const isCustom = presetExpr === "custom";

  function onPresetPick(value: string) {
    if (value === "custom") {
      // "Switch to custom" = freeze the current window as absolute so
      // the user has a concrete starting point to tweak. Drops the rel
      // param. This also flips `presetExpr` to "custom" above (because
      // isAbsolute is now true) so the inline inputs reveal.
      if (isAbsolute) {
        // Already custom-absolute — nothing to do.
        return;
      }
      const seed = resolveTimeRangeShared(relValue || control.default);
      onChange({ start: seed.start, end: seed.end, rel: null });
      return;
    }
    // Adopting a preset means: clear any frozen absolute window, set
    // the relative expression so the page re-evaluates each render.
    onChange({ start: null, end: null, rel: value });
  }

  return (
    <div className="flex items-end gap-2">
      <div className="space-y-1">
        <Label className="text-xs">{control.label || control.name}</Label>
        <Select value={presetExpr} onValueChange={onPresetPick}>
          <SelectTrigger className="w-44">
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
      {isCustom && (
        <>
          <div className="space-y-1">
            <Label className="text-xs">Relative</Label>
            <RelativeInput
              value={relValue}
              onCommit={(next) =>
                onChange({ start: null, end: null, rel: next || null })
              }
            />
          </div>
          <div className="space-y-1">
            <Label className="text-xs">Start (absolute)</Label>
            <Input
              type="datetime-local"
              value={isoToLocal(startValue)}
              onChange={(e) =>
                onChange({
                  start: localToISO(e.target.value),
                  end: endValue,
                  rel: null,
                })
              }
              className="w-52 font-mono text-xs"
            />
          </div>
          <div className="space-y-1">
            <Label className="text-xs">End (absolute)</Label>
            <Input
              type="datetime-local"
              value={isoToLocal(endValue)}
              onChange={(e) =>
                onChange({
                  start: startValue,
                  end: localToISO(e.target.value),
                  rel: null,
                })
              }
              className="w-52 font-mono text-xs"
            />
          </div>
        </>
      )}
    </div>
  );
}

/**
 * RefreshControl — viewer-side auto-refresh dropdown. Reads `?refresh=`
 * from the URL; when set to a non-`off` value, invalidates every
 * `["dashboards", "query", …]` cache entry on the interval so widgets
 * re-fire. Off (default) preserves the pre-Slice-E behaviour where
 * widgets only re-fetched on React Query's stale-time expiry.
 */
const REFRESH_OPTIONS: { value: string; label: string; ms: number }[] = [
  { value: "off", label: "Off", ms: 0 },
  { value: "30s", label: "30s", ms: 30_000 },
  { value: "1m", label: "1 min", ms: 60_000 },
  { value: "5m", label: "5 min", ms: 5 * 60_000 },
  { value: "15m", label: "15 min", ms: 15 * 60_000 },
];

function RefreshControl() {
  const [searchParams, setSearchParams] = useSearchParams();
  const qc = useQueryClient();
  const raw = searchParams.get("refresh") ?? "off";
  const opt =
    REFRESH_OPTIONS.find((o) => o.value === raw) ?? REFRESH_OPTIONS[0];

  useEffect(() => {
    if (opt.ms <= 0) return;
    const id = window.setInterval(() => {
      // Hits every widget query AND the dataset-column probes; matches
      // the React-Query key prefix used by useDashboardQuery /
      // useDatasetColumns / SqlPreview.
      void qc.invalidateQueries({ queryKey: ["dashboards", "query"] });
    }, opt.ms);
    return () => window.clearInterval(id);
  }, [opt.ms, qc]);

  function onPick(value: string) {
    const sp = new URLSearchParams(searchParams);
    if (value === "off") sp.delete("refresh");
    else sp.set("refresh", value);
    setSearchParams(sp, { replace: true });
  }

  return (
    <div className="space-y-1">
      <Label className="text-xs">Refresh</Label>
      <Select value={opt.value} onValueChange={onPick}>
        <SelectTrigger className="w-24">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {REFRESH_OPTIONS.map((o) => (
            <SelectItem key={o.value} value={o.value}>
              {o.label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
}

/**
 * pickPresetExpr — match the current `rel` against the preset table,
 * falling back to "custom" when it's something the dropdown can't
 * represent (typed relative expression, or absolute-only state). If
 * `rel` is empty, the declared default takes its place so a saved
 * dashboard's `last_24h` reads back as "Last 24 hours" on the picker.
 */
function pickPresetExpr(relValue: string, controlDefault: string): string {
  const candidate = normaliseExpr(relValue || controlDefault || "now-30d");
  if (TIME_RANGE_PRESETS.some((p) => p.expr === candidate)) return candidate;
  return "custom";
}

/**
 * RelativeInput — small text input that validates a `now-<n><unit>`
 * expression on commit (Enter / blur). Empty input clears the relative
 * filter; an invalid expression doesn't update URL state — the input
 * stays red until corrected. Cheap UX without a Form library.
 */
function RelativeInput({
  value,
  onCommit,
}: {
  value: string;
  onCommit: (next: string) => void;
}) {
  const [draft, setDraft] = useMemoizedDraft(value);
  let bad = false;
  if (draft.trim() !== "") {
    try {
      parseRelative(draft);
    } catch {
      bad = true;
    }
  }
  function commit() {
    if (draft === value) return;
    if (bad) return;
    onCommit(draft.trim());
  }
  return (
    <Input
      value={draft}
      onChange={(e) => setDraft(e.target.value)}
      onBlur={commit}
      onKeyDown={(e) => {
        if (e.key === "Enter") {
          e.preventDefault();
          commit();
        }
      }}
      placeholder="now-1h"
      className={`w-32 font-mono text-xs${bad ? " border-destructive" : ""}`}
    />
  );
}

/**
 * useMemoizedDraft — mirror an upstream value into local state. Resets
 * when the upstream changes (e.g. user picks a preset) so the draft
 * doesn't drift away from the canonical source.
 */
function useMemoizedDraft(value: string): [string, (v: string) => void] {
  const [draft, setDraft] = useState(value);
  useEffect(() => {
    setDraft(value);
  }, [value]);
  return [draft, setDraft];
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

