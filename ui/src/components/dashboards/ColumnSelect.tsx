/**
 * ColumnSelect / ColumnMultiSelect — pick result columns for chart fields.
 *
 * Prefers a dropdown of the bound dataset's actual columns. Falls back to
 * a free-text input when columns aren't available (no dataset bound, SQL
 * errored, or query not run yet) so authoring is never blocked. A saved
 * value that isn't in the current column list is kept and flagged rather
 * than dropped — silently clearing it would corrupt the saved spec.
 *
 * `expect` is a soft type hint: when set, columns whose type matches the
 * role's expected family render first, with non-matching options under a
 * divider. The author can always override; the filter is a guide, not a
 * gate. Used by the chart-first drawer to surface numeric columns for
 * `y_field` / `value_field`, temporal for time axes, etc.
 */

import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectLabel,
  SelectSeparator,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { DatasetColumn, DatasetColumns } from "@/hooks/useDatasetColumns";
import { cn } from "@/lib/utils";

export type ColumnExpect = "any" | "numeric" | "temporal" | "string";

/**
 * matchesExpect — soft type check for a column against a role's expected
 * family. Always returns true for `any`. Pattern-matches on the substring
 * of the underlying SQL type so it works across Spark and Athena dialects
 * (`bigint`, `int`, `decimal(10,2)`, `double precision`, `timestamp(3)`).
 */
export function matchesExpect(type: string, expect: ColumnExpect): boolean {
  if (expect === "any") return true;
  const t = type.toLowerCase();
  if (expect === "numeric") {
    return /(^|[^a-z])(int|bigint|smallint|tinyint|long|float|double|decimal|numeric|number|real)/.test(
      t,
    );
  }
  if (expect === "temporal") {
    return /(date|time|timestamp)/.test(t);
  }
  if (expect === "string") {
    return /(string|varchar|char|text)/.test(t);
  }
  return true;
}

interface ColumnSelectProps {
  label: string;
  value: string;
  columns: DatasetColumns | undefined;
  onChange: (v: string) => void;
  /** Soft type filter: matching columns appear above a divider. */
  expect?: ColumnExpect;
}

export function ColumnSelect({
  label,
  value,
  columns,
  onChange,
  expect = "any",
}: ColumnSelectProps) {
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
          columns unavailable — edit the SQL and pause to populate
        </p>
      </div>
    );
  }

  // Partition by the soft type match so the expected ones are easiest to
  // find. Stable order within each group preserves the dataset's column
  // order, which authors tend to use intentionally.
  const matching = cols.filter((c) => matchesExpect(c.type, expect));
  const other = cols.filter((c) => !matchesExpect(c.type, expect));

  // Keep a saved value the current result no longer returns. Looks for
  // the value in either bucket; if missing, appends to `other` as stale.
  const allNames = cols.map((c) => c.name);
  const stale = value !== "" && !allNames.includes(value);
  const staleCol: DatasetColumn | null = stale
    ? { name: value, type: "" }
    : null;

  return (
    <div className="flex-1 space-y-1">
      <Label className="text-xs">{label}</Label>
      <Select value={value || undefined} onValueChange={onChange}>
        <SelectTrigger>
          <SelectValue placeholder="pick a column" />
        </SelectTrigger>
        <SelectContent>
          {matching.length > 0 && (
            <SelectGroup>
              {expect !== "any" && other.length > 0 && (
                <SelectLabel className="text-[10px] uppercase">
                  {expect}
                </SelectLabel>
              )}
              {matching.map((c) => (
                <ColumnOption key={c.name} col={c} selected={c.name === value} />
              ))}
            </SelectGroup>
          )}
          {other.length > 0 && matching.length > 0 && <SelectSeparator />}
          {other.length > 0 && (
            <SelectGroup>
              {expect !== "any" && matching.length > 0 && (
                <SelectLabel className="text-[10px] uppercase">
                  other
                </SelectLabel>
              )}
              {other.map((c) => (
                <ColumnOption key={c.name} col={c} selected={c.name === value} />
              ))}
            </SelectGroup>
          )}
          {/* When expect=any and everything's in `matching`, render once. */}
          {matching.length > 0 && other.length === 0 && expect === "any" && null}
          {staleCol && (
            <SelectGroup>
              <SelectSeparator />
              <ColumnOption col={staleCol} selected stale />
            </SelectGroup>
          )}
        </SelectContent>
      </Select>
    </div>
  );
}

function ColumnOption({
  col,
  selected,
  stale,
}: {
  col: DatasetColumn;
  selected: boolean;
  stale?: boolean;
}) {
  return (
    <SelectItem value={col.name} className="font-mono">
      <span>{col.name}</span>
      {col.type && (
        <span className="ml-2 text-[10px] uppercase text-muted-foreground">
          {col.type}
        </span>
      )}
      {selected && stale && (
        <span className="ml-1 text-muted-foreground">(not in result)</span>
      )}
    </SelectItem>
  );
}

interface ColumnMultiSelectProps {
  label: string;
  value: string[];
  columns: DatasetColumns | undefined;
  /** Column to leave out of the choices (e.g. the x axis already picked). */
  exclude?: string;
  onChange: (v: string[]) => void;
  expect?: ColumnExpect;
}

export function ColumnMultiSelect({
  label,
  value,
  columns,
  exclude,
  onChange,
  expect = "any",
}: ColumnMultiSelectProps) {
  if (columns?.isLoading) {
    return (
      <div className="flex-1 space-y-1">
        <Label className="text-xs">{label}</Label>
        <p className="text-xs text-muted-foreground">loading columns…</p>
      </div>
    );
  }

  const cols = (columns?.columns ?? []).filter((c) => c.name !== exclude);
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
          columns unavailable — edit the SQL and pause to populate
        </p>
      </div>
    );
  }

  function toggle(name: string) {
    onChange(
      value.includes(name) ? value.filter((v) => v !== name) : [...value, name],
    );
  }

  return (
    <div className="flex-1 space-y-1">
      <Label className="text-xs">{label}</Label>
      <div className="flex flex-wrap gap-1.5 rounded-md border border-border bg-background p-2">
        {cols.map((c) => {
          const matches = matchesExpect(c.type, expect);
          const picked = value.includes(c.name);
          return (
            <button
              key={c.name}
              type="button"
              onClick={() => toggle(c.name)}
              title={c.type ? `${c.name} · ${c.type}` : c.name}
              className={cn(
                "rounded px-2 py-0.5 font-mono text-xs transition-colors",
                picked
                  ? "bg-primary text-primary-foreground"
                  : matches
                    ? "bg-muted text-muted-foreground hover:bg-muted/70"
                    : "bg-muted/40 text-muted-foreground/70 hover:bg-muted/60",
              )}
            >
              {c.name}
            </button>
          );
        })}
      </div>
      {value.length === 0 && (
        <p className="text-xs text-muted-foreground">
          none picked — every column except x is stacked
        </p>
      )}
    </div>
  );
}
