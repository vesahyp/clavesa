/**
 * validateDraft — publish-time safety net for the dashboard editor.
 *
 * Cheap pure check the Save button runs before the API POST. The
 * placeholder linter (Slice B) already flags `{{nope}}` references
 * in the editor's gutter, but a user can still hit Save before the
 * linter fires or while ignoring its warnings. validateDraft is
 * the gate: if it returns any errors, the save is blocked and the
 * editor opens the first offending widget's drawer with the
 * relevant section highlighted.
 *
 * Coverage:
 *   - Widget bound to a non-existent dataset (e.g. renamed by another tab).
 *   - Widget missing a required field for its type (`big_number` with
 *     no `value_field`, `line` with no `x_field` / `y_field`, etc.).
 *   - Dataset SQL references `{{name}}` that no declared control
 *     produces. Same regex as the server-side parser.
 *   - Widget field-mapping references a column that the dataset's
 *     last successful query didn't return. Sticky-columns cache
 *     stays populated through transient errors so this stays
 *     accurate mid-edit.
 *
 * Not covered: SQL parse errors (the runner is the source of truth
 * for SQL; we don't ship a Spark parser in the browser), per-widget
 * data shape (e.g. line widget bound to a dataset that returns no
 * rows — the empty state hint covers that at render).
 */

import type { Dashboard, DashboardWidget } from "@/lib/queries";
import type { DatasetColumns } from "@/hooks/useDatasetColumns";

import { availablePlaceholders } from "./placeholderLinter";

export interface ValidationError {
  /** ID of the widget that's affected (drives drawer-open on Save). */
  widgetId: string;
  /** Which section of the drawer the error points at. */
  section: "data" | "field-mapping";
  /** Short human-readable message rendered inline. */
  message: string;
}

/** Same regex as `dashboardsql/expand.go` and `placeholderLinter.ts`. */
const PLACEHOLDER_RE = /\{\{\s*([A-Za-z_][A-Za-z0-9_.\-]*)\s*\}\}/g;

/**
 * REQUIRED_FIELDS — per widget type, the field-mapping props that
 * MUST be set for the widget to render anything sensible. `table`
 * has none (renders the whole result). `world_map` requires the
 * region + value pair.
 *
 * Keyed against `DashboardWidget` to keep the type-script honest;
 * a new widget type without an entry here gets no required-field
 * check (safer than crashing).
 */
const REQUIRED_FIELDS: Partial<Record<
  DashboardWidget["type"],
  Array<keyof DashboardWidget>
>> = {
  big_number: ["value_field"],
  line: ["x_field", "y_field"],
  bar: ["x_field", "y_field"],
  stacked_bar: ["x_field"],
  bar_line: ["x_field", "y_field", "line_field"],
  pie: ["x_field", "value_field"],
  donut: ["x_field", "value_field"],
  world_map: ["region_field", "value_field"],
};

export function validateDraft(
  spec: Dashboard,
  columnsByDataset: Map<string, DatasetColumns>,
): ValidationError[] {
  const errors: ValidationError[] = [];
  const datasetNames = new Set(spec.datasets.map((d) => d.name));
  const availableControls = new Set(availablePlaceholders(spec.controls));

  // 1. Dataset-level: placeholders must reference declared controls.
  //    Errors land on the first widget bound to the offending dataset
  //    so the drawer opens somewhere actionable. An orphaned dataset
  //    (no widget binds it) is its own kind of warning today — left
  //    silent so the save flow doesn't get stuck on a dataset the
  //    user is mid-renaming.
  for (const ds of spec.datasets) {
    const unknown = collectUnknownPlaceholders(ds.sql, availableControls);
    if (unknown.length === 0) continue;
    const bound = spec.widgets.find((w) => w.dataset === ds.name);
    if (!bound) continue;
    errors.push({
      widgetId: bound.id,
      section: "data",
      message: `Dataset "${ds.name}" references unknown placeholder ${unknown
        .slice(0, 2)
        .map((n) => `{{${n}}}`)
        .join(", ")}${unknown.length > 2 ? ", …" : ""}`,
    });
  }

  // 2. Widget-level: dataset binding + required fields + column refs.
  for (const w of spec.widgets) {
    if (!datasetNames.has(w.dataset)) {
      errors.push({
        widgetId: w.id,
        section: "data",
        message: w.dataset
          ? `Bound to missing dataset "${w.dataset}"`
          : "No dataset bound",
      });
      continue; // field checks below are meaningless without a dataset
    }

    const required = REQUIRED_FIELDS[w.type] ?? [];
    for (const f of required) {
      const v = w[f];
      const empty = Array.isArray(v) ? v.length === 0 : !v;
      if (empty) {
        errors.push({
          widgetId: w.id,
          section: "field-mapping",
          message: `${formatRole(String(f))} is required for a ${w.type} widget`,
        });
      }
    }

    // Stale-column check: any field-mapping value that isn't in the
    // dataset's last successful column set. Inline datasets share
    // the same column cache as the rest; the check is uniform.
    const cols = columnsByDataset.get(w.dataset)?.columns ?? [];
    if (cols.length > 0) {
      const colNames = new Set(cols.map((c) => c.name));
      for (const f of required) {
        const v = w[f];
        if (typeof v !== "string" || v === "") continue;
        if (!colNames.has(v)) {
          errors.push({
            widgetId: w.id,
            section: "field-mapping",
            message: `${formatRole(String(f))} "${v}" is not a column in the dataset's result`,
          });
        }
      }
    }
  }

  // De-dupe by (widget, section, message) so a save with the same
  // problem mentioned twice doesn't flood the surface.
  const seen = new Set<string>();
  return errors.filter((e) => {
    const k = `${e.widgetId}|${e.section}|${e.message}`;
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

function collectUnknownPlaceholders(
  sql: string,
  available: Set<string>,
): string[] {
  const out: string[] = [];
  PLACEHOLDER_RE.lastIndex = 0;
  let m: RegExpExecArray | null;
  while ((m = PLACEHOLDER_RE.exec(sql)) !== null) {
    if (!available.has(m[1])) out.push(m[1]);
  }
  return out;
}

function formatRole(field: string): string {
  // Field names are snake_case (`region_field`, `value_field`); the
  // user-facing label is title-cased without the `_field` suffix.
  return field
    .replace(/_field$/, "")
    .replace(/_/g, " ")
    .replace(/^./, (c) => c.toUpperCase());
}

