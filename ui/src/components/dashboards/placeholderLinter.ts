/**
 * placeholderLinter — flag {{name}} references that no control declares.
 *
 * Dataset SQL substitutes `{{name}}` (or `{{name.start}}`/`{{name.end}}`
 * for time_range) at render time. A typo today still saves cleanly and
 * 400s at render with a "no control sets …" error. This linter surfaces
 * the typo while the author is still editing, with a warning-level
 * gutter marker and a hint listing the placeholders the dashboard
 * actually declares.
 *
 * The regex mirrors the server-side parser
 * (`internal/dashboardsql/expand.go: PlaceholderRE`) — if the two
 * disagree, the linter silently passes invalid SQL or red-flags valid
 * SQL. Keep them in sync.
 */

import type { Diagnostic } from "@codemirror/lint";
import type { EditorView } from "@codemirror/view";

import type { DashboardControl } from "@/lib/queries";

/** Mirrors `internal/dashboardsql/expand.go: PlaceholderRE`. */
const PLACEHOLDER_RE = /\{\{\s*([A-Za-z_][A-Za-z0-9_.\-]*)\s*\}\}/g;

/**
 * availablePlaceholders — every placeholder name the declared controls
 * accept. `time_range` controls emit `<name>.start` and `<name>.end`;
 * `select` controls emit `<name>`. Unknown control types contribute
 * nothing (so a future control type doesn't accidentally pass the
 * linter on its first use).
 */
export function availablePlaceholders(
  controls: DashboardControl[],
): string[] {
  const out: string[] = [];
  for (const c of controls) {
    if (!c.name) continue;
    if (c.type === "time_range") {
      out.push(`${c.name}.start`, `${c.name}.end`);
    } else if (c.type === "select") {
      out.push(c.name);
    }
  }
  return out;
}

/**
 * placeholderLinter — CodeMirror `linter` source. Pass the result into
 * `CodeEditor`'s `sqlLint` prop.
 *
 * The hint includes up to three of the declared placeholders so the
 * author has something actionable inline; a longer list belongs in the
 * "Available placeholders" chip strip rendered next to the editor.
 */
export function placeholderLinter(controls: DashboardControl[]) {
  const available = availablePlaceholders(controls);
  const availableSet = new Set(available);
  const hint = available.length === 0
    ? "no controls declared — add one to use placeholders"
    : `available: ${available
        .slice(0, 3)
        .map((p) => `{{${p}}}`)
        .join(", ")}${available.length > 3 ? ", …" : ""}`;

  return function lint(doc: string, _view: EditorView): Diagnostic[] {
    const diagnostics: Diagnostic[] = [];
    // The regex carries a `g` flag — explicit `exec` loop so we can
    // collect the match index for the gutter marker. lastIndex is
    // mutated as we go; that's fine because we own the regex above.
    PLACEHOLDER_RE.lastIndex = 0;
    let m: RegExpExecArray | null;
    while ((m = PLACEHOLDER_RE.exec(doc)) !== null) {
      const name = m[1];
      if (availableSet.has(name)) continue;
      diagnostics.push({
        from: m.index,
        to: m.index + m[0].length,
        severity: "warning",
        message: `Unknown placeholder {{${name}}}; ${hint}`,
      });
    }
    return diagnostics;
  };
}
