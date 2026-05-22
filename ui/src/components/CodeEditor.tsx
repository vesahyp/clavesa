/**
 * CodeEditor — CodeMirror 6 wrapper used by every editing surface.
 *
 * Two Compartments (language, lint) reconfigure live on prop change so
 * the editor view is never remounted: cursor, selection, history, and
 * scroll position survive switching SQL ↔ Python or swapping the SQL
 * completion schema. The view handle is exposed via onReady for
 * callers that need to dispatch transactions (catalog insert today;
 * future ghost-text / accept-reject diff hunks).
 *
 * SQL completion runs through @codemirror/lang-sql's schema option —
 * it handles alias.column resolution and partial-column filter
 * idiomatically. Spark keywords are layered on as a separate
 * completion source bound to the language's data facet.
 * autocompletion is enabled exactly once (basicSetup.autocompletion is
 * disabled; the language config provides the sources).
 */

import { useEffect, useMemo, useRef } from "react";
import CodeMirror from "@uiw/react-codemirror";
import { Compartment, type Extension } from "@codemirror/state";
import { EditorView, lineNumbers as lineNumbersExt } from "@codemirror/view";
import {
  autocompletion,
  completeFromList,
  type Completion,
} from "@codemirror/autocomplete";
import { linter, type Diagnostic } from "@codemirror/lint";
import { sql, StandardSQL, type SQLConfig } from "@codemirror/lang-sql";
import { python } from "@codemirror/lang-python";
import { oneDark } from "@codemirror/theme-one-dark";

export interface SqlSchemaTable {
  label: string;
  columns: { label: string; type?: string }[];
}

export interface CodeEditorProps {
  value: string;
  onValueChange: (next: string) => void;
  language: "sql" | "python";

  height?: number | string;
  lineNumbers?: boolean;
  wordWrap?: boolean;
  readOnly?: boolean;
  className?: string;

  // SQL-only, ignored when language !== "sql".
  sqlSchema?: SqlSchemaTable[];
  sqlKeywords?: string[];
  sqlLint?: (doc: string, view: EditorView) => Diagnostic[];

  // Imperative view handle for catalog-insert and future agent UI.
  onReady?: (view: EditorView) => void;

  // Caller-injected extensions — the seam for ghost text, gutter
  // widgets, dock panels without rewriting the wrapper.
  extensions?: Extension[];
}

// Shadcn-tuned visual polish on top of oneDark.
const themeExt = EditorView.theme({
  "&": { fontSize: "12px" },
  ".cm-scroller": { fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace" },
  ".cm-content": { padding: "8px 0" },
});

function buildSqlExt(
  sqlSchema: SqlSchemaTable[] | undefined,
  sqlKeywords: string[] | undefined,
): Extension {
  // lang-sql's schema config — { tableName: ["col1", "col2", ...] }.
  // Drives alias.column completion, partial-column filter on `.`,
  // and table-name autocomplete in FROM clauses for free.
  const schemaMap: Record<string, string[]> = {};
  for (const t of sqlSchema ?? []) {
    schemaMap[t.label] = t.columns.map((c) => c.label);
  }
  const cfg: SQLConfig = {
    dialect: StandardSQL,
    upperCaseKeywords: true,
    schema: schemaMap,
  };
  const sqlLang = sql(cfg);
  const extras = sqlKeywords ?? [];
  if (extras.length === 0) return sqlLang;
  // Spark-specific keywords layered on as a second completion source
  // bound to the SQL language's data facet — merges with lang-sql's
  // own keyword + schema sources, no override.
  const completions: Completion[] = extras.map((k) => ({
    label: k,
    type: "keyword",
  }));
  return [
    sqlLang,
    sqlLang.language.data.of({
      autocomplete: completeFromList(completions),
    }),
  ];
}

function buildLanguageExt(
  language: "sql" | "python",
  sqlSchema: SqlSchemaTable[] | undefined,
  sqlKeywords: string[] | undefined,
): Extension {
  if (language === "python") return python();
  return buildSqlExt(sqlSchema, sqlKeywords);
}

export function CodeEditor({
  value,
  onValueChange,
  language,
  height = 300,
  lineNumbers = true,
  wordWrap = true,
  readOnly,
  className,
  sqlSchema,
  sqlKeywords,
  sqlLint,
  onReady,
  extensions,
}: CodeEditorProps) {
  // Compartments are stable refs — created once, reconfigured live.
  const langCompRef = useRef(new Compartment());
  const lintCompRef = useRef(new Compartment());

  const viewRef = useRef<EditorView | null>(null);
  // Guards the controlled-value echo loop: when onValueChange fires and
  // the parent re-renders with the same string, the value-sync effect
  // skips the dispatch (otherwise every keystroke double-dispatches).
  const lastEmitted = useRef<string>(value);

  const heightStr = typeof height === "number" ? `${height}px` : height;

  // Base extensions — order matters: lineNumbers before language so the
  // gutter doesn't fight with the language's own decorations.
  const baseExtensions = useMemo<Extension[]>(() => {
    const exts: Extension[] = [
      themeExt,
      EditorView.theme({ "&": { height: heightStr } }),
      // basicSetup adds its own autocompletion() which doesn't see our
      // sqlSchema/sqlKeywords. Configure exactly one instance here so
      // the language-data sources actually fire.
      autocompletion(),
      langCompRef.current.of(buildLanguageExt(language, sqlSchema, sqlKeywords)),
      lintCompRef.current.of([]),
    ];
    if (lineNumbers) exts.unshift(lineNumbersExt());
    if (wordWrap) exts.push(EditorView.lineWrapping);
    if (extensions) exts.push(...extensions);
    return exts;
    // The language/lint compartments are reconfigured by the effects
    // below; we only want the base array rebuilt when the layout-style
    // props change (initial language/schema is captured at mount).
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [heightStr, lineNumbers, wordWrap, extensions]);

  // Live language swap — also fires on sqlSchema / sqlKeywords change
  // so a freshly-arrived schema feeds the completion source without
  // remounting the editor.
  useEffect(() => {
    const view = viewRef.current;
    if (!view) return;
    view.dispatch({
      effects: langCompRef.current.reconfigure(
        buildLanguageExt(language, sqlSchema, sqlKeywords),
      ),
    });
  }, [language, sqlSchema, sqlKeywords]);

  // Live lint swap.
  useEffect(() => {
    const view = viewRef.current;
    if (!view) return;
    const ext: Extension =
      language === "sql" && sqlLint
        ? linter((v) => sqlLint(v.state.doc.toString(), v), { delay: 300 })
        : [];
    view.dispatch({ effects: lintCompRef.current.reconfigure(ext) });
  }, [language, sqlLint]);

  // Controlled-value sync — only reach in when the parent's value
  // genuinely diverges from what we last emitted, so typing doesn't
  // double-dispatch.
  useEffect(() => {
    const view = viewRef.current;
    if (!view) return;
    const current = view.state.doc.toString();
    if (value === current) return;
    if (value === lastEmitted.current) return;
    view.dispatch({
      changes: { from: 0, to: current.length, insert: value },
    });
  }, [value]);

  return (
    <CodeMirror
      value={value}
      height={heightStr}
      theme={oneDark}
      readOnly={readOnly}
      basicSetup={{
        lineNumbers: false, // we add our own conditionally
        foldGutter: false,
        highlightActiveLine: false,
        highlightActiveLineGutter: false,
        // The single autocompletion() in baseExtensions owns the
        // completion config; basicSetup's would be a second instance
        // that ignores our sqlSchema/sqlKeywords.
        autocompletion: false,
      }}
      extensions={baseExtensions}
      onChange={(v) => {
        lastEmitted.current = v;
        onValueChange(v);
      }}
      onCreateEditor={(view) => {
        viewRef.current = view;
        onReady?.(view);
      }}
      className={className}
    />
  );
}

export default CodeEditor;
