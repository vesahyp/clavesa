/**
 * TransformEditor — SQL/Python tab editor for transform nodes.
 *
 * Tab bar lets users switch between SQL and Python.
 * Each language's content is persisted to its own file via putScript.
 * On tab switch, the current content is saved immediately.
 * The Save button calls updateNode to set the active language + file reference.
 *
 * The SQL tab has schema-aware autocomplete — SparkSQL keywords plus the
 * transform's input aliases and their columns — and a best-effort parse
 * check that flags syntax errors as advisory warnings. The Preview button
 * remains the authoritative check (it runs real Spark via the runner).
 */

import { useEffect, useMemo, useRef, useState, useCallback } from "react";
import { Parser } from "node-sql-parser";
import { Loader2 } from "lucide-react";
import type { EditorView } from "@codemirror/view";

import { CodeEditor, type SqlSchemaTable } from "./CodeEditor";
import { TransformInputsBrowser } from "./TransformInputsBrowser";
import { getScript, putScript, updateNode } from "../api/pipeline";
import type { Column } from "../types/pipeline";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

export interface SqlInput {
  alias: string;
  columns: Column[];
}

export interface TransformEditorProps {
  dir: string;
  nodeId: string;
  config: Record<string, unknown>;
  /** Upstream inputs — drives the SQL editor's autocomplete. */
  sqlInputs?: SqlInput[];
  /**
   * This transform's own output column list and sample-loading state.
   * Surfaced in the right-side inputs browser so authors see what the
   * table contains without scrolling to the inline sample panel below.
   */
  output?: { columns: Column[]; loading: boolean };
  onSaved: () => void;
}

const SPARK_SQL_KEYWORDS = [
  "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "HAVING", "LIMIT",
  "JOIN", "LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "FULL JOIN", "ON",
  "AS", "AND", "OR", "NOT", "IN", "IS", "NULL", "LIKE", "BETWEEN",
  "CASE", "WHEN", "THEN", "ELSE", "END", "DISTINCT", "UNION", "UNION ALL",
  "WITH", "OVER", "PARTITION BY", "ASC", "DESC",
  "COUNT", "SUM", "AVG", "MIN", "MAX", "ROUND", "COALESCE", "NULLIF",
  "CAST", "CONCAT", "SUBSTRING", "TRIM", "LOWER", "UPPER", "DATE_TRUNC",
];

const sqlParser = new Parser();

const FILE_REF_RE = /^file\(["'](.+)["']\)$/;

function parseFileRef(expr: string): string | null {
  const m = String(expr).match(FILE_REF_RE);
  return m ? m[1] : null;
}

export function TransformEditor({ dir, nodeId, config, sqlInputs, output, onSaved }: TransformEditorProps) {
  const sqlExpr = String(config.sql ?? "");
  const pyExpr  = String(config.python ?? "");
  const configLang = String(config.language ?? "sql");

  const [activeLang, setActiveLang] = useState<"sql" | "python">(
    configLang === "python" ? "python" : "sql"
  );
  const [sqlContent, setSqlContent]       = useState<string | null>(null);
  const [pythonContent, setPythonContent] = useState<string | null>(null);
  const [saving, setSaving]   = useState(false);
  const [error, setError]     = useState<string | null>(null);

  // Load SQL content on mount
  useEffect(() => {
    const filePath = parseFileRef(sqlExpr);
    if (filePath) {
      getScript(dir, filePath)
        .then(setSqlContent)
        .catch(() => setSqlContent(""));
    } else {
      setSqlContent(sqlExpr);
    }
  }, [dir, nodeId]); // eslint-disable-line react-hooks/exhaustive-deps

  // Load Python content on mount
  useEffect(() => {
    const filePath = parseFileRef(pyExpr);
    if (filePath) {
      getScript(dir, filePath)
        .then(setPythonContent)
        .catch(() => setPythonContent(""));
    } else {
      setPythonContent(pyExpr || "");
    }
  }, [dir, nodeId]); // eslint-disable-line react-hooks/exhaustive-deps

  const sqlFileName    = `${nodeId}.sql`;
  const pythonFileName = `${nodeId}.py`;

  const sqlSchema = useMemo<SqlSchemaTable[]>(
    () =>
      (sqlInputs ?? []).map((i) => ({
        label: i.alias,
        columns: i.columns.map((c) => ({ label: c.name, type: c.type })),
      })),
    [sqlInputs],
  );

  // node-sql-parser returns 1-based {line, column}; CM6 wants a doc
  // offset. The fallback covers parser errors with no location.
  const sqlLint = useCallback((doc: string) => {
    if (!doc.trim()) return [];
    try {
      sqlParser.astify(doc, { database: "hive" });
      return [];
    } catch (err) {
      const e = err as {
        message?: string;
        location?: { start?: { line?: number; column?: number } };
      };
      const line = e.location?.start?.line ?? 1;
      const col = e.location?.start?.column ?? 1;
      // Build a doc-offset by walking the doc lines; bail safely if the
      // reported line is past the end.
      const lines = doc.split("\n");
      let from = 0;
      for (let i = 0; i < Math.min(line - 1, lines.length); i++) {
        from += lines[i].length + 1; // +1 for the newline
      }
      from += Math.max(0, col - 1);
      from = Math.min(from, doc.length);
      const to = Math.min(from + 1, doc.length);
      return [
        {
          from,
          to,
          severity: "warning" as const,
          message:
            "SQL may not parse (advisory — Preview runs the real check): " +
            String(e.message ?? err).split("\n")[0],
        },
      ];
    }
  }, []);

  const switchTo = useCallback(async (lang: "sql" | "python") => {
    if (lang === activeLang) return;
    setError(null);
    try {
      if (activeLang === "sql" && sqlContent !== null) {
        await putScript(dir, sqlFileName, sqlContent);
      } else if (activeLang === "python" && pythonContent !== null) {
        await putScript(dir, pythonFileName, pythonContent);
      }
      setActiveLang(lang);
    } catch (e) {
      setError(String(e));
    }
  }, [activeLang, dir, sqlFileName, pythonFileName, sqlContent, pythonContent]);

  async function handleSave() {
    setSaving(true);
    setError(null);
    try {
      // Strip parser-synthetic keys so the saved .tf doesn't grow a literal
      // `source_inputs = ...` attribute. The real attribute is `inputs`,
      // populated by /api/sources/{name}/attach; source_inputs is just how
      // the parser surfaces source-registry references in the graph JSON
      // (ADR-017).
      const { source_inputs: _src, ...restConfig } = config;
      void _src;
      if (activeLang === "sql") {
        await putScript(dir, sqlFileName, sqlContent ?? "");
        await updateNode(dir, nodeId, {
          ...restConfig,
          language: "sql",
          sql: `file("${sqlFileName}")`,
          python: undefined,
        });
      } else {
        await putScript(dir, pythonFileName, pythonContent ?? "");
        await updateNode(dir, nodeId, {
          ...restConfig,
          language: "python",
          python: `file("${pythonFileName}")`,
          sql: undefined,
        });
      }
      onSaved();
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  }

  const editorValue  = activeLang === "sql" ? (sqlContent ?? "")    : (pythonContent ?? "");
  const editorLoading = activeLang === "sql" ? sqlContent === null  : pythonContent === null;

  // CM6 view, captured via onReady. The inputs browser uses it to
  // insert an alias or column name at the cursor.
  const viewRef = useRef<EditorView | null>(null);

  function insertAtCursor(text: string) {
    const view = viewRef.current;
    if (!view) return;
    const { from, to } = view.state.selection.main;
    view.dispatch({
      changes: { from, to, insert: text },
      selection: { anchor: from + text.length },
    });
    view.focus();
  }

  const inputsForBrowser = sqlInputs ?? [];

  return (
    <div>
      <div className="mb-2 flex border-b border-border">
        {(["sql", "python"] as const).map((lang) => (
          <button
            key={lang}
            onClick={() => switchTo(lang)}
            className={cn(
              "border-b-2 px-3.5 py-1.5 text-xs font-semibold uppercase transition-colors",
              activeLang === lang
                ? "border-primary text-foreground"
                : "border-transparent text-muted-foreground hover:text-foreground"
            )}
          >
            {lang}
          </button>
        ))}
      </div>

      {editorLoading ? (
        <div className="flex items-center gap-2 py-2 text-xs text-muted-foreground">
          <Loader2 className="h-3 w-3 animate-spin" />
          Loading…
        </div>
      ) : (
        <div className="flex gap-2">
          <div
            className="min-w-0 flex-1 overflow-hidden rounded-md border border-border"
            data-testid="sql-editor"
          >
            <CodeEditor
              value={editorValue}
              onValueChange={(v) => {
                if (activeLang === "sql") setSqlContent(v);
                else setPythonContent(v);
              }}
              language={activeLang}
              height={300}
              lineNumbers
              wordWrap
              sqlSchema={activeLang === "sql" ? sqlSchema : undefined}
              sqlKeywords={SPARK_SQL_KEYWORDS}
              sqlLint={activeLang === "sql" ? sqlLint : undefined}
              onReady={(view) => {
                viewRef.current = view;
              }}
            />
          </div>
          <TransformInputsBrowser
            inputs={inputsForBrowser}
            onInsert={insertAtCursor}
            output={output}
          />
        </div>
      )}

      {activeLang === "sql" && (
        <p className="mt-1 text-[11px] text-muted-foreground">
          Autocomplete covers SparkSQL keywords and this transform&apos;s
          inputs. Syntax warnings are advisory — Preview runs the real check.
        </p>
      )}

      {error && (
        <div className="mt-1.5 text-[11px] text-status-failed">{error}</div>
      )}

      <Button
        onClick={handleSave}
        disabled={saving || editorLoading}
        size="sm"
        className="mt-2"
        data-testid="save-sql"
      >
        {saving ? (
          <>
            <Loader2 className="h-3 w-3 animate-spin" />
            Saving…
          </>
        ) : (
          "Save"
        )}
      </Button>
    </div>
  );
}

export default TransformEditor;
