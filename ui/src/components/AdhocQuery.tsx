/**
 * AdhocQuery — reusable SQL-editor-and-result-grid component.
 *
 * Used by the top-level /query page and by the collapsible SQL pane on
 * /tables/:catalog/:schema/:table. Caller supplies the initial SQL, the
 * editor handles edits + Run + an inline result grid below.
 *
 * Picks any pipeline dir from the workspace to satisfy the provider-
 * dispatch requirement (POST /api/data/query needs a dir). All pipeline
 * dirs in a workspace route to the same local Hadoop catalog, so the
 * choice is arbitrary.
 *
 * On the /query page (showCatalog=true) the editor sits next to a
 * CatalogBrowser sidebar — click a table or column to insert its
 * identifier at the cursor. The collapsible table-pane variant hides
 * the browser since the pre-filled SQL already names the table.
 */

import { useEffect, useRef, useState } from "react";
import { Loader2, Play } from "lucide-react";
import { EditorView } from "@codemirror/view";
import { toast } from "sonner";

import { CatalogBrowser } from "@/components/CatalogBrowser";
import { CodeEditor } from "@/components/CodeEditor";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { runAdhocQuery, usePipelines, type AdhocQueryResult } from "@/lib/queries";

export interface AdhocQueryProps {
  /** Initial SQL to show. Subsequent edits are local-only. */
  initialSql: string;
  /** Optional override label on the Run button. */
  runLabel?: string;
  /** Hide the card chrome (used when embedding inside another card). */
  bare?: boolean;
  /** Show the CatalogBrowser sidebar next to the editor. */
  showCatalog?: boolean;
  /** Fire the query once on mount + whenever initialSql changes — used by
   * the TableDetail "Query this table" pane so the user sees the LIMIT 100
   * result without an extra click. */
  autoRun?: boolean;
}

export function AdhocQuery({
  initialSql,
  runLabel = "Run",
  bare,
  showCatalog,
  autoRun,
}: AdhocQueryProps) {
  const [sql, setSql] = useState(initialSql);
  const [result, setResult] = useState<AdhocQueryResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const pipelines = usePipelines();
  // CM6 view, captured via onReady — CatalogBrowser uses it to insert
  // an identifier at the cursor (same pattern as the dashboards dataset
  // editor).
  const viewRef = useRef<EditorView | null>(null);

  // If the caller swaps initialSql (e.g. user navigates to a different
  // table page), reset the editor + clear any stale result.
  useEffect(() => {
    setSql(initialSql);
    setResult(null);
    setError(null);
  }, [initialSql]);

  const firstDir = pipelines.data?.[0]?.dir ?? "";

  const runRef = useRef<() => Promise<void>>(async () => {});
  async function run() {
    if (!sql.trim()) return;
    // Spark parses "..." as a string literal, not an identifier. Catch the
    // common `FROM "db"."table"` / `FROM "table"` shape before it hits the
    // runner so the user sees a friendly hint instead of an opaque
    // "STRING_LITERAL is not a table" stack trace.
    const dqIdent = doubleQuotedIdentifierHint(sql);
    if (dqIdent) {
      const msg =
        'Double-quotes are string literals in Spark, not identifier quoting. Use backticks for special-character identifiers, or no quoting for normal ones.';
      setError(msg);
      toast.error(msg);
      return;
    }
    setBusy(true);
    setError(null);
    try {
      const res = await runAdhocQuery(sql, firstDir);
      setResult(res);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      setError(msg);
      toast.error(msg);
    } finally {
      setBusy(false);
    }
  }
  runRef.current = run;

  // autoRun: fire once on mount or whenever initialSql shifts (caller swaps
  // the seed SQL on table-page navigation). Waits for the pipelines list to
  // resolve so firstDir is real, not "". Silently tolerates the toast-on-
  // error from run() — autorun errors land in the inline error pane.
  useEffect(() => {
    if (!autoRun) return;
    if (!firstDir) return;
    void runRef.current();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [autoRun, firstDir, initialSql]);

  function insertAtCursor(text: string) {
    const view = viewRef.current;
    if (!view) {
      // No view yet (editor not mounted) — append as a fallback so
      // the click still does something useful.
      setSql((cur) => cur + text);
      return;
    }
    const { from, to } = view.state.selection.main;
    view.dispatch({
      changes: { from, to, insert: text },
      selection: { anchor: from + text.length },
    });
    view.focus();
  }

  const editor = (
    <CodeEditor
      value={sql}
      onValueChange={setSql}
      language="sql"
      height={Math.max(120, Math.min(360, sql.split("\n").length * 18 + 24))}
      lineNumbers
      wordWrap
      onReady={(view) => {
        viewRef.current = view;
      }}
    />
  );

  const body = (
    <div className="space-y-3">
      {showCatalog ? (
        <div className="flex gap-3">
          <div className="min-w-0 flex-1">{editor}</div>
          <CatalogBrowser
            scope="workspace"
            onInsert={insertAtCursor}
          />
        </div>
      ) : (
        editor
      )}
      <div className="flex items-center gap-2">
        <Button onClick={run} disabled={busy || !sql.trim() || !firstDir}>
          {busy ? <Loader2 className="mr-1 h-4 w-4 animate-spin" /> : <Play className="mr-1 h-4 w-4" />}
          {runLabel}
        </Button>
        {!firstDir && (
          <span className="text-xs text-muted-foreground">
            Create a pipeline first — ad-hoc queries dispatch through any pipeline's compute scope.
          </span>
        )}
        {result && !error && (
          <span className="text-xs text-muted-foreground">
            {result.rows.length} row{result.rows.length === 1 ? "" : "s"}
            {result.truncated && <> · truncated</>}
          </span>
        )}
      </div>

      {error && (
        <pre className="max-h-64 overflow-auto whitespace-pre-wrap rounded border border-destructive/40 bg-destructive/5 p-3 font-mono text-xs text-destructive">
          {error}
        </pre>
      )}

      {result && !error && <ResultGrid result={result} />}
    </div>
  );

  if (bare) return body;
  return (
    <Card>
      <CardContent className="p-4">{body}</CardContent>
    </Card>
  );
}

// doubleQuotedIdentifierHint returns true when the SQL has the shape
// `FROM "x"` / `FROM "x"."y"` / `JOIN "x"`, suggesting the user is
// double-quoting identifiers. Spark would treat those as string literals
// and reject the query with a confusing error.
function doubleQuotedIdentifierHint(sql: string): boolean {
  return /\b(?:from|join)\s+"[^"]+"(?:\s*\.\s*"[^"]+")?/i.test(sql);
}

function ResultGrid({ result }: { result: AdhocQueryResult }) {
  if (result.rows.length === 0) {
    return (
      <div className="rounded border bg-muted/40 p-3 text-xs text-muted-foreground">
        Query returned no rows.
      </div>
    );
  }
  return (
    <div className="overflow-auto rounded border">
      <table className="w-full border-collapse text-xs">
        <thead className="sticky top-0 bg-muted">
          <tr>
            {result.columns.map((c) => (
              <th
                key={c.name}
                className="border-b px-2 py-1.5 text-left font-mono font-semibold"
                title={c.type}
              >
                {c.name}
                {c.type && (
                  <span className="ml-1 text-muted-foreground/70">({c.type})</span>
                )}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {result.rows.slice(0, 200).map((row, i) => (
            <tr key={i} className={i % 2 ? "bg-muted/20" : undefined}>
              {row.map((cell, j) => (
                <td key={j} className="border-b px-2 py-1 align-top font-mono">
                  {cell === "" ? (
                    <span className="italic text-muted-foreground">(empty)</span>
                  ) : (
                    cell
                  )}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {result.rows.length > 200 && (
        <div className="border-t bg-muted/30 px-2 py-1 text-[11px] text-muted-foreground">
          Showing first 200 of {result.rows.length} rows
        </div>
      )}
    </div>
  );
}
