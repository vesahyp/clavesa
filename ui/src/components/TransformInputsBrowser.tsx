/**
 * TransformInputsBrowser — clickable inputs+columns reference next to
 * the SQL editor.
 *
 * Renders the same alias → columns map that powers the editor's
 * autocompletion, but visibly: one row per input, expandable to its
 * columns. Click the alias to insert it (good for `FROM <alias>`);
 * click a column to insert just the column name. Mirrors the catalog
 * browser pattern from the dashboard editor.
 */

import { useState } from "react";
import { ChevronDown, ChevronRight, Columns3, Loader2, Table2 } from "lucide-react";

import type { SqlInput } from "./TransformEditor";
import type { Column } from "../types/pipeline";

interface TransformInputsBrowserProps {
  inputs: SqlInput[];
  onInsert: (text: string) => void;
  /**
   * This transform's own output columns + sample-loading state. Surfaced
   * directly under the input list so a user editing SQL can see what's in
   * the table without scrolling to the inline sample panel. `loading` is
   * the in-flight signal from the auto-sample fetch; columns are the
   * already-resolved catalog or live-preview list.
   */
  output?: { columns: Column[]; loading: boolean };
}

export function TransformInputsBrowser({
  inputs,
  onInsert,
  output,
}: TransformInputsBrowserProps) {
  // Default-expand single-input transforms; collapse-by-default when
  // there's a list, so the panel stays scannable.
  const [open, setOpen] = useState<Set<string>>(
    () => new Set(inputs.length === 1 ? [inputs[0].alias] : []),
  );
  const [outputOpen, setOutputOpen] = useState(true);

  function toggle(alias: string) {
    setOpen((prev) => {
      const next = new Set(prev);
      if (next.has(alias)) next.delete(alias);
      else next.add(alias);
      return next;
    });
  }

  return (
    <div className="flex w-56 flex-shrink-0 flex-col rounded-md border border-border bg-card">
      <div className="border-b border-border px-2 py-1.5 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
        Inputs
      </div>
      <div className="max-h-[200px] overflow-auto p-1 text-xs">
        {inputs.length === 0 && (
          <p className="p-2 text-muted-foreground">
            No inputs yet. Attach a source or wire an upstream transform
            to see its columns here.
          </p>
        )}
        {inputs.map((input) => {
          const expanded = open.has(input.alias);
          const hasColumns = input.columns.length > 0;
          return (
            <div key={input.alias} className="mb-1">
              <div className="flex items-center gap-0.5 rounded hover:bg-muted">
                <button
                  type="button"
                  onClick={() => toggle(input.alias)}
                  className="flex-shrink-0 p-0.5 text-muted-foreground"
                  aria-label={expanded ? "Collapse" : "Expand"}
                >
                  {expanded ? (
                    <ChevronDown className="h-3 w-3" />
                  ) : (
                    <ChevronRight className="h-3 w-3" />
                  )}
                </button>
                <button
                  type="button"
                  onClick={() => onInsert(input.alias)}
                  title={`Insert ${input.alias}`}
                  className="flex min-w-0 flex-1 items-center gap-1 py-0.5 pr-1 text-left font-mono"
                >
                  <Table2 className="h-3 w-3 flex-shrink-0 text-muted-foreground" />
                  <span className="truncate">{input.alias}</span>
                </button>
              </div>
              {expanded && (
                <ul className="ml-5 border-l border-border">
                  {!hasColumns && (
                    <li className="py-0.5 pl-2 text-muted-foreground">
                      Columns appear after the upstream has run, or for
                      registry sources after the editor opens.
                    </li>
                  )}
                  {input.columns.map((c) => (
                    <li key={c.name}>
                      <button
                        type="button"
                        onClick={() => onInsert(c.name)}
                        title={`Insert ${c.name}`}
                        className="flex w-full items-center gap-1 rounded py-0.5 pl-2 pr-1 text-left font-mono hover:bg-muted"
                      >
                        <Columns3 className="h-3 w-3 flex-shrink-0 text-muted-foreground" />
                        <span className="truncate">{c.name}</span>
                        <span className="ml-auto flex-shrink-0 text-[10px] text-muted-foreground">
                          {c.type}
                        </span>
                      </button>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          );
        })}
      </div>

      {output && (
        <>
          <div className="flex items-center justify-between border-y border-border bg-muted/30 px-2 py-1.5 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
            <button
              type="button"
              onClick={() => setOutputOpen((v) => !v)}
              className="flex items-center gap-1"
              aria-label={outputOpen ? "Collapse output" : "Expand output"}
            >
              {outputOpen ? (
                <ChevronDown className="h-3 w-3" />
              ) : (
                <ChevronRight className="h-3 w-3" />
              )}
              Output
            </button>
            {output.loading && (
              <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />
            )}
          </div>
          {outputOpen && (
            <div className="max-h-[200px] overflow-auto p-1 text-xs">
              {output.columns.length === 0 ? (
                <p className="p-2 text-muted-foreground">
                  {output.loading
                    ? "Sampling output…"
                    : "Output columns appear after this transform runs."}
                </p>
              ) : (
                <ul className="ml-1 border-l border-border">
                  {output.columns.map((c) => (
                    <li key={c.name}>
                      <button
                        type="button"
                        onClick={() => onInsert(c.name)}
                        title={`Insert ${c.name}`}
                        className="flex w-full items-center gap-1 rounded py-0.5 pl-2 pr-1 text-left font-mono hover:bg-muted"
                      >
                        <Columns3 className="h-3 w-3 flex-shrink-0 text-muted-foreground" />
                        <span className="truncate">{c.name}</span>
                        <span className="ml-auto flex-shrink-0 text-[10px] text-muted-foreground">
                          {c.type}
                        </span>
                      </button>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          )}
        </>
      )}
    </div>
  );
}
