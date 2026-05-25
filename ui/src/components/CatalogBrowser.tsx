/**
 * CatalogBrowser — click-to-insert table + column picker for SQL editors.
 *
 * Shared across the dashboards dataset editor, the workspace /query page,
 * and notebook editors. Lists tables grouped by database; clicking a table
 * inserts its fully-qualified name (`clavesa.<db>.<table>`), clicking a
 * column inserts the bare column name. Authoring SQL stops being blind
 * guessing of identifiers.
 *
 * Two scopes:
 *   - `pipeline`: filter to one pipeline's tables (dashboards dataset path —
 *     a dataset is bound to one dir, this matches that scope).
 *   - `workspace`: every table in the catalog (the /query + notebooks paths
 *     where the user isn't pinned to a single pipeline).
 */

import { useMemo, useState } from "react";
import { ChevronDown, ChevronRight, Columns3, Table2 } from "lucide-react";

import { useCatalogTables, type CatalogTable } from "@/lib/queries";

interface CatalogBrowserProps {
  /** Insert text at the SQL editor's cursor. */
  onInsert: (text: string) => void;
  /**
   * Scope: "pipeline" filters by `dir`, "workspace" shows every table.
   * Defaults to "pipeline" to preserve the original dashboards behavior.
   */
  scope?: "pipeline" | "workspace";
  /** Pipeline dir; required when scope="pipeline", ignored otherwise. */
  dir?: string;
  /** Visual width tweak — "narrow" for sidebar use, "auto" for inline. */
  width?: "narrow" | "auto";
}

export function CatalogBrowser({
  onInsert,
  scope = "pipeline",
  dir = "",
  width = "narrow",
}: CatalogBrowserProps) {
  const catalog = useCatalogTables();
  const [open, setOpen] = useState<Set<string>>(new Set());

  // Group tables by database (catalog__schema). Filter scope: workspace
  // shows everything we have, pipeline shows only this dir's tables.
  const groups = useMemo(() => {
    const all = catalog.data?.tables ?? [];
    const tables =
      scope === "workspace"
        ? all
        : all.filter((t) => dir !== "" && t.dir === dir);
    const m = new Map<string, CatalogTable[]>();
    for (const t of tables) {
      const g = m.get(t.database) ?? [];
      g.push(t);
      m.set(t.database, g);
    }
    for (const g of m.values()) g.sort((a, b) => a.name.localeCompare(b.name));
    return [...m.entries()].sort((a, b) => a[0].localeCompare(b[0]));
  }, [catalog.data, dir, scope]);

  function toggle(key: string) {
    setOpen((prev) => {
      const next = new Set(prev);
      next.has(key) ? next.delete(key) : next.add(key);
      return next;
    });
  }

  const widthClass = width === "narrow" ? "w-64 flex-shrink-0" : "w-full";

  return (
    <div className={`${widthClass} flex flex-col rounded-md border border-border bg-card`}>
      <div className="border-b border-border px-2 py-1.5 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
        Tables
      </div>
      <div className="max-h-[60vh] overflow-auto p-1 text-xs">
        {scope === "pipeline" && dir === "" && (
          <p className="p-2 text-muted-foreground">
            Pick a pipeline to see its tables.
          </p>
        )}
        {((scope === "workspace") || (scope === "pipeline" && dir !== "")) && catalog.isLoading && (
          <p className="p-2 text-muted-foreground">Loading tables…</p>
        )}
        {catalog.error != null && (
          <p className="p-2 text-muted-foreground">Couldn't load tables.</p>
        )}
        {!catalog.isLoading && catalog.error == null && groups.length === 0 && (
          <p className="p-2 text-muted-foreground">
            {scope === "workspace"
              ? "No tables yet — run a pipeline first."
              : "No tables yet — run the pipeline."}
          </p>
        )}
        {groups.map(([database, tables]) => (
          <div key={database} className="mb-1">
            <div className="truncate px-1 py-0.5 font-mono text-[10px] text-muted-foreground">
              {database}
            </div>
            {tables.map((t) => {
              const key = `${database}.${t.name}`;
              const expanded = open.has(key);
              const qualified = `clavesa.${t.database}.${t.name}`;
              return (
                <div key={key}>
                  <div className="flex items-center gap-0.5 rounded hover:bg-muted">
                    <button
                      type="button"
                      onClick={() => toggle(key)}
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
                      onClick={() => onInsert(qualified)}
                      title={`Insert ${qualified}`}
                      className="flex min-w-0 flex-1 items-center gap-1 py-0.5 pr-1 text-left font-mono"
                    >
                      <Table2 className="h-3 w-3 flex-shrink-0 text-muted-foreground" />
                      <span className="truncate">{t.name}</span>
                    </button>
                  </div>
                  {expanded && (
                    <ul className="ml-5 border-l border-border">
                      {t.columns.length === 0 && (
                        <li className="py-0.5 pl-2 text-muted-foreground">
                          no columns
                        </li>
                      )}
                      {t.columns.map((c) => (
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
        ))}
      </div>
    </div>
  );
}
