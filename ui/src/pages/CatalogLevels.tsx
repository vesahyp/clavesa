/**
 * CatalogLevels — the Catalog of the hosted and multi-workspace UI, one
 * level per page (docs/ideas/hosted-navigation.md, Vesa 2026-11-21):
 *   /catalog                          the catalogs
 *   /catalog?catalog=C                C's schemas
 *   /catalog?catalog=C&schema=S       S's tables
 * and a table opens TableDetail (/tables/C/S/T). Each level declares the
 * full path as its breadcrumbs, so back goes one level up.
 *
 * Phone: every level is a list of rows, the name on the first line
 * (always visible, wrapping when long) and two or three facts on the
 * second; nothing scrolls sideways. Desktop: a tree of the catalogs,
 * open down to the current schema, beside the level; the tables of a
 * schema are a table whose name column stays in place when the table
 * scrolls sideways inside its own box.
 *
 * The local UI keeps its one-page Catalog (pages/Catalog.tsx), which
 * `make verify-readme` walks.
 */

import { useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { ChevronRight, Database, FileWarning, Table2 } from "lucide-react";

import { useChrome, type PageChrome } from "@/components/PageChrome";
import { QueryShell } from "@/components/QueryShell";
import { ListSearch } from "@/components/ListSearch";
import { Skeleton } from "@/components/ui/skeleton";
import { useCatalogTables, type CatalogTable } from "@/lib/queries";
import { displayTableName } from "@/lib/format";
import { catalogPath } from "@/lib/runtimeConfig";
import { cn } from "@/lib/utils";
import {
  RowsCell,
  UpdatedCell,
  groupByCatalog,
  isSystemTable,
  tableHref,
  type CatalogGroup,
} from "./Catalog";

const levelHref = (catalog?: string, schema?: string) => {
  if (!catalog) return catalogPath;
  const q = `catalog=${encodeURIComponent(catalog)}`;
  return schema ? `${catalogPath}?${q}&schema=${encodeURIComponent(schema)}` : `${catalogPath}?${q}`;
};

const plural = (n: number, one: string, many = `${one}s`) => `${n} ${n === 1 ? one : many}`;

function Row({
  to,
  name,
  meta,
  icon,
  testid,
  data = {},
}: {
  to: string;
  name: React.ReactNode;
  meta?: React.ReactNode;
  icon?: React.ReactNode;
  testid: string;
  data?: Record<string, string>;
}) {
  return (
    <Link
      to={to}
      data-testid={testid}
      {...Object.fromEntries(Object.entries(data).map(([k, v]) => [`data-${k}`, v]))}
      className="flex min-h-14 items-center gap-3 border-t border-border px-4 py-2.5 first:border-t-0 hover:bg-secondary/50"
    >
      {icon && <span className="shrink-0 text-muted-foreground">{icon}</span>}
      <span className="min-w-0 flex-1">
        <span className="block break-all font-mono text-sm font-medium">{name}</span>
        {meta && (
          <span className="mt-0.5 flex flex-wrap items-center gap-x-3 gap-y-0.5 text-xs text-muted-foreground">
            {meta}
          </span>
        )}
      </span>
      <ChevronRight className="h-4 w-4 shrink-0 text-muted-foreground" />
    </Link>
  );
}

function List({ children, className }: { children: React.ReactNode; className?: string }) {
  return <div className={cn("overflow-hidden rounded-xl border border-border bg-card", className)}>{children}</div>;
}

function Tree({ catalogs, catalog, schema }: { catalogs: CatalogGroup[]; catalog: string; schema: string }) {
  const item = "block truncate rounded-md px-2 py-1 font-mono text-xs hover:bg-secondary/60 hover:text-foreground";
  return (
    <nav
      aria-label="Catalog tree"
      data-testid="catalog-tree"
      className="sticky top-0 hidden max-h-[calc(100dvh-8rem)] overflow-y-auto rounded-xl border border-border bg-card p-2 md:block"
    >
      {catalogs.map((c) => (
        <div key={c.catalog}>
          <Link
            to={levelHref(c.catalog)}
            title={c.catalog}
            className={cn(item, c.catalog === catalog && !schema ? "bg-secondary text-foreground" : "text-muted-foreground")}
          >
            {c.catalog}
          </Link>
          {c.catalog === catalog &&
            c.schemas.map((s) => (
              <div key={s.schema}>
                <Link
                  to={levelHref(c.catalog, s.schema)}
                  title={s.schema}
                  className={cn(item, "pl-5", s.schema === schema ? "bg-secondary text-foreground" : "text-muted-foreground")}
                >
                  {s.schema || "(default)"}
                </Link>
                {s.schema === schema &&
                  s.tables.map((t) => (
                    <Link key={t.name} to={tableHref(t)} title={t.name} className={cn(item, "pl-8 text-muted-foreground")}>
                      {displayTableName(t)}
                    </Link>
                  ))}
              </div>
            ))}
        </div>
      ))}
    </nav>
  );
}

function TableRows({ tables }: { tables: CatalogTable[] }) {
  return (
    <>
      {/* Phone: one row per table. */}
      <List className="md:hidden">
        {tables.map((t) => (
          <Row
            key={t.name}
            to={tableHref(t)}
            testid="catalog-table-row"
            data={{ "table-catalog": t.catalog, "table-schema": t.schema, "table-name": t.name }}
            name={displayTableName(t)}
            meta={
              <>
                <span className="flex items-center gap-1">
                  <RowsCell table={t} /> rows
                </span>
                <span className="flex items-center gap-1">
                  updated <UpdatedCell table={t} />
                </span>
                {!t.owning_node && isSystemTable(t.name) && <span className="italic">clavesa system table</span>}
              </>
            }
          />
        ))}
      </List>
      {/* Desktop: a table; it scrolls sideways only inside its own box,
          and the name column stays in place. */}
      <div className="hidden overflow-x-auto rounded-xl border border-border bg-card md:block">
        <table className="w-full border-collapse text-sm">
          <thead>
            <tr className="text-left text-[11px] uppercase tracking-wide text-muted-foreground">
              <th className="sticky left-0 bg-card px-4 py-2 font-semibold">Table</th>
              <th className="px-4 py-2 font-semibold">Format</th>
              <th className="px-4 py-2 font-semibold">Columns</th>
              <th className="px-4 py-2 font-semibold">Rows</th>
              <th className="px-4 py-2 font-semibold">Updated</th>
            </tr>
          </thead>
          <tbody>
            {tables.map((t) => (
              <tr key={t.name} className="group border-t border-border hover:bg-secondary/40">
                <td className="sticky left-0 bg-card px-4 py-2 group-hover:bg-secondary/40">
                  <Link to={tableHref(t)} data-testid="catalog-table-link" className="font-mono font-medium hover:text-primary hover:underline">
                    {displayTableName(t)}
                  </Link>
                </td>
                <td className="px-4 py-2 font-mono text-xs text-muted-foreground">{t.table_type || "—"}</td>
                <td className="px-4 py-2 text-xs text-muted-foreground">{t.columns.length ? plural(t.columns.length, "column") : "—"}</td>
                <td className="px-4 py-2"><RowsCell table={t} /></td>
                <td className="px-4 py-2"><UpdatedCell table={t} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}

export function CatalogLevels() {
  const { data, isLoading, error } = useCatalogTables();
  const [params] = useSearchParams();
  const catalog = params.get("catalog") ?? "";
  const schema = params.get("schema") ?? "";
  const [query, setQuery] = useState("");
  const q = query.trim().toLowerCase();

  const catalogs = useMemo(() => groupByCatalog(data?.tables ?? []), [data]);
  const cur = catalogs.find((c) => c.catalog === catalog);
  const curSchema = cur?.schemas.find((s) => s.schema === schema);

  useChrome(
    useMemo<PageChrome>(
      () => ({
        breadcrumbs: [
          { label: "Catalog", to: catalogPath },
          ...(catalog ? [{ label: catalog, to: levelHref(catalog) }] : []),
          ...(catalog && schema ? [{ label: schema, to: levelHref(catalog, schema) }] : []),
        ],
      }),
      [catalog, schema],
    ),
  );

  const title = schema || catalog || "Catalog";
  const matches = useMemo(() => {
    if (!q) return [];
    return (data?.tables ?? []).filter((t) =>
      [t.name, t.owning_node, t.output_key, t.catalog, t.schema].some((f) => (f ?? "").toLowerCase().includes(q)),
    );
  }, [data, q]);

  return (
    <div data-testid="catalog-levels" className="mx-auto w-full max-w-6xl px-4 py-4 md:px-6 md:py-6">
      <div className="grid grid-cols-1 items-start gap-5 md:grid-cols-[17rem_minmax(0,1fr)]">
        <Tree catalogs={catalogs} catalog={catalog} schema={schema} />
        <div className="min-w-0">
          <h1 className={cn("break-all text-xl font-semibold tracking-tight md:text-2xl", (catalog || schema) && "font-mono")}>
            {title}
          </h1>
          <QueryShell
            query={{ isLoading, error, data }}
            loading={
              <div className="mt-4 space-y-3">
                <Skeleton className="h-14 w-full" />
                <Skeleton className="h-14 w-full" />
              </div>
            }
            renderError={(err) => (
              <p className="mt-4 flex items-start gap-2 text-sm text-status-failed">
                <FileWarning className="mt-0.5 h-4 w-4 shrink-0" />
                {err instanceof Error ? err.message : String(err)}
              </p>
            )}
          >
            {(d) => {
              if (!catalog) {
                return (
                  <>
                    <p className="mb-3 mt-1 text-sm text-muted-foreground">
                      {plural(catalogs.length, "catalog")}, {plural(d.tables.length, "table")}
                    </p>
                    {d.tables.length > 0 && (
                      <div className="mb-3">
                        <ListSearch value={query} onChange={setQuery} placeholder="Find a table…" />
                      </div>
                    )}
                    {q ? (
                      matches.length ? (
                        <TableRows tables={matches} />
                      ) : (
                        <p className="text-sm text-muted-foreground">No table matches.</p>
                      )
                    ) : catalogs.length === 0 ? (
                      <p className="text-sm text-muted-foreground">No tables yet: deploy and run a pipeline.</p>
                    ) : (
                      <List>
                        {catalogs.map((c) => (
                          <Row
                            key={c.catalog}
                            to={levelHref(c.catalog)}
                            testid="catalog-row"
                            data={{ catalog: c.catalog }}
                            icon={<Database className="h-4 w-4" />}
                            name={c.catalog}
                            meta={
                              <>
                                <span>{plural(c.schemas.length, "schema")}</span>
                                <span>{plural(c.tableCount, "table")}</span>
                                {c.isSystem && <span className="text-amber-400">system: what clavesa records about runs</span>}
                              </>
                            }
                          />
                        ))}
                      </List>
                    )}
                  </>
                );
              }
              if (!cur) {
                return <p className="mt-2 text-sm text-muted-foreground">No catalog named {catalog}.</p>;
              }
              if (!schema) {
                return (
                  <>
                    <p className="mb-3 mt-1 text-sm text-muted-foreground">
                      {cur.isSystem ? "System catalog: what clavesa records about runs. " : ""}
                      {plural(cur.schemas.length, "schema")}
                    </p>
                    <List>
                      {cur.schemas.map((s) => {
                        const owner = s.tables.find((t) => t.owning_pipeline);
                        return (
                          <Row
                            key={s.schema}
                            to={levelHref(cur.catalog, s.schema)}
                            testid="schema-row"
                            data={{ schema: s.schema }}
                            icon={<Table2 className="h-4 w-4" />}
                            name={s.schema || "(default)"}
                            meta={
                              <>
                                <span>{plural(s.tables.length, "table")}</span>
                                {owner?.owning_pipeline && <span>written by pipeline {owner.owning_pipeline}</span>}
                              </>
                            }
                          />
                        );
                      })}
                    </List>
                  </>
                );
              }
              if (!curSchema) {
                return <p className="mt-2 text-sm text-muted-foreground">No schema named {schema} in {catalog}.</p>;
              }
              const owner = curSchema.tables.find((t) => t.owning_pipeline && t.dir);
              return (
                <>
                  <p className="mb-3 mt-1 text-sm text-muted-foreground">
                    {plural(curSchema.tables.length, "table")} in <span className="font-mono">{catalog}</span>
                    {owner && (
                      <>
                        {" · written by "}
                        <Link
                          to={`/pipelines/dashboard?dir=${encodeURIComponent(owner.dir)}`}
                          className="font-mono text-foreground hover:text-primary hover:underline"
                        >
                          {owner.owning_pipeline}
                        </Link>
                      </>
                    )}
                  </p>
                  <TableRows tables={curSchema.tables} />
                </>
              );
            }}
          </QueryShell>
        </div>
      </div>
    </div>
  );
}
