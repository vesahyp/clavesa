/**
 * Catalog — workspace data-catalog landing page.
 *
 * Lists every Delta table the workspace owns — both Glue-registered
 * tables from deployed pipelines and Hadoop-catalog tables produced by
 * compute = "local" pipelines (ADR-014). Grouped by owning pipeline,
 * each row clickable through to the table detail page.
 *
 * This is the new front door of the data-first UI rebuild — pipelines
 * are reachable via /pipelines but no longer the home.
 */

import { useMemo, useState } from "react";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import { formatDistanceToNow } from "date-fns";
import {
  ArrowRight,
  ChevronDown,
  ChevronRight,
  CloudOff,
  Database,
  FileWarning,
  Plus,
  Workflow,
} from "lucide-react";
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  createColumnHelper,
  type RowData,
} from "@tanstack/react-table";

import { useChrome, type PageChrome } from "@/components/PageChrome";
import { Highlight } from "@/components/Highlight";
import { ListSearch } from "@/components/ListSearch";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { useCatalogTables, usePipelines, useTableSnapshots, type CatalogTable } from "@/lib/queries";
import { displayTableName, showOutputKey } from "@/lib/format";

const columnHelper = createColumnHelper<CatalogTable>();

// The active search query is threaded into the table via TanStack's
// `meta` so the (module-level) column cells can highlight matches.
declare module "@tanstack/react-table" {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  interface TableMeta<TData extends RowData> {
    query: string;
  }
}

function formatUpdate(t: CatalogTable): string {
  if (!t.update_time) return "—";
  try {
    return formatDistanceToNow(new Date(t.update_time), { addSuffix: true });
  } catch {
    return t.update_time;
  }
}

function formatRelative(iso: string): string {
  try {
    return formatDistanceToNow(new Date(iso), { addSuffix: true });
  } catch {
    return iso;
  }
}

// Tables clavesa owns at the database level but doesn't expose under
// the `<node>__<key>` naming convention. The runner appends node_runs and
// tables (the latter capturing per-output state per pipeline run); the
// EventBridge writer (cloud) / pipeline-run orchestrator (local) writes
// runs. Recognising them here keeps them out of the "non-clavesa" bucket
// in the catalog UI.
const CLAVESA_SYSTEM_TABLES = new Set([
  "node_runs",
  "runs",
  "tables",
  "column_stats",
  "dashboards",
]);

function isSystemTable(name: string): boolean {
  return CLAVESA_SYSTEM_TABLES.has(name);
}

function formatRowCount(n: number): string {
  if (n < 1_000) return n.toString();
  if (n < 1_000_000) return `${(n / 1_000).toFixed(n < 10_000 ? 1 : 0)}k`;
  if (n < 1_000_000_000) return `${(n / 1_000_000).toFixed(n < 10_000_000 ? 1 : 0)}M`;
  return `${(n / 1_000_000_000).toFixed(1)}B`;
}

// Workspace system catalogs end in `_system` by convention
// (`DefaultSystemCatalog(catalog) = catalog + "_system"` in
// internal/workspace/workspace.go). Used purely for UI affordance — the
// per-row data already routes through the right backend regardless.
function isSystemCatalog(catalog: string): boolean {
  return catalog.endsWith("_system");
}

function tableHref(t: CatalogTable): string {
  return `/tables/${encodeURIComponent(t.catalog)}/${encodeURIComponent(t.schema)}/${encodeURIComponent(t.name)}`;
}

const columns = [
  columnHelper.accessor("name", {
    header: "Table",
    cell: (info) => {
      const t = info.row.original;
      // The key sub-label only earns its place for genuine multi-output
      // nodes; "default" is the implicit single-output key.
      const showKey = showOutputKey(t);
      const q = info.table.options.meta?.query ?? "";
      const kind = t.owning_node
        ? null
        : isSystemTable(t.name)
          ? "clavesa system table"
          : "non-clavesa table";
      return (
        <div className="flex flex-col">
          <span className="font-medium text-foreground">
            <Highlight text={displayTableName(t)} query={q} />
          </span>
          {showKey && (
            <span className="font-mono text-xs text-muted-foreground">
              · <Highlight text={t.output_key} query={q} />
            </span>
          )}
          {kind && (
            <span className="text-xs italic text-muted-foreground">{kind}</span>
          )}
        </div>
      );
    },
  }),
  columnHelper.accessor("table_type", {
    header: "Format",
    cell: (info) => {
      const v = info.getValue();
      if (!v) return <span className="text-xs text-muted-foreground">—</span>;
      return (
        <Badge variant="outline" className="font-mono text-[10px]">
          {v}
        </Badge>
      );
    },
  }),
  columnHelper.accessor("columns", {
    header: "Columns",
    cell: (info) => {
      const cols = info.getValue();
      if (cols.length === 0) {
        return <span className="text-xs text-muted-foreground">—</span>;
      }
      return (
        <Tooltip>
          <TooltipTrigger asChild>
            <span className="cursor-default text-xs text-muted-foreground">
              {cols.length} {cols.length === 1 ? "column" : "columns"}
            </span>
          </TooltipTrigger>
          <TooltipContent className="max-w-sm font-mono">
            {cols.slice(0, 12).map((c) => (
              <div key={c.name}>
                <span className="text-foreground">{c.name}</span>{" "}
                <span className="text-muted-foreground">{c.type}</span>
              </div>
            ))}
            {cols.length > 12 && (
              <div className="text-muted-foreground">
                +{cols.length - 12} more
              </div>
            )}
          </TooltipContent>
        </Tooltip>
      );
    },
  }),
  columnHelper.display({
    id: "rows",
    header: "Rows",
    cell: (info) => <RowsCell table={info.row.original} />,
  }),
  columnHelper.display({
    id: "snapshots",
    header: "Commits",
    cell: (info) => <SnapshotsCell table={info.row.original} />,
  }),
  columnHelper.display({
    id: "updated",
    header: "Updated",
    cell: (info) => <UpdatedCell table={info.row.original} />,
  }),
  columnHelper.display({
    id: "open",
    header: "",
    cell: () => (
      <ArrowRight className="h-4 w-4 text-muted-foreground transition-colors group-hover:text-foreground" />
    ),
  }),
];

/**
 * Per-row commit data hooks. Each visible Delta-managed row issues one
 * commit-history fetch — TanStack Query dedupes via the cache key,
 * parallelizes across rows, and serves stale data for 60s before
 * refetching, so the cost stays bounded for typical workspaces.
 *
 * Errors are silent: a non-Delta table or a missing commit history
 * shouldn't break the rest of the catalog. We just fall back to Glue's
 * UpdateTime and dashes for the data we couldn't fetch.
 */

function RowsCell({ table }: { table: CatalogTable }) {
  const isClavesaManaged = table.table_type === "DELTA";
  const { data, isLoading } = useTableSnapshots(
    isClavesaManaged ? table.database : "",
    isClavesaManaged ? table.name : "",
    1,
    { dir: table.dir },
  );
  if (!isClavesaManaged) {
    return <span className="text-xs text-muted-foreground">—</span>;
  }
  if (isLoading) {
    return <Skeleton className="h-4 w-12" />;
  }
  if (data?.latest_record_count == null) {
    return <span className="text-xs text-muted-foreground">—</span>;
  }
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Badge variant="secondary" className="cursor-default font-mono text-[10px]">
          {formatRowCount(data.latest_record_count)}
        </Badge>
      </TooltipTrigger>
      <TooltipContent>
        {data.latest_record_count.toLocaleString()} rows
      </TooltipContent>
    </Tooltip>
  );
}

function SnapshotsCell({ table }: { table: CatalogTable }) {
  const isClavesaManaged = table.table_type === "DELTA";
  const { data, isLoading } = useTableSnapshots(
    isClavesaManaged ? table.database : "",
    isClavesaManaged ? table.name : "",
    20,
    { dir: table.dir },
  );
  if (!isClavesaManaged) {
    return <span className="text-xs text-muted-foreground">—</span>;
  }
  if (isLoading) {
    return <Skeleton className="h-4 w-8" />;
  }
  const snaps = data?.snapshots ?? [];
  if (snaps.length === 0) {
    return <span className="text-xs text-muted-foreground">—</span>;
  }
  const label = data?.truncated ? `${snaps.length}+` : String(snaps.length);
  return (
    <Badge variant="outline" className="font-mono text-[10px]">
      {label}
    </Badge>
  );
}

function UpdatedCell({ table }: { table: CatalogTable }) {
  const isClavesaManaged = table.table_type === "DELTA";
  const { data } = useTableSnapshots(
    isClavesaManaged ? table.database : "",
    isClavesaManaged ? table.name : "",
    1,
    { dir: table.dir },
  );
  // Prefer the latest Delta commit's committed_at when available — it's
  // the actual data freshness signal. Glue's UpdateTime is only the catalog
  // metadata mtime and lags behind real writes.
  const latest = data?.snapshots?.[0]?.committed_at;
  const text = latest ? formatRelative(latest) : formatUpdate(table);

  // SLA chip — green when within budget, amber at >50%, red over.
  // Hidden when freshness_sla_seconds is 0 (HCL didn't declare one) or
  // when we don't have a real snapshot timestamp to compare against.
  const slaSec = table.freshness_sla_seconds ?? 0;
  let chip: { variant: "success" | "running" | "failed"; label: string } | null =
    null;
  if (slaSec > 0 && latest) {
    const ageSec = Math.max(0, (Date.now() - new Date(latest).getTime()) / 1000);
    const ratio = ageSec / slaSec;
    if (ratio >= 1) {
      chip = { variant: "failed", label: "stale" };
    } else if (ratio >= 0.5) {
      chip = { variant: "running", label: "aging" };
    } else {
      chip = { variant: "success", label: "fresh" };
    }
  }

  return (
    <span className="flex items-center gap-2 text-xs text-muted-foreground">
      {chip && (
        <Badge
          variant={chip.variant}
          className="text-[10px] capitalize"
          title={`Freshness SLA: ${formatSLABudget(slaSec)}`}
        >
          {chip.label}
        </Badge>
      )}
      <span>{text}</span>
    </span>
  );
}

// Render the SLA budget back as the human shorthand the user wrote
// (4h, 30m, 7d). Hours is the most common write; default to it for
// inputs that don't divide cleanly into a single unit.
function formatSLABudget(seconds: number): string {
  if (seconds <= 0) return "—";
  if (seconds % 86400 === 0) return `${seconds / 86400}d`;
  if (seconds % 3600 === 0) return `${seconds / 3600}h`;
  if (seconds % 60 === 0) return `${seconds / 60}m`;
  return `${seconds}s`;
}

interface SchemaGroup {
  schema: string;
  tables: CatalogTable[];
}

interface CatalogGroup {
  catalog: string;
  isSystem: boolean;
  schemas: SchemaGroup[];
  tableCount: number;
}

// ADR-016's three-level namespace: a catalog is a whole workspace (one
// user catalog plus its `_system` companion), a schema is a pipeline.
// The Catalog page gives each catalog a single box and nests its
// schemas inside, so the eye reads one catalog as one coherent data
// model rather than a pile of unrelated pipelines. ADR-020: the API
// surfaces `catalog`/`schema`/`table` as separate fields (Slice 7), so
// the UI consumes them directly — no `__`-marker parsing client-side.
// System catalogs sort last so the user's pipeline outputs sit where
// the eye looks first.
function groupByCatalog(tables: CatalogTable[]): CatalogGroup[] {
  const byCatalog = new Map<string, Map<string, CatalogTable[]>>();
  for (const t of tables) {
    let schemas = byCatalog.get(t.catalog);
    if (!schemas) {
      schemas = new Map();
      byCatalog.set(t.catalog, schemas);
    }
    const arr = schemas.get(t.schema) ?? [];
    arr.push(t);
    schemas.set(t.schema, arr);
  }
  return Array.from(byCatalog.entries())
    .map(([catalog, schemaMap]) => {
      const schemas = Array.from(schemaMap.entries())
        .map(([schema, ts]) => ({
          schema,
          tables: [...ts].sort((a, b) => a.name.localeCompare(b.name)),
        }))
        .sort((a, b) => a.schema.localeCompare(b.schema));
      return {
        catalog,
        isSystem: isSystemCatalog(catalog),
        schemas,
        tableCount: schemas.reduce((n, s) => n + s.tables.length, 0),
      };
    })
    .sort((a, b) => {
      if (a.isSystem !== b.isSystem) return a.isSystem ? 1 : -1;
      return a.catalog.localeCompare(b.catalog);
    });
}

const CATALOG_CHROME: PageChrome = {
  breadcrumbs: [{ label: "Catalog", to: "/" }],
};

export function Catalog() {
  const { data, isLoading, error } = useCatalogTables();
  const pipelines = usePipelines();
  const [searchParams] = useSearchParams();
  const catalogFilter = searchParams.get("catalog") ?? "";
  const schemaFilter = searchParams.get("schema") ?? "";

  // Free-text filter — composes with the ?catalog=/?schema= URL filters.
  // Matches case-insensitively against table name, owning node, output
  // key, catalog, and schema; empty groups drop out of groupByCatalog.
  const [query, setQuery] = useState("");
  const q = query.trim().toLowerCase();
  const isFiltered = !!(catalogFilter || schemaFilter || q);

  const textFiltered = useMemo(() => {
    const all = data?.tables ?? [];
    if (!q) return all;
    return all.filter((t) =>
      [t.name, t.owning_node, t.output_key, t.catalog, t.schema].some((f) =>
        f.toLowerCase().includes(q),
      ),
    );
  }, [data, q]);

  const catalogs = useMemo(
    () => groupByCatalog(textFiltered),
    [textFiltered]
  );
  // ADR-016 schema-scoped view: ?catalog= / ?schema= narrow the listing
  // to one catalog or one schema. One fetch, filtered client-side.
  const filtered = useMemo(() => {
    let cs = catalogs;
    if (catalogFilter) {
      cs = cs.filter((c) => c.catalog === catalogFilter);
    }
    if (schemaFilter) {
      cs = cs
        .map((c) => {
          const schemas = c.schemas.filter((s) => s.schema === schemaFilter);
          return {
            ...c,
            schemas,
            tableCount: schemas.reduce((n, s) => n + s.tables.length, 0),
          };
        })
        .filter((c) => c.schemas.length > 0);
    }
    return cs;
  }, [catalogs, catalogFilter, schemaFilter]);
  const shownTables = useMemo(
    () => filtered.reduce((n, c) => n + c.tableCount, 0),
    [filtered]
  );
  const schemaCount = useMemo(
    () => filtered.reduce((n, c) => n + c.schemas.length, 0),
    [filtered]
  );

  useChrome(CATALOG_CHROME);

  return (
    <div className="mx-auto w-full max-w-6xl px-6 py-8">
        <div className="mb-6 flex items-end justify-between gap-4">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight">Catalog</h1>
            <p className="mt-1 text-sm text-muted-foreground">
              Delta tables in this workspace — deployed pipelines via Glue
              (queryable from Athena) and local pipelines via the Hadoop
              catalog under <code className="font-mono">.clavesa/warehouse/</code>.
            </p>
          </div>
          {data && (
            <div className="flex items-center gap-3 text-xs text-muted-foreground">
              <span>
                <span className="font-semibold text-foreground">
                  {isFiltered
                    ? `${shownTables} of ${data.tables.length}`
                    : data.tables.length}
                </span>{" "}
                table{data.tables.length === 1 ? "" : "s"}
              </span>
              <span>·</span>
              <span>
                <span className="font-semibold text-foreground">
                  {schemaCount}
                </span>{" "}
                schema{schemaCount === 1 ? "" : "s"}
              </span>
            </div>
          )}
        </div>

        {data && data.tables.length > 0 && (
          <div className="mb-4">
            <ListSearch
              value={query}
              onChange={setQuery}
              placeholder="Filter tables…"
            />
          </div>
        )}

        {(catalogFilter || schemaFilter) && (
          <div className="mb-4 flex flex-wrap items-center gap-2 text-sm">
            <span className="text-muted-foreground">Filtered to</span>
            {catalogFilter && (
              <Badge variant="secondary" className="font-mono text-xs">
                catalog: {catalogFilter}
              </Badge>
            )}
            {schemaFilter && (
              <Badge variant="secondary" className="font-mono text-xs">
                schema: {schemaFilter}
              </Badge>
            )}
            <Link
              to="/"
              onClick={() => setQuery("")}
              className="text-xs text-primary hover:underline"
            >
              clear filter
            </Link>
          </div>
        )}

        {error && (
          <Card className="border-destructive/40 bg-destructive/5">
            <CardHeader className="flex-row items-start gap-3">
              <FileWarning className="mt-0.5 h-5 w-5 text-destructive" />
              <div>
                <CardTitle className="text-destructive">
                  Failed to load catalog
                </CardTitle>
                <p className="mt-1 text-xs text-muted-foreground">
                  {error instanceof Error ? error.message : String(error)}
                </p>
              </div>
            </CardHeader>
          </Card>
        )}

        {isLoading && (
          <div className="space-y-3">
            <Skeleton className="h-6 w-40" />
            <Skeleton className="h-24 w-full" />
            <Skeleton className="h-24 w-full" />
          </div>
        )}

        {data && data.tables.length === 0 && (
          <WelcomeEmptyState
            hasPipelines={(pipelines.data?.length ?? 0) > 0}
            awsAvailable={data.aws_available}
          />
        )}

        {data && data.tables.length > 0 && !data.aws_available && (
          <Card className="mb-4 border-muted bg-muted/30">
            <CardContent className="flex items-center gap-3 py-3 text-xs text-muted-foreground">
              <CloudOff className="h-4 w-4 flex-shrink-0" />
              <span>
                AWS not configured — showing local pipelines only. Set{" "}
                <code className="font-mono">AWS_PROFILE</code> and restart to
                include Glue-registered tables from deployed pipelines.
              </span>
            </CardContent>
          </Card>
        )}

        {filtered.length > 0 && (
          <div className="space-y-6">
            {filtered.map((c) => (
              <CatalogCard key={c.catalog} catalog={c} query={q} />
            ))}
          </div>
        )}

        {data && data.tables.length > 0 && isFiltered && filtered.length === 0 && (
          <Card className="border-dashed">
            <CardContent className="flex flex-col items-center gap-2 py-10 text-center text-sm text-muted-foreground">
              <span>Nothing matches this filter.</span>
              <Link
                to="/"
                onClick={() => setQuery("")}
                className="text-xs text-primary hover:underline"
              >
                clear filter
              </Link>
            </CardContent>
          </Card>
        )}
    </div>
  );
}

// CatalogCard is one box per catalog. The catalog == the workspace, so
// there is normally one user-data card plus the `_system` card; every
// schema (== pipeline) the catalog owns nests inside as its own section.
function CatalogCard({
  catalog,
  query,
}: {
  catalog: CatalogGroup;
  query: string;
}) {
  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between gap-4 border-b border-border bg-card/50 py-3">
        <div className="flex items-center gap-3">
          <CardTitle className="font-mono text-sm">
            <Link
              to={`/?catalog=${encodeURIComponent(catalog.catalog)}`}
              className="hover:text-primary hover:underline"
            >
              <Highlight text={catalog.catalog} query={query} />
            </Link>
          </CardTitle>
          <Badge
            variant="outline"
            className={
              catalog.isSystem
                ? "border-amber-500/50 bg-amber-500/10 font-mono text-[10px] text-amber-700 dark:text-amber-400"
                : "font-mono text-[10px]"
            }
          >
            {catalog.isSystem ? "system catalog" : "workspace catalog"}
          </Badge>
        </div>
        <Badge variant="secondary" className="font-mono text-[10px]">
          {catalog.tableCount} {catalog.tableCount === 1 ? "table" : "tables"}
        </Badge>
      </CardHeader>
      <CardContent className="divide-y divide-border p-0">
        {catalog.schemas.map((s) => (
          <SchemaTableSection
            key={s.schema}
            catalog={catalog.catalog}
            schema={s}
            query={query}
          />
        ))}
      </CardContent>
    </Card>
  );
}

// SchemaTableSection renders one schema's tables inside a catalog box as
// a collapsible section. The labelled sub-header makes the (catalog,
// schema) split — ambiguous in the old `<catalog> / <schema>` heading —
// explicit; the table nests below it (indented past the schema label,
// never to its left) so the catalog > schema > table hierarchy reads
// straight down the page.
function SchemaTableSection({
  catalog,
  schema,
  query,
}: {
  catalog: string;
  schema: SchemaGroup;
  query: string;
}) {
  const navigate = useNavigate();
  const [open, setOpen] = useState(true);
  const tableInstance = useReactTable({
    data: schema.tables,
    columns,
    getCoreRowModel: getCoreRowModel(),
    meta: { query },
  });

  // ADR-016: a schema is owned by exactly one pipeline. Surface that
  // pipeline so the schema≡pipeline equivalence is visible — every
  // table in the schema carries the same owning pipeline + dir; system
  // and orphaned (un-owned) schemas have neither.
  const owner = schema.tables.find((t) => t.owning_pipeline && t.dir);
  const schemaFilterHref = `/?catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema.schema)}`;

  return (
    <div>
      {/* Chevron toggles collapse. When the schema has a producing
          pipeline the header leads with the pipeline name (→ its
          dashboard) and shows the schema id as a secondary link to the
          schema-scoped Catalog view; an un-owned schema just shows the
          schema id. Separate targets so no click action is ambiguous. */}
      <div className="flex items-center gap-1.5 px-6 py-2.5">
        <button
          type="button"
          onClick={() => setOpen((v) => !v)}
          aria-expanded={open}
          aria-label={open ? "Collapse schema" : "Expand schema"}
          className="-m-1 flex-shrink-0 rounded p-1 text-muted-foreground transition-colors hover:bg-muted/60 hover:text-foreground"
        >
          {open ? (
            <ChevronDown className="h-4 w-4" />
          ) : (
            <ChevronRight className="h-4 w-4" />
          )}
        </button>
        {owner ? (
          <>
            <Workflow className="ml-0.5 h-3.5 w-3.5 flex-shrink-0 text-muted-foreground" />
            <Link
              to={`/pipelines/dashboard?dir=${encodeURIComponent(owner.dir)}`}
              className="font-mono text-sm font-medium hover:text-primary hover:underline"
              title="Open this pipeline's dashboard"
            >
              {owner.owning_pipeline}
            </Link>
            <span className="font-mono text-[10px] uppercase tracking-wide text-muted-foreground/70">
              schema
            </span>
            <Link
              to={schemaFilterHref}
              className="font-mono text-xs text-muted-foreground hover:text-primary hover:underline"
              title="Filter the catalog to this schema"
            >
              <Highlight text={schema.schema} query={query} />
            </Link>
          </>
        ) : (
          <>
            <span className="ml-0.5 font-mono text-[10px] uppercase tracking-wide text-muted-foreground/70">
              schema
            </span>
            <Link
              to={schemaFilterHref}
              className="font-mono text-sm font-medium hover:text-primary hover:underline"
            >
              {schema.schema ? (
                <Highlight text={schema.schema} query={query} />
              ) : (
                "(default)"
              )}
            </Link>
          </>
        )}
        <Badge variant="secondary" className="font-mono text-[10px]">
          {schema.tables.length} {schema.tables.length === 1 ? "table" : "tables"}
        </Badge>
      </div>
      {open && (
        <div className="pb-2 pl-9">
          <Table>
            <TableHeader>
              {tableInstance.getHeaderGroups().map((hg) => (
                <TableRow key={hg.id} className="hover:bg-transparent">
                  {hg.headers.map((h) => (
                    <TableHead key={h.id}>
                      {h.isPlaceholder
                        ? null
                        : flexRender(h.column.columnDef.header, h.getContext())}
                    </TableHead>
                  ))}
                </TableRow>
              ))}
            </TableHeader>
            <TableBody>
              {tableInstance.getRowModel().rows.map((row) => (
                <TableRow
                  key={row.id}
                  onClick={() => navigate(tableHref(row.original))}
                  className="group cursor-pointer"
                >
                  {row.getVisibleCells().map((cell) => (
                    <TableCell key={cell.id}>
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </TableCell>
                  ))}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
    </div>
  );
}

interface EmptyStateProps {
  icon: React.ReactNode;
  title: string;
  body: string;
  action?: React.ReactNode;
}

function EmptyState({ icon, title, body, action }: EmptyStateProps) {
  return (
    <Card className="border-dashed">
      <CardContent className="flex flex-col items-center gap-3 py-12 text-center">
        <span className="flex h-12 w-12 items-center justify-center rounded-full bg-muted text-muted-foreground">
          {icon}
        </span>
        <div className="text-base font-semibold">{title}</div>
        <p className="max-w-md text-sm text-muted-foreground">{body}</p>
        {action}
      </CardContent>
    </Card>
  );
}

// WelcomeEmptyState renders the first-launch hero on Catalog when the
// workspace has no Delta tables yet. Two shapes:
//   - no pipelines exist → 3-step quickstart with primary CTA "Create a
//     pipeline"; the AWS-availability state is a soft footnote because
//     the local-only path doesn't need it.
//   - pipelines exist but haven't produced tables → "Run one of your
//     pipelines" with a Pipelines CTA. The user has authored something
//     but hasn't fired it yet.
function WelcomeEmptyState({
  hasPipelines,
  awsAvailable,
}: {
  hasPipelines: boolean;
  awsAvailable: boolean;
}) {
  if (hasPipelines) {
    return (
      <EmptyState
        icon={<Database className="h-6 w-6" />}
        title="No tables yet"
        body="Your pipelines haven't produced output tables. Open Pipelines, pick one, and click Run pipeline — the catalog populates once a transform succeeds."
        action={
          <Button asChild size="sm">
            <Link to="/pipelines">
              <Workflow className="h-4 w-4" />
              Go to Pipelines
            </Link>
          </Button>
        }
      />
    );
  }

  return (
    <Card className="border-dashed">
      <CardContent className="space-y-6 py-10">
        <div className="flex flex-col items-center gap-3 text-center">
          <span className="flex h-12 w-12 items-center justify-center rounded-full bg-muted text-muted-foreground">
            <Database className="h-6 w-6" />
          </span>
          <div>
            <div className="text-base font-semibold">Welcome to your workspace</div>
            <p className="mt-1 max-w-md text-sm text-muted-foreground">
              Every pipeline output lands here as a Delta table. To get
              your first row, do the three steps below — no terminal needed
              after this point.
            </p>
          </div>
        </div>

        <ol className="mx-auto grid max-w-2xl gap-3 text-sm sm:grid-cols-3">
          <QuickstartStep n={1} title="Register a source" body="Workspace input registry — a public URL or an s3:// prefix." />
          <QuickstartStep n={2} title="Create a pipeline" body="Add a SQL Transform, attach the source as an input, set the SQL." />
          <QuickstartStep n={3} title="Run it" body="One click on the pipeline dashboard. The output table appears here." />
        </ol>

        <div className="flex flex-wrap items-center justify-center gap-2">
          <Button asChild size="sm">
            <Link to="/pipelines">
              <Plus className="h-4 w-4" />
              Create a pipeline
            </Link>
          </Button>
          <Button asChild size="sm" variant="outline">
            <Link to="/sources">
              <Database className="h-4 w-4" />
              Manage sources
            </Link>
          </Button>
        </div>

        {!awsAvailable && (
          <p className="text-center text-[11px] text-muted-foreground">
            <CloudOff className="mr-1 inline h-3 w-3" />
            AWS not configured — the laptop path works without it; set{" "}
            <code className="font-mono">AWS_PROFILE</code> later when you&apos;re ready
            to deploy.
          </p>
        )}
      </CardContent>
    </Card>
  );
}

function QuickstartStep({ n, title, body }: { n: number; title: string; body: string }) {
  return (
    <div className="rounded-md border border-border bg-background p-3">
      <div className="mb-1 flex items-center gap-2">
        <span className="flex h-5 w-5 items-center justify-center rounded-full bg-muted text-[11px] font-semibold text-foreground">
          {n}
        </span>
        <span className="text-sm font-semibold">{title}</span>
      </div>
      <p className="text-xs text-muted-foreground">{body}</p>
    </div>
  );
}

