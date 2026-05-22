/**
 * TableDetail — per-table view: schema + sample rows.
 *
 * The "is my data OK?" page. Hits Athena for a sample via the existing
 * /api/data/table endpoint. Volume timeline, lineage, and writer history
 * are subsequent slices once the observability tables exist.
 */

import { useMemo } from "react";
import { Navigate, useParams, NavLink } from "react-router-dom";
import { formatDistanceToNow } from "date-fns";
import { Database, FileWarning } from "lucide-react";

import { useChrome, type PageChrome } from "@/components/PageChrome";
import { CopyButton } from "@/components/CopyButton";
import { LineagePanel } from "@/components/LineagePanel";
import { Badge } from "@/components/ui/badge";
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
  useCatalogTables,
  useColumnStats,
  usePipelines,
  useTableSample,
  useTableSnapshots,
  type CatalogTable,
  type ColumnStat,
  type ColumnStatsResult,
  type SnapshotInfo,
  type SnapshotsResult,
} from "@/lib/queries";

const SAMPLE_LIMIT = 100;
const SNAPSHOTS_LIMIT = 20;

// stripDefaultSuffix drops the `__default` output-key suffix from a table
// identifier for display. A single-output transform keys its output to
// "default", so `revenue_by_payment__default` reads as noise; multi-output
// tables (`node__clean`) keep their suffix untouched.
function stripDefaultSuffix(name: string): string {
  return name.endsWith("__default") ? name.slice(0, -"__default".length) : name;
}

export function TableDetail() {
  const params = useParams<{ catalog: string; schema: string; table: string }>();
  const catalogName = decodeURIComponent(params.catalog ?? "");
  const schemaName = decodeURIComponent(params.schema ?? "");
  const tableName = decodeURIComponent(params.table ?? "");
  // Glue / Hadoop catalog identifier stays `<catalog>__<schema>` per
  // ADR-016 — that's what the backend queries use. The URL surfaces the
  // three pieces separately but the on-disk shape is unchanged.
  const database = schemaName ? `${catalogName}__${schemaName}` : catalogName;
  // Fully-qualified name as written in dashboard / ad-hoc SQL:
  // `clavesa.<catalog>__<schema>.<table>` — the runner's Iceberg
  // catalog is `clavesa`, and the same three-part name resolves on
  // Athena. This is the string a user pastes into a dataset query.
  const tablePath = `clavesa.${database}.${tableName}`;

  const catalog = useCatalogTables();
  const tableMeta = useMemo<CatalogTable | undefined>(() => {
    return catalog.data?.tables.find(
      (t) => t.database === database && t.name === tableName
    );
  }, [catalog.data, database, tableName]);

  // The catalog response stamps `dir` (relative to the workspace root) on
  // every table when the owning pipeline is in the user's workspace — both
  // cloud and local pipelines (ADR-014 parity). Threading it through the
  // sample/snapshot queries is what lets the server's observability.Resolver
  // pick the right Provider (cloud vs local) from the pipeline's `compute`
  // attr. Falling back to the pipelines-list lookup catches the rare case
  // where the catalog stamp is missing — the API takes either form.
  const pipelines = usePipelines();
  const owningPipelineDir = useMemo(() => {
    if (tableMeta?.dir) return tableMeta.dir;
    if (!tableMeta?.owning_pipeline) return "";
    return pipelines.data?.find((p) => p.name === tableMeta.owning_pipeline)?.dir ?? "";
  }, [pipelines.data, tableMeta]);
  // Gate sample-fetch on the catalog having resolved so `dir` is known
  // before we hit /data/table — otherwise the first render fires the
  // cloud-fallback path (no dir → Athena), 500s for local tables, and
  // the user sees a "Query failed" flash before the second request with
  // dir lands. Cloud tables also benefit: the catalog payload tells us
  // whether the table even exists.
  const sample = useTableSample(database, tableName, SAMPLE_LIMIT, {
    dir: owningPipelineDir,
    enabled: catalog.data !== undefined,
  });
  const isIceberg = tableMeta?.table_type === "ICEBERG";
  const snapshots = useTableSnapshots(
    isIceberg ? database : "",
    isIceberg ? tableName : "",
    SNAPSHOTS_LIMIT,
    { dir: owningPipelineDir },
  );
  const columnStats = useColumnStats(
    isIceberg ? database : "",
    isIceberg ? tableName : "",
    { dir: owningPipelineDir },
  );

  useChrome(
    useMemo<PageChrome>(() => {
      const tableHref = `/tables/${encodeURIComponent(catalogName)}/${encodeURIComponent(schemaName)}/${encodeURIComponent(tableName)}`;
      return {
        breadcrumbs: [
          { label: "Catalog", to: "/" },
          {
            label: catalogName,
            to: `/?catalog=${encodeURIComponent(catalogName)}`,
          },
          ...(schemaName
            ? [
                {
                  label: schemaName,
                  to: `/?catalog=${encodeURIComponent(catalogName)}&schema=${encodeURIComponent(schemaName)}`,
                },
              ]
            : []),
          { label: stripDefaultSuffix(tableName), to: tableHref },
        ],
      };
    }, [catalogName, schemaName, tableName]),
  );

  return (
    <div className="mx-auto w-full max-w-6xl px-6 py-8">
        <div className="mb-6 flex items-end justify-between gap-6">
          <div className="min-w-0">
            <h1 className="font-mono text-2xl font-semibold tracking-tight">
              {tableMeta?.owning_node || stripDefaultSuffix(tableName)}
              {tableMeta?.output_key && tableMeta.output_key !== "default" && (
                <span className="text-muted-foreground"> · {tableMeta.output_key}</span>
              )}
            </h1>
            <div className="mt-2 flex items-center gap-1.5">
              <code className="break-all rounded bg-muted px-1.5 py-0.5 font-mono text-xs text-foreground">
                {tablePath}
              </code>
              <CopyButton value={tablePath} label="Copy table path" />
            </div>
            {tableMeta?.owning_pipeline && (
              <p className="mt-1 text-sm text-muted-foreground">
                Produced by pipeline{" "}
                {owningPipelineDir ? (
                  <NavLink
                    to={`/pipelines/dashboard?dir=${encodeURIComponent(owningPipelineDir)}`}
                    className="rounded bg-muted px-1.5 py-0.5 font-mono text-xs text-foreground hover:bg-muted/70 hover:text-primary"
                  >
                    {tableMeta.owning_pipeline}
                  </NavLink>
                ) : (
                  <code className="rounded bg-muted px-1.5 py-0.5 font-mono text-xs text-foreground">
                    {tableMeta.owning_pipeline}
                  </code>
                )}
              </p>
            )}
          </div>
          {tableMeta?.table_type && (
            <Badge variant="outline" className="font-mono">
              {tableMeta.table_type}
            </Badge>
          )}
        </div>

        <div className="grid gap-6 lg:grid-cols-3">
          {/* Schema */}
          <Card className="lg:col-span-1">
            <CardHeader className="pb-3">
              <CardTitle>Schema</CardTitle>
            </CardHeader>
            <CardContent className="p-0">
              {catalog.isLoading && (
                <div className="space-y-2 p-6">
                  <Skeleton className="h-4 w-full" />
                  <Skeleton className="h-4 w-3/4" />
                  <Skeleton className="h-4 w-2/3" />
                </div>
              )}
              {tableMeta && tableMeta.columns.length === 0 && (
                <div className="p-6 text-sm text-muted-foreground">
                  No columns reported.
                </div>
              )}
              {tableMeta && tableMeta.columns.length > 0 && (
                <Table>
                  <TableBody>
                    {tableMeta.columns.map((c) => (
                      <TableRow key={c.name} className="hover:bg-transparent">
                        <TableCell className="font-mono text-xs">{c.name}</TableCell>
                        <TableCell className="text-right font-mono text-xs text-muted-foreground">
                          {c.type}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              )}
            </CardContent>
          </Card>

          {/* Sample rows */}
          <Card className="lg:col-span-2">
            <CardHeader className="flex-row items-center justify-between pb-3">
              <CardTitle>Sample</CardTitle>
              {sample.data && (
                <span className="text-xs text-muted-foreground">
                  {sample.data.row_count}
                  {sample.data.truncated ? "+" : ""} row
                  {sample.data.row_count === 1 ? "" : "s"}
                </span>
              )}
            </CardHeader>
            <CardContent className="p-0">
              {sample.isLoading && (
                <div className="space-y-2 p-6">
                  <Skeleton className="h-4 w-full" />
                  <Skeleton className="h-4 w-full" />
                  <Skeleton className="h-4 w-full" />
                  <Skeleton className="h-4 w-full" />
                </div>
              )}
              {sample.error && (
                <div className="m-6 flex items-start gap-3 rounded-md border border-destructive/40 bg-destructive/5 p-4 text-sm">
                  <FileWarning className="mt-0.5 h-4 w-4 flex-shrink-0 text-destructive" />
                  <div className="min-w-0">
                    <div className="font-medium text-destructive">Query failed</div>
                    <p className="mt-1 break-words text-xs text-muted-foreground">
                      {sample.error instanceof Error
                        ? sample.error.message
                        : String(sample.error)}
                    </p>
                  </div>
                </div>
              )}
              {sample.data && sample.data.rows.length === 0 && (
                <div className="flex flex-col items-center gap-2 p-12 text-center">
                  <Database className="h-6 w-6 text-muted-foreground" />
                  <div className="text-sm text-muted-foreground">
                    Table is empty.
                  </div>
                </div>
              )}
              {sample.data && sample.data.rows.length > 0 && (
                <SampleTable data={sample.data} />
              )}
            </CardContent>
          </Card>
        </div>

        {isIceberg && columnStats.data && columnStats.data.stats.length > 0 && (
          <div className="mt-6">
            <ColumnStatsCard data={columnStats.data} />
          </div>
        )}

        {isIceberg && (
          <div className="mt-6">
            <VolumeTimelineCard
              isLoading={snapshots.isLoading}
              error={snapshots.error}
              data={snapshots.data}
            />
          </div>
        )}

        {/* Lineage requires a workspace dir AND an owning_node — system tables
            (node_runs, runs, tables) lack an owning_node and live outside the
            user's DAG, so showing them empty Upstream/Downstream messaging
            misleads about why nothing's there. */}
        {tableMeta && owningPipelineDir && tableMeta.owning_node && (
          <div className="mt-6">
            <LineagePanel
              dir={owningPipelineDir}
              database={database}
              table={tableName}
              owningNode={tableMeta.owning_node}
            />
          </div>
        )}
    </div>
  );
}

interface SampleTableProps {
  data: NonNullable<ReturnType<typeof useTableSample>["data"]>;
}

function SampleTable({ data }: SampleTableProps) {
  return (
    <div className="max-h-[60vh] overflow-auto">
      <Table>
        <TableHeader className="sticky top-0 z-10 bg-card">
          <TableRow className="hover:bg-transparent">
            {data.columns.map((c) => (
              <TableHead key={c.name} className="whitespace-nowrap">
                <div className="flex flex-col gap-0.5">
                  <span className="font-mono text-xs text-foreground">{c.name}</span>
                  <span className="font-mono text-[10px] font-normal normal-case text-muted-foreground">
                    {c.type}
                  </span>
                </div>
              </TableHead>
            ))}
          </TableRow>
        </TableHeader>
        <TableBody>
          {data.rows.map((row, i) => (
            <TableRow key={i}>
              {row.map((cell, j) => (
                <TableCell
                  key={j}
                  className="whitespace-nowrap font-mono text-xs"
                >
                  {cell === "" || cell == null ? (
                    <span className="text-muted-foreground/50">—</span>
                  ) : (
                    cell
                  )}
                </TableCell>
              ))}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

interface VolumeTimelineCardProps {
  isLoading: boolean;
  error: unknown;
  data: SnapshotsResult | undefined;
}

function VolumeTimelineCard({ isLoading, error, data }: VolumeTimelineCardProps) {
  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between pb-3">
        <CardTitle>Volume timeline</CardTitle>
        {data && data.snapshots.length > 0 && (
          <span className="text-xs text-muted-foreground">
            {data.snapshots.length}
            {data.truncated ? "+" : ""} snapshot
            {data.snapshots.length === 1 ? "" : "s"}
          </span>
        )}
      </CardHeader>
      <CardContent className="p-0">
        {isLoading && (
          <div className="space-y-2 p-6">
            <Skeleton className="h-4 w-full" />
            <Skeleton className="h-4 w-5/6" />
            <Skeleton className="h-4 w-2/3" />
          </div>
        )}
        {Boolean(error) && (
          <div className="m-6 flex items-start gap-3 rounded-md border border-destructive/40 bg-destructive/5 p-4 text-sm">
            <FileWarning className="mt-0.5 h-4 w-4 flex-shrink-0 text-destructive" />
            <div className="min-w-0">
              <div className="font-medium text-destructive">
                Snapshot history unavailable
              </div>
              <p className="mt-1 break-words text-xs text-muted-foreground">
                {error instanceof Error ? error.message : String(error)}
              </p>
            </div>
          </div>
        )}
        {data && data.snapshots.length === 0 && (
          <div className="p-6 text-sm text-muted-foreground">
            No snapshots reported for this table.
          </div>
        )}
        {data && data.snapshots.length > 0 && (
          <ol className="divide-y divide-border">
            {data.snapshots.map((s) => (
              <SnapshotRow key={s.snapshot_id} snap={s} />
            ))}
          </ol>
        )}
      </CardContent>
    </Card>
  );
}

// Transform-output snapshots written by clavesa carry an
// `clavesa.trigger` summary key naming the run that produced them. A
// snapshot with no key gets no badge: it was either a manual Athena/Spark
// INSERT, or written by a pre-provenance runner. Either way the bare row
// stands out against the badged ones, which is the signal users want.
const TRIGGER_DISPLAY: Record<string, { label: string; className: string }> = {
  backfill: {
    label: "backfill",
    className: "border-amber-500/40 bg-amber-500/10 text-amber-600",
  },
  event: {
    label: "triggered",
    className: "border-sky-500/40 bg-sky-500/10 text-sky-600",
  },
  scheduled: {
    label: "scheduled",
    className: "border-violet-500/40 bg-violet-500/10 text-violet-600",
  },
  manual: {
    label: "manual",
    className: "border-border bg-muted text-muted-foreground",
  },
};

function TriggerBadge({ trigger }: { trigger: string }) {
  if (!trigger) return null;
  const known = TRIGGER_DISPLAY[trigger];
  const display = known ?? {
    label: trigger,
    className: "border-dashed border-border bg-transparent text-muted-foreground",
  };
  return (
    <Badge
      variant="outline"
      className={`text-[10px] ${display.className}`}
      title={`Written by a clavesa ${trigger} run`}
    >
      {display.label}
    </Badge>
  );
}

function SnapshotRow({ snap }: { snap: SnapshotInfo }) {
  const added = snap.added_records ?? null;
  const deleted = snap.deleted_records ?? null;
  const total = snap.total_records ?? null;

  let when = snap.committed_at;
  try {
    when = formatDistanceToNow(new Date(snap.committed_at), { addSuffix: true });
  } catch {
    /* keep ISO */
  }

  // Render +N / -N / total in a single row. Most snapshots are appends, so
  // emphasize the added count; deletions and replaces still show through.
  const op = snap.operation || "—";
  return (
    <li className="flex items-center justify-between gap-4 px-6 py-2.5">
      <div className="flex min-w-0 items-center gap-3">
        <Badge variant="outline" className="font-mono text-[10px] uppercase">
          {op}
        </Badge>
        <TriggerBadge trigger={snap.trigger} />
        <div className="font-mono text-xs">
          {added != null && (
            <span className="text-emerald-500">
              +{added.toLocaleString()}
            </span>
          )}
          {deleted != null && deleted > 0 && (
            <span className="ml-2 text-destructive">
              -{deleted.toLocaleString()}
            </span>
          )}
          {added == null && deleted == null && (
            <span className="text-muted-foreground">no row delta</span>
          )}
          {total != null && (
            <span className="ml-3 text-muted-foreground">
              · {total.toLocaleString()} total
            </span>
          )}
        </div>
      </div>
      <div className="flex flex-shrink-0 items-center gap-3">
        {snap.writer_run_id && (
          <span
            className="font-mono text-[10px] text-muted-foreground/70"
            title={`Runner invocation ${snap.writer_run_id} — joins to node_runs.run_id`}
          >
            {snap.writer_run_id.slice(0, 8)}
          </span>
        )}
        <span className="whitespace-nowrap text-xs text-muted-foreground">
          {when}
        </span>
      </div>
    </li>
  );
}

// Numeric / timestamp Spark types render percentile chips; everything
// else hides them. Matches the runner's _is_numeric_type allow-list plus
// timestamps (the runner doesn't percentile timestamps today, but the
// shape leaves room).
function isNumericType(t: string): boolean {
  const s = t.toLowerCase();
  return (
    s.startsWith("bigint") ||
    s.startsWith("int") ||
    s.startsWith("smallint") ||
    s.startsWith("tinyint") ||
    s.startsWith("float") ||
    s.startsWith("double") ||
    s.startsWith("decimal")
  );
}

function formatCount(n: number | null | undefined): string {
  if (n == null) return "—";
  if (n < 1000) return String(n);
  if (n < 1_000_000) return `${(n / 1000).toFixed(n < 10_000 ? 1 : 0)}k`;
  return `${(n / 1_000_000).toFixed(n < 10_000_000 ? 1 : 0)}M`;
}

function ColumnStatsCard({ data }: { data: ColumnStatsResult }) {
  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between pb-3">
        <CardTitle>Column profile</CardTitle>
        <span className="text-xs text-muted-foreground">
          {data.stats.length} column{data.stats.length === 1 ? "" : "s"}
          {data.snapshot_id ? ` · snapshot ${data.snapshot_id.slice(0, 12)}` : ""}
        </span>
      </CardHeader>
      <CardContent className="p-0">
        <ol className="divide-y divide-border">
          {data.stats.map((stat) => (
            <ColumnStatRow key={stat.column_name} stat={stat} />
          ))}
        </ol>
      </CardContent>
    </Card>
  );
}

function ColumnStatRow({ stat }: { stat: ColumnStat }) {
  const distinct = stat.approx_count_distinct ?? null;
  const nullPct = stat.null_pct ?? null;
  const total = stat.row_count ?? null;
  const top = stat.top_10 ?? [];
  const topTotal = top.reduce((acc, b) => acc + (b.count ?? 0), 0);
  const isNumeric = isNumericType(stat.column_type);

  return (
    <li className="grid grid-cols-12 items-start gap-3 px-6 py-3 text-xs">
      <div className="col-span-3 min-w-0">
        <div className="truncate font-mono text-foreground">{stat.column_name}</div>
        <div className="truncate font-mono text-[10px] text-muted-foreground">
          {stat.column_type}
        </div>
      </div>
      <div className="col-span-2 flex flex-col gap-1">
        <span className="text-[10px] uppercase tracking-wider text-muted-foreground">
          Null
        </span>
        {nullPct == null ? (
          <span className="text-muted-foreground">—</span>
        ) : (
          <div className="flex items-center gap-2">
            <div className="relative h-1.5 w-full overflow-hidden rounded-full bg-muted">
              <div
                className="absolute inset-y-0 left-0 bg-amber-500/70"
                style={{ width: `${Math.min(100, nullPct * 100)}%` }}
              />
            </div>
            <span className="font-mono text-foreground">
              {(nullPct * 100).toFixed(nullPct >= 0.1 ? 0 : 1)}%
            </span>
          </div>
        )}
      </div>
      <div className="col-span-2 flex flex-col gap-1">
        <span className="text-[10px] uppercase tracking-wider text-muted-foreground">
          Distinct
        </span>
        {distinct == null ? (
          <span className="text-muted-foreground">—</span>
        ) : (
          <span className="font-mono">
            ≈{formatCount(distinct)}
            {total != null && total > 0 && (
              <span className="ml-1 text-muted-foreground">
                / {formatCount(total)}
              </span>
            )}
          </span>
        )}
      </div>
      <div className="col-span-3 flex flex-col gap-1">
        <span className="text-[10px] uppercase tracking-wider text-muted-foreground">
          {top.length > 0 ? "Top values" : "Range"}
        </span>
        {top.length > 0 ? (
          <ol className="space-y-0.5">
            {top.slice(0, 5).map((b, i) => {
              const pct = topTotal > 0 ? (b.count / topTotal) * 100 : 0;
              const label = b.value === "" ? "∅" : b.value;
              return (
                <li
                  key={i}
                  className="grid grid-cols-[1fr,2.5rem] items-center gap-2"
                >
                  <div className="relative h-3.5 overflow-hidden rounded-sm bg-muted">
                    <div
                      className="absolute inset-y-0 left-0 bg-sky-500/30"
                      style={{ width: `${pct}%` }}
                    />
                    <span className="absolute inset-y-0 left-1.5 right-1 flex items-center truncate font-mono text-[10px] text-foreground">
                      {label}
                    </span>
                  </div>
                  <span className="text-right font-mono text-[10px] text-muted-foreground">
                    {formatCount(b.count)}
                  </span>
                </li>
              );
            })}
          </ol>
        ) : distinct != null && distinct > 1000 ? (
          <Badge
            variant="outline"
            className="w-fit border-dashed font-mono text-[10px]"
            title="Top-K skipped — high-cardinality column"
          >
            high cardinality
          </Badge>
        ) : (
          <div className="font-mono text-muted-foreground">
            {stat.min_value !== "" || stat.max_value !== "" ? (
              <>
                <span title="min">{stat.min_value || "—"}</span>
                <span className="mx-1 text-muted-foreground/50">→</span>
                <span title="max">{stat.max_value || "—"}</span>
              </>
            ) : (
              "—"
            )}
          </div>
        )}
      </div>
      <div className="col-span-2 flex flex-col gap-1">
        <span className="text-[10px] uppercase tracking-wider text-muted-foreground">
          {isNumeric ? "p50 / p95" : "min / max"}
        </span>
        {isNumeric ? (
          <span className="font-mono">
            {stat.approx_p50 != null
              ? Number(stat.approx_p50).toLocaleString(undefined, {
                  maximumFractionDigits: 2,
                })
              : "—"}
            <span className="mx-1 text-muted-foreground/50">/</span>
            {stat.approx_p95 != null
              ? Number(stat.approx_p95).toLocaleString(undefined, {
                  maximumFractionDigits: 2,
                })
              : "—"}
          </span>
        ) : (
          <span className="truncate font-mono text-muted-foreground">
            {stat.min_value || "—"}
            <span className="mx-1 text-muted-foreground/50">→</span>
            {stat.max_value || "—"}
          </span>
        )}
      </div>
    </li>
  );
}

// LegacyTableDetailRedirect rescues bookmarks against the pre-ADR-016
// `/tables/:database/:table` URL by splitting the encoded
// `<catalog>__<schema>` Glue DB name back into the three-level form
// and forwarding the navigation. DBs without the `__` marker (legacy
// pre-v0.18 workspaces) round-trip with an empty schema segment so the
// page still renders against the unsplit identifier.
export function LegacyTableDetailRedirect() {
  const params = useParams<{ database: string; table: string }>();
  const database = decodeURIComponent(params.database ?? "");
  const tableName = decodeURIComponent(params.table ?? "");
  const i = database.indexOf("__");
  const catalog = i >= 0 ? database.slice(0, i) : database;
  const schema = i >= 0 ? database.slice(i + 2) : "";
  const to = `/tables/${encodeURIComponent(catalog)}/${encodeURIComponent(schema)}/${encodeURIComponent(tableName)}`;
  return <Navigate to={to} replace />;
}
