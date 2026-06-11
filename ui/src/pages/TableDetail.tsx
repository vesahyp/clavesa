/**
 * TableDetail — per-table view: schema + sample rows.
 *
 * The "is my data OK?" page. Hits Athena for a sample via the existing
 * /api/data/table endpoint. Volume timeline, lineage, and writer history
 * are subsequent slices once the observability tables exist.
 */

import { useMemo, useState } from "react";
import { Navigate, useNavigate, useParams, NavLink } from "react-router-dom";
import { formatDistanceToNow } from "date-fns";
import {
  ChevronDown,
  ChevronRight,
  FileWarning,
  LayoutDashboard,
  Loader2,
  Terminal,
} from "lucide-react";

import { useChrome, type PageChrome } from "@/components/PageChrome";
import { AdhocQuery } from "@/components/AdhocQuery";
import { CopyButton } from "@/components/CopyButton";
import { LineagePanel } from "@/components/LineagePanel";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  useCatalogTables,
  useColumnStats,
  useDashboards,
  usePipelines,
  useTableSample,
  useTableSnapshots,
  type CatalogTable,
  type ColumnStat,
  type ColumnStatsResult,
  type Dashboard,
  type SnapshotInfo,
  type SnapshotsResult,
} from "@/lib/queries";
import { displayTablePath } from "@/lib/format";

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
  // Wire form (ADR-020) — what the engine accepts when the user copies
  // this for Athena / Spark / dbt. Two-segment `<catalog>__<schema>.<table>`.
  const sqlPath = `${database}.${tableName}`;

  const catalog = useCatalogTables();
  // ADR-019 dropped the `__default` suffix from single-output tables. Existing
  // dashboard links + bookmarks may still carry the suffix; new tables on disk
  // don't. Match the URL against either form so both bookmark shapes resolve.
  const tableNameBare = tableName.replace(/__default$/, "");
  const tableMeta = useMemo<CatalogTable | undefined>(() => {
    return catalog.data?.tables.find(
      (t) =>
        t.database === database &&
        (t.name === tableName ||
          t.name === tableNameBare ||
          t.name === `${tableNameBare}__default`)
    );
  }, [catalog.data, database, tableName, tableNameBare]);

  // Three-level logical path (ADR-020) — what the header chip renders.
  // Resolved from `tableMeta` once the catalog has loaded so the
  // `__default` suffix and owning-node display rules match displayTablePath
  // everywhere else; until then fall back to the URL pieces literally so
  // a legacy bookmark still shows a coherent string before the API lands.
  const displayPath = tableMeta
    ? displayTablePath(tableMeta)
    : [catalogName, schemaName, stripDefaultSuffix(tableName)]
        .filter(Boolean)
        .join(".");

  // The catalog response stamps `dir` (relative to the workspace root) on
  // every table when the owning pipeline is in the user's workspace — both
  // cloud and local pipelines (ADR-014 parity). Threading it through the
  // sample/snapshot queries is what lets the server's observability.Resolver
  // pick the right Provider (cloud vs local) from the pipeline's `compute`
  // attr. Falling back to the pipelines-list lookup catches the rare case
  // where the catalog stamp is missing — the API takes either form.
  const pipelines = usePipelines();
  // Resolve the owning pipeline `dir` without blocking on the slow
  // whole-catalog query. Priority: (a) the catalog stamp once it has
  // loaded (most precise — carries `dir` for system tables that have no
  // schema-owning pipeline); else (b) the fast pipelines list, matching
  // the table's schema against the pipeline that owns it (ADR-016: a
  // pipeline's schema identifier == its `schema` field, one schema one
  // producing pipeline); else (c) the owning_pipeline-name fallback for
  // catalog rows that name a pipeline but lack `dir`. (b) is what makes
  // `dir` available as soon as the (fast) pipelines query resolves so the
  // sample fires without waiting ~14s on the catalog.
  const owningPipelineDir = useMemo(() => {
    if (tableMeta?.dir) return tableMeta.dir;
    if (schemaName) {
      const bySchema = pipelines.data?.find((p) => p.schema === schemaName)?.dir;
      if (bySchema) return bySchema;
    }
    if (!tableMeta?.owning_pipeline) return "";
    return pipelines.data?.find((p) => p.name === tableMeta.owning_pipeline)?.dir ?? "";
  }, [pipelines.data, tableMeta, schemaName]);
  // Fire the sample as soon as the (fast) pipelines list resolves and we
  // have a `dir` — no longer gated on the slow whole-catalog query. The
  // `dir` keeps the request on the right Provider (local vs cloud), so we
  // don't hit the cloud-fallback "Query failed" flash. When `dir` is empty
  // (no pipeline owns this schema — e.g. an external table), fall back to
  // firing once pipelines resolved so the cloud path can still answer.
  const pipelinesReady = pipelines.data !== undefined;
  const sample = useTableSample(database, tableName, SAMPLE_LIMIT, {
    dir: owningPipelineDir,
    enabled: pipelinesReady,
  });
  // Every clavesa-managed table is Delta under v2.0.0. The commit timeline
  // and column-stats panels render for any Delta table; non-clavesa tables
  // (external, non-Delta) fall through to the lite-columns view.
  const isClavesaManaged = tableMeta?.table_type === "DELTA";
  const snapshots = useTableSnapshots(
    isClavesaManaged ? database : "",
    isClavesaManaged ? tableName : "",
    SNAPSHOTS_LIMIT,
    { dir: owningPipelineDir },
  );
  const columnStats = useColumnStats(
    isClavesaManaged ? database : "",
    isClavesaManaged ? tableName : "",
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
              <code
                className="break-all rounded bg-muted px-1.5 py-0.5 font-mono text-xs text-foreground"
                title="Three-level logical path — display only. The SQL pane below uses the wire form the engine accepts."
              >
                {displayPath}
              </code>
              <CopyButton value={sqlPath} label="Copy SQL identifier" />
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
          <div className="flex shrink-0 items-center gap-2">
            <CreateDashboardButton
              tableName={tableName}
              tablePath={sqlPath}
              owningPipelineDir={owningPipelineDir}
            />
            {tableMeta?.table_type && (
              <Badge variant="outline" className="font-mono">
                {tableMeta.table_type}
              </Badge>
            )}
          </div>
        </div>

        {/* Columns — one column-oriented overview. Rich mode (null %,
            distinct, top-K, percentiles) when the transform opted into
            stats=true; lite mode (name + type + first few example values
            from the sample) otherwise. Either way, this replaces the
            Schema-on-left / Sample-on-right grid — row data lives in the
            Query pane below now. */}
        <div data-testid="table-schema">
          {isClavesaManaged && columnStats.data && columnStats.data.stats.length > 0 ? (
            <ColumnStatsCard data={columnStats.data} />
          ) : (
            <LiteColumnsCard
              tableMeta={tableMeta}
              sample={sample.data ?? null}
              // Skeleton only while the schema is unavailable from BOTH
              // sources: the sample (fast, fires on pipelines) hasn't
              // returned yet AND the catalog (slow) hasn't either. Once
              // either resolves the card renders real columns.
              isLoading={
                !sample.data &&
                !tableMeta &&
                (sample.isLoading || catalog.isLoading)
              }
              // Schema may render (from tableMeta) before the sample
              // returns; while we're still waiting on the sample query the
              // per-column NULL%/examples cells show a skeleton instead of
              // "—", so "—" post-load unambiguously means "no data".
              sampleLoading={
                !sample.data && (sample.isLoading || sample.isFetching)
              }
            />
          )}
        </div>

        <div className="mt-6">
          <TableQueryPane
            sql={`SELECT * FROM ${database}.${tableName} LIMIT 100`}
            defaultOpen
            autoRun
          />
        </div>

        {sample.error && (
          <div className="mt-4 flex items-start gap-3 rounded-md border border-destructive/40 bg-destructive/5 p-4 text-sm">
            <FileWarning className="mt-0.5 h-4 w-4 flex-shrink-0 text-destructive" />
            <div className="min-w-0">
              <div className="font-medium text-destructive">Sample query failed</div>
              <p className="mt-1 break-words text-xs text-muted-foreground">
                {sample.error instanceof Error
                  ? sample.error.message
                  : String(sample.error)}
              </p>
            </div>
          </div>
        )}

        {isClavesaManaged && (
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

interface VolumeTimelineCardProps {
  isLoading: boolean;
  error: unknown;
  data: SnapshotsResult | undefined;
}

function VolumeTimelineCard({ isLoading, error, data }: VolumeTimelineCardProps) {
  return (
    <Card data-testid="commit-timeline">
      <CardHeader className="flex-row items-center justify-between pb-3">
        <CardTitle>Commit timeline</CardTitle>
        {data && data.snapshots.length > 0 && (
          <span className="text-xs text-muted-foreground">
            {data.snapshots.length}
            {data.truncated ? "+" : ""} commit
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
                Commit history unavailable
              </div>
              <p className="mt-1 break-words text-xs text-muted-foreground">
                {error instanceof Error ? error.message : String(error)}
              </p>
            </div>
          </div>
        )}
        {data && data.snapshots.length === 0 && (
          <div className="p-6 text-sm text-muted-foreground">
            No commits recorded for this table.
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

// Transform-output commits written by clavesa carry a trigger key in the
// Delta commitInfo userMetadata naming the run that produced them. A
// commit with no key gets no badge: it was either a manual write or
// written by a pre-provenance runner. Either way the bare row stands out
// against the badged ones, which is the signal users want.
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
    <Card data-testid="column-profile">
      <CardHeader className="flex-row items-center justify-between pb-3">
        <CardTitle>Column profile</CardTitle>
        <span className="text-xs text-muted-foreground">
          {data.stats.length} column{data.stats.length === 1 ? "" : "s"}
          {data.snapshot_id ? ` · commit ${data.snapshot_id.slice(0, 12)}` : ""}
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
          (() => {
            const shown = top.slice(0, 5);
            const shownSum = shown.reduce((acc, b) => acc + (b.count ?? 0), 0);
            return (
              <ol className="space-y-0.5">
                {shown.map((b, i) => {
                  const pct =
                    total != null && total > 0
                      ? (b.count / total) * 100
                      : topTotal > 0
                        ? (b.count / topTotal) * 100
                        : 0;
                  const label = b.value == null || b.value === "" ? "∅" : b.value;
                  return (
                    <Tooltip key={i}>
                      <TooltipTrigger asChild>
                        <li className="grid grid-cols-[minmax(0,4.5rem)_1fr_auto] items-center gap-2">
                          <span
                            className="truncate font-mono text-[10px] text-foreground"
                            title={label}
                          >
                            {label}
                          </span>
                          <div className="relative h-3.5 overflow-hidden rounded-sm bg-muted">
                            <div
                              className="absolute inset-y-0 left-0 bg-sky-500/50"
                              style={{ width: `${pct}%` }}
                            />
                          </div>
                          <span className="text-right font-mono text-[10px] text-foreground">
                            {`${pct.toFixed(1)}%`}
                          </span>
                        </li>
                      </TooltipTrigger>
                      <TooltipContent>
                        {`${formatCount(b.count)} / ${formatCount(total ?? topTotal)} rows`}
                      </TooltipContent>
                    </Tooltip>
                  );
                })}
                {total != null && total > 0 && total > shownSum
                  ? (() => {
                      const otherPct = ((total - shownSum) / total) * 100;
                      return (
                        <li
                          className="grid grid-cols-[minmax(0,4.5rem)_1fr_auto] items-center gap-2"
                          title={`${formatCount(total - shownSum)} / ${formatCount(total)} rows`}
                        >
                          <span className="truncate font-mono text-[10px] text-muted-foreground">
                            other
                          </span>
                          <div className="relative h-3.5 overflow-hidden rounded-sm bg-muted">
                            <div
                              className="absolute inset-y-0 left-0 bg-muted-foreground/30"
                              style={{ width: `${otherPct}%` }}
                            />
                          </div>
                          <span className="text-right font-mono text-[10px] text-muted-foreground">
                            {`${otherPct.toFixed(1)}%`}
                          </span>
                        </li>
                      );
                    })()
                  : null}
              </ol>
            );
          })()
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

// TableQueryPane wraps the AdhocQuery component for the table-detail page.
// defaultOpen + autoRun together give the user the LIMIT 100 result on
// page load without an extra click — moving what the old Sample card used
// to show into the same surface where they can edit the SQL.
function TableQueryPane({
  sql,
  defaultOpen,
  autoRun,
}: {
  sql: string;
  defaultOpen?: boolean;
  autoRun?: boolean;
}) {
  const [open, setOpen] = useState(!!defaultOpen);
  return (
    <Card data-testid="table-sample-rows">
      <CardHeader className="flex-row items-center justify-between pb-2">
        <button
          type="button"
          onClick={() => setOpen((v) => !v)}
          className="flex items-center gap-2 text-left"
        >
          {open ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
          <Terminal className="h-4 w-4 text-muted-foreground" />
          <CardTitle className="text-base">Query this table</CardTitle>
        </button>
        {open && (
          <Button asChild size="sm" variant="ghost">
            <a href={`/query?sql=${encodeURIComponent(sql)}`}>Open in /query</a>
          </Button>
        )}
      </CardHeader>
      {open && (
        <CardContent className="pt-0">
          <AdhocQuery initialSql={sql} bare autoRun={autoRun} />
        </CardContent>
      )}
    </Card>
  );
}

// LiteColumnsCard is the column-oriented overview for tables that didn't
// opt into stats=true. Each row: column name + type + a few example values
// pulled from the sample query + a non-null-count derived from the sample.
// Degrades gracefully to "name + type" while the sample is still loading.
function LiteColumnsCard({
  tableMeta,
  sample,
  isLoading,
  sampleLoading,
}: {
  tableMeta: CatalogTable | undefined;
  /**
   * Sample query result — `columns` is the real engine schema (used as
   * the column source when the catalog hasn't loaded yet), `rows` the
   * example data we read by column index. Falsy = sample not yet loaded.
   */
  sample: { columns: { name: string; type: string }[]; rows: string[][]; row_count: number } | null;
  isLoading: boolean;
  /**
   * Schema is rendered (from tableMeta) but the sample query — which feeds
   * the per-column NULL%/examples cells — is still in flight. Drives a
   * per-cell skeleton instead of "—" so a loading cell is distinct from a
   * genuinely-empty column.
   */
  sampleLoading: boolean;
}) {
  // Column source: prefer whichever resolved first. The catalog stamp
  // (tableMeta.columns) and the sample's engine schema (sample.columns)
  // agree, so when only one is available we render from it — that's what
  // lets the schema show as soon as the fast sample returns without
  // waiting on the slow whole-catalog query.
  const columns =
    tableMeta && tableMeta.columns.length > 0
      ? tableMeta.columns
      : sample?.columns ?? [];

  if (isLoading && columns.length === 0) {
    return (
      <Card>
        <CardHeader className="pb-3">
          <CardTitle>Columns</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2 p-6">
          <Skeleton className="h-4 w-full" />
          <Skeleton className="h-4 w-3/4" />
          <Skeleton className="h-4 w-2/3" />
        </CardContent>
      </Card>
    );
  }
  if (columns.length === 0) {
    return (
      <Card>
        <CardHeader className="pb-3">
          <CardTitle>Columns</CardTitle>
        </CardHeader>
        <CardContent className="p-6 text-sm text-muted-foreground">
          No columns reported.
        </CardContent>
      </Card>
    );
  }
  const sampleRows = sample?.rows ?? [];
  const sampleN = sampleRows.length;
  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between pb-3">
        <CardTitle>Columns</CardTitle>
        <span className="flex items-center text-xs text-muted-foreground">
          {columns.length} column{columns.length === 1 ? "" : "s"}
          {sampleLoading ? (
            <>
              {" · "}
              <Loader2 className="ml-1 mr-1 h-3 w-3 animate-spin" />
              querying…
            </>
          ) : (
            sampleN > 0 && <> · examples from first {sampleN} rows</>
          )}
        </span>
      </CardHeader>
      <CardContent className="p-0">
        <ol className="divide-y divide-border">
          {columns.map((c, colIdx) => {
            const colValues = sampleRows.map((row) => row[colIdx] ?? "");
            const nonEmpty = colValues.filter((v) => v !== "");
            const nullCount = colValues.length - nonEmpty.length;
            // first 3 unique example values, preserve sample order
            const seen = new Set<string>();
            const examples: string[] = [];
            for (const v of nonEmpty) {
              if (!seen.has(v)) {
                seen.add(v);
                examples.push(v);
                if (examples.length === 3) break;
              }
            }
            const nullPct = sampleN > 0 ? nullCount / sampleN : null;
            return (
              <li key={c.name} className="grid grid-cols-12 items-start gap-3 px-6 py-3 text-xs">
                <div className="col-span-3 min-w-0">
                  <div className="truncate font-mono text-foreground">{c.name}</div>
                  <div className="truncate font-mono text-[10px] text-muted-foreground">
                    {c.type}
                  </div>
                </div>
                <div className="col-span-2 flex flex-col gap-1">
                  <span className="text-[10px] uppercase tracking-wider text-muted-foreground">
                    Null
                  </span>
                  {sampleLoading ? (
                    <Skeleton className="h-1.5 w-full" />
                  ) : nullPct == null ? (
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
                <div className="col-span-7 flex flex-col gap-1">
                  <span className="text-[10px] uppercase tracking-wider text-muted-foreground">
                    Examples
                  </span>
                  {sampleLoading ? (
                    <Skeleton className="h-4 w-32" />
                  ) : examples.length === 0 ? (
                    <span className="text-muted-foreground">
                      {sampleN === 0 ? "—" : "(all null in sample)"}
                    </span>
                  ) : (
                    <div className="flex flex-wrap gap-1">
                      {examples.map((v, i) => (
                        <code
                          key={i}
                          className="rounded bg-muted px-1.5 py-0.5 font-mono text-[11px] text-foreground"
                          title={v}
                        >
                          {v.length > 60 ? v.slice(0, 57) + "…" : v}
                        </code>
                      ))}
                    </div>
                  )}
                </div>
              </li>
            );
          })}
        </ol>
      </CardContent>
    </Card>
  );
}

/**
 * CreateDashboardButton — one-click "build a dashboard with this table
 * preloaded". Constructs an initial spec (one named dataset reading the
 * table at `LIMIT 100`, one full-width `table` widget bound to it),
 * generates a slug that doesn't collide with existing dashboards, and
 * navigates to the editor at `/dashboards/<slug>?new=1&title=…` carrying
 * the spec via React Router location state. Dashboard.tsx reads that
 * state and seeds the editor with it instead of the empty default.
 *
 * If the owning pipeline isn't resolved (legacy table without a pipeline
 * mapping), the button stays visible but is disabled — a dashboard
 * dataset needs a `dir` to dispatch through.
 */
function CreateDashboardButton({
  tableName,
  tablePath,
  owningPipelineDir,
}: {
  tableName: string;
  tablePath: string;
  owningPipelineDir: string;
}) {
  const navigate = useNavigate();
  const dashboards = useDashboards();

  function onClick() {
    const taken = new Set(dashboards.data?.dashboards.map((d) => d.slug) ?? []);
    const base = slugifyForDashboard(tableName) || "explore";
    let slug = base;
    for (let n = 2; taken.has(slug); n++) slug = `${base}-${n}`;
    const title = stripDefaultSuffix(tableName);
    const spec: Dashboard = {
      slug,
      title,
      datasets: [
        { name: "default", dir: owningPipelineDir, sql: `SELECT * FROM ${tablePath} LIMIT 100` },
      ],
      widgets: [
        {
          id: "explore",
          type: "table",
          title,
          dataset: "default",
          value_field: "",
          x_field: "",
          y_field: "",
          series_fields: [],
          line_field: "",
          region_field: "",
          tooltip_field: "",
          layout: { x: 0, y: 0, w: 12, h: 8 },
        },
      ],
      controls: [],
      updated_at: "",
    };
    navigate(`/dashboards/${encodeURIComponent(slug)}?new=1&title=${encodeURIComponent(title)}`, {
      state: { prefilled: spec },
    });
  }

  return (
    <Button
      variant="outline"
      size="sm"
      onClick={onClick}
      disabled={!owningPipelineDir}
      title={
        owningPipelineDir
          ? "Open a new dashboard preloaded with a SELECT against this table"
          : "Needs a known owning pipeline to dispatch the dataset query"
      }
    >
      <LayoutDashboard className="mr-1 h-3.5 w-3.5" />
      Create dashboard
    </Button>
  );
}

/**
 * slugifyForDashboard — mirror of `Dashboards.tsx: slugify` (lowercase,
 * `[a-z0-9_-]` only, trim hyphens, ≤64 chars). Duplicated here rather
 * than imported because TableDetail shouldn't need the page module just
 * for one helper; the rule is small enough that a copy is cheaper than
 * a coupling.
 */
function slugifyForDashboard(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 64);
}
