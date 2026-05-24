/**
 * BackfillDetail — review one staged backfill before promote/discard.
 *
 * URL: /backfills?dir=<pipelineDir>&run=<runId>
 *
 * Drill-down from the Backfills card on PipelineDashboard. Surfaces the
 * staging vs. canonical diff the CLI's `pipeline backfill diff` returns —
 * row counts, schema match, merge-key match counts — plus the Promote /
 * Discard actions the CLI exposes as subcommands. Same service layer
 * underneath; ADR-015 parity.
 *
 * Service layer dispatches on workspace env: cloud goes through the
 * deployed Lambda + Glue + Athena, local replays through the workspace
 * runner and reads from the Hadoop catalog warehouse (ADR-014). Same
 * response shape either way, so this page is environment-agnostic.
 */

import { useEffect, useMemo, useState } from "react";
import { useSearchParams, useNavigate } from "react-router-dom";
import { useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, Check, Loader2, Trash2 } from "lucide-react";
import { toast } from "sonner";

import { useChrome, type PageChrome } from "@/components/PageChrome";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { Skeleton } from "@/components/ui/skeleton";
import { formatRowCount } from "@/lib/format";
import {
  discardBackfill,
  promoteBackfill,
  useBackfillDedupCheck,
  useBackfillDiff,
  useBackfills,
} from "@/lib/queries";

// Pick a sensible default dedup column from the staging schema. We
// prefer exact "id", then anything ending in "_id" (event_id, user_id,
// trip_id), then the first column. The user can override; this just
// saves a click on the common case.
function defaultDedupColumn(columns: { name: string }[]): string {
  if (columns.length === 0) return "";
  const exactId = columns.find((c) => c.name.toLowerCase() === "id");
  if (exactId) return exactId.name;
  const endingId = columns.find((c) => c.name.toLowerCase().endsWith("_id"));
  if (endingId) return endingId.name;
  return columns[0].name;
}

export function BackfillDetail() {
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const qc = useQueryClient();
  const dir = searchParams.get("dir") ?? "";
  const runID = searchParams.get("run") ?? "";

  // Locate the run row to surface node + window + canonical without a
  // separate per-run GET — the list call is cheap and already cached.
  const list = useBackfills(dir);
  const run = useMemo(
    () => list.data?.backfills.find((b) => b.run_id === runID),
    [list.data, runID],
  );

  const diff = useBackfillDiff(dir, runID);

  // Promote state. For append mode the natural action is "merge on a
  // column so we don't dupe"; only when the user opts into "just
  // append, I know" does allowDuplicates win. The two are mutually
  // exclusive in the API contract (`force_dedup` vs `allow_duplicates`).
  const [forceDedup, setForceDedup] = useState("");
  const [appendAnyway, setAppendAnyway] = useState(false);
  const [promoting, setPromoting] = useState(false);
  const [discarding, setDiscarding] = useState(false);

  const stagingColumns = diff.data?.staging_columns ?? [];

  // Pre-pick a default dedup column once the schema lands. Without
  // this the user faces an empty dropdown and has to make the first
  // selection themselves; the heuristic catches the common case
  // (event_id, user_id) so Promote is one click away for most users.
  useEffect(() => {
    if (!forceDedup && stagingColumns.length > 0 && !appendAnyway) {
      setForceDedup(defaultDedupColumn(stagingColumns));
    }
  }, [stagingColumns, forceDedup, appendAnyway]);

  // Live "what would happen on Promote" preview. Fires once the user
  // (or our heuristic) has picked a column. Skipped when they've
  // chosen the "append anyway" path — no dedup math applies.
  const dedupCheck = useBackfillDedupCheck(
    dir,
    runID,
    appendAnyway ? "" : forceDedup,
  );

  async function handlePromote() {
    if (!dir || !runID) return;
    setPromoting(true);
    try {
      const result = await promoteBackfill(runID, {
        dir,
        force_dedup: appendAnyway ? undefined : forceDedup || undefined,
        allow_duplicates: appendAnyway || undefined,
      });
      if (result.columns_added.length > 0) {
        toast.success(
          `Promoted to canonical. Schema evolved: added ${result.columns_added.length} column${
            result.columns_added.length === 1 ? "" : "s"
          } — ${result.columns_added.join(", ")}.`,
        );
      } else {
        toast.success("Promoted to canonical. Staging table dropped.");
      }
      // Navigate away first: unmounting this page drops the diff /
      // dedup-check query observers, so they can't refetch against the
      // now-dropped staging table (a 502). Do NOT removeQueries here —
      // removing a query that still has a mounted observer triggers an
      // immediate refetch, exactly the 502 we're avoiding; the stale
      // per-run entries are gc'd on their own once unobserved.
      navigate(`/pipelines/dashboard?dir=${encodeURIComponent(dir)}`);
      // Refresh the dashboard we just landed on.
      void qc.invalidateQueries({ queryKey: ["backfills", dir] });
      void qc.invalidateQueries({ queryKey: ["catalog"] });
    } catch (err) {
      toast.error(err instanceof Error ? err.message : String(err));
    } finally {
      setPromoting(false);
    }
  }

  async function handleDiscard() {
    if (!dir || !runID) return;
    if (!window.confirm("Drop staging table without promoting?")) return;
    setDiscarding(true);
    try {
      await discardBackfill(runID, { dir });
      toast.success("Discarded.");
      // See handlePromote: navigate away so the diff / dedup-check
      // observers unmount; don't removeQueries (it would refetch
      // against the dropped staging table and 502).
      navigate(`/pipelines/dashboard?dir=${encodeURIComponent(dir)}`);
      void qc.invalidateQueries({ queryKey: ["backfills", dir] });
      void qc.invalidateQueries({ queryKey: ["catalog"] });
    } catch (err) {
      toast.error(err instanceof Error ? err.message : String(err));
    } finally {
      setDiscarding(false);
    }
  }

  useChrome(
    useMemo<PageChrome>(() => {
      const crumbs = [{ label: "Pipelines", to: "/pipelines" }];
      if (dir) {
        crumbs.push({
          label: dir.split("/").pop() || dir,
          to: `/pipelines/dashboard?dir=${encodeURIComponent(dir)}`,
        });
        if (runID) {
          crumbs.push({
            label: `Backfill ${runID.slice(0, 8)}`,
            to: `/backfills?dir=${encodeURIComponent(dir)}&run=${encodeURIComponent(runID)}`,
          });
        }
      }
      return { breadcrumbs: crumbs };
    }, [dir, runID]),
  );

  if (!dir || !runID) {
    return (
      <div className="mx-auto w-full max-w-6xl px-6 py-8">
          <p className="text-sm text-muted-foreground">
            Missing required <code>dir</code> and <code>run</code> query
            parameters.
          </p>
      </div>
    );
  }

  const mode = diff.data?.output_mode ?? "";
  const mergeKeys = diff.data?.merge_keys ?? [];
  const canonicalHasRows =
    (diff.data?.canonical_rows ?? 0) > 0;
  const promoteBlocked =
    mode === "replace" ||
    (mode === "append" && !forceDedup && !appendAnyway);

  return (
    <div className="mx-auto w-full max-w-4xl px-6 py-8">
        <div className="mb-6 flex items-center gap-3">
          <Badge variant="outline" className="font-mono text-[10px] uppercase">
            backfill
          </Badge>
          <h1 className="font-mono text-lg tracking-tight">{runID}</h1>
        </div>

        {run && (
          <p className="mb-6 text-xs text-muted-foreground">
            node <code className="font-mono text-foreground">{run.node}</code>
            <span className="mx-2">·</span>
            window{" "}
            <code className="font-mono text-foreground">
              [{run.from_cursor.join("/")}, {run.to_cursor.join("/")}]
            </code>
          </p>
        )}

        {/* Diff card */}
        <Card>
          <CardHeader className="pb-3">
            <CardTitle>Staging vs canonical</CardTitle>
          </CardHeader>
          <CardContent>
            {diff.isLoading && (
              <div className="space-y-2">
                <Skeleton className="h-4 w-2/3" />
                <Skeleton className="h-4 w-1/2" />
              </div>
            )}
            {Boolean(diff.error) && (
              <div className="text-xs text-status-failed">
                {diff.error instanceof Error
                  ? diff.error.message
                  : String(diff.error)}
              </div>
            )}
            {diff.data && (
              <dl className="grid grid-cols-1 gap-y-2 text-sm sm:grid-cols-2">
                <DiffRow
                  label="Staging"
                  value={diff.data.staging_table}
                  meta={formatRowCount(diff.data.staging_rows)}
                />
                <DiffRow
                  label="Canonical"
                  value={diff.data.canonical_table}
                  meta={
                    diff.data.canonical_rows < 0
                      ? "does not exist — first backfill creates target"
                      : formatRowCount(diff.data.canonical_rows)
                  }
                />
                <DiffRow
                  label="Output mode"
                  value={mode || "—"}
                  meta={
                    mergeKeys.length > 0
                      ? `merge keys: ${mergeKeys.join(", ")}`
                      : undefined
                  }
                />
                <DiffRow
                  label="Schema match"
                  value={diff.data.schema_matches ? "yes" : "no"}
                  variant={diff.data.schema_matches ? "success" : "failed"}
                />
                {mergeKeys.length > 0 && (
                  <>
                    <DiffRow
                      label="Matching keys"
                      value={formatRowCount(diff.data.matching_key_rows)}
                      meta="would UPDATE on promote"
                    />
                    <DiffRow
                      label="New keys"
                      value={formatRowCount(diff.data.new_key_rows)}
                      meta="would INSERT on promote"
                    />
                  </>
                )}
              </dl>
            )}
            {diff.data && diff.data.schema_diff && (
              <pre className="mt-4 overflow-x-auto whitespace-pre-wrap rounded bg-muted/40 p-3 font-mono text-[11px]">
                {diff.data.schema_diff}
              </pre>
            )}
          </CardContent>
        </Card>

        {/* Promote / Discard */}
        <Card className="mt-6">
          <CardHeader className="pb-3">
            <CardTitle>Promote into canonical</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {mode === "merge" && (
              <p className="text-sm text-muted-foreground">
                The canonical table is keyed on{" "}
                <code className="font-mono text-foreground">
                  {mergeKeys.join(", ")}
                </code>
                . Promote will update rows that already exist on those keys
                and insert the rest. Safe to re-run.
              </p>
            )}

            {mode === "append" && (
              <>
                {canonicalHasRows ? (
                  <div className="flex gap-3 rounded-md border border-amber-500/40 bg-amber-500/5 p-3 text-sm">
                    <AlertTriangle className="mt-0.5 h-4 w-4 flex-shrink-0 text-amber-500" />
                    <div className="space-y-1">
                      <p>
                        The canonical table already has{" "}
                        <strong>
                          {formatRowCount(diff.data?.canonical_rows ?? 0)}
                        </strong>
                        . Just appending {formatRowCount(diff.data?.staging_rows ?? 0)} from
                        staging on top would duplicate any row that overlaps.
                      </p>
                      <p className="text-xs text-muted-foreground">
                        Pick a column that uniquely identifies a row.
                        Matching rows update in place; new rows insert.
                      </p>
                    </div>
                  </div>
                ) : (
                  <p className="text-sm text-muted-foreground">
                    Canonical is empty — first backfill creates it. Pick a
                    natural-key column so re-runs of this same window stay
                    idempotent.
                  </p>
                )}

                <div className="space-y-2">
                  <Label htmlFor="force-dedup" className="text-xs">
                    Dedup column
                  </Label>
                  {stagingColumns.length > 0 ? (
                    <select
                      id="force-dedup"
                      value={appendAnyway ? "" : forceDedup}
                      onChange={(e) => {
                        setForceDedup(e.target.value);
                        setAppendAnyway(false);
                      }}
                      disabled={appendAnyway}
                      className="w-full rounded-md border border-input bg-background px-3 py-2 font-mono text-xs disabled:opacity-50"
                    >
                      {stagingColumns.map((c) => (
                        <option key={c.name} value={c.name}>
                          {c.name} ({c.type})
                        </option>
                      ))}
                    </select>
                  ) : (
                    <div className="text-xs text-muted-foreground">
                      Loading staging schema…
                    </div>
                  )}

                  {/* Live preview of what Promote would do with the
                      currently-selected column. Tells the user the
                      consequence before they press the button. */}
                  {!appendAnyway && forceDedup && (
                    <DedupPreview
                      stagingRows={diff.data?.staging_rows ?? 0}
                      loading={dedupCheck.isLoading}
                      result={dedupCheck.data}
                      error={dedupCheck.error}
                    />
                  )}
                </div>

                <details className="text-xs text-muted-foreground">
                  <summary className="cursor-pointer select-none hover:text-foreground">
                    Advanced: append rows even if they duplicate canonical
                  </summary>
                  <label className="mt-2 flex items-start gap-2 pl-3">
                    <input
                      type="checkbox"
                      checked={appendAnyway}
                      onChange={(e) => setAppendAnyway(e.target.checked)}
                      className="mt-0.5"
                    />
                    <span>
                      I know these rows don't overlap canonical (or I want the
                      duplicates). Promote runs a plain insert with no merge.
                    </span>
                  </label>
                </details>
              </>
            )}

            {mode === "replace" && (
              <div className="flex gap-3 rounded-md border border-destructive/40 bg-destructive/5 p-3 text-sm">
                <AlertTriangle className="mt-0.5 h-4 w-4 flex-shrink-0 text-destructive" />
                <div className="space-y-1">
                  <p className="text-foreground">
                    Replace-mode outputs don't support partial-window promote
                    yet.
                  </p>
                  <p className="text-xs">
                    Re-stage with the <strong>direct</strong> option ticked to
                    rewrite the canonical target in place, or discard this
                    staging table and run a full-table backfill.
                  </p>
                </div>
              </div>
            )}

            <div className="flex justify-end gap-2 pt-2">
              <Button
                variant="outline"
                size="sm"
                onClick={handleDiscard}
                disabled={discarding || promoting}
              >
                {discarding ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <Trash2 className="h-4 w-4" />
                )}
                Discard staging
              </Button>
              <Button
                size="sm"
                onClick={handlePromote}
                disabled={promoteBlocked || promoting || discarding}
              >
                {promoting ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <Check className="h-4 w-4" />
                )}
                Promote
              </Button>
            </div>
          </CardContent>
        </Card>
    </div>
  );
}

// DedupPreview surfaces the would-update / would-insert counts the
// dedup-check endpoint returns, in plain language so the user can
// commit to Promote with their eyes open. Empty when staging has rows
// but none match — that's the column-is-unique case, which is fine.
function DedupPreview({
  stagingRows,
  loading,
  result,
  error,
}: {
  stagingRows: number;
  loading: boolean;
  result: import("@/lib/queries").BackfillDedupCheckResult | undefined;
  error: unknown;
}) {
  if (loading) {
    return (
      <p className="text-xs text-muted-foreground">
        Checking what this column does to canonical…
      </p>
    );
  }
  if (error) {
    return (
      <p className="text-xs text-status-failed">
        Couldn't preview Promote consequences:{" "}
        {error instanceof Error ? error.message : String(error)}
      </p>
    );
  }
  if (!result) return null;
  const updates = result.matching_rows;
  const inserts = result.new_rows;
  const total = updates + inserts;
  // Sanity check — counts should sum to the staging row count when the
  // chosen column is unique within staging. A delta means the column
  // repeats in staging itself, which makes it a poor dedup key.
  const collapse = stagingRows > 0 && total < stagingRows;
  return (
    <p className="text-xs text-muted-foreground">
      Promote would{" "}
      <strong className="text-foreground">
        update {formatRowCount(updates)}
      </strong>{" "}
      in canonical and{" "}
      <strong className="text-foreground">
        insert {formatRowCount(inserts)}
      </strong>
      .
      {collapse && (
        <span className="ml-1 text-status-failed">
          Heads-up: {formatRowCount(stagingRows - total)} from staging would
          collapse because this column repeats in staging — pick a column
          that's unique per row.
        </span>
      )}
    </p>
  );
}

function DiffRow({
  label,
  value,
  meta,
  variant,
}: {
  label: string;
  value: string;
  meta?: string;
  variant?: "success" | "failed";
}) {
  return (
    <div className="flex flex-col">
      <dt className="text-[11px] uppercase tracking-wide text-muted-foreground">
        {label}
      </dt>
      <dd className="font-mono text-xs">
        {variant ? (
          <Badge
            variant={variant === "success" ? "success" : "failed"}
            className="text-[10px]"
          >
            {value}
          </Badge>
        ) : (
          value
        )}
        {meta && (
          <span className="ml-2 font-sans text-[11px] text-muted-foreground">
            {meta}
          </span>
        )}
      </dd>
    </div>
  );
}
