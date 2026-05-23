# Backfill historical data

> **When you have one:** a deployed pipeline that's running incrementally on new data, and you need to (re-)process a window of older data — files that landed before the pipeline existed, a quarter of archived logs after a vendor finally exported them, or any range of partitions after a transform-logic change made the existing rows wrong.

Backfill stages the historical window into a parallel Iceberg table, lets you review the result side-by-side with the canonical target, then promotes via a single MERGE INTO. Direct-to-target is the escape hatch, not the default — `append` outputs dupe on overlap, `replace` outputs can overwrite newer data with older reconstructions; only `merge` outputs are naturally safe.

## What you'll end up with

- A parallel Iceberg table `<target>__backfill__<run_id>` in the same Glue DB as the canonical target — same schema, tagged with the window range so it's obvious in the Catalog page.
- One `runs` row per backfill execution with `trigger = "backfill"` and a `target_table` column pointing at the staging table.
- A reviewed promote that MERGEs staging into the canonical target in one statement; or a discard that drops staging without touching the target.

## Prerequisites

- Works against both `compute = "local"` (runs the same image directly against the workspace warehouse) and the cloud computes (`lambda` / `fargate` / `emr-serverless`, which invoke the deployed transform Lambda). The CLI is identical in both modes — workspace env mode picks the backend.
- A transform with at least one partitioned input — backfill needs cursors to range over. The cursor format matches the source's `partitions = [...]` declaration: `year,month,day,hour` cursors look like `2026/01/01/00`; `dt` cursors look like `2026-01-01`.
- Recommended: `mode = "merge"` + `merge_keys` on the output. Promote semantics depend on mode (see below); merge is the only mode that's safe by default.

## The recipe — UI

1. Go to `/pipelines/dashboard?dir=<your-pipeline>`.
2. In the **Backfills** card, click **Stage backfill**.
3. Pick the transform (the dialog lists every transform with a partitioned input).
4. **From cursor:** the earliest partition you want, e.g. `2026/01/01/00`.
5. **To cursor:** the latest partition (inclusive), e.g. `2026/05/12/23`.
6. Click **Stage**. The runner Lambda invokes against the full historical window — minutes for a few months, longer for years. The dialog button reads "Staging…" while it runs. When the staging table lands, the page auto-navigates to the review screen.
7. The review screen (`/backfills?dir=<dir>&run=<run_id>`) shows staging side-by-side with canonical: row counts, schema match, and (when `merge_keys` is declared) a live "Promote would update X rows, insert Y" preview as you pick the dedup column. The default column is auto-picked from the schema (`event_id` for synthetic-event-shaped data, `*_id` columns in general).
8. Click **Promote** (or **Discard**). Promote runs the MERGE; discard drops the staging table.

## The recipe — CLI

```bash
# 1. Stage. Same shape as the UI dialog. The command returns a run_id
#    once the staging Lambda invocation completes (so this blocks until
#    the historical window has been read end-to-end).
bin/clavesa pipeline backfill stage stream \
  --node passthrough \
  --from 2026/01/01/00 \
  --to 2026/05/12/23

# returns: run_id <id>, staging table <path>

# 2. Diff: row count, schema, sample-diff on merge_keys when declared.
bin/clavesa pipeline backfill diff stream <run_id>

# 3. Promote (or discard).
bin/clavesa pipeline backfill promote stream <run_id>
bin/clavesa pipeline backfill discard stream <run_id>   # alternative
```

`pipeline backfill list <pipeline-dir>` shows open staging tables — useful for cleanup or resuming a review later.

## Why stage-then-promote, not direct write

Backfill reads a wide partition window — often millions of rows. If the transform has a bug (wrong cast, missing filter, off-by-one timestamp), you don't want it landing straight in canonical and contaminating production. Staging is the parallel table you diff against canonical and inspect via the Catalog UI before committing.

When the transform is `mode = "merge"` and you trust the keys, you can skip staging: pass `--direct` to `stage` (or check the **direct** box in the Stage dialog). The write goes straight to the canonical target; the run stamps `trigger = "backfill-direct"` instead of `backfill` so it shows up in run history distinctly. Still idempotent under retry because of the merge keys.

## Promote semantics depend on output mode

| Output mode | Promote behavior |
|---|---|
| `merge` (with `merge_keys`) | `MERGE INTO target USING staging ON <merge_keys> WHEN MATCHED UPDATE * WHEN NOT MATCHED INSERT *`. Idempotent. The default-and-recommended shape. |
| `append` | Refuses by default — appending a window that overlaps existing data is always wrong. Pass `--force-dedup <key>` (runs a MERGE that drops duplicates on that key) or `--allow-duplicates` (explicit accept). |
| `replace` | Refuses unless the output declares `replace_partitions = ["<col>"]` (partitioned-replace by partition is meaningful). Otherwise `replace` implies whole-table; partial-window backfill doesn't fit. Run a full-table backfill instead. |

## How it fits with event-driven triggers

When a backfill runs against a pipeline that's also receiving live S3-event triggers, the two paths are independent:

- **Live triggers** (see [s3-trigger](s3-trigger.md)) advance the partition watermark and write to canonical.
- **Backfill** invokes the transform Lambda directly with a `_backfill` event payload that overrides the partition window and routes the write to a parallel staging table. The watermark is neither read nor advanced. Same runner code path otherwise; same Iceberg writer; same IAM scope.

So you can backfill last quarter while new files keep streaming in — the staging table accumulates the historical window, the canonical target keeps growing from live events, and promote merges them when you're ready.

## Auto-expiry

Unpromoted staging tables get swept by the Iceberg-maintenance slice after `backfill_retention_days` (workspace-level, default 30). Tied to the broader Iceberg maintenance work; for now, explicit `backfill discard` is the deterministic cleanup.

## Troubleshooting

**Backfill stage runs for 15 minutes then times out.** Lambda's 15-minute hard ceiling. Narrow the cursor range and run multiple smaller backfills, or move the transform to `compute = "fargate"` (longer-running, similar cost shape) for the backfill.

**Promote button is disabled.** Either the output is `mode = "replace"` (partial-window replace not yet supported — re-stage with `--direct` instead), or your `append`-mode target hasn't declared `merge_keys`. Set them with `clavesa node edit … --output-merge-keys <col>` and re-deploy; future backfills get the merge path automatically.

**Diff shows zero rows in staging.** The cursor format doesn't match the source's `partitions` declaration, or the window predates any data in the source bucket. Check `clavesa source show <name>` for the declared partition keys and start-from baseline.

**State machine not found.** Cloud-mode backfill needs the SFN deployed. Run `clavesa pipeline deploy <pipeline>` first, then re-run the backfill command. (Not applicable to `compute = "local"` — local mode resolves catalog/schema from workspace + pipeline config and invokes the runner image directly; nothing to deploy.)

## See also

- [s3-trigger](s3-trigger.md) — the event-driven counterpart; once a pipeline is wired for live triggers, backfill is what loads everything older.
- [merge-dim-table](merge-dim-table.md) — the SCD-Type-1 shape; merge_keys-on-append makes backfill promote naturally safe.
- [s3-bulk-ingest](s3-bulk-ingest.md) — when there is no incremental pipeline at all and you just want the whole bucket mirrored once.
