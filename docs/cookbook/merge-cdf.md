# Merge + change data feed (keyed upserts, incremental downstream)

> **When you have one:** data that arrives in batches over time — a new month, a new day, a corrected file — and a table you want to keep *one row per key* in, updated in place, without re-processing everything downstream each time.

This recipe builds a keyed table whose output is **idempotent** (re-running with the same input doesn't duplicate rows) and a downstream transform that reads only what changed via Delta's **Change Data Feed (CDF)**. It's the two-stage shape every incremental warehouse grows into.

> **Continues from** [multi-stage-pipeline](multi-stage-pipeline.md) — same `cookbook` workspace. It registers its own source so it won't disturb the `demo`/`taxis` pipelines. If you're starting here, the **Setup** block is self-contained.

## Setup (self-contained)

```bash
make build
export WS=/tmp/clavesa-cookbook
mkdir -p $WS
bin/clavesa workspace init cookbook --workspace $WS    # no-op if it exists
export CLAVESA_WORKSPACE=$WS                            # drop --workspace below
```

## What you'll end up with

- `daily_revenue` — one row per `trip_date`, upserted in place. Re-runnable as often as you like without growing duplicates.
- `daily_flagged` — a downstream that reads `daily_revenue` **incrementally**: each run processes only the days that changed since its last run, not the whole table.
- A pipeline that folds a new month of data in with a one-line source edit.

## The recipe

```bash
# 1. A source that starts at January. We'll repoint it at February later.
bin/clavesa source register src_monthly \
  --from https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet

bin/clavesa pipeline create daily

# 2. daily_revenue: one row per calendar day. trip_date is the natural key.
bin/clavesa node add daily --type transform --name daily_revenue
bin/clavesa node edit daily daily_revenue --set "sql=
  SELECT
    DATE(CAST(tpep_pickup_datetime AS TIMESTAMP)) AS trip_date,
    COUNT(*)                                       AS trips,
    ROUND(SUM(total_amount), 2)                    AS revenue
  FROM monthly
  WHERE CAST(tpep_pickup_datetime AS TIMESTAMP) >= '2024-01-01'
    AND CAST(tpep_pickup_datetime AS TIMESTAMP) <  '2024-03-01'
  GROUP BY 1"

# 3. Declare the merge key. This flips the output mode to `merge`
#    automatically — no --output-mode needed.
bin/clavesa node edit daily daily_revenue --output-merge-keys trip_date

# 4. daily_flagged: reads daily_revenue, flags big days. --incremental-input
#    makes it consume only changed rows via CDF; --output-merge-keys makes
#    that safe (each changed day upserts its one flagged row).
bin/clavesa node add daily --type transform --name daily_flagged
bin/clavesa node edit daily daily_flagged --set "sql=
  SELECT trip_date, trips, revenue, revenue > 2000000 AS high_revenue_day
  FROM daily_revenue"
bin/clavesa node edit daily daily_flagged \
  --output-merge-keys trip_date \
  --incremental-input daily_revenue

# 5. Wire the edge, then attach the source.
bin/clavesa node connect daily --from daily_revenue --to daily_flagged --input daily_revenue
bin/clavesa source attach daily src_monthly --to daily_revenue --as monthly

# 6. Run (January).
bin/clavesa pipeline run daily
```

The UI equivalent for steps 3–4: select the node, expand **Output**, set Mode `merge` and Merge Keys `trip_date`; for the downstream, also tick `daily_revenue` under **Incremental upstream reads**.

## Idempotency: re-run without duplicating

After the January run, `daily_revenue` has one row per day:

```bash
bin/clavesa query "SELECT COUNT(*) AS days, MIN(trip_date) first, MAX(trip_date) last FROM clavesa_cookbook__daily.daily_revenue"
```

```
days  first                    last
32    2024-01-01T00:00:00.000  2024-02-01T00:00:00.000
```

(31 January days plus one just-after-midnight Feb 1 pickup — real, slightly dirty data.) Run the **same** source again:

```bash
bin/clavesa pipeline run daily
bin/clavesa query "SELECT COUNT(*) AS days FROM clavesa_cookbook__daily.daily_revenue" --json
# → {"columns":["days"],"column_types":["bigint"],"rows":[[32]]}
```

Still 32. The runner ran `MERGE INTO daily_revenue USING staging ON daily_revenue.trip_date = staging.trip_date WHEN MATCHED UPDATE * WHEN NOT MATCHED INSERT *` — every existing day was updated in place, none duplicated. That's what `merge_keys` buys you: a pipeline you can re-run any time.

## Fold in a new month with a one-line edit

Repoint the source at February and run. The runner reads the new file; the merge inserts the new days and leaves January untouched:

```bash
bin/clavesa source edit src_monthly \
  --from https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet
bin/clavesa pipeline run daily

bin/clavesa query "SELECT COUNT(*) AS days, MIN(trip_date) first, MAX(trip_date) last FROM clavesa_cookbook__daily.daily_revenue"
```

```
days  first                    last
60    2024-01-01T00:00:00.000  2024-02-29T00:00:00.000
```

60 days now — January's 32 rows survived (they're not in the February file, so the merge never touched them) and 28 February days were inserted.

## The downstream read only what changed

Here's the payoff of `--incremental-input`. The February run touched only 28 rows of `daily_revenue` (the new February days). `daily_flagged` consumes `daily_revenue` through its Change Data Feed, so it processed **only those 28 days**, not all 60:

```bash
bin/clavesa query "
  SELECT node, input_records, output_rows, status
  FROM clavesa_cookbook_system__pipelines.node_runs
  WHERE pipeline='daily' AND node='daily_flagged'
  ORDER BY started_at"
```

The most recent run shows `output_rows = 28` — the February days merged into the flagged table, while January's flagged rows stayed put. The flagged table still ends up complete:

```bash
bin/clavesa query "SELECT COUNT(*) days, SUM(CASE WHEN high_revenue_day THEN 1 ELSE 0 END) high_days FROM clavesa_cookbook__daily.daily_flagged"
# days  high_days
# 60    57
```

On a real high-throughput pipeline, "process 28 rows instead of 60" is the difference between a job that scales with *yesterday's* data and one that scales with *all* data ever — the core of the cost-per-record story.

## What CDF actually does

`daily_revenue` is Delta with Change Data Feed on (the clavesa default). When `daily_flagged` runs, the runner reads:

```python
spark.read.format("delta") \
  .option("readChangeFeed", "true") \
  .option("startingVersion", last_seen_version + 1) \
  .option("endingVersion",   current_version) \
  .table("clavesa_cookbook__daily.daily_revenue")
```

CDF returns rows tagged with `_change_type` (`insert`, `update_postimage`, `update_preimage`, `delete`) and `_commit_version`. The runner keeps `insert` and `update_postimage`, dedupes to the latest row per merge key, and feeds those to your SQL — so your transform sees clean current rows, no `_change_type` plumbing in your query.

**At-least-once on retry.** The watermark advances *after* the output commits. A crash mid-write leaves it at the prior version, so the next attempt re-reads the same range. That's why the downstream output is `merge` on `trip_date`: a re-read re-upserts the same days instead of duplicating them. Pair `--incremental-input` with `--output-merge-keys`; plain `append` would dupe on retry.

## Multi-column keys (SCD-Type-2 dimensions)

If a single column isn't unique, list every column that together forms the key:

```bash
bin/clavesa node edit crm dim_customers --output-merge-keys customer_id,as_of_date
```

The `MERGE ON` clause becomes `target.customer_id = source.customer_id AND target.as_of_date = source.as_of_date`. A two-part key like `(entity_id, as_of_date)` is how you keep every historical version of a row as its own row — a slowly-changing-dimension Type-2 table — instead of overwriting in place.

## Corrections / backfill

Discover the transform had a bug and the historical rows are wrong? Because the output is keyed, you have two clean paths:

1. **Fix the SQL and re-run.** The next run upserts the corrected rows in place — no manual cleanup. The bad commit stays in Delta history for forensics; new queries see the fix.
2. **Backfill a corrected window.** Stage a backfill from the Backfills card on `/pipelines/dashboard?dir=daily`. Merge keys make Promote safe by default — see [backfill](backfill.md).

## Verify

```bash
# Idempotency: two runs of the same month, row count flat.
bin/clavesa pipeline run daily
bin/clavesa query "SELECT COUNT(*) AS days FROM clavesa_cookbook__daily.daily_revenue" --json   # → [[32]]
bin/clavesa pipeline run daily
bin/clavesa query "SELECT COUNT(*) AS days FROM clavesa_cookbook__daily.daily_revenue" --json   # → [[32]]  (unchanged)

# Accumulation: fold in February, days grow, January preserved.
bin/clavesa source edit src_monthly --from https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet
bin/clavesa pipeline run daily
bin/clavesa query "SELECT COUNT(*) AS days FROM clavesa_cookbook__daily.daily_revenue" --json   # → [[60]]

# Incremental downstream: the February run's daily_flagged wrote 28 rows, not 60.
bin/clavesa query "SELECT output_rows FROM clavesa_cookbook_system__pipelines.node_runs WHERE pipeline='daily' AND node='daily_flagged' ORDER BY started_at DESC LIMIT 1" --json
```

Assertable signals: `daily_revenue` stays at 32 across same-month re-runs (idempotent), grows to 60 after the February fold-in (accumulating), and the final `daily_flagged` run's `output_rows` is the new-day count (28), not the full table (60). Counts are deterministic for the 2024-01 / 2024-02 TLC files.

## Troubleshooting

**Row count grows every run instead of staying flat.** `merge_keys` isn't set, so the output fell back to `append`. Confirm with `bin/clavesa node show daily daily_revenue` — Mode should be `merge`.

**MERGE fails: "matched a single target row with multiple source rows."** The source has duplicate keys. De-dupe in the SQL (`GROUP BY trip_date`, as here) or pick a genuinely unique key.

**The downstream re-reads everything every run.** The first run on `--incremental-input` always reads the full table (no watermark yet) and stamps the watermark; incremental savings kick in from the second run. Also: a `replace`-mode upstream rewrites its table each run and resets Delta's version counter, which forces a full re-read — incremental savings only materialize against `merge`/`append` upstreams whose version increases monotonically.

## Next

- **[Explore in a notebook](notebooks.md)** — poke at these tables interactively before committing more SQL.
- **[Build a dashboard](dashboards.md)** — chart `daily_revenue` as a time series.

## See also

- [s3-trigger](s3-trigger.md) — the same `merge_keys` idempotency, triggered by new files landing in S3 instead of a manual source edit.
- [backfill](backfill.md) — stage and review a corrected window before promoting it.
