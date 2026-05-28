# MERGE / slowly-changing dimension table

> **When you have one:** a daily (or hourly) snapshot of an entity — customers, products, locations — and you want one row per entity in your warehouse, updated in place when the source changes. The classic SCD-Type-1 dimension shape.

This recipe builds a transform whose output is keyed: re-running it with overlapping or duplicate input data is **idempotent**. Same input twice → same output. New row in input → new row in output. Changed row in input → existing output row updated in place.

## What you'll end up with

- One Delta dimension table you can join against from fact tables in other pipelines.
- A pipeline you can re-run as often as you like — daily, hourly, ad-hoc — without growing duplicates or needing to truncate first.
- The same machinery [s3-trigger](s3-trigger.md) uses to make event-driven appends safe under retry.

## Prerequisites

- A workspace per the [README quick-start](../../README.md#quick-start).
- A source where each pull is a fresh snapshot of the full dimension (e.g. `customers_2026-05-13.parquet` overwritten daily) **or** a CDC stream where each row carries a stable natural key.

For this recipe we'll point at the synth-events generator (`scripts/synth_events.py --shape dim`) so the example is reproducible end-to-end without any external fixture. For real workloads, swap the source register line for your own URL — the SQL is what carries the dimension shape, not the source.

## The recipe

```bash
# 0. (One-time) Generate a small customers parquet to S3. Re-run with
#    --revision 1 later to mutate customer #1's tier, so the "change a
#    single row and re-run" step at the bottom of this recipe has
#    something to land. Requires AWS_PROFILE pointed at the same
#    account as your workspace.
python scripts/synth_events.py --shape dim \
  --bucket clavesa-synth-vk --prefix synth-customers \
  --customers 50 --revision 0

# 1. Source — your real one would point at a daily-refreshed parquet
#    file. The synth-events bucket above is what the cookbook walks
#    against; substitute your own bucket/prefix or `--from https://...`
#    for real data.
bin/clavesa source register customers \
  --from s3://clavesa-synth-vk/synth-customers/ \
  --format parquet

bin/clavesa pipeline create crm
bin/clavesa node add crm --type transform --name dim_customers

# 2. SQL — keyed projection. customer_id is the natural key; everything
#    else is mutable per snapshot.
bin/clavesa node edit crm dim_customers --set "sql=
  SELECT
    CAST(customer_id    AS BIGINT)  AS customer_id,
    name,
    email,
    CAST(signup_date    AS DATE)    AS signup_date,
    tier,
    current_timestamp()             AS dim_loaded_at
  FROM customers"

# 3. Declare the merge key. Setting --output-merge-keys flips the
#    output mode to merge automatically — no --output-mode needed.
bin/clavesa node edit crm dim_customers --output-merge-keys customer_id

# 4. Wire the source and you're done.
bin/clavesa source attach crm customers --to dim_customers --as customers
```

The UI equivalent: select `dim_customers` in the editor, expand **Output** on the right panel, set Mode to `merge` and Merge Keys to `customer_id`, click **Save Output Config**.

`merge_keys` is the natural key — the column (or columns) that uniquely identifies a row in the dimension. Pick something stable: a database primary key, an external system's customer id, a UUID. Avoid keys that drift over time (email addresses change; phone numbers churn) unless you genuinely want a new dim row when they do.

## Run it

```bash
bin/clavesa pipeline run crm
```

First run creates `dim_customers` in Glue with the rows from the snapshot. Run it again with the same source — the row count doesn't grow; the runner runs `MERGE INTO dim_customers USING staging ON dim_customers.customer_id = staging.customer_id WHEN MATCHED UPDATE * WHEN NOT MATCHED INSERT *`.

## What you should see

- `/` (Catalog) shows `dim_customers` under `clavesa_<workspace>__crm`.
- Click through: schema + sample rows.
- Run the pipeline twice in a row. The **Volume timeline** on TableDetail shows two commits (one per run); the first is `write +N` (initial create), the second is `merge +N/-N` (the MERGE rewrote every matched row). Row count stays at N either way.
- Change a single row in the source (re-run `python scripts/synth_events.py --shape dim ... --revision 1` to bump customer #1's tier one band), re-run the pipeline. A third commit lands as `merge +N/-N`; the dim table has the same row count but customer #1's tier reflects the new value.

## What MERGE actually runs

`mode = "merge"` with the `merge_keys` above gets the runner to translate the write into:

```sql
MERGE INTO clavesa_<workspace>__crm.dim_customers AS target
USING <staging> AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

`UPDATE SET *` updates every non-key column from the source row. `INSERT *` inserts the whole source row. Each run produces a new Delta commit, so time-travel queries (`SELECT ... VERSION AS OF <version>`) let you see the dimension as it was on any past run.

## Backfill / corrections

If you discover the dimension transform had a bug and the historical rows are wrong, you have two paths:

1. **Fix the transform, re-run.** Because the output is keyed, the next run upserts the corrected rows in place — no manual cleanup. The corrupted commit stays in Delta history for forensics; new queries see the fixed data.
2. **Backfill against a corrected source.** Stage a backfill from the Backfills card on `/pipelines/dashboard?dir=crm`. The merge keys mean Promote is safe by default — see [backfill](backfill.md) for the full flow.

## Multi-column keys

If a single column isn't unique, list every column that together forms the natural key:

```bash
bin/clavesa node edit crm dim_customers \
  --output-merge-keys customer_id,as_of_date
```

The `MERGE` ON clause becomes `target.customer_id = source.customer_id AND target.as_of_date = source.as_of_date`. Two-column keys are how you build SCD-Type-2 dimensions where every version of a customer's row over time is kept as a separate row.

## Troubleshooting

**Row count grows on each run instead of staying flat.** Confirm `mode = "merge"` (not `"append"`) and that `merge_keys` matches the column name exactly. Without merge keys, the runner falls back to `append` which adds rows on every run.

**MERGE fails with "the ON search condition matched a single row from the target with multiple rows of the source".** Your source has duplicates on the merge key. Either de-dupe in the SQL (`GROUP BY customer_id ... LIMIT 1`-style) or pick a key that's actually unique in the source.

**The dimension reflects partial data.** A run failed mid-way and only some keys got updated. Delta writes are atomic per commit — either every row in the run committed or none did. If you see partial updates, something else is writing to the table (a second pipeline targeting the same name? a stale CLI session?). The lineage panel on TableDetail surfaces the producing node.

## Consuming this dim incrementally downstream

A fact pipeline that joins against `dim_customers` doesn't need to re-read every customer row on every run. Because Delta CDF is enabled by default on all clavesa-managed tables, downstream transforms can read only the rows that changed since their last run:

```bash
bin/clavesa node edit facts dim_customers_incremental \
  --incremental-input dim_customers
```

The runner then reads `(last_version, current_version]` from `dim_customers` via Change Data Feed. CDF returns rows tagged with `_change_type` (`insert`, `update_postimage`, `update_preimage`, `delete`) and `_commit_version`. For an SCD-Type-1 dim, you only need `insert` and `update_postimage`; the runner filters to those and dedupes to the latest row per `merge_keys` (`_commit_version DESC`) automatically. No `ROW_NUMBER()` wrapper in your SQL.

This is the pattern the v2.0.0 cutover exists to enable. On Iceberg v2, incremental reads of a MOR-MERGE upstream returned nothing useful ([apache/iceberg#1949](https://github.com/apache/iceberg/issues/1949)). Delta CDF materialises the change rows at write time, so the downstream consumer gets real data regardless of whether the upstream used COW or MOR.

## See also

- [scheduled-rollup](scheduled-rollup.md) — the aggregation variant; pair a daily dim refresh with a daily KPI rollup.
- [s3-trigger](s3-trigger.md) — `merge_keys` makes append-mode pipelines safe under retry; this recipe makes them the whole point.
- [backfill](backfill.md) — when a dim-transform bug needs fixing, stage a corrected backfill and review before promote.
