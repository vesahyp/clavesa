# Multi-stage pipeline (bronze → silver)

> **When you have one:** a raw source you want to land as-is for auditability **and** a cleaned-up, aggregated view of the same data for analysts. The classic medallion shape: bronze does ingest, silver does the joins and the cleanup, gold does the rollups.

This recipe builds bronze + silver. Gold is the same pattern repeated — one more transform reading from silver's Iceberg output.

## What you'll end up with

- Two Iceberg tables in the same pipeline:
  - `<node>__default` for bronze (raw, passthrough)
  - `<node>__default` for silver (cleaned + typed)
- A lineage panel on TableDetail showing the source → bronze → silver chain.
- One pipeline that runs both transforms in order on every invocation; each transform's output is the next one's input.

## Prerequisites

- Workspace inited per the [README quick-start](../../README.md#quick-start).
- A source — this recipe uses the NYC TLC trip data from the README's quick-start so you can compare side-by-side. Substitute your own bucket / URL.

## The recipe

```bash
# 1. Register the source (same as the README quick-start).
bin/clavesa source register trips \
  --from https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet

# 2. Create the pipeline.
bin/clavesa pipeline create taxis

# 3. Bronze: passthrough + type cast. The raw parquet has timestamps as
#    strings in some months; pin them now so silver doesn't have to.
bin/clavesa node add taxis --type transform --name trips_bronze
bin/clavesa node edit taxis trips_bronze --set "sql=
  SELECT
    CAST(tpep_pickup_datetime  AS TIMESTAMP) AS pickup_ts,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_ts,
    CAST(passenger_count       AS INT)       AS passenger_count,
    CAST(trip_distance         AS DOUBLE)    AS trip_distance,
    CAST(payment_type          AS INT)       AS payment_type,
    CAST(fare_amount           AS DOUBLE)    AS fare_amount,
    CAST(tip_amount            AS DOUBLE)    AS tip_amount,
    CAST(total_amount          AS DOUBLE)    AS total_amount
  FROM trips"

# 4. Silver: aggregate by payment type. Reads from bronze's Iceberg
#    output, not from the source — that's what makes it 'silver' and
#    not just a second bronze.
bin/clavesa node add taxis --type transform --name revenue_by_payment
bin/clavesa node edit taxis revenue_by_payment --set "sql=
  SELECT
    payment_type,
    COUNT(*)                                                 AS trips,
    ROUND(SUM(total_amount), 2)                              AS revenue,
    ROUND(AVG(tip_amount / NULLIF(fare_amount, 0)) * 100, 1) AS avg_tip_pct
  FROM trips_bronze
  WHERE pickup_ts IS NOT NULL
  GROUP BY payment_type
  ORDER BY revenue DESC"

# 5. Wire the edges. Source → bronze, bronze → silver.
bin/clavesa source attach taxis trips --to trips_bronze --as trips
bin/clavesa node connect taxis --from trips_bronze --to revenue_by_payment --input trips_bronze

# 6. Run.
bin/clavesa pipeline run taxis
```

## What you should see

- `pipeline run taxis` reports both nodes `ok` after ~30–60s.
- `/` (Catalog) shows two new tables: `trips_bronze__default` and `revenue_by_payment__default`.
- `/tables/.../revenue_by_payment__default` → the Lineage panel shows `trips_bronze__default` as Upstream. Click that → its Lineage panel shows the `trips` source as Upstream and `revenue_by_payment__default` as Downstream.
- `/pipelines/run?dir=taxis&run=<id>` shows both nodes in the DAG, colored by status.

## How the stages talk to each other

Each transform writes an Iceberg table at `clavesa_<workspace>__<pipeline>.<node>__<output_key>`. Downstream transforms reference upstream tables through their `inputs` map. In the `.tf` Clavesa emits, that wiring shows up as:

```hcl
module "revenue_by_payment" {
  # ...
  inputs = {
    trips_bronze = module.trips_bronze.outputs["default"]
  }
}
```

`module.trips_bronze.outputs["default"]` is the catalog table id `trips_bronze__default` — silver reads it as a regular Iceberg table, not as a Parquet path. That means each silver run sees the **full** current contents of bronze.

## Incremental upstream reads

Default behaviour: every silver run full-reads the bronze Iceberg table. For nightly aggregations over small/medium data that's the right call; the planner re-derives the answer from the current state every time.

For high-throughput pipelines, mark the upstream alias as incremental. The runner then stores a watermark per `(consumer, alias)` pair and reads only the snapshot range Iceberg has committed since the consumer's last successful run:

```bash
bin/clavesa node edit taxis revenue_by_payment --incremental-input trips_bronze
```

(UI equivalent: select silver in the editor, then check the box next to `trips_bronze` in the right panel's **Incremental upstream reads** section.)

On the next `pipeline run`, silver does:

```python
spark.read \
  .option("start-snapshot-id", last_seen) \
  .option("end-snapshot-id",   current) \
  .table("clavesa_<workspace>__taxis.trips_bronze__default")
```

First run on the flag reads everything (no watermark yet) and stamps the watermark to bronze's current snapshot. Each later run reads only what bronze appended since.

**At-least-once on retry.** The watermark advances *after* outputs commit. A runner crash mid-write leaves the watermark at the prior snapshot, so the next attempt re-reads the same range. Pair an incremental input with an **`append`-mode output** that declares `merge_keys` (so retries upsert instead of duplicating); same shape as the event-driven recipe at [s3-trigger](s3-trigger.md). Plain `append` with no merge keys will dupe on retry.

**Replace-mode upstreams reset the watermark.** Bronze defaults to `mode = "replace"`: every run wipes the upstream table and writes a fresh snapshot tree, orphaning the previous head. The runner detects this (the stored watermark isn't an ancestor of the new current snapshot) and falls back to a full read for that run, re-stamping the watermark to the new current. So `incremental_input` is correct against replace-mode upstreams but only delivers incremental savings against `append`-mode upstreams whose snapshot lineage extends across runs.

## Adding a gold layer

Same pattern repeated. Add a third transform that reads from `revenue_by_payment`. Silver is keyed by payment type; gold rolls those rows up into a single KPI row for the whole dataset:

```bash
bin/clavesa node add taxis --type transform --name revenue_kpis
bin/clavesa node edit taxis revenue_kpis --set "sql=
  SELECT
    SUM(trips)                          AS total_trips,
    ROUND(SUM(revenue), 2)              AS total_revenue,
    ROUND(SUM(revenue) / SUM(trips), 2) AS revenue_per_trip
  FROM revenue_by_payment"
bin/clavesa node connect taxis --from revenue_by_payment --to revenue_kpis --input revenue_by_payment
```

Gold can only see silver's four columns: `payment_type`, `trips`, `revenue`, `avg_tip_pct`. Silver already aggregated the timestamps away, so gold can roll up silver's metrics but can't reach back for per-day or per-zone cuts. For a date-keyed gold table, carry a `DATE(pickup_ts)` column through silver, or point gold at `trips_bronze` instead.

## Troubleshooting

**Silver reads `trips_bronze` and gets "table not found".** The bronze run hasn't created the table yet. `pipeline run` runs nodes in dependency order — silver waits for bronze to finish. If you ran silver in isolation (`--node silver` is not a flag), you need bronze to have run successfully at least once.

**Type casts in bronze blow up.** Some TLC parquet files use `TIMESTAMP_NTZ`; pre-2024 files used different column names. If `tpep_pickup_datetime` doesn't exist, run the bronze transform without the casts first (just `SELECT *`) to inspect the schema in TableDetail, then add the casts that match what you see.

**Lineage panel shows no upstream / downstream.** Either the edge wasn't written (verify `node connect`'s exit code) or the pipeline hasn't been parsed since the last edit (the panel reads `.tf` directly, but a stale UI cache can lag for ~30s).

## See also

- [s3-bulk-ingest](s3-bulk-ingest.md) — replace the URL-based source above with an S3 bucket as bronze.
- [scheduled-rollup](scheduled-rollup.md) — put a cron schedule on this pipeline so it runs every night.
- [merge-dim-table](merge-dim-table.md) — for silver tables that are slowly-changing dimensions rather than aggregations.
