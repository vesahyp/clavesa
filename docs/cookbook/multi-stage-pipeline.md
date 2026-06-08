# Multi-stage pipeline (bronze → silver → gold)

> **When you have one:** a raw source you want to land as-is for auditability **and** cleaned-up, aggregated views of the same data for analysts. The classic medallion shape: bronze ingests, silver cleans and joins, gold rolls up.

This is the "get going with a real pipeline" recipe — three transforms chained so each one's Delta output is the next one's input.

> **Continues from** the [README quick-start](../../README.md#quick-start): it assumes the `cookbook` workspace exists with the `src_trips` source registered. If you've done the quick-start you have both — skip to [The recipe](#the-recipe). Otherwise run the **Setup** block.

## Setup (self-contained)

```bash
make build                                   # produces ./bin/clavesa
export WS=/tmp/clavesa-cookbook
mkdir -p $WS
bin/clavesa workspace init cookbook --workspace $WS    # no-op if it already exists
bin/clavesa source register src_trips \
  --from https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet \
  --workspace $WS
```

Set `export CLAVESA_WORKSPACE=$WS` to drop the `--workspace` flag from the commands below.

## What you'll end up with

- Three Delta tables in one pipeline: `trips_bronze` (raw, typed), `revenue_by_payment` (silver aggregate), `revenue_kpis` (gold single-row rollup).
- A lineage chain `src_trips → trips_bronze → revenue_by_payment → revenue_kpis` visible on each table's TableDetail page.
- One pipeline that runs all three in dependency order on every invocation.

## The recipe

```bash
# 1. Create the pipeline.
bin/clavesa pipeline create taxis --workspace $WS

# 2. Bronze: passthrough + type cast. The raw parquet stores timestamps
#    as TIMESTAMP_NTZ and several numerics loosely; pin the types now so
#    silver and gold don't have to.
bin/clavesa node add taxis --type transform --name trips_bronze --workspace $WS
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
  FROM trips" --workspace $WS

# 3. Silver: aggregate by payment type. Reads bronze's Delta output, not
#    the source — that's what makes it silver and not a second bronze.
bin/clavesa node add taxis --type transform --name revenue_by_payment --workspace $WS
bin/clavesa node edit taxis revenue_by_payment --set "sql=
  SELECT
    payment_type,
    COUNT(*)                                                 AS trips,
    ROUND(SUM(total_amount), 2)                              AS revenue,
    ROUND(AVG(tip_amount / NULLIF(fare_amount, 0)) * 100, 1) AS avg_tip_pct
  FROM trips_bronze
  WHERE pickup_ts IS NOT NULL
  GROUP BY payment_type
  ORDER BY revenue DESC" --workspace $WS

# 4. Gold: roll silver's per-payment-type rows into one KPI row.
bin/clavesa node add taxis --type transform --name revenue_kpis --workspace $WS
bin/clavesa node edit taxis revenue_kpis --set "sql=
  SELECT
    SUM(trips)                          AS total_trips,
    ROUND(SUM(revenue), 2)              AS total_revenue,
    ROUND(SUM(revenue) / SUM(trips), 2) AS revenue_per_trip
  FROM revenue_by_payment" --workspace $WS

# 5. Wire the edges, THEN attach the source. `source attach` triggers an
#    orchestration sync that rejects a DAG with a dangling root, so the
#    downstream edges must exist first.
bin/clavesa node connect taxis --from trips_bronze       --to revenue_by_payment --input trips_bronze       --workspace $WS
bin/clavesa node connect taxis --from revenue_by_payment --to revenue_kpis       --input revenue_by_payment --workspace $WS
bin/clavesa source attach taxis src_trips --to trips_bronze --as trips --workspace $WS

# 6. Run.
bin/clavesa pipeline run taxis --workspace $WS
```

The run reports each node in dependency order:

```
NODE                TYPE       STATUS  OUTPUT
trips_bronze        transform  ok      clavesa_cookbook__taxis.trips_bronze
revenue_by_payment  transform  ok      clavesa_cookbook__taxis.revenue_by_payment
revenue_kpis        transform  ok      clavesa_cookbook__taxis.revenue_kpis
```

## How the stages talk to each other

Each transform writes a Delta table at `clavesa_<workspace>__<pipeline>.<node>`. Downstream transforms reference upstream tables through their `inputs` map. In the `.tf` Clavesa emits, that wiring shows up as:

```hcl
module "revenue_by_payment" {
  # ...
  inputs = {
    trips_bronze = module.trips_bronze.outputs["default"]
  }
}
```

`module.trips_bronze.outputs["default"]` is the catalog table id `trips_bronze` — silver reads it as a regular Delta table, not a Parquet path. So each silver run sees the **full** current contents of bronze. Gold reads silver the same way. `pipeline run` topologically sorts the DAG, so bronze always finishes before silver starts.

Gold can only see silver's four columns (`payment_type`, `trips`, `revenue`, `avg_tip_pct`) — silver already aggregated the timestamps away. For a date-keyed gold table, carry a `DATE(pickup_ts)` column through silver, or point gold at `trips_bronze` directly.

## Incremental upstream reads

Default behaviour: every silver run full-reads the bronze Delta table. For nightly aggregations over small/medium data that's the right call. For high-throughput pipelines, mark the upstream alias incremental — the runner stores a watermark per `(consumer, alias)` and reads only the Delta commits since the consumer's last successful run via Change Data Feed:

```bash
bin/clavesa node edit taxis revenue_by_payment --incremental-input trips_bronze --workspace $WS
```

(UI equivalent: select silver in the editor, then check the box next to `trips_bronze` under **Incremental upstream reads**.)

The first run on the flag reads everything and stamps the watermark to bronze's current Delta version; each later run reads only what bronze committed since. The full mechanics — at-least-once on retry, why replace-mode upstreams reset the watermark, and how to pair incremental input with `merge_keys` — are covered in [merge-cdf](merge-cdf.md), which builds a pipeline around exactly this.

## Verify

```bash
# 1. All three nodes report `ok` (exit 0).
bin/clavesa pipeline run taxis --workspace $WS
#    NODE                TYPE       STATUS  OUTPUT
#    trips_bronze        transform  ok      clavesa_cookbook__taxis.trips_bronze
#    revenue_by_payment  transform  ok      clavesa_cookbook__taxis.revenue_by_payment
#    revenue_kpis        transform  ok      clavesa_cookbook__taxis.revenue_kpis

# 2. The three tables exist.
bin/clavesa query "SHOW TABLES IN clavesa_cookbook__taxis" --workspace $WS
#    → rows for trips_bronze, revenue_by_payment, revenue_kpis

# 3. Bronze carried every row through.
bin/clavesa query "SELECT COUNT(*) AS rows FROM clavesa_cookbook__taxis.trips_bronze" --json --workspace $WS
#    → {"columns":["rows"],"column_types":["bigint"],"rows":[[2964624]]}

# 4. Gold is one row that reconciles with silver.
bin/clavesa query "SELECT * FROM clavesa_cookbook__taxis.revenue_kpis" --workspace $WS
#    total_trips  total_revenue  revenue_per_trip
#    2964624      79456384.28    26.8
```

In the UI (`bin/clavesa ui --workspace $WS`): `/tables/clavesa_cookbook__taxis/taxis/revenue_by_payment` → the **Lineage** panel shows `taxis.trips_bronze` upstream and `taxis.revenue_kpis` downstream; `/pipelines/run?dir=taxis&run=<id>` shows all three nodes in the DAG colored by status. Load each page and confirm `playwright-cli console error` reports 0.

Row counts are deterministic for `yellow_tripdata_2024-01.parquet` (2,964,624 rows); the dollar totals are too. A different month differs.

## Troubleshooting

**Silver reads `trips_bronze` and gets "table not found".** The bronze run hasn't created the table yet. `pipeline run` runs nodes in dependency order, so this only happens if bronze itself failed — check bronze's status in the run output.

**Type casts in bronze blow up.** Older TLC files use different column names; `tpep_pickup_datetime` may not exist pre-2022. Run bronze as a plain `SELECT *` first, inspect the schema on the TableDetail page, then add the casts that match what you see.

**`source attach` fails with a DAG / dangling-root error.** You attached the source before wiring the downstream edges. Connect bronze→silver→gold first (step 5 order), then attach.

**Lineage panel shows no upstream/downstream.** Either the edge wasn't written (check `node connect`'s exit code) or the UI's `.tf` cache is lagging (~30s); reload.

## Next

- **[Merge + CDF](merge-cdf.md)** — fold a second month of data in with keyed upserts and incremental change-feed reads.
- **[Query your data](query-your-data.md)** — explore these tables with ad-hoc SQL.

## See also

- [s3-bulk-ingest](s3-bulk-ingest.md) — swap the URL source for an S3 bucket as bronze.
- [scheduled-rollup](scheduled-rollup.md) — put a cron schedule on this pipeline.
