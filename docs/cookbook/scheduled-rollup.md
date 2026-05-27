# Scheduled rollup

> **When you have one:** a Delta table that grows over time (a bronze ingest, an event log, a session table) and you want a daily/hourly summary computed on a schedule — the kind of thing you'd otherwise put in a dbt run on Airflow.

This recipe wires a cron-triggered pipeline whose only job is to recompute an aggregate from an upstream table. No source files involved — the input is another Delta table you already have.

## What you'll end up with

- A deployed pipeline with one transform.
- An EventBridge schedule rule that starts the pipeline at a cadence you set (e.g. nightly at 02:00 UTC).
- A Delta summary table that gets rewritten on each run from the current contents of the upstream.

## Prerequisites

- A deployed workspace (`clavesa workspace deploy`).
- An existing Delta table you want to roll up. Could be the output of a transform in another pipeline ([multi-stage-pipeline](multi-stage-pipeline.md) builds one), or any table already in the workspace catalog.
- AWS credentials in your shell so the eventual `pipeline deploy` can read state.

> **Local iteration works too.** The recipe deploys to Lambda because the *scheduled* trigger needs a deployed pipeline. Cross-pipeline `--from-table` reads themselves work locally too. Every local pipeline in a workspace shares one Delta warehouse, so a local `clavesa pipeline run` of this recipe resolves the upstream table. Iterate on the SQL with `clavesa pipeline run` before deploying; deploy once you wire the schedule.

For this recipe we'll assume the upstream is `clavesa_<workspace>__taxis.trips_bronze__default` — the bronze table the [multi-stage-pipeline](multi-stage-pipeline.md) recipe produces. Substitute your own table.

## The recipe

```bash
# 1. New pipeline; the transform doesn't have a source — its input is
#    a cross-pipeline reference to the bronze table.
bin/clavesa pipeline create taxi_daily

# 2. Add the transform, then set its SQL. Compute target is decided
#    at run time: `clavesa pipeline run` executes locally, the deployed
#    scheduler invokes Lambda. No per-node attr to set.
bin/clavesa node add taxi_daily --type transform --name daily_trips
bin/clavesa node edit taxi_daily daily_trips \
  --set "sql=
  SELECT
    DATE(pickup_ts)         AS dt,
    COUNT(*)                AS trips,
    SUM(total_amount)       AS revenue,
    AVG(trip_distance)      AS avg_distance
  FROM trips_bronze
  GROUP BY DATE(pickup_ts)"

# 3. Wire the upstream table as an input. node connect --from-table
#    is the cross-pipeline path: <schema>.<table> reference, no edge
#    against another node in *this* pipeline.
bin/clavesa node connect taxi_daily \
  --from-table taxis.trips_bronze__default \
  --to daily_trips \
  --input trips_bronze
```

## Set the schedule

The pipeline carries a `trigger_schedule` variable that the orchestration module passes to EventBridge. Set it in `<workspace>/taxi_daily/terraform.tfvars`:

```hcl
trigger_schedule = "cron(0 2 * * ? *)"   # 02:00 UTC every day
```

Common cron expressions:

| When | Expression |
|---|---|
| Every day at 02:00 UTC | `cron(0 2 * * ? *)` |
| Every hour at :15 | `cron(15 * * * ? *)` |
| Every Monday at 09:00 UTC | `cron(0 9 ? * MON *)` |
| Every 15 minutes | `rate(15 minutes)` |

(EventBridge cron uses six fields — the day-of-week / day-of-month conflict resolution requires one of them to be `?`. Use `rate(...)` for simple intervals — usually clearer.)

## Deploy

```bash
bin/clavesa pipeline deploy taxi_daily
```

Deploy provisions: the transform Lambda with read permission on `taxis.trips_bronze__default` (cross-pipeline read is handled by IAM), an EventBridge schedule rule that invokes the pipeline at the configured cadence, and the system catalog row tracking this pipeline's runs.

## What you should see

- At the scheduled time, `/pipelines/dashboard?dir=taxi_daily` gets a new run with `trigger = "scheduled"`.
- The Delta summary table at `clavesa_<workspace>__taxi_daily.daily_trips__default` either appears (first run) or gets overwritten (subsequent runs).
- Catalog → click through → daily summary rows, one per pickup date.

To trigger manually for testing, use the **Run pipeline** button on the dashboard (CLI: `pipeline run`). Manual runs and scheduled runs are indistinguishable downstream — they just stamp `runs.trigger` differently for observability.

## Incremental input: read only new bronze data

By default the rollup full-reads the bronze table on every cron tick. For a daily aggregate that's cheap (Lambda, cents per day), and re-aggregating from scratch is the simplest correctness story. But on a high-volume bronze, you can pay for re-aggregating the same months of history every night.

Flip the upstream to incremental reads and the rollup only sees rows from new Delta commits since its last successful run:

```bash
bin/clavesa node edit taxi_daily daily_trips --incremental-input trips_bronze
```

Pair this with `mode = "merge"` and `merge_keys = ["dt"]` on the output (see below) so each run's per-`dt` slice upserts into the canonical summary in place. Without merge mode the daily totals would be a partial sum of only-the-new-rows since last run, not the whole day; with merge mode they're idempotent per-day rollups that accumulate as new rows land. This is the standard pattern for cron-driven aggregations over an append-only bronze.

## Output mode — replace or append

Default is `mode = "replace"`: every run wipes the summary table and writes the fresh aggregate. Right when:

- The upstream covers all history you want in the summary (e.g. the rollup is "this year so far").
- Cost is fine — re-aggregating millions of rows nightly on Lambda is cents per day.

For append-style accumulation (one row per `(dt, run_id)` to track how the summary evolves), set `mode = "append"`. For idempotent daily upserts keyed on `dt`, set `mode = "merge"` with `merge_keys = ["dt"]` — that's the recommended shape if you want re-runs to overwrite a specific day's row in place without growing the table:

```hcl
# In <workspace>/taxi_daily/pipeline.tf

module "daily_trips" {
  # ...
  output_definitions = {
    default = {
      mode       = "merge"
      merge_keys = ["dt"]
    }
  }
}
```

See [merge-dim-table](merge-dim-table.md) for the deeper version of this pattern.

## Troubleshooting

**`pipeline deploy` errors on `trigger_schedule`.** Confirm the expression is wrapped in `cron(...)` or `rate(...)`. AWS EventBridge doesn't accept bare cron strings.

**Schedule fires but the run fails with `AccessDenied`.** Cross-pipeline reads need the consumer pipeline's Lambda role to have read permission on the producer pipeline's Glue table. The orchestration emitter handles this when `node connect --from-table` is used — verify your pipeline's `.tf` has a `module.<node>.external_inputs` block (or whatever the current shape is) referencing the upstream table. Re-run `clavesa pipeline orchestration sync <pipeline-dir>` if it's missing.

**Scheduled runs land but produce zero rows.** The upstream is empty for the time window. Cross-pipeline reads see the producer's table at the moment the consumer fires — if the producer hasn't run yet today, the daily-aggregate query returns nothing. Either order the schedules (producer at 01:00, consumer at 02:00) or use `rate(...)` to decouple.

**Schedule doesn't fire at all.** Check the EventBridge rule directly: `aws events list-rules --name-prefix clavesa-<pipeline>`. The rule should exist and be enabled. If it's there but never invokes the Step Function, the EventBridge → SFN role likely lacks `states:StartExecution` — re-apply to refresh IAM.

## See also

- [multi-stage-pipeline](multi-stage-pipeline.md) — for chaining a bronze ingest and the rollup in one pipeline rather than two.
- [merge-dim-table](merge-dim-table.md) — for the idempotent-upsert variant of this pattern.
