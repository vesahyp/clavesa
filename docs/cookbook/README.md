# Cookbook

Worked recipes for common pipeline shapes. The main [README quick-start](../../README.md#quick-start) covers the simplest URL → table → dashboard case; these are the next-most-common patterns.

Each recipe is self-contained: prereqs, the commands or click-through to run, what you should see, and a Troubleshooting section for the gotchas.

## Patterns

### Ingestion

- **[s3-bulk-ingest](s3-bulk-ingest.md)** — read an entire S3 bucket into a Delta table in one shot. For static archives, one-time vendor dumps, or anything you want to mirror as-is.
- **[s3-trigger](s3-trigger.md)** — event-driven processing of an S3 bucket where new files keep arriving. EventBridge → SQS → poller → SFN per partition.
- **[http-changing-source](http-changing-source.md)** — a public HTTP API whose data keeps moving (feeds, "latest N", leaderboards). Full re-fetch + merge accumulates a deduped table; + append builds a snapshot fact with the change history the API never exposes.
- **[backfill](backfill.md)** — load historical files that landed before a pipeline existed, or replay a window after a transform-logic fix. Stage → review → promote, with a `--direct` escape hatch.

### Composition

- **[multi-stage-pipeline](multi-stage-pipeline.md)** — bronze + silver in one pipeline. The medallion shape every real workload eventually grows into.
- **[scheduled-rollup](scheduled-rollup.md)** — cron-triggered transform reading from an existing Delta table and writing a daily summary. The dbt-on-Airflow-style nightly aggregation pattern.

### Output shapes

- **[merge-dim-table](merge-dim-table.md)** — keyed upserts into an SCD-Type-1 dimension. Re-runnable, idempotent, the safety net beneath every event-driven append pipeline.

### Transform languages

- **[python-transform](python-transform.md)** — when SQL isn't enough. Same skeleton, swap `language = "sql"` for `language = "python"` and ship a `transform(spark, inputs) -> dict[str, DataFrame]` function.

### Upgrading

- **[migrate-to-v2](migrate-to-v2.md)** — upgrading from v1.x (Iceberg) to v2.0.0 (Delta). No automated tool; the path is recreate from source.

## Not yet written

Patterns waiting on capability that hasn't shipped:

- **Cross-account S3** — waiting on the `assume-role` credential kind in TODO under "Input sources (ADR-017 implementation)".
- **Streaming source (Kafka / Kinesis)** — no source kind shipped yet.
- **HTTP source with auth** — credentials registry exists; recipe waiting until there's a public authed API worth pointing at.
