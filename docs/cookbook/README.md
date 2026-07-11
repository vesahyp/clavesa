# Cookbook

Worked, runnable recipes for building with clavesa. Every command here is real — walked against the built `bin/clavesa`, with the output you should see and an assertable **Verify** block. If a step doesn't work, that's a bug in the product, not the recipe.

## The path

Five recipes that build on each other, top to bottom — one `cookbook` workspace, one dataset (NYC taxi trips), from a first pipeline to charts. The main [README quick-start](../../README.md#quick-start) is the on-ramp (URL → table in three commands); start there if you haven't. Each recipe also has a self-contained **Setup** block, so you can jump straight to any one of them.

1. **[Multi-stage pipeline](multi-stage-pipeline.md)** — bronze → silver → gold. Chain three transforms so each one's Delta output feeds the next. The medallion shape every real workload grows into.
2. **[Merge + CDF](merge-cdf.md)** — keyed upserts and incremental change-feed reads. Fold a second month of data in with a one-line edit; the downstream processes only what changed. Idempotent, re-runnable, the core of the cost-per-record story.
3. **[Query your data](query-your-data.md)** — ad-hoc SparkSQL from the terminal with `clavesa query`, cross-pipeline joins, `sql lint`, and the honest limits (local Spark vs cloud Athena dialect).
4. **[Explore in a notebook](notebooks.md)** — a multi-cell SQL + PySpark scratchpad on the same engine, then `graduate` a cell straight into a pipeline transform.
5. **[Build a dashboard](dashboards.md)** — saved SQL widgets over your tables: big numbers, bar and line charts, controls. Smoke-tested from the CLI, viewed in the UI.

## Real-world ingestion

How data actually gets in — each standalone, swap in your own source.

- **[cloudfront-web-analytics](cloudfront-web-analytics.md)** — cookieless, EU-hosted web analytics: a tracking pixel writes to CloudFront's access logs, and clavesa turns the gzipped-TSV logs into sessions / pageviews / referrers. GA-style insight that never leaves your account.
- **[s3-bulk-ingest](s3-bulk-ingest.md)** — read an entire S3 bucket into a Delta table in one shot. Static archives, vendor dumps, anything you mirror as-is.
- **[s3-trigger](s3-trigger.md)** — event-driven processing where new S3 files trigger runs automatically. EventBridge → SQS → poller → Step Functions per partition.
- **[http-changing-source](http-changing-source.md)** — a public HTTP API whose data keeps moving (feeds, leaderboards). Full re-fetch + merge to dedupe; + append to capture the change history the API never exposes.
- **[backfill](backfill.md)** — load historical files that predate the pipeline, or replay a window after fixing a transform. Stage → review → promote.

## Operations & languages

- **[scheduled-rollup](scheduled-rollup.md)** — a cron-triggered transform reading an existing Delta table and writing a daily summary. The dbt-on-Airflow nightly-aggregation pattern.
- **[python-transform](python-transform.md)** — when SQL isn't enough. Swap `language = "sql"` for `language = "python"` and ship a `transform(spark, inputs) -> dict[str, DataFrame]`.
- **[runner-deps](runner-deps.md)** — add third-party Python packages (pyasn, crawlerdetect, …) to the runner image for your UDFs, via `clavesa runner requirements` or the `/runner` UI.

## Walk every feature (nothing-broke pass)

The recipes double as a feature-test sweep. Walk them in order against a fresh build to confirm the whole CLI + UI surface still works; each contributes one assertable check (full details in each recipe's **Verify** block):

| Step | Recipe | Assertable check |
|------|--------|------------------|
| 0 | [Quick-start](../../README.md#quick-start) | `pipeline run demo` → both transforms `ok` |
| 1 | [multi-stage-pipeline](multi-stage-pipeline.md) | all 3 nodes `ok`; gold `revenue_kpis` = 2,964,624 trips / $79,456,384.28 |
| 2 | [merge-cdf](merge-cdf.md) | `daily_revenue` idempotent at 32 days, accumulates to 60; Feb run's downstream `output_rows` = 28 |
| 3 | [query-your-data](query-your-data.md) | `query` count = 2964624; `sql lint` exits 0 (good) / 1 (bad); missing-table query exits 1 |
| 4 | [notebooks](notebooks.md) | `notebook run` → cells `ok`; `graduate` registers a transform node |
| 5 | [dashboards](dashboards.md) | `dashboards render` exits 0 (and non-zero on a broken widget); UI `/dashboards/<slug>` renders all widgets, 0 console errors |

Counts are deterministic for the `yellow_tripdata_2024-01` / `2024-02` TLC files.

## Not yet written

Patterns waiting on capability that hasn't shipped:

- **Cross-account S3** — waiting on the `assume-role` credential kind.
- **Streaming source (Kafka / Kinesis)** — no source kind shipped yet.
- **HTTP source with auth** — credentials registry exists; recipe waiting on a public authed API worth pointing at.
