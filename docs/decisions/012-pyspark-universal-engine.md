# ADR 012: PySpark as Universal Execution Engine

**Status**: Accepted (supersedes ADR 010; updates ADR 003)

## Context

Clavesa's original design ran multiple execution engines side by side: **DuckDB** for local preview (ADR 010), **Athena** for SQL transforms in production, **Lambda + pandas** for Python transforms, with **Glue** and **ECS** as escape hatches. This was the "right-sized compute per step" idea from ADR 003.

In practice, the multi-engine design produced friction we hadn't anticipated:

- **Dialect drift.** DuckDB SQL, Athena/Trino SQL, and SparkSQL agree on the basics but diverge on date functions, regex syntax, struct/array handling, and window function behaviour. A query that previewed correctly locally could fail or produce different results in production. Users had no way to know which dialect they'd accidentally landed on until deployment.
- **UDF asymmetry.** DuckDB supports Python UDFs registered into SQL; Athena does not. Pipelines that needed Python logic mid-SQL (parsers, hashes, lookups) worked locally but couldn't be ported to production without restructuring as separate Python+SQL stages. The first real workload (a CloudFront log analytics pipeline) hit this directly.
- **Operational surface.** Each engine had its own IAM, its own failure modes, its own deployment story, its own debugging. Maintaining four felt like maintaining four products.

The question we kept dodging: **could a single engine cover the local↔Lambda↔Fargate↔EMR-Serverless range without sacrificing too much at the small end?**

### Options reconsidered

**DuckDB everywhere.** Single-node only at the free tier. Caps the "scale up" promise — clavesa couldn't run pipelines bigger than one machine. Rejected.

**Polars everywhere.** Same shape as DuckDB — fast, single-node, distributed only via Polars Cloud (proprietary, paid). Rejected.

**Ibis** as a dialect-portability layer over multiple backends. Sounds appealing but the dialect drift just moves into the backends Ibis targets — a query that works on Ibis-DuckDB might fail on Ibis-Spark. Hides the problem instead of solving it. Rejected.

**Spark / PySpark everywhere.** Mature. Has SQL, Python UDFs, joins, windows, partitioning. Runs from a laptop (`local[*]`) to thousands of cores (EMR Serverless). One dialect — what runs locally runs in production. The cost is a JVM cold start (~5s) and image size (~1.3 GB).

The Lambda question. Spark wasn't built for Lambda; PySpark's launcher scripts use bash process substitution and `ps`, neither of which work in Lambda's restricted runtime. We discovered this on first deploy. Two paths from there:

1. **Drop Lambda; use Fargate as the small tier.** Spark runs cleanly in Fargate. Loses Lambda's per-ms billing and warm starts; gains simplicity.
2. **Adopt the [Spark on AWS Lambda (SoAL)](https://github.com/aws-samples/spark-on-aws-lambda) pattern.** AWS-published, AWS-maintained. Replaces upstream Spark's `spark-class` with a stripped-down `exec java` script that sidesteps the bash and `procps` issues. Bakes in `hadoop-aws` JARs for native S3, optional Iceberg/Delta/Hudi JARs.

We chose **(2)** — Lambda stays as the small tier, with the SoAL adaptation in our runner image. Specifics required to make Spark run in Lambda:

- Java 17 with the full `--add-opens` set Spark 4 expects. The stripped `spark-class` fork injects the 15 module-opens flags that upstream Spark 4 ships in `JavaModuleOptions`, covering the off-heap shuffle and DirectBuffer paths (e.g. `StorageUtils.bufferCleaner`).
- `spark.driver.bindAddress=127.0.0.1` and `spark.driver.host=127.0.0.1`. Lambda's hostname doesn't resolve to a bindable interface.
- `spark.ui.enabled=false`. The SparkUI HTTP server can't bind to `0.0.0.0` in Lambda either, and we don't need it for batch ETL.

## Decision

**Clavesa uses PySpark as its single execution engine across all compute targets**: `local`, `lambda`, `fargate`, `emr-serverless`. Same Python module, same SQL dialect, same UDF mechanism, same observability. The same `transform(spark, inputs) -> dict[str, DataFrame]` function and the same `spark.sql(...)` query that runs in `clavesa pipeline run` (local) runs unchanged in Lambda.

The runner image is built once (per workspace, into a per-workspace ECR) and used everywhere. Compute targets vary; the engine doesn't.

### Mapping to compute targets

| Target | Use when | Notes |
|--------|----------|-------|
| `local` | dev, CI, manual `clavesa pipeline run` | No AWS resources. Runs the same image via Docker. |
| `lambda` | small batch (≤15 min, ≤10 GB) | Default cloud tier. SoAL pattern; JVM cold start ~5 s. |
| `fargate` | medium batch (long-running, 30 GB+) | Planned. Per-second billing; scale-to-zero via task lifecycle. |
| `emr-serverless` | distributed Spark | Planned. ~5× cheaper than Glue at the same scale. |

`local` and `lambda` ship today. `fargate` and `emr-serverless` are accepted by validation but unimplemented — they're the natural next steps once the Lambda tier is shaken out.

### What is gone

- DuckDB. No second engine for previews. Local preview boots `local[*]` Spark in-process — slower (~2 s per cold preview) but truthful.
- Athena as a transform compute target. Dropped from the validation enum. (Athena may return as a *query* engine for browsing already-written tables; that's a different concern.)
- Glue Jobs. Replaced by EMR Serverless when distributed Spark is needed. Glue is ~5× more expensive for the same workload.
- Pure-pandas runner. The original Lambda-Python runner was pandas-only, with no SQL. Subsumed by PySpark.
- The `nodejs` transform language. Never implemented; quietly dropped.

## Consequences

**Positive:**

- **Identity of behaviour across environments.** A query that runs locally runs identically in Lambda. The "works in dev, fails in prod" class of bugs that motivated this rewrite is closed.
- **Native UDFs everywhere.** PySpark UDFs work in `local`, `lambda`, `fargate`, `emr-serverless`. Existing parsers (e.g. `analytics/src/parsers/`) port directly.
- **One operational surface.** One image, one IAM pattern (Lambda invokes runner; runner reads/writes S3), one set of integration tests, one observability setup.
- **Scale ceiling matches the laptop floor.** A pipeline that fits in Lambda today scales to EMR Serverless tomorrow with a `compute = "emr-serverless"` config change — no transform rewrite.
- **Iceberg / Delta / Hudi for free.** SoAL ships JARs as a build arg. Closes the Iceberg-output story (ADR 007) when we want it.

**Negative:**

- **JVM cold-start cost.** ~5 s per cold preview locally; ~5 s per cold Lambda invoke. For batch ETL (cron-driven, daily/hourly) this is irrelevant. For sub-second interactive use, PySpark is the wrong tool — but clavesa isn't targeting that use case.
- **Image size ~1.3 GB.** Includes a JRE plus PySpark plus our runner. Lambda accepts up to 10 GB; ECR storage is ~$0.10/GB-month. Acceptable.
- **SoAL coupling.** We replace upstream Spark's `spark-class` with SoAL's stripped version. If SoAL drifts from upstream Spark in a way that affects us, we maintain the patch. The launcher is short (~50 lines), the patch is well-understood.
- **One engine means one set of weak spots.** PySpark's small-data overhead (vs. DuckDB's microseconds) is real. We accept it.

**Tradeoffs accepted:**

- Cold-start latency in exchange for engine identity across environments.
- Image size in exchange for a single artifact pipeline.
- Patch maintenance against SoAL upstream in exchange for keeping Lambda as a viable tier.

## What this changes elsewhere

- **ADR 003 (transform runtime)** is updated. Compute targets are `local | lambda | fargate | emr-serverless`. The decoupling principle — logic separate from compute — still holds; the compute *list* simplified.
- **ADR 010 (DuckDB local preview)** is superseded. DuckDB is no longer in the codebase.
- **ADR 007 (Iceberg storage format)** is unchanged. Iceberg returns once we wire SoAL's Iceberg JARs into the runner.

## References

- [Spark on AWS Lambda — AWS Big Data blog](https://aws.amazon.com/blogs/big-data/spark-on-aws-lambda-an-apache-spark-runtime-for-aws-lambda/)
- [aws-samples/spark-on-aws-lambda](https://github.com/aws-samples/spark-on-aws-lambda) — Dockerfile and `spark-class` we adopt.
