# ADR 013: Table Format — Iceberg, Delta, or Hudi

**Status**: Accepted. The catalog/database naming illustrated below (`clavesa` / `clavesa_<pipeline>`) was generalized in ADR 016 — catalog identifier is now a workspace-level setting (default `clavesa_<sanitize(workspace_name)>`) and schema identifier is per-pipeline. The format decision and write semantics in this ADR are unchanged; only the identifiers in the examples have moved.

## Context

ADR 007 committed clavesa to "managed table format on S3" without picking a specific one — Iceberg was the placeholder, but the choice was deferred. With the runner now running Spark on Lambda (ADR 012, SoAL pattern) and source/destination modules reduced to pure path declarations (v0.10.0), we need to pick the actual format before transforms start writing tables instead of plain Parquet.

Three credible open-source options in 2026:

- **Apache Iceberg** — Netflix-origin (2018), Apache-governed, broad vendor adoption (Snowflake, Dremio, AWS, Databricks-via-Tabular-acquisition).
- **Delta Lake** — Databricks-origin (2019), Linux Foundation-hosted spec, Databricks-dominated codebase.
- **Apache Hudi** — Uber-origin (2017), Apache-governed, Onehouse-backed commercially.

All three give us ACID writes, time travel, schema evolution, and Spark-native readers/writers. They diverge on engine breadth, AWS-native query support, and operational ergonomics.

## Constraints from earlier decisions

- **Spark is the universal engine** (ADR 012). All three formats have first-class Spark integration; the choice is not constrained here.
- **Glue Data Catalog as metastore is acceptable; Glue Jobs are not.** The Data Catalog is free metadata storage and is the AWS-native metastore for Athena. Glue Jobs (compute) are gone.
- **Lambda runner via SoAL** (ADR 012). The format we pick must work in a memory-constrained, time-limited Lambda environment. All three do, in principle, but Iceberg's footprint is smallest.
- **Clavesa targets cloud-agnostic pipelines** — the format should be readable by query engines on AWS, GCP, and Azure without a Databricks-only escape hatch.

## Options considered

### Apache Iceberg

**Pros:**
- **Athena native read + write** since 2022. Plain `SELECT *`, plain `INSERT`, plain `MERGE`. No "read-only" caveat. This is the largest single differentiator on AWS.
- **AWS S3 Tables** (launched late 2024) — managed Iceberg tables as a first-class S3 primitive. AWS is clearly betting on Iceberg as the canonical AWS table format. Iceberg-formatted tables get cross-service support automatically (Athena, EMR, Glue Data Catalog, Redshift Spectrum, Spark on Lambda, etc.).
- **Broadest engine support.** Spark, Trino, Flink, Athena, Snowflake, BigQuery (via BigLake), DuckDB (experimental), ClickHouse. Clavesa isn't locked in to any one engine.
- **Hidden partitioning + partition evolution.** Users write `WHERE event_time > X`; Iceberg figures out which files to scan based on the partition spec — no `partition_date = ...` shenanigans. Critically, you can change the partition spec later without rewriting data.
- **Branches and tags** (since 1.4). Useful for staging-vs-production data flows, debugging, "what did the table look like at 09:00?".
- **`iceberg-aws-bundle` JAR** ships the GlueCatalog impl + S3FileIO. Drop one JAR into the Spark image, point at Glue, write tables. SoAL already supports this via the `FRAMEWORK=ICEBERG` build arg — zero new infra.

**Cons:**
- **Append + light-update workloads are its sweet spot; very-update-heavy CDC is less ergonomic.** Our cloudfront-analytics workload is append-mostly with a few `MERGE` operations for `user_identity_map`-style dims, which fits Iceberg fine. A pipeline doing 60%+ updates would be friction.
- **Maintenance ops (compaction, expire snapshots, rewrite manifests) need scheduling.** Athena does some automatically for tables it owns; for Spark-written tables we'd run Iceberg's `CALL system.rewrite_data_files(...)` periodically. Not free, not hard.
- **Manifest reads are an extra round-trip vs. Delta's transaction-log-only model.** For very small tables, Delta planning is microseconds faster. Doesn't matter for our scale.

### Delta Lake

**Pros:**
- **Strongest streaming story** — Spark Structured Streaming has Delta as its first-class sink/source. If clavesa ever does streaming pipelines, this is friction-free.
- **`OPTIMIZE` and `Z-ORDER`** commands are mature; small-files compaction is straightforward.
- **Strong on Databricks** — if a future clavesa user is on Databricks, Delta is the path of least resistance. (We probably won't be one of those users; clavesa is anti-Databricks-lock-in by design.)
- **Wider mind-share** in some segments because of Databricks marketing reach.

**Cons:**
- **Athena: read-only.** AWS announced Delta Lake read support in 2023; write support has been "coming" for years but isn't there. For an AWS-first tool, this is a real gap — every clavesa user wants to `SELECT *` in Athena, but a chunk would also want their analysts to use Athena to do further work, which would mean writing summary tables. Iceberg supports that natively; Delta doesn't.
- **Glue Data Catalog integration is weaker** than Iceberg's. Delta uses `_delta_log/` as the source of truth in S3; you can sync to Glue catalog via crawler or manifest generation, but it's not native.
- **Codebase is Databricks-driven.** OSS Delta works fine, but the cadence of features is set by Databricks priorities. Iceberg has a more diverse maintainer base.
- **Partition evolution is limited.** Changing partition columns means rewriting all data. Iceberg doesn't.
- **Lock-in pressure** — Databricks markets Delta heavily; tool builders who pick Delta tend to assume Databricks is in the loop. Clavesa's positioning is the opposite.

### Apache Hudi

**Pros:**
- **MoR (Merge-on-Read) tables.** Hudi is uniquely good for high-update / CDC workloads — log files store recent updates, base files store the bulk, queries merge on read. Lower write amplification than CoW formats.
- **Designed for incremental ingestion** from the ground up. If our pipelines were CDC-driven from a database, Hudi would be the natural fit.
- **Active Apache project** with steady commits.

**Cons:**
- **Athena: read-only**, and historically the worst-supported of the three on AWS. Schema-evolution edge cases have been a recurring source of issues.
- **Smaller ecosystem.** Trino and Flink integrations exist but lag Iceberg's. Cross-vendor adoption is lighter.
- **Higher operational complexity.** MoR tables need explicit compaction and cleaner runs. CoW Hudi is simpler but loses Hudi's main edge.
- **Wrong workload fit.** CloudFront analytics is append-mostly; we'd be paying Hudi's update-heavy complexity tax for a workload that doesn't need it.

## Comparison summary

| Dimension | Iceberg | Delta | Hudi |
|---|---|---|---|
| Athena read | Native | Read-only | Read-only |
| Athena write | Native | None | None |
| AWS strategic alignment (S3 Tables, Glue, EMR) | Iceberg | Some | Some |
| Glue Data Catalog as metastore | Native (`iceberg-aws-bundle`) | Workable | Workable |
| Partition evolution | Yes | No | Limited |
| Streaming | Good | Excellent | Excellent |
| Update-heavy workloads | Good | Good | Best (MoR) |
| Append-heavy workloads | Excellent | Excellent | Good |
| Ecosystem breadth | Broadest | Wide but Databricks-lean | Smaller |
| Cloud-agnostic posture | Yes | Yes-but-Databricks | Yes |
| Maintenance burden | Compaction / snapshot expiry | OPTIMIZE / VACUUM | Compaction / cleaner (MoR) |
| Spark-on-Lambda (SoAL) integration | Built-in (`FRAMEWORK=ICEBERG`) | Built-in (`FRAMEWORK=DELTA`) | Built-in (`FRAMEWORK=HUDI`) |

## Decision

**Apache Iceberg, with Glue Data Catalog as the metastore.**

This is decisive on three fronts:
1. **Athena native read + write** — every clavesa user gets `SELECT *` and `MERGE` from Athena out of the box. Delta and Hudi don't offer this on AWS.
2. **AWS strategic alignment** — S3 Tables, Glue Data Catalog, Athena, Redshift Spectrum, EMR all converged on Iceberg by 2025. Picking Iceberg means swimming with the current.
3. **Workload fit** — clavesa's first real workload (CloudFront analytics) is append-mostly with light updates. Iceberg is sized perfectly for that without paying Hudi's MoR complexity tax or Delta's Databricks lock-in.

We use:
- Spark catalog impl: `org.apache.iceberg.spark.SparkCatalog`
- Catalog impl: `org.apache.iceberg.aws.glue.GlueCatalog` (Glue **Data Catalog** for metadata; no Glue Jobs anywhere)
- File IO: `org.apache.iceberg.aws.s3.S3FileIO`
- Warehouse: `s3://<workspace-bucket>/<pipeline>/`
- JARs: pulled into the runner image via the SoAL `download_jars.sh` `FRAMEWORK=ICEBERG` path.

### Auto-table per transform output

Every transform's output gets registered as a table at write time. The runner maps:

| Transform | Database | Table |
|---|---|---|
| `dim_status` (output `default`) | `clavesa_<pipeline>` | `dim_status__default` |
| `validate_orders` (output `default`) | `clavesa_<pipeline>` | `validate_orders__default` |

Writes happen via Spark's Iceberg DataFrameWriter:

```python
df.writeTo(f"clavesa.{database}.{table}").createOrReplace()
```

Spark+Iceberg+GlueCatalog handles file layout, manifest building, and catalog registration. The runner doesn't make boto3 Glue calls — it doesn't need to.

## Consequences

**Positive:**

- **`SELECT *` in Athena from day one.** Users can browse and query every transform's output without writing DDL or running crawlers.
- **MERGE works in both Spark and Athena.** Slowly-changing dimensions (e.g., `user_identity_map`) port directly from the manual pipeline's MERGE-into-Iceberg pattern.
- **Schema evolution is free.** Add a column, rename a column, drop a column — Iceberg tracks it; existing readers don't break.
- **Partition evolution is free.** Repartitioning later doesn't require rewriting historical data.
- **Time travel for free.** `SELECT * FROM table FOR TIMESTAMP AS OF '...'` works in Athena and Spark.
- **AWS S3 Tables compatibility** — if AWS's managed Iceberg offering ever makes sense for clavesa users, the format is identical; only the storage location moves.

**Negative:**

- **Compaction and snapshot expiry need scheduling.** Iceberg writes a new snapshot per write; without periodic `expire_snapshots`, manifests accumulate and reads slow down. We need either: (a) a per-pipeline maintenance Lambda that runs weekly, or (b) lean on Athena's automatic optimization for tables it owns. Open question.
- **JAR weight in the runner image.** The Iceberg runtime + AWS bundle adds ~250 MB to the image (~1.55 GB total from the current ~1.3 GB). Lambda's 10 GB image limit is fine; ECR storage is ~$0.10/GB-month. Acceptable.
- **Iceberg-specific SQL.** Spark+Iceberg uses some Iceberg-specific extensions (`CALL system.rewrite_data_files`, branches, etc.). They're vendor-neutral but Iceberg-flavored. Users learning Iceberg pick this up; no real cost.

**Tradeoffs accepted:**

- We're committing to Iceberg-the-spec, not Iceberg-the-engine. Future Iceberg version migrations (V2 → V3 → ...) are an obligation. The Iceberg community has handled past migrations gracefully; we accept the risk.
- We tie clavesa's queryability story to Athena being good. If someone wants to use clavesa entirely via Trino on EMR, they can — Iceberg works there too — but Athena is the assumed default.

## What this changes elsewhere

- **ADR 007 (Iceberg as storage format)** — was ambivalent about which format; this makes the choice explicit and adds the Glue Data Catalog metastore decision.
- **Runner**: `_spark()` gains Iceberg + Glue catalog config; `handler()`'s output writer changes from `df.write.parquet(...)` + boto3 upload to `df.writeTo(...).createOrReplace()`. The boto3 `_stage_input` / `_publish_output` paths can be removed (Iceberg+SoAL JARs include `hadoop-aws` for native S3).
- **Transform module IAM**: add `glue:GetDatabase`, `glue:GetTable`, `glue:CreateTable`, `glue:UpdateTable`, `glue:GetPartitions` (read-only, plus the table writes Iceberg needs). No Glue *Jobs* permissions.
- **Pipeline TF**: a single `aws_glue_catalog_database` per pipeline, named `clavesa_<pipeline>`. Free.
- **Open maintenance question**: who runs `expire_snapshots` and on what cadence.

## References

- [Apache Iceberg](https://iceberg.apache.org/)
- [Iceberg + AWS integration](https://iceberg.apache.org/docs/latest/aws/)
- [SoAL Iceberg path](https://github.com/aws-samples/spark-on-aws-lambda) — pre-built JAR set, `FRAMEWORK=ICEBERG` build arg
- [Athena Iceberg support](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html) — the read+write story this ADR depends on
- AWS S3 Tables announcement (re:Invent 2024) — AWS picking Iceberg as the canonical S3 table format
