# ADR 018: Delta Lake as the table format (supersedes ADR 013)

**Status**: Accepted (2026-05-26); factual error in the v3-status claim corrected same day — see "Update 2026-05-26" below. Supersedes ADR-013. Cutover ships with clavesa v2.0.0.

## Context

ADR-013 picked Apache Iceberg as clavesa's default table format on three claims:

1. **Athena native read + write.** Real, still true.
2. **AWS strategic alignment** (S3 Tables, Glue, EMR converged on Iceberg). Real, still true.
3. **Workload fit** — "append-mostly with light updates" was the explicit framing. **This was wrong.**

The cloudfront-analytics pipeline — clavesa's first real production user — turned out to be a high-frequency medallion: a `mode = "merge"` silver layer (`enriched`, keyed on `x_edge_request_id`) feeding downstream fact tables that read the silver incrementally and MERGE into their own outputs (`fact_page_views`, `fact_events`, etc.). That's the canonical CDC pattern, not append-mostly.

Two days of investigation in late May 2026 confirmed the gap is structural, not a clavesa-side bug:

- **Iceberg's `start-snapshot-id`/`end-snapshot-id` incremental scan does not support overwrite or MERGE snapshots.** Documented limitation; [apache/iceberg#1949](https://github.com/apache/iceberg/issues/1949) closed `not planned`. For a MOR-MERGE upstream the snapshot diff between writes returns nothing useful to downstream consumers.
- **Iceberg's `create_changelog_view` procedure only works on copy-on-write tables.** It fails on MOR — "the scan doesn't return delete files making it unsuitable for MOR tables" ([Jack Vanlightly's v2 analysis](https://jack-vanlightly.com/analyses/2024/9/23/change-query-support-in-apache-iceberg-v2)). The procedure's `net_changes` + `identifier_columns` combination — the one shape that would have given clavesa what it needed — [is not implemented today](https://github.com/apache/iceberg/issues/14249).
- **Iceberg v3 row lineage was believed to close this gap on a multi-quarter horizon** — same-day re-check (see "Update 2026-05-26" below) found this was wrong on the spec/engine side: v3 shipped in Iceberg 1.11.0 on 2026-05-19 with AWS support across EMR/Glue/S3 Tables. The blocker is on a different axis: Athena cannot read v3 tables and AWS has not announced a timeline. The Delta cutover conclusion survives the correction, but the reasoning is replaced — see the Update section.

We spent a slice trying to dedupe the snapshot diff on the consumer side (`recency_column` field on `output_definitions`, framework `row_number()` over `merge_keys`). The Go-side wiring proved correct but had nothing to dedupe — Iceberg's incremental scan was returning empty for MOR-MERGE upstream snapshots, exactly per the documented limitation. The fix wasn't on clavesa's side; the read API itself doesn't surface what we needed.

Three real options surfaced:

1. **Force `write.merge.mode = copy-on-write` on every merge output**, so the CDC API actually has data. Pessimizes the high-frequency MOR-MERGE workload class (rewrites data files on every update; defeats the purpose of MOR). Wrong default for the workload clavesa is selling.
2. **Document the limitation and tell users to use `row_number()` wrappers manually**, which is the cloudfront-analytics workaround today. Honest but contradicts clavesa's product principle ("absorb the lakehouse complexity into opinionated defaults"). Every new user hits the same trap.
3. **Switch the table format.** The CDC patterns clavesa needs are well-trodden in Delta Lake (CDF, native, OSS) and Apache Hudi (native MOR + CDC). Iceberg is the wrong tool for this specific workload shape today.

We chose (3), with Delta Lake — see "Decision" below.

## Decision

**Apache Delta Lake (Linux Foundation, OSS) replaces Apache Iceberg as clavesa's default table format. Hard cutover at v2.0.0.**

Specifically:

- **No dual-format runtime.** v2.0.0 ships Delta only; v1.x stays the Iceberg release line. The runner does not branch on per-output format. ADR-012's "one engine, identical semantics" principle extends here: one format too.
- **Delta Change Data Feed (CDF) is the canonical incremental-read mechanism.** `delta.enableChangeDataFeed = true` on producers; `spark.read.format("delta").option("readChangeFeed", "true")` on consumers. Returns rows tagged with `_change_type` (insert / update_preimage / update_postimage / delete), `_commit_version`, `_commit_timestamp` — exactly the shape downstream MERGE consumers need, regardless of MOR/COW choice.
- **`_commit_version` replaces clavesa's would-be `recency_column` field.** Delta's commit ordering is the natural tie-breaker for "latest row per key" semantics on the snapshot diff. The `recency_column` field designed in the prior slice is dropped.
- **Glue Data Catalog stays the cloud metastore.** Delta registers tables in Glue via Spark's Hive metastore federation (`aws-glue-datacatalog-spark-client`). Athena reads Delta natively; no DDL or crawler needed.
- **Single-writer log store for S3.** clavesa is single-writer per table by design (one Step Functions execution per pipeline run). Use `org.apache.spark.sql.delta.storage.S3SingleDriverLogStore`; no DynamoDB lock table needed.
- **`identutil.EncodeGlueDatabase` flat-namespace encoding stays.** Glue Hive metastore is still 2-level; the `<catalog>__<schema>` workaround for ADR-016's 3-level namespace is format-agnostic.

### What clavesa loses

- **Athena INSERT / MERGE on clavesa-managed tables.** Athena's Delta support is read-only; analyst workflows that previously did `MERGE INTO clavesa.<db>.<table>` from the Athena console are no longer supported. Users who want to author transforms still author them through clavesa; users who want ad-hoc Athena writes can do it against their own non-clavesa tables.
- **AWS S3 Tables alignment.** The managed-Iceberg-on-S3 offering announced at re:Invent 2024 stays unused. If a future workload genuinely benefits from S3 Tables, it's a different product.
- **Iceberg's broader ecosystem.** Snowflake, Trino, BigQuery, etc. all read Iceberg natively; Delta read support is more uneven (Trino reads Delta; Snowflake reads Delta via UniForm). Athena + EMR + the runner's own Spark cover clavesa's read paths.

### What clavesa gains

- **A working medallion with MERGE upstreams.** The cloudfront-analytics pattern actually delivers without manual `row_number()` wrappers — Delta CDF returns post-image rows with explicit commit ordering, framework dedupes naturally.
- **Simpler operational story.** No COW/MOR choice exposed; no Iceberg-specific properties to set at table creation; no compaction-needed-for-CDC dependency.
- **Mature CDF.** Delta CDF has been GA since 2022; Databricks runs it at scale; the OSS Spark path is well-trodden.

### What stays the same

- One Spark engine (ADR-012).
- Local–cloud parity (ADR-014). Local runs use the same Delta classpath as cloud; Hadoop catalog locally, Glue + Hive metastore federation in cloud. Same DataFrame APIs at the runner level.
- CLI/UI parity (ADR-015).
- Three-level namespace (ADR-016). Catalog/schema/table identifiers carry through unchanged.
- Workspace source registry (ADR-017).

## Migration

**No automatic Iceberg→Delta data migration tool ships in v2.0.0.** Existing Iceberg workspaces (`ui-test`, `cloudfront-analytics`) stay on the v1.x line until they're ready to recreate under v2.0.0. The migration path is:

1. Stand up a fresh v2.0.0 workspace via `clavesa workspace init`.
2. Re-author the pipelines (same `.tf`, no syntax change at the user level).
3. Re-run from sources. Delta tables materialise on first write; system tables (`runs`, `node_runs`, `tables`, `column_stats`, `dashboards`) recreate too.

The cloudfront-analytics source is CloudFront access logs in S3 — re-readable from the start of the retention window, so a recreate is fast.

A future v2.x might ship `clavesa workspace migrate-format` if demand surfaces; explicitly deferred.

## Consequences

**Positive:**
- The headline CDC pattern works without user workarounds.
- Delta's `OPTIMIZE` + `VACUUM` are well-trodden; compaction-as-pipeline-node (TODO bucket 11) becomes a smaller slice than it would have been on Iceberg.
- Removing the Iceberg JAR set + `iceberg-aws-bundle` drops ~250 MB from the runner image; Delta + delta-storage adds ~30 MB. Net image size goes down.
- Runs-writer Lambda gets simpler: switches from Athena INSERT (which doesn't work for Delta) to direct Delta writes via [delta-rs](https://github.com/delta-io/delta-rs) (Rust Delta with Python bindings, JVMless). Resolves the CLAUDE.md gotcha "runs_writer is the hard part: Athena INSERT can't set `snapshot-property.*`."

**Negative:**
- Real loss of capability for analysts who used Athena to MERGE into clavesa outputs.
- Delta on Athena is read-only; some Iceberg-specific Athena features (e.g. `OPTIMIZE` via Athena) no longer apply.
- Engine breadth regression vs. Iceberg — Snowflake-native-read, BigQuery-native-read story is weaker (UniForm bridge exists but adds operational complexity).
- v2.0.0 is a breaking change. Existing workspaces don't auto-upgrade.

**Tradeoffs accepted:**
- ADR-013's "swimming with the current of AWS strategic alignment" framing was correct in 2024; we're explicitly choosing to swim against it on the table-format axis, in exchange for the CDC pattern actually working.
- Future Athena v3 read support + an OSS Iceberg CDF-equivalent read API may make the original decision tenable again. Re-evaluating when those two gates clear is honest; the workload doesn't have an open-ended runway to wait. (The original draft said "Iceberg v3 row lineage in 12+ months"; corrected — v3 shipped on the producer side, the gate is now on Athena's read path. See Update section.)

## What this changes elsewhere

- **ADR-007 (storage format) + ADR-013 (table format)**: both superseded by this ADR on the format choice. The Glue Data Catalog as metastore decision (ADR-013) is preserved.
- **Runner**: complete swap of the write path (`df.write.format("delta")...` instead of `df.writeTo(...).createOrReplace()`), the metadata-table reads (`DESCRIBE HISTORY` instead of `<table>.history` / `<table>.snapshots`), and the incremental-read path (CDF instead of `start-snapshot-id`/`end-snapshot-id`).
- **Transform module IAM**: Glue actions stay (`glue:GetDatabase`, `glue:CreateTable`, etc.); S3 paths under the warehouse remain the same shape. `iceberg-aws-bundle` permission grants are dropped.
- **runs_writer sidecar Lambda**: switches from Athena INSERT to delta-rs direct writes.
- **UI catalog page + table-detail snapshot timeline**: format-agnostic abstraction. The `ICEBERG` chip becomes `DELTA`. Volume timeline reads Delta history instead of Iceberg snapshots.
- **CHANGELOG**: v2.0.0 section under Keep-a-Changelog; documents the cutover and the recreate-from-source migration path.

## Update 2026-05-26 (same day): Iceberg v3 facts corrected

Within hours of accepting this ADR, a re-check of the v3 status claim on line 19 turned up that the "12+ months out, no production engine consumes it yet" framing was wrong. The cutover decision survives intact, but on a different axis. Recording the corrected facts here so future readers do not act on the original premise.

**What's actually shipped (as of 2026-05-26):**

- Apache Iceberg **1.11.0 released 2026-05-19** (one week before this ADR), bringing v3 to "full production stability."
- **AWS announced v3 support in November 2025** for EMR 7.12, AWS Glue, Amazon SageMaker notebooks, Amazon S3 Tables, and the Glue Data Catalog. Row lineage (`_row_id`, `_last_updated_sequence_number`) and deletion vectors are queryable from Spark today on those runtimes.
- Snowflake shipped v3 read in preview on 2026-03-04. Databricks shipped v3 in public preview.

**What's NOT shipped — and is the actual blocker for clavesa:**

- **Athena cannot read Iceberg v3 tables.** Athena's SQL engine is pinned at Iceberg 1.4.2 / format v2. AWS has not announced a timeline for Athena v3 read support.
- The re:Invent 2025 "EMR runtime is shared with Athena" framing refers to Athena's **Spark notebook** product, not the SQL engine that analysts hit via the Athena console / JDBC / `aws athena start-query-execution`. Those are different products; the SQL engine stays v2-only.
- Athena read on clavesa-managed tables is half of why ADR-013 picked Iceberg in the first place. Switching producers to v3 to unlock CDC primitives would forfeit that read path with no committed recovery date.

**Row lineage is a primitive, not a CDF API:**

- Even with v3 fully adopted everywhere, downstream consumers compute change feeds by joining `_first_row_id > last_known_max` filters across snapshots. The `readChangeFeed`-equivalent operator on top of row lineage is still emergent in OSS Iceberg.
- That is not the "absorb the lakehouse complexity into opinionated defaults" product story; users would still be hand-rolling CDF semantics in their transforms.
- Delta CDF is a single OSS API call (`option("readChangeFeed", "true")`), GA since 2022, and Athena reads Delta natively — so the analyst path stays intact.

**Conclusion:**

The Delta cutover stands. The original ADR justified it as "v3 is too far away to wait for"; the corrected justification is **"v3 is here on the producer side but Athena cannot read it and AWS has not committed a timeline, so clavesa-on-v3 would forfeit Athena reads — the very property ADR-013 picked Iceberg for."** This is a stronger argument because it is verifiable today rather than a soft bet on future engine adoption.

**What would re-open this decision:**

1. AWS announcing Athena v3 read support with a near-term GA, **and**
2. An OSS Iceberg release that ships a CDF-equivalent read operator on top of row lineage (single-call API, returns rows tagged with change-type + commit version, parity with Delta CDF's shape).

Both gates need to clear. If they do, this ADR should be re-evaluated against the cost of carrying Delta in clavesa long-term.

**References for the corrected facts:**

- [AWS announces Iceberg V3 deletion vectors and row lineage (Nov 2025)](https://aws.amazon.com/about-aws/whats-new/2025/11/aws-apache-iceberg-v3-deletion-vectors-row-lineage/)
- [Accelerate data lake operations with Iceberg V3 (AWS Big Data Blog)](https://aws.amazon.com/blogs/big-data/accelerate-data-lake-operations-with-apache-iceberg-v3-deletion-vectors-and-row-lineage/)
- [Apache Iceberg V3: Is It Ready? — "Athena does not support the V3 spec yet"](https://www.ryft.io/blog/apache-iceberg-v3-is-it-ready)
- [Working with Iceberg tables by using Athena SQL (AWS Prescriptive Guidance) — Athena pinned at Iceberg 1.4.2 / format v2](https://docs.aws.amazon.com/prescriptive-guidance/latest/apache-iceberg-on-aws/iceberg-athena.html)
- [Apache Iceberg Releases — 1.11.0 on 2026-05-19](https://iceberg.apache.org/releases/)
- [Snowflake Iceberg v3 preview (2026-03-04)](https://docs.snowflake.com/en/release-notes/2026/other/2026-03-04-iceberg-v3-support-preview)

## References

- [Delta Lake CDF documentation (OSS, Linux Foundation)](https://docs.delta.io/delta-change-data-feed/)
- [Delta CDF deep-dive blog (storage model + materialization)](https://delta.io/blog/2023-07-14-delta-lake-change-data-feed-cdf/)
- [AWS — Delta Lake UPSERTs with open-source Delta + AWS Glue](https://aws.amazon.com/blogs/big-data/handle-upsert-data-operations-using-open-source-delta-lake-and-aws-glue/)
- [Athena native read support for Delta Lake](https://aws.amazon.com/about-aws/whats-new/2022/12/athena-enhances-read-support-delta-lake-table-format/)
- [Jack Vanlightly — Change query support in Iceberg v2 (MOR limitation)](https://jack-vanlightly.com/analyses/2024/9/23/change-query-support-in-apache-iceberg-v2)
- [apache/iceberg#1949 — IncrementalDataScan on overwrite snapshots (closed `not planned`)](https://github.com/apache/iceberg/issues/1949)
- [apache/iceberg#14249 — `net_changes` + `identifier_columns` optimization](https://github.com/apache/iceberg/issues/14249)
- [delta-io/delta-rs — Rust Delta library with Python bindings](https://github.com/delta-io/delta-rs)
