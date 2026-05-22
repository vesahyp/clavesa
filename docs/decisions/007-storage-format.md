# ADR 007: Apache Iceberg as Storage Format

**Status**: Accepted (extended by ADR 013, which makes the table format an explicit Iceberg-everywhere commitment and aligns the engine with ADR 012's PySpark-only decision).

## Context

Every node in a Clavesa pipeline materializes its output as a table on object storage (S3, GCS, ADLS). These intermediate tables are not ephemeral — they are queryable, browsable from the UI, and persist between pipeline runs. The storage format choice affects:

- **Data preview**: the UI needs to query output tables efficiently
- **Write safety**: pipeline steps must not produce partial/corrupt outputs on failure
- **Run history**: users want to inspect outputs from previous pipeline runs
- **Schema handling**: Clavesa supports optional schemas with a `_rescued_data` pattern for non-conforming fields
- **Cross-cloud**: the same format must work on AWS, GCP, and Azure
- **Right-sized compute**: the format must be readable/writable from Athena (SQL), Lambda (Python/Node.js), and Spark — not just one engine

Options considered:

1. **Plain Parquet** — simplest, but no transactions, no time travel, no schema evolution, non-atomic writes
2. **Apache Iceberg** — open table format with ACID writes, time travel, schema evolution, wide cloud/engine support
3. **Delta Lake** — similar capabilities to Iceberg but stronger Databricks ecosystem tilt, weaker AWS-native support
4. **Apache Hudi** — designed for upsert-heavy/CDC workloads, most complex, overkill for batch ETL

## Decision

**Apache Iceberg** as the table format for all pipeline output tables.

### Engine support

| Runtime | Read | Write |
|---|---|---|
| SQL (Athena) | Native (Athena v3) | Native |
| PySpark (Lambda / Fargate / EMR Serverless) | Native via `iceberg-spark-runtime` | Native via `df.writeTo(...)` |

(Earlier drafts of this ADR also listed Python/PyIceberg and Node.js engine rows. Both are gone — ADR 012 made PySpark the single transform engine across all compute targets.)

### Cloud support

| Cloud | Query engines | Catalog |
|---|---|---|
| AWS | Athena, Glue, EMR, Redshift Spectrum | Glue Data Catalog |
| GCP | BigQuery (BigLake), Dataproc | BigQuery / HMS |
| Azure | Synapse, Fabric, HDInsight | Unity Catalog / HMS |

## Consequences

**Positive:**
- **Time travel** — users can preview outputs from any previous pipeline run, not just the latest
- **Atomic writes** — no partial reads during pipeline execution; a step either commits fully or not at all
- **Schema evolution** — columns can be added/renamed/reordered without rewriting data, supporting the `_rescued_data` pattern naturally
- **Cross-cloud** — same table format on S3, GCS, and ADLS; cloud-native query engines on all three clouds support Iceberg
- **Right-sized compute** — works with Athena (SQL), PyIceberg (Lambda), and Spark (heavy transforms) without format conversion
- **Open standard** — governed by Apache, no vendor lock-in

**Negative:**
- **Catalog dependency** — requires a metadata catalog (Glue Data Catalog on AWS); more infrastructure than plain Parquet
- **Complexity** — more moving parts than writing raw Parquet files; compaction, snapshot expiry, and catalog management need to be handled

**Tradeoffs accepted:**
- Extra catalog infrastructure in exchange for ACID writes and time travel
