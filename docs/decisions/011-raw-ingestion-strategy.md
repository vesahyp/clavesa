# ADR 011: Raw Ingestion Strategy and Transform Time Travel

**Status**: Superseded by ADR 012 (PySpark universal engine) and ADR 013 (Iceberg as table format).

The schema-free source-ingestion philosophy in §1 still holds in spirit. The mechanics in §3 (DELETE + INSERT / three-state Step Functions pattern) and §4 (Glue source job comparing schemas to populate `_rescued_data`) are obsolete: transforms now write via `df.writeTo(...).createOrReplace()` (or `.append()`) in PySpark, snapshot history is preserved by Iceberg natively, and Glue Jobs were removed. Sources are pass-through path declarations in the current model — schema lands wherever the first transform run shapes it.

## Context

The initial implementation of source nodes inferred schemas at ingest time (JSON fields expanded to columns, CSV with `inferSchema=true`, Parquet used as-is). Transform nodes used a DROP TABLE + CTAS pattern, which recreated the Iceberg table from scratch on every pipeline run. This had two problems:

1. **Schema coupling at the source**: inferring schema at ingest ties the pipeline to whatever shape the source data happens to have at the time of the first run. New fields, renamed fields, or format changes cause Glue job failures.
2. **No time travel**: DROP TABLE + CTAS destroys all Iceberg snapshot history on every run. The time travel capability that motivated choosing Iceberg (ADR 007) was never actually usable.

The goal is a model where sources are schema-agnostic at ingest, and transforms define schema by what they emit — regardless of transform language.

## Decision

### 1. Source ingestion is format-aware but schema-free

Each format has a natural representation that defers schema enforcement to downstream transforms:

| Format | Source writes | Rationale |
|--------|--------------|-----------|
| JSON | Single `_data string` column (raw JSON blob) | No schema at ingest; transforms extract fields via `json_extract` |
| CSV | Named string columns from headers, all `string` type, no type inference | Headers are structural metadata, values are not coerced |
| Parquet / ORC | Typed columns from file schema + `_rescued_data string` | Format already carries a schema commitment; new fields that appear in later files are serialized to `_rescued_data` as JSON |

This matches the Databricks philosophy of treating the source layer as a raw landing zone. The transform layer defines the schema by what it selects and casts.

### 2. Transforms define schema via their output — language-agnostic

Schema is not declared upfront. It emerges from what the transform emits:

- **SQL**: `SELECT col1, CAST(col2 AS bigint) AS col2 ...` defines the output schema
- **Python**: the DataFrame or dict structure returned defines the output schema
- **Node.js**: the objects yielded define the output schema
- **Future languages**: same principle

No schema registry, no schema declaration in Terraform. The Iceberg table schema is whatever the first successful transform run produces.

### 3. Transform outputs preserve Iceberg snapshot history (time travel)

Instead of DROP TABLE + CTAS on every run, transforms use a create-or-overwrite pattern:

- **First run** (table does not exist): CTAS — creates the Iceberg table and writes initial data
- **Subsequent runs** (table exists): `DELETE FROM` to remove current rows + `INSERT INTO ... SELECT` to write new data

Each run adds Iceberg snapshots. Time travel queries (`SELECT ... FOR TIMESTAMP AS OF '...'`) return the state of the table at any previous run. The table itself persists across pipeline runs; only its contents change.

The Step Functions state machine implements this via a three-state pattern per transform node:

```
<id>_clear → (success) → <id>_insert → next
           ↓ (error: table not found)
           <id>_create → next
```

`<id>_clear` catches task failure (which covers "table not found") and routes to `<id>_create` (CTAS). On all subsequent runs `<id>_clear` succeeds and `<id>_insert` runs.

### 4. `_rescued_data` for Parquet/ORC only

Parquet and ORC files carry a typed schema that is used as-is at ingest. When source files evolve (new columns appear), the Glue source job detects columns absent from the existing Iceberg table schema and serializes them to a `_rescued_data string` column (JSON). This column is present from the first run (null for rows where no rescue occurred).

`_rescued_data` is not implemented for JSON or CSV sources because those formats are already written without a fixed schema — new JSON keys appear in the `_data` blob naturally, and new CSV columns appear as new string columns.

## Consequences

**Positive:**
- **Time travel is now real**: every pipeline run is an Iceberg snapshot, queryable by timestamp
- **Source schema changes don't break pipelines**: JSON sources absorb new fields in the blob; CSV absorbs new columns as strings; Parquet/ORC routes new fields to `_rescued_data`
- **Language-agnostic schema definition**: Python, SQL, Node.js transforms all follow the same contract — emit what you want the schema to be
- **Simpler mental model**: sources are raw landing zones, transforms are where data takes shape

**Negative:**
- **JSON transforms require `json_extract`**: querying a `_data string` column is more verbose than querying named columns (`json_extract_scalar(_data, '$.price')` vs `price`)
- **`_rescued_data` requires Glue job schema comparison**: the source Glue job for Parquet/ORC must compare file schema against live catalog schema at runtime, which adds complexity and a Glue catalog read on every run
- **First-run detection adds Step Functions states**: three states per transform instead of two; the state machine is larger for pipelines with many transforms

**Tradeoffs accepted:**
- Verbose JSON SQL in exchange for zero schema coupling at the source
- Larger state machine in exchange for genuine Iceberg time travel
- `_rescued_data` complexity for Parquet/ORC in exchange for safe schema evolution on typed formats
