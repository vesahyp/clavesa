# `clavesa/transform/aws`

Terraform module for a transform node.

Transforms run **PySpark on Lambda** (the Clavesa runner container) by
default. Inputs are read with `spark.read` from upstream sources or
Iceberg tables; outputs land as **Iceberg tables in the Glue Data Catalog**
under `<catalog>.<schema>.<node>__<output>`, queryable from Athena with
no DDL. The transform body is either SparkSQL (`language = "sql"`) or
PySpark (`language = "python"`) — the same code runs unchanged on local,
Lambda, and (planned) Fargate / EMR Serverless.

See ADR-012 (PySpark everywhere) and ADR-013 (Iceberg as the table format)
for the architectural reasoning.

## Usage

### Single-input SQL

```hcl
module "validate" {
  source = "../.clavesa/modules/v0.30.0/transform/aws" # written by clavesa CLI; embedded in the binary since v0.30.0

  pipeline_name = var.pipeline_name
  name          = "validate"
  bucket        = var.pipeline_bucket
  catalog       = var.catalog        # workspace catalog (ADR-016)
  schema        = var.schema         # pipeline schema  (ADR-016)

  inputs = {
    raw = module.raw_events.outputs["default"]
  }

  language = "sql"
  sql      = "SELECT * FROM raw WHERE amount > 0"

  output_definitions = {
    "default" = {
      schema = {
        columns = [
          { name = "id",     type = "string" },
          { name = "amount", type = "decimal(10,2)" },
          { name = "ts",     type = "timestamp" }
        ]
      }
    }
  }
}

# Reference downstream:
# module.warehouse.input = module.validate.outputs["default"]
```

### Multi-input join

```hcl
module "enrich" {
  source = "../../"
  # ... pipeline_name, name, bucket, catalog, schema ...

  inputs = {
    raw    = module.raw_events.outputs["default"]
    lookup = module.reference_data.outputs["default"]
  }

  language = "sql"
  sql      = "SELECT r.*, l.category FROM raw r JOIN lookup l ON r.type = l.type_code"

  output_definitions = { "default" = {} }
}
```

### PySpark transform

```hcl
module "score" {
  source = "../../"
  # ... pipeline_name, name, bucket, catalog, schema ...

  inputs = { raw = module.raw_events.outputs["default"] }

  language = "python"
  python = file("transforms/score.py")  # defines transform(spark, inputs) -> dict[str, DataFrame]

  output_definitions = { "default" = {} }
}
```

### Append-mode output (incremental facts)

Pair an `append`-mode output with a partitioned source so the runner only
reads new partitions per run:

```hcl
output_definitions = {
  "events" = {
    mode = "append"
  }
}
```

## Variables

### Required

| Name                 | Type            | Description |
|----------------------|-----------------|-------------|
| `pipeline_name`      | `string`        | Pipeline-level namespace. |
| `name`               | `string`        | Unique node identifier within the pipeline. |
| `bucket`             | `string`        | Pipeline output bucket (Iceberg warehouse + per-node intermediates). |
| `catalog`            | `string`        | Workspace catalog identifier (ADR-016). |
| `schema`             | `string`        | Pipeline schema identifier (ADR-016). |
| `inputs`             | `any`           | Named input map; keys become SQL table aliases. Values are upstream module-output objects, cross-pipeline strings (`"<schema>.<table>"`), or registered-source strings (`"sources.<name>"`). |
| `output_definitions` | `map(object)`   | Named outputs with optional schemas and `mode` (`replace` default, `append`, `merge`). |

### Optional

| Name             | Default    | Description |
|------------------|------------|-------------|
| `language`       | `"sql"`    | `sql` (SparkSQL) or `python` (PySpark transform function). |
| `sql`            | `null`     | SQL body for `language = "sql"`. String for single-output, `map(string)` for multi-output. |
| `python`         | `null`     | Python source for `language = "python"`. Use `file(...)` to load. |
| `compute`        | `"lambda"` | One of `lambda`, `fargate` (planned), `emr-serverless` (planned). |
| `freshness_sla`  | `""`       | Staleness budget for Catalog UI (e.g. `"4h"`, `"24h"`). |

## Outputs

| Name                  | Description |
|-----------------------|-------------|
| `outputs`             | Named output map — one entry per `output_definitions` key, each carrying `table_path`, `catalog_db`, `catalog_table`, `schema`. |
| `lambda_function_arn` | Runner Lambda ARN. Pass to the orchestration module's `nodes[*].lambda_function_arn`. Empty string for non-`lambda` compute targets. |

## Resources created (compute = lambda)

| Resource              | Purpose |
|-----------------------|---------|
| `aws_s3_object.logic` | SQL or Python body uploaded to `s3://<bucket>/<pipeline>/<name>/_runtime/logic.<py|sql>`. The pipeline-level Lambda (emitted by `internal/orchestration/tfgen`) fetches this at runtime. |

The per-transform Lambda + IAM role were collapsed in v2.2.0 — the pipeline
Lambda handles every node now; this module is config-only.

## Notes

- **Delta auto-tables.** Output paths default to
  `<catalog>.<schema>.<name>__<output_key>` in the Glue Data Catalog;
  the runner registers them on first write via Delta's `saveAsTable`.
  No `aws_glue_catalog_table` resource is needed.
