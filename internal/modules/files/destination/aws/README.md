# `clavesa/destination/aws`

Terraform module for an S3 destination node.

Destinations are **pass-through path declarations** — they create no
Lambda, no Glue Job, no IAM role. The module computes the final
`target_path` from `bucket / prefix / name` and exposes it for the
orchestration emitter to thread back into the upstream transform's
runner outputs. Writing happens inside the upstream transform's runner
process; the destination is just a routing label.

Destinations are **terminal nodes** — `outputs = {}`. Pipeline introspection
identifies them by the empty outputs map.

## Usage

### Parquet destination (default)

```hcl
module "warehouse" {
  source = "../.clavesa/modules/v0.30.0/destination/aws" # written by clavesa CLI; embedded in the binary since v0.30.0

  pipeline_name = var.pipeline_name
  name          = "warehouse"

  input  = module.validate.outputs["valid"]
  bucket = "analytics-warehouse"
  prefix = "clean/events/"
  format = "parquet"
}
```

### CSV destination with append mode

```hcl
module "dead_letter" {
  source = "../../"

  pipeline_name = var.pipeline_name
  name          = "dead_letter"

  input       = module.validate.outputs["invalid"]
  bucket      = "quarantine-bucket"
  prefix      = "invalid/events/"
  format      = "csv"
  compression = "none"
  write_mode  = "append"
}
```

## Variables

| Name            | Type     | Required | Default       | Description |
|-----------------|----------|---------:|---------------|-------------|
| `pipeline_name` | `string` | Yes      | —             | Pipeline-level namespace. |
| `name`          | `string` | Yes      | —             | Unique node identifier within the pipeline. |
| `input`         | `object` | Yes      | —             | Upstream output reference (`module.<name>.outputs["<key>"]`). |
| `bucket`        | `string` | Yes      | —             | Target S3 bucket. |
| `prefix`        | `string` | No       | `""`          | Target S3 key prefix. Empty writes to bucket root. |
| `format`        | `string` | No       | `"parquet"`   | One of `parquet`, `csv`, `json`. No auto-detection. |
| `write_mode`    | `string` | No       | `"overwrite"` | `overwrite` (replace files under prefix) or `append` (add only). |
| `compression`   | `string` | No       | `null`        | One of `snappy`, `gzip`, `none`. Null uses format default: `parquet → snappy`, `csv → none`, `json → none`. |

## Outputs

| Name          | Description |
|---------------|-------------|
| `outputs`     | Always `{}` — destinations are terminal. |
| `target_path` | Final S3 path the upstream transform writes to. The orchestration emitter threads this into the upstream node's `outputs[*]` so the runner picks it up. |
| `metadata`    | `{ target_path, format, write_mode, compression }` for observability. |

## Resources created

None. Destinations are declarative routing labels only — the upstream
transform's runner role gets the write permission via the orchestration
emitter, and the runner does the actual write at execution time.

## Notes

- **Destination overrides land at `target_path`, not in the Iceberg
  warehouse.** Use destinations when the consumer of the data is *not*
  another Clavesa transform (e.g. an analytics warehouse, an external
  vendor's bucket, a hand-off to a non-Clavesa downstream system).
  When the consumer *is* another transform, leave the upstream's outputs
  pointing at the warehouse — Iceberg auto-tables are the cheaper path.
- **Cross-account targets.** The upstream transform's runner role needs
  write access to the target bucket. For cross-account, add a bucket
  policy on the target granting write access to that role's ARN
  (exposed as `module.<transform>.lambda_function_arn` → derive role).
