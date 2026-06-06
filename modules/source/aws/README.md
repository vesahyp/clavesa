# `clavesa/source/aws`

Terraform module for an S3 source node.

Sources are **pass-through path declarations**, not data movers. The module
publishes the user-configured S3 location as `outputs["default"].table_path`
so downstream transforms can read directly with `spark.read.<format>`. No
staging, no Glue Job, no Iceberg registration on the source side. An SQS
queue + EventBridge rule are created for orchestration triggering on new-
data events; pass `trigger_queue_arn` to the orchestration module's
`trigger_queue_arns`.

## Usage

```hcl
module "raw_events" {
  source = "../.clavesa/modules/v0.30.0/source/aws" # written by clavesa CLI; embedded in the binary since v0.30.0

  pipeline_name = var.pipeline_name
  name          = "raw_events"
  bucket        = "acme-data-lake"
  prefix        = "events/"
  format        = "json"

  # Optional: hand the runner an explicit schema instead of inferring at
  # read time. Inferred schemas are fine for development; explicit schemas
  # catch upstream surprises.
  schema = {
    columns = [
      { name = "id",     type = "string",        nullable = false },
      { name = "amount", type = "decimal(10,2)", nullable = false },
      { name = "status", type = "string" },
      { name = "ts",     type = "timestamp" }
    ]
    include_rescued_data = true
  }
}

# Reference in a downstream transform:
module "validate" {
  # ...
  inputs = {
    raw = module.raw_events.outputs["default"]
  }
}
```

### Incremental reads (v0.12+)

Set `partitions` to a Hive-style partition column list and the source
becomes incrementally readable: downstream transforms see only partitions
that have arrived since the last successful run, tracked via a watermark
in the pipeline bucket. `start_from = "all"` (default) backfills history;
`"now"` skips backfill.

## Variables

| Name            | Type           | Required | Default | Description |
|-----------------|----------------|---------:|---------|-------------|
| `pipeline_name` | `string`       | Yes      | —       | Pipeline-level namespace. |
| `name`          | `string`       | Yes      | —       | Unique node identifier within the pipeline. |
| `bucket`        | `string`       | Yes      | —       | Source S3 bucket. |
| `prefix`        | `string`       | No       | `""`    | S3 key prefix filter. |
| `format`        | `string`       | Yes      | —       | `csv`, `json`, or `parquet`. No auto-detection. |
| `schema`        | `object`       | No       | `null`  | Optional column schema; null means infer at read time. |
| `json_path`     | `string`       | No       | `null`  | Top-level key to unwrap when JSON payload is nested under one root key. |
| `partitions`    | `list(string)` | No       | `[]`    | Hive-style partition columns for incremental reads. |
| `start_from`    | `string`       | No       | `"all"` | First-run cursor for partitioned sources: `"all"`, `"now"`, or a literal cursor string. |
| `trigger_visibility_timeout_seconds` | `number` | No | `900` | Visibility timeout for the trigger queue. Must cover a full run so received-but-undeleted messages stay invisible until the Delta write commits. |
| `trigger_max_receive_count` | `number` | No | `5` | Receives before a trigger message moves to the DLQ. Bounds retries on a poison/unreadable object. |
| `manage_bucket_notifications` | `bool` | No | `false` | Have terraform own `aws_s3_bucket_notification` on the source bucket. Authoritative; replaces other notification config. Set on one pipeline only when a source attaches to multiple. |

## Outputs

| Name                | Description |
|---------------------|-------------|
| `outputs`           | Map keyed by `"default"`. Each value carries `table_path`, `catalog_db`, `catalog_table`, `schema`, `partitions`, `start_from`. |
| `trigger_queue_arn` | SQS queue ARN. Pass to the orchestration module's `trigger_queue_arns`. |
| `trigger_queue_url` | SQS queue URL. The runner Lambda uses it to `ReceiveMessage` / `DeleteMessage` when draining new-data events. |

## Resources created

| Resource                      | Purpose |
|-------------------------------|---------|
| `aws_sqs_queue.trigger`       | Queue that the orchestration poller checks for new-data events; drained by the runner Lambda. |
| `aws_sqs_queue.trigger_dlq`   | Dead-letter queue (14-day retention) for trigger messages that exceed `trigger_max_receive_count`. |
| `aws_cloudwatch_event_rule`   | EventBridge rule on S3:ObjectCreated, scoped to `bucket`/`prefix`. |
| `aws_cloudwatch_event_target` | Routes matched events to the SQS queue. |
| `aws_sqs_queue_policy`        | Lets EventBridge publish to the trigger queue. The DLQ needs none — it only receives via redrive. |

All resources are tagged `clavesa:pipeline`, `clavesa:node`,
`clavesa:type = "source"`.

## Notes

- The source bucket must have **EventBridge notifications enabled** for the
  S3 trigger to fire. Set `manage_bucket_notifications = true` to let this
  module own that config, or enable it once out-of-band with
  `aws s3api put-bucket-notification-configuration --bucket <b> --notification-configuration '{"EventBridgeConfiguration":{}}'`.
- Cross-account source buckets are not supported.
- The `outputs[*].catalog_db` / `catalog_table` fields are exported for
  shape parity with `transform` outputs; sources don't actually register a
  Glue catalog entry — downstream code reads `table_path` directly.
