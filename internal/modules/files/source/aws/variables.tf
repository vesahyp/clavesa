variable "tags" {
  description = "Additional tags merged on top of the module's own `clavesa:*` tags. Workspace-wide tags also apply via the AWS provider's default_tags block."
  type        = map(string)
  default     = {}
}

variable "pipeline_name" {
  type        = string
  description = "Pipeline-level namespace. Used in S3 paths, Glue catalog naming, resource tagging, and Step Functions state machine naming. Must be unique within an AWS account and region."
}

variable "name" {
  type        = string
  description = "Unique node identifier within the pipeline. Used in S3 paths, Glue catalog entries, and resource naming."
}

variable "bucket" {
  type        = string
  description = "Source S3 bucket name to read from."
}

variable "prefix" {
  type        = string
  description = "S3 key prefix filter. Empty string reads from the bucket root."
  default     = ""
}

variable "format" {
  type        = string
  description = "File format of the source data. Must be one of: csv, json, parquet. No auto-detection — always explicit."
  validation {
    condition     = contains(["csv", "json", "parquet"], var.format)
    error_message = "Format must be one of: csv, json, parquet."
  }
}

variable "schema" {
  type = object({
    columns = list(object({
      name     = string
      type     = string
      nullable = optional(bool, true)
    }))
    include_rescued_data = optional(bool, false)
  })
  description = "Optional schema to apply to source data. When null, the output table is schemaless and column types are inferred at runtime."
  default     = null
}

variable "json_path" {
  type        = string
  default     = null
  description = "Top-level key to unwrap when reading JSON files whose payload is nested under a single root key (e.g. \"cars\" for {\"cars\": [...]}). Null reads the JSON document as-is."
}

# --- Incremental processing (v0.12+) ---

variable "partitions" {
  type        = list(string)
  default     = []
  description = <<-EOT
    Hive-style partition column names, in path order. When set, the source is
    treated as incrementally readable: downstream transforms see only the
    partitions that have arrived since the last successful run, tracked via a
    watermark. Empty list means full re-read on every run (default).

    Example for CloudFront paths .../year=YYYY/month=MM/day=DD/hour=HH/ where
    var.prefix already pins year+month: partitions = ["day", "hour"].
  EOT
}

variable "start_from" {
  type        = string
  default     = "all"
  description = <<-EOT
    First-run cursor for partitioned sources:
    - "all"    backfills all upstream history (default).
    - "now"    skips backfill; only data arriving after the first run.
    - any other string is treated as a literal partition cursor (e.g.
      "2026-04-26" matches the first partition column lexicographically).
    Ignored when var.partitions is empty.
  EOT
}

variable "manage_bucket_notifications" {
  type        = bool
  default     = false
  description = <<-EOT
    When true, the module creates an `aws_s3_bucket_notification`
    resource on var.bucket with `eventbridge = true`, so S3
    object-create events reach the module's EventBridge rule without
    an out-of-band `aws s3api put-bucket-notification-configuration`
    step.

    Opt-in because the resource is authoritative; it replaces any
    other notification configuration on the bucket. Leave false when
    Clavesa doesn't own the bucket, and enable bucket notifications
    yourself.

    If the same source is attached to multiple pipelines, set this on
    one pipeline only; otherwise two `terraform apply` runs fight over
    the bucket's notification config.
  EOT
}
