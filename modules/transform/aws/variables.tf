# --- Common variables (Sub-artifact A) ---

variable "tags" {
  description = "Additional tags merged on top of the module's own `clavesa:*` tags. Workspace-wide tags also apply via the AWS provider's default_tags block — no need to pass workspace/managed-by here."
  type        = map(string)
  default     = {}
}

variable "pipeline_name" {
  description = "Pipeline-level namespace. Used in S3 paths, Glue catalog naming, and resource tagging."
  type        = string
}

variable "name" {
  description = "Unique node identifier within the pipeline. Used in S3 paths, Glue catalog entries, and resource naming."
  type        = string
}

# --- Three-level namespace (ADR-016) ---

variable "catalog" {
  description = <<-EOT
    Workspace catalog identifier (top level of <catalog>.<schema>.<table>).
    Required — the runner encodes the Glue DB as `<catalog>__<schema>`,
    so an empty value would land data in a malformed namespace. Pipeline
    .tf files thread the workspace's catalog identifier as a literal
    (single source of truth: clavesa.json). Pre-v0.18.0 pipelines
    that didn't pass this variable must migrate before consuming this
    module version.
  EOT
  type        = string
}

variable "schema" {
  description = <<-EOT
    Pipeline schema identifier (middle level of <catalog>.<schema>.<table>).
    Required — same reasoning as `catalog`. The default is set on the
    pipeline's own `variable "schema"` (sanitized pipeline name);
    transform module calls thread it via `schema = var.schema`. Two
    pipelines can share a domain by using the same value here, but only
    one pipeline may write into any given schema.
  EOT
  type        = string
}

variable "system_catalog" {
  description = <<-EOT
    Workspace-owned observability catalog (ADR-016 "Workspace system
    catalog"). The runner writes node_runs / tables to
    `<system_catalog>.pipelines.*` regardless of which pipeline schema
    owns the transform's user outputs — multi-writer by design,
    distinguished by the `pipeline` column on each row. Passed through
    to the Lambda as CLAVESA_SYSTEM_CATALOG.
  EOT
  type        = string
}

# --- Transform-specific variables ---

variable "bucket" {
  description = "S3 bucket for output Iceberg tables. Output paths follow s3://<bucket>/<pipeline_name>/<name>/<output_name>/."
  type        = string
}

variable "inputs" {
  description = "Named input map. Keys become SQL table aliases. Values are upstream module output objects (transform→transform Iceberg edges)."
  type = map(object({
    table_path    = string
    catalog_db    = string
    catalog_table = string
    schema = optional(object({
      columns = list(object({
        name     = string
        type     = string
        nullable = optional(bool, true)
      }))
      include_rescued_data = optional(bool, false)
    }))
    # Incremental processing fields (v0.12+). Sources expose these; transforms
    # leave them empty (transform→transform incremental is via Iceberg snapshots,
    # tracked separately).
    partitions = optional(list(string), [])
    start_from = optional(string, "all")
  }))
  default = {}
}

# Registered-source attachments (ADR-017 v0.22.0). Separate variable from
# var.inputs because:
#   - var.inputs is typed against transform→transform module-output
#     objects (table_path / catalog_db / …); a registered-source descriptor
#     can't satisfy that shape, and Terraform rejects "any" mixed-type
#     maps at plan time.
#   - The runner event payload comes from the orchestration module (which
#     resolves the registry name → descriptor at SFN execution time).
#     This module only consumes the bucket list for IAM read scope.
#
# Populated by `clavesa source attach` and refreshed from the registry
# at every `SyncOrchestration` call. Hand-edits get overwritten — treat
# it like orchestration.tf, generated content.
variable "source_inputs" {
  description = "Workspace registered-source attachments. Keys are SQL aliases; each value carries the resolved bucket/prefix/format so terraform validate passes without re-reading the registry. Bucket is the only field this module consumes (IAM read scope)."
  type = map(object({
    spec_name  = string
    bucket     = string
    prefix     = optional(string, "")
    format     = optional(string, "parquet")
    partitions = optional(list(string), [])
    start_from = optional(string, "all")
  }))
  default = {}
}

variable "language" {
  description = "Transform language: 'sql' (SparkSQL) or 'python' (PySpark transform function). Both run on the same Spark runtime."
  type        = string
  default     = "sql"

  validation {
    condition     = contains(["sql", "python"], var.language)
    error_message = "language must be one of: sql, python."
  }
}

variable "sql" {
  description = <<-EOT
    SQL body for the transform.
    - String: single-output transform. output_definitions must have exactly one key.
    - map(string): multi-output transform. Keys must match output_definitions keys exactly.
      Each key is an independent Athena CTAS query producing that named output.

    Invariant (enforced by cross-variable validation on Terraform >= 1.9, otherwise by convention):
      string sql  → exactly one output_definitions key
      map sql     → sql keys == output_definitions keys (bijection)
  EOT
  type    = any
  default = null
}

variable "python" {
  description = <<-EOT
    Python script source for language = "python" transforms.
    The script receives input DataFrames via the `inputs` dict (keys match var.inputs)
    and must return a dict mapping each output_definitions key to a pandas DataFrame.
    Use file("transforms/myscript.py") to load from a file.
  EOT
  type    = string
  default = null
}

variable "compute" {
  description = <<-EOT
    Compute target. All targets run the same PySpark runtime — pick the one
    that matches the workload size and cost shape:
    - local:  no AWS resources; runs in-process for development and CI.
    - lambda: Clavesa runner container on Lambda. Cheapest cloud option;
              pay-per-ms with scale-to-zero. Suits batches under ~15 min.
    - fargate: (planned) ECS Fargate task. Long-running medium jobs.
    - emr-serverless: (planned) Distributed Spark for large jobs;
              vCPU-second billing, ~5x cheaper than Glue at the same scale.
  EOT
  type    = string
  default = "lambda"

  validation {
    condition     = contains(["local", "lambda", "fargate", "emr-serverless"], var.compute)
    error_message = "compute must be one of: local, lambda, fargate, emr-serverless."
  }
}

variable "runner_image" {
  description = <<-EOT
    Clavesa transform runner container image URI (PySpark). Used for
    lambda, fargate, and emr-serverless compute targets.

    Required: must be a *private* ECR URI in the form
      <account>.dkr.ecr.<region>.amazonaws.com/<repo>:<tag>
    Public ECR Public Gallery URIs (public.ecr.aws/...) are not supported:
    Lambda image-digest pinning depends on the `aws_ecr_image` data source,
    which only resolves private ECR repositories.

    `clavesa workspace init` creates the workspace's ECR repo and pushes
    the runner image into it; pipelines pass that URI through verbatim.
  EOT
  type        = string

  validation {
    condition     = can(regex("\\.dkr\\.ecr\\.[a-z0-9-]+\\.amazonaws\\.com/", var.runner_image))
    error_message = "runner_image must be a private ECR URI (<account>.dkr.ecr.<region>.amazonaws.com/<repo>:<tag>). Public ECR Public Gallery is not supported because Lambda image-digest pinning requires aws_ecr_image."
  }
}

variable "output_definitions" {
  description = <<-EOT
    Named output declarations. Each key becomes an entry in the module's outputs map.
    Single-output transforms use "default" by convention.
    schema is optional; omitting it produces a schema-less Iceberg output.
    mode controls write semantics (v0.12+):
      - "replace" (default): overwrite the Iceberg table on each run. Correct
        for full-recompute aggregations; wrong for monotonically-growing facts.
      - "append":            append rows to the Iceberg table on each run.
        Pair with a partitioned source so the runner only reads new partitions.
      - "merge":             MERGE rows into the Iceberg table keyed on
        merge_keys — matched rows update in place, new rows insert. The
        idempotent shape for dimension tables and backfill promotes.
    merge_keys lists the columns that uniquely identify a row; required when
    mode is "merge", and usable on "append" outputs so a later backfill can
    promote via MERGE instead of refusing.
    stats = true opts this output into per-column profile computation
      (null %, approx distinct, min/max, percentiles, top-10) at write
      time. Rows land in the workspace system column_stats table and
      surface on the Catalog table-detail page. Default false; cost is
      one extra aggregation pass + a per-low-cardinality-column group-by
      per run, paid only by transforms that opted in.
  EOT
  type = map(object({
    schema = optional(object({
      columns = list(object({
        name     = string
        type     = string
        nullable = optional(bool, true)
      }))
      include_rescued_data = optional(bool, false)
    }))
    mode       = optional(string, "replace")
    merge_keys = optional(list(string), [])
    stats      = optional(bool, false)
  }))

  validation {
    condition     = alltrue([for k, v in var.output_definitions : contains(["replace", "append", "merge"], coalesce(v.mode, "replace"))])
    error_message = "output_definitions[*].mode must be one of: replace, append, merge."
  }

  validation {
    condition     = alltrue([for k, v in var.output_definitions : coalesce(v.mode, "replace") != "merge" || length(coalesce(v.merge_keys, [])) > 0])
    error_message = "output_definitions[*] with mode = \"merge\" must declare a non-empty merge_keys."
  }
}

variable "freshness_sla" {
  type        = string
  default     = ""
  description = <<-EOT
    Optional staleness budget for this transform's output tables. Examples:
    "4h", "24h", "30m", "7d". The Catalog page renders a colored chip per
    table when the latest snapshot exceeds this age — green when within
    the SLA, yellow at >50% of it, red over. Empty (the default) hides
    the chip; existing pipelines see no behavior change.

    Suffixes: 's' (seconds), 'm' (minutes), 'h' (hours), 'd' (days).
  EOT

  validation {
    condition     = var.freshness_sla == "" || can(regex("^[0-9]+(s|m|h|d)$", var.freshness_sla))
    error_message = "freshness_sla must be empty or a number followed by one of s/m/h/d (e.g. \"4h\", \"30m\", \"7d\")."
  }
}
