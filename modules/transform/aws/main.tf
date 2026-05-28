locals {
  # Glue Data Catalog database name — flat encoding of the ADR-016
  # (catalog, schema) pair. Mirrors `internal/identutil.EncodeGlueDatabase`
  # on the Go side and `_glue_db()` in the runner; all three encoders
  # must stay byte-identical so downstream readers find what the runner
  # writes. Both catalog and schema are required (v0.18.0+).
  _safe_catalog = replace(var.catalog, "-", "_")
  _safe_schema  = replace(var.schema, "-", "_")
  catalog_db    = "${local._safe_catalog}__${local._safe_schema}"

  common_tags = merge(var.tags, {
    "clavesa:pipeline" = var.pipeline_name
    "clavesa:node"     = var.name
    "clavesa:type"     = "transform"
  })
}

# ---------------------------------------------------------------------------
# S3 object — transform logic script
#
# Uploaded for any compute target that runs the runner container against S3
# (lambda today; fargate / emr-serverless to follow). Skipped for `local`,
# which executes against the local filesystem.
# ---------------------------------------------------------------------------

locals {
  _logic_ext = var.language == "python" ? "py" : "sql"
  # var.sql is `any` to support map-sql for multi-output transforms (an
  # Athena-era feature). Lambda runs one transform per node — coerce to string
  # and surface a clear error if a map sneaks in.
  _logic_content = var.language == "python" ? (var.python != null ? var.python : "") : try(tostring(var.sql), "")
  _logic_key     = "${var.pipeline_name}/${var.name}/_runtime/logic.${local._logic_ext}"
}

resource "aws_s3_object" "logic" {
  count = var.compute == "lambda" ? 1 : 0

  bucket  = var.bucket
  key     = local._logic_key
  content = local._logic_content

  tags = local.common_tags
}

# ---------------------------------------------------------------------------
# compute = "lambda" — per-transform Lambda emission collapsed (v0.30+).
# The pipeline-level Lambda emitted by internal/orchestration/tfgen handles
# every transform in the pipeline now; it fetches each transform's logic
# from s3://<bucket>/<pipeline>/<node>/_runtime/logic.<py|sql> at runtime
# via _read_text(). This module's lambda role/policy/function and the
# aws_ecr_image data source moved with it.
#
# compute = "local" — no AWS resources. Used for development and CI.
# Output paths still point at the bucket so downstream nodes can resolve them
# the same way they would in cloud mode; nothing is actually written until
# you run the pipeline against a real backend.
# ---------------------------------------------------------------------------
