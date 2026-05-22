resource "aws_s3_bucket" "workspace_bucket" {
  bucket        = "${var.workspace_name}-clavesa"
  force_destroy = var.force_destroy
  tags          = merge(var.tags, { "clavesa:workspace" = var.workspace_name })
}

# Versioning: accidental object deletes (or `aws s3 rm` typos) are
# recoverable. Iceberg metadata files are tiny; the cost overhead is
# negligible.
resource "aws_s3_bucket_versioning" "workspace_bucket" {
  bucket = aws_s3_bucket.workspace_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Default encryption at rest. SSE-S3 unless var.kms_key_id is set, in
# which case the bucket uses SSE-KMS with the supplied key.
resource "aws_s3_bucket_server_side_encryption_configuration" "workspace_bucket" {
  bucket = aws_s3_bucket.workspace_bucket.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = var.kms_key_id == null ? "AES256" : "aws:kms"
      kms_master_key_id = var.kms_key_id
    }
    bucket_key_enabled = var.kms_key_id != null
  }
}

# No accidental public exposure of warehouse data, Athena results, or
# observability tables. All four flags on; no override.
resource "aws_s3_bucket_public_access_block" "workspace_bucket" {
  bucket = aws_s3_bucket.workspace_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ---------------------------------------------------------------------------
# System observability Glue database — workspace-owned (ADR-016 "Workspace
# system catalog"). Holds runs / node_runs / tables for every pipeline in
# the workspace, distinguished by the `pipeline` column. Database name
# follows the same `<catalog>__<schema>` encoding as user-pipeline DBs;
# the `pipelines` schema is the first one created here. Future system
# schemas (`query`, `billing`, `access`) get their own DBs as they ship.
# Multi-writer is intentional — the slice-4 schema-ownership validator
# exempts the system catalog from the "one pipeline per schema" rule.
# ---------------------------------------------------------------------------

locals {
  _safe_system_catalog = replace(var.system_catalog, "-", "_")
  system_pipelines_db  = "${local._safe_system_catalog}__pipelines"
}

resource "aws_glue_catalog_database" "system_pipelines" {
  name        = local.system_pipelines_db
  description = "Clavesa workspace observability — runs / node_runs / tables across every pipeline in ${var.workspace_name}."
  tags = merge(var.tags, {
    "clavesa:workspace" = var.workspace_name
    "clavesa:catalog"   = var.system_catalog
    "clavesa:schema"    = "pipelines"
    "clavesa:managed-by" = "clavesa"
  })
}

resource "aws_s3_bucket_lifecycle_configuration" "workspace_bucket" {
  count  = var.athena_results_retention_days > 0 ? 1 : 0
  bucket = aws_s3_bucket.workspace_bucket.id

  # Athena workgroup writes one result object per query (Catalog page,
  # dashboard widget, ad-hoc preview). Useful only until the caller
  # reads it. Per-pipeline `<pipeline>/_athena-results/` written by the
  # runs_writer Lambda is NOT covered here — S3 lifecycle prefix filters
  # can't match a wildcard segment, and pipeline names aren't known at
  # workspace-module time. Lower-volume than workspace-level Athena.
  rule {
    id     = "expire-athena-results"
    status = "Enabled"

    filter {
      prefix = "athena-results/"
    }

    expiration {
      days = var.athena_results_retention_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

resource "aws_athena_workgroup" "workspace" {
  name                   = "clavesa-${var.workspace_name}"
  state                  = "ENABLED"
  force_destroy          = var.force_destroy

  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.workspace_bucket.bucket}/athena-results/"
    }
  }

  tags = merge(var.tags, { "clavesa:workspace" = var.workspace_name })
}
