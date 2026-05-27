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

  # Extract bucket names for IAM read policy. Two upstream shapes contribute:
  #   - var.inputs        : transform→transform Iceberg upstreams. Bucket
  #                         lives under `table_path` ("s3://<bucket>/.../warehouse").
  #   - var.source_inputs : registered s3 sources (ADR-017 v0.22.0).
  #                         Bucket is on the spec directly.
  input_buckets = distinct(concat(
    [for alias, inp in var.inputs : split("/", trimprefix(inp.table_path, "s3://"))[0]],
    [for alias, src in var.source_inputs : src.bucket if src.bucket != ""],
  ))
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
# IAM — Lambda execution role (compute = lambda)
# ---------------------------------------------------------------------------

resource "aws_iam_role" "lambda_exec" {
  count = var.compute == "lambda" ? 1 : 0

  name = "${var.pipeline_name}-${var.name}-lambda-exec"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "LambdaTrust"
      Effect = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action = "sts:AssumeRole"
    }]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "lambda_exec" {
  count = var.compute == "lambda" ? 1 : 0

  name = "${var.pipeline_name}-${var.name}-lambda-exec"
  role = aws_iam_role.lambda_exec[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3ReadLogic"
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = "arn:aws:s3:::${var.bucket}/${local._logic_key}"
      },
      {
        Sid    = "S3ReadInputs"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = flatten([
          for bucket in local.input_buckets : [
            "arn:aws:s3:::${bucket}",
            "arn:aws:s3:::${bucket}/*",
          ]
        ])
      },
      {
        # ADR-016 cross-pipeline reads: transforms can address any
        # `<catalog>.<schema>.<table>` in the workspace, not just nodes
        # in their own pipeline. The Delta data behind those tables
        # lives under every pipeline's warehouse prefix in the shared
        # workspace bucket. Grant read access to all warehouses + the
        # workspace system warehouse so cross-pipeline `inputs` resolve
        # without each transform having to enumerate every upstream
        # bucket. Writes stay scoped to this pipeline's prefix
        # (S3WriteOutputs below).
        Sid    = "S3ReadWorkspaceWarehouses"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [
          "arn:aws:s3:::${var.bucket}",
          "arn:aws:s3:::${var.bucket}/*/_warehouse/*",
          "arn:aws:s3:::${var.bucket}/_system/*",
        ]
      },
      {
        Sid    = "S3WriteOutputs"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts",
        ]
        Resource = [
          "arn:aws:s3:::${var.bucket}",
          # Delta warehouse — shared by all transforms in this pipeline.
          "arn:aws:s3:::${var.bucket}/${var.pipeline_name}/_warehouse/*",
          # Per-node path (legacy Parquet output, kept for path-mode writes
          # when a destination override sends data outside the warehouse).
          "arn:aws:s3:::${var.bucket}/${var.pipeline_name}/${var.name}/*",
          # Pipeline-shared watermarks (incremental processing, v0.12+).
          # One JSON object per partitioned source, written by the runner
          # after a successful Delta commit.
          "arn:aws:s3:::${var.bucket}/${var.pipeline_name}/_watermarks/*",
          # Workspace-shared system warehouse — every pipeline's runner
          # appends to `node_runs` and `tables` here, distinguished by the
          # `pipeline` column. Matches CLAVESA_SYSTEM_WAREHOUSE in the
          # Lambda env.
          "arn:aws:s3:::${var.bucket}/_system/pipelines/*",
        ]
      },
      {
        Sid    = "Logs"
        Effect = "Allow"
        Action = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        # ADR-016 scope: read any database/table in the workspace
        # catalog OR the workspace system catalog. Cross-pipeline
        # reads are first-class; the per-pipeline scoping the prior
        # iteration carried meant a transform addressing a sibling
        # pipeline's Delta table 403'd at the catalog lookup.
        # Wildcard `*` (Session F P2) is gone — other workspaces in
        # the same AWS account are now out of scope.
        #
        # `database/default` is in the resource list because the Hive
        # metastore client (sub-slice 15) probes Glue for the "default"
        # DB during session init, regardless of whether the transform
        # ever references it. Without this the very first `spark.sql`
        # call 403s before any user SQL runs.
        Sid    = "GlueDataCatalogRead"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
        ]
        Resource = [
          "arn:aws:glue:*:*:catalog",
          "arn:aws:glue:*:*:database/default",
          "arn:aws:glue:*:*:database/${local._safe_catalog}__*",
          "arn:aws:glue:*:*:table/${local._safe_catalog}__*/*",
          "arn:aws:glue:*:*:database/${replace(var.system_catalog, "-", "_")}__*",
          "arn:aws:glue:*:*:table/${replace(var.system_catalog, "-", "_")}__*/*",
        ]
      },
      {
        # Writes stay scoped to this pipeline's own user-schema DB
        # plus the workspace system observability DB (runs/node_runs/
        # tables append from every pipeline). Schema-ownership rule
        # (§5) is enforced via Slice 4's validator; the IAM here is
        # the second line of defence.
        Sid    = "GlueDataCatalogWrite"
        Effect = "Allow"
        Action = [
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:CreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
          "glue:BatchCreatePartition",
          "glue:BatchDeletePartition",
        ]
        Resource = [
          "arn:aws:glue:*:*:catalog",
          "arn:aws:glue:*:*:database/${local.catalog_db}",
          "arn:aws:glue:*:*:table/${local.catalog_db}/*",
          "arn:aws:glue:*:*:database/${replace(var.system_catalog, "-", "_")}__pipelines",
          "arn:aws:glue:*:*:table/${replace(var.system_catalog, "-", "_")}__pipelines/*",
        ]
      },
      {
        # First-run database creation. The Hive-metastore-over-Glue
        # path (sub-slice 15) goes through `CREATE DATABASE IF NOT
        # EXISTS <catalog>__<schema>` on first transform write,
        # which Glue serves via `glue:CreateDatabase`. The action
        # is catalog-scoped (you cannot name an unborn database in
        # an ARN), but the GlueDataCatalogWrite statement above
        # still restricts every other write action to this pipeline's
        # own DB. Without this, the first run of a freshly-deployed
        # pipeline 403s at metastore init.
        Sid    = "GlueDataCatalogDatabaseCreate"
        Effect = "Allow"
        Action = [
          "glue:CreateDatabase",
        ]
        Resource = [
          "arn:aws:glue:*:*:catalog",
          "arn:aws:glue:*:*:database/${local.catalog_db}",
          "arn:aws:glue:*:*:database/${replace(var.system_catalog, "-", "_")}__pipelines",
        ]
      },
    ]
  })
}

# ---------------------------------------------------------------------------
# Lambda function — Clavesa PySpark runner container (compute = lambda)
#
# The runner_image variable is a tag-form ECR URI like
#   <account>.dkr.ecr.<region>.amazonaws.com/<repo>:<tag>
# Lambda pins to the digest at deploy time, so re-pushing under the same tag
# would not update the function. We resolve the tag's current digest at plan
# time via the aws_ecr_image data source and pin Lambda to repo@<digest>;
# every `terraform apply` then picks up image changes automatically — no
# manual `aws lambda update-function-code` dance.
# ---------------------------------------------------------------------------

locals {
  _runner_image_uri_match = regex("^([^:]+):(.+)$", var.runner_image)
  _runner_repo_uri        = local._runner_image_uri_match[0]
  _runner_tag             = local._runner_image_uri_match[1]
  # Repository name is everything after the registry hostname:
  #   "<acct>.dkr.ecr.<region>.amazonaws.com/<repo>" → "<repo>"
  _runner_repo_name = regex("^[^/]+/(.+)$", local._runner_repo_uri)[0]
}

data "aws_ecr_image" "runner" {
  count           = var.compute == "lambda" ? 1 : 0
  repository_name = local._runner_repo_name
  image_tag       = local._runner_tag
}

resource "aws_lambda_function" "runner" {
  count = var.compute == "lambda" ? 1 : 0

  function_name = "${var.pipeline_name}-${var.name}"
  role          = aws_iam_role.lambda_exec[0].arn
  package_type  = "Image"
  image_uri     = "${local._runner_repo_uri}@${data.aws_ecr_image.runner[0].image_digest}"
  timeout       = 900
  memory_size   = 3008  # PySpark needs headroom for the JVM + driver

  environment {
    variables = {
      CLAVESA_LOGIC_S3_PATH       = "s3://${var.bucket}/${local._logic_key}"
      CLAVESA_LANGUAGE            = var.language
      CLAVESA_PIPELINE            = var.pipeline_name
      CLAVESA_NODE                = var.name
      # Three-level namespace inputs (ADR-016). Empty values are the
      # legacy signal — runner encodes Glue DB as `clavesa_<schema>`
      # (where schema falls back to pipeline_name). Non-empty values
      # take the encoded `<catalog>__<schema>` path.
      CLAVESA_CATALOG             = var.catalog
      CLAVESA_SCHEMA              = var.schema
      # Workspace system catalog (ADR-016 v0.20.0). The runner routes
      # node_runs / tables writes here; the `pipelines` schema is hard-
      # coded on the runner side (workspace-wide, multi-writer).
      CLAVESA_SYSTEM_CATALOG      = var.system_catalog
      # Delta warehouse path — runner federates against the Glue Data
      # Catalog (Hive metastore protocol) when this is s3://.
      CLAVESA_WAREHOUSE           = "s3://${var.bucket}/${var.pipeline_name}/_warehouse/"
      # Workspace-shared system warehouse — node_runs / tables land here
      # regardless of which pipeline's runner writes them, so two pipelines
      # in the same workspace converge on the same Iceberg data path
      # (matches the workspace-wide `pipelines` Glue DB they register in).
      # Distinct from CLAVESA_WAREHOUSE (per-pipeline transform outputs).
      CLAVESA_SYSTEM_WAREHOUSE    = "s3://${var.bucket}/_system/pipelines/"
      # Watermark base path — one JSON object per partitioned input, written
      # by the runner after a successful Iceberg commit. Pipeline-shared so
      # multiple transforms reading the same source see the same cursor.
      CLAVESA_WATERMARKS          = "s3://${var.bucket}/${var.pipeline_name}/_watermarks/"
      # Triage columns: the runner stamps every node_runs row with the
      # ECR-content-addressable digest of the image Lambda is pinned to,
      # plus the orchestration-module version. Both make "which build of
      # the runner produced this row?" answerable from a SQL query without
      # cross-referencing CloudFormation state. The module_version comes
      # from the Dockerfile ARG baked at build time (CLAVESA_MODULE_VERSION
      # ENV in the image), so we don't need to thread it through Terraform.
      CLAVESA_RUNNER_IMAGE_DIGEST = data.aws_ecr_image.runner[0].image_digest
    }
  }

  tags = local.common_tags

  depends_on = [aws_s3_object.logic]
}

# ---------------------------------------------------------------------------
# compute = "local" — no AWS resources. Used for development and CI.
# Output paths still point at the bucket so downstream nodes can resolve them
# the same way they would in cloud mode; nothing is actually written until
# you run the pipeline against a real backend.
# ---------------------------------------------------------------------------
