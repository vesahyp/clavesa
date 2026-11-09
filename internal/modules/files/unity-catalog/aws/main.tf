# Unity Catalog objects for one clavesa workspace (ADR-027).
#
# Default (create_catalog = false): the team's catalog and storage are
# used as they are; this module creates only the runner's Secrets Manager
# secret. With create_catalog = true (`workspace connect databricks
# --create-catalog`, for a Databricks workspace with no real catalog) it
# also creates, as P0 established:
#   - one IAM role Unity Catalog assumes, reading the whole workspace
#     bucket and writing only under _unity_catalog/ (the catalog's
#     managed location; clavesa's own data stays clavesa-written);
#   - one storage credential over it and one external location over the
#     bucket root. The location is writable as far as Unity Catalog is
#     concerned (a managed location must sit on one), and validation is
#     skipped because validation writes at the root, which IAM refuses;
#   - the catalog, with storage_root under _unity_catalog/;
#   - EXTERNAL USE grants for the service principal: owners do not get
#     them implicitly (P0), and registering an external table needs both.
#
# The role ARN is built from its name so the storage credential can be
# created first and its external ID fed into the role's trust policy.

data "aws_caller_identity" "current" {}

locals {
  create_storage = var.create_catalog && var.external_location == null
  # IAM role names cap at 64 characters.
  role_name = substr("clavesa-${var.workspace_name}-unity-catalog", 0, 64)
  role_arn  = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.role_name}"
  # Unity Catalog object names: letters, digits, underscores.
  uc_name        = "clavesa_${replace(var.workspace_name, "-", "_")}"
  location_name  = local.create_storage ? databricks_external_location.workspace[0].name : var.external_location
  managed_prefix = "_unity_catalog"
  tags           = merge(var.tags, { "clavesa:workspace" = var.workspace_name, "clavesa:managed-by" = "clavesa" })
}

resource "databricks_storage_credential" "workspace" {
  count = local.create_storage ? 1 : 0

  name    = local.uc_name
  comment = "clavesa workspace ${var.workspace_name} (managed by clavesa)"
  aws_iam_role {
    role_arn = local.role_arn
  }
  # The role does not exist yet when the credential is created.
  skip_validation = true
}

data "databricks_aws_unity_catalog_assume_role_policy" "workspace" {
  count = local.create_storage ? 1 : 0

  aws_account_id = data.aws_caller_identity.current.account_id
  role_name      = local.role_name
  external_id    = databricks_storage_credential.workspace[0].aws_iam_role[0].external_id
}

resource "aws_iam_role" "unity_catalog" {
  count = local.create_storage ? 1 : 0

  name               = local.role_name
  assume_role_policy = data.databricks_aws_unity_catalog_assume_role_policy.workspace[0].json
  tags               = local.tags
}

# Read everything, write only the catalog's managed location. Written by
# hand: databricks_aws_unity_catalog_policy grants write on the whole bucket.
resource "aws_iam_role_policy" "unity_catalog" {
  count = local.create_storage ? 1 : 0

  name = "unity-catalog-bucket"
  role = aws_iam_role.unity_catalog[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      {
        Sid      = "ReadBucket"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:GetObjectVersion", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = ["arn:aws:s3:::${var.bucket}", "arn:aws:s3:::${var.bucket}/*"]
      },
      {
        Sid      = "WriteManagedLocation"
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]
        Resource = ["arn:aws:s3:::${var.bucket}/${local.managed_prefix}/*"]
      },
      {
        Sid      = "SelfAssume"
        Effect   = "Allow"
        Action   = ["sts:AssumeRole"]
        Resource = [local.role_arn]
      },
      ], var.kms_key_arn == null ? [] : [{
        Sid      = "BucketKey"
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey*"]
        Resource = [var.kms_key_arn]
    }])
  })
}

# IAM is eventually consistent: Unity Catalog's first assume-role right
# after the role is created can fail.
resource "time_sleep" "iam_propagation" {
  count = local.create_storage ? 1 : 0

  create_duration = "20s"
  depends_on      = [aws_iam_role_policy.unity_catalog]
}

resource "databricks_external_location" "workspace" {
  count = local.create_storage ? 1 : 0

  name            = local.uc_name
  url             = "s3://${var.bucket}/"
  credential_name = databricks_storage_credential.workspace[0].name
  comment         = "clavesa workspace ${var.workspace_name} bucket (managed by clavesa)"
  skip_validation = true
  depends_on      = [time_sleep.iam_propagation]
}

resource "databricks_grants" "external_location" {
  count = local.create_storage ? 1 : 0

  external_location = databricks_external_location.workspace[0].id
  grant {
    principal  = var.databricks_client_id
    privileges = ["ALL_PRIVILEGES", "EXTERNAL_USE_LOCATION"]
  }
}

resource "databricks_catalog" "workspace" {
  count = var.create_catalog ? 1 : 0

  name         = var.catalog
  storage_root = "s3://${var.bucket}/${local.managed_prefix}/${var.catalog}"
  comment      = "clavesa workspace ${var.workspace_name} (created by clavesa --create-catalog)"
  properties = {
    "clavesa.workspace"  = var.workspace_name
    "clavesa.managed-by" = "clavesa"
  }
  depends_on = [databricks_external_location.workspace]
}

# Schemas inherit EXTERNAL USE SCHEMA from the catalog.
resource "databricks_grants" "catalog" {
  count = var.create_catalog ? 1 : 0

  catalog = databricks_catalog.workspace[0].name
  grant {
    principal  = var.databricks_client_id
    privileges = ["ALL_PRIVILEGES", "EXTERNAL_USE_SCHEMA"]
  }
  # SQL warehouse users: without USE CATALOG every query is refused,
  # whatever the table grants (ADR-027 update, the warehouse 403).
  dynamic "grant" {
    for_each = toset(var.readers)
    content {
      principal  = grant.value
      privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT"]
    }
  }
}

# The runner's copy of the service principal's OAuth secret. Terraform
# creates the container only; `clavesa deploy` writes the value with
# PutSecretValue so the secret never enters the Terraform state.
resource "aws_secretsmanager_secret" "client_secret" {
  count = var.create_runner_secret ? 1 : 0

  name        = "clavesa/${var.workspace_name}/databricks-client-secret"
  description = "Databricks service principal OAuth secret for the clavesa runner (value written by clavesa deploy)"
  tags        = local.tags
}
