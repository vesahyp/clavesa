# Lake Formation for one clavesa workspace (docs/ideas/column-tags.md,
# slice 1a). Created in the workspace's own account when clavesa.json has
# an "access" block:
#   - the registration role: Lake Formation assumes it to read the table
#     data of the workspace for engines that ask it for credentials
#     (Athena). It cannot write: the runners write with their own IAM S3
#     rights and never ask Lake Formation for credentials. Its S3 reach is the table prefixes
#     only: _system/ (the system catalog) and <pipeline>/_warehouse/ (each
#     pipeline's tables). Never athena-results/, _clavesa/, _progress/ or
#     anything else in the bucket;
#   - the registration of _system/ with that role. Each pipeline registers
#     its own <pipeline>/_warehouse/ in its own stack, with this role.
#
# The trust allows only lakeformation.amazonaws.com, for this account.

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}
data "aws_region" "current" {}

locals {
  account   = data.aws_caller_identity.current.account_id
  partition = data.aws_partition.current.partition
  region    = data.aws_region.current.region
  bucket    = "arn:${local.partition}:s3:::${var.pipeline_bucket}"
  tags      = merge(var.tags, { "clavesa:workspace" = var.workspace_name, "clavesa:managed-by" = "clavesa" })
}

resource "aws_iam_role" "register" {
  name                 = var.role_name
  description          = "Lake Formation reads the table data of clavesa workspace ${var.workspace_name} (column tags, slice 1a)."
  max_session_duration = 3600
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "LakeFormation"
      Effect    = "Allow"
      Principal = { Service = "lakeformation.amazonaws.com" }
      Action    = ["sts:AssumeRole", "sts:SetContext"]
      # Confused deputy guard: only Lake Formation acting for this account.
      # https://docs.aws.amazon.com/lake-formation/latest/dg/cross-service-confused-deputy-prevention.html
      Condition = {
        StringEquals = { "aws:SourceAccount" = local.account }
        ArnLike      = { "aws:SourceArn" = "arn:${local.partition}:lakeformation:${local.region}:${local.account}:*" }
      }
    }]
  })
  tags = local.tags
}

resource "aws_iam_role_policy" "register" {
  name = "table-prefixes"
  role = aws_iam_role.register.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "TableObjects"
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = [
          "${local.bucket}/_system/*",
          "${local.bucket}/*/_warehouse/*",
        ]
      },
      {
        Sid      = "ListTablePrefixes"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [local.bucket]
        Condition = { StringLike = { "s3:prefix" = [
          "_system", "_system/*",
          "*/_warehouse", "*/_warehouse/*",
        ] } }
      },
    ]
  })
}

resource "aws_lakeformation_resource" "system" {
  arn                     = "${local.bucket}/_system"
  role_arn                = aws_iam_role.register.arn
  use_service_linked_role = false
  hybrid_access_enabled   = false

  # IAM is eventually consistent: Lake Formation checks the role at once.
  depends_on = [aws_iam_role_policy.register]
}

# Column tags (slice 1b): one Lake Formation tag key per workspace, with the
# values clavesa.json's "access" block declares plus "unreviewed", which no
# audience reads. The system database is "unreviewed" too, like each
# pipeline database (its stack tags it): a table or column no declaration
# tags is hidden from every audience. clavesa deploy tags the system tables
# it knows (runs, node_runs, tables "open"; column_stats and the error_msg
# columns hidden), so a system table a run creates later stays hidden until
# the next deploy or `clavesa workspace access sync`.
resource "aws_lakeformation_lf_tag" "workspace" {
  key    = var.tag_key
  values = var.tag_values
}

resource "aws_lakeformation_resource_lf_tags" "system_db" {
  database {
    name = var.system_database
  }
  lf_tag {
    key   = aws_lakeformation_lf_tag.workspace.key
    value = "unreviewed"
  }
}
