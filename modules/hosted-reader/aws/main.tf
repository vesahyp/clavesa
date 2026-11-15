# Read-only access to one clavesa workspace for the hosted UI (ADR-032
# section 2). Created in the workspace's own account when clavesa.json
# has a `hosted` block:
#   - the reader role: trusted only by var.reader_principals, with
#     sts:ExternalId = the workspace name, and a policy that reads this
#     workspace's bucket, Glue databases, state machines and log groups;
#     the one write is Athena results under athena-results/clavesa-ui/;
#   - an Athena workgroup with an enforced output location and a
#     per-query scan limit, so a bad query has a capped cost;
#   - with var.event_bus_arn: EventBridge rules that forward the
#     workspace's Step Functions status changes and its _progress run
#     markers to the hosted bus, so the hosted summary refreshes on a run.
#
# No access key is created. Terraform state is never readable: the
# policy has an explicit Deny on the state object patterns.

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
data "aws_partition" "current" {}

locals {
  account   = data.aws_caller_identity.current.account_id
  region    = data.aws_region.current.region
  partition = data.aws_partition.current.partition
  bucket    = "arn:${local.partition}:s3:::${var.pipeline_bucket}"
  tags      = merge(var.tags, { "clavesa:workspace" = var.workspace_name, "clavesa:managed-by" = "clavesa" })

  athena_results_prefix = "athena-results/clavesa-ui/"

  state_machine_arns = [for p in var.state_machine_patterns : "arn:${local.partition}:states:${local.region}:${local.account}:stateMachine:${p}"]
  execution_arns     = [for p in var.state_machine_patterns : "arn:${local.partition}:states:${local.region}:${local.account}:execution:${p}:*"]
  log_group_arns = flatten([for p in var.log_group_patterns : [
    "arn:${local.partition}:logs:${local.region}:${local.account}:log-group:${p}",
    "arn:${local.partition}:logs:${local.region}:${local.account}:log-group:${p}:*",
  ]])
  glue_arns = concat(
    ["arn:${local.partition}:glue:${local.region}:${local.account}:catalog"],
    [for p in var.glue_database_patterns : "arn:${local.partition}:glue:${local.region}:${local.account}:database/${p}"],
    [for p in var.glue_database_patterns : "arn:${local.partition}:glue:${local.region}:${local.account}:table/${p}/*"],
  )

  forward = var.event_bus_arn != ""
}

resource "aws_iam_role" "reader" {
  name                 = var.role_name
  description          = "Read-only access to clavesa workspace ${var.workspace_name} for the hosted UI (ADR-032)."
  max_session_duration = 3600
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "HostedUI"
      Effect    = "Allow"
      Principal = { AWS = var.reader_principals }
      Action    = "sts:AssumeRole"
      Condition = { StringEquals = { "sts:ExternalId" = var.workspace_name } }
    }]
  })
  tags = local.tags
}

resource "aws_iam_role_policy" "reader" {
  name = "read-workspace"
  role = aws_iam_role.reader.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ReadBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [local.bucket]
      },
      {
        Sid      = "ReadObjects"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["${local.bucket}/*"]
      },
      {
        Sid      = "WriteAthenaResults"
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:AbortMultipartUpload"]
        Resource = ["${local.bucket}/${local.athena_results_prefix}*"]
      },
      {
        Sid      = "NoTerraformState"
        Effect   = "Deny"
        Action   = ["s3:GetObject", "s3:GetObjectVersion"]
        Resource = [for p in var.state_object_patterns : "${local.bucket}/${p}"]
      },
      {
        Sid      = "ReadGlue"
        Effect   = "Allow"
        Action   = ["glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:GetPartitions"]
        Resource = local.glue_arns
      },
      {
        Sid      = "QueryAthena"
        Effect   = "Allow"
        Action   = ["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults", "athena:GetWorkGroup"]
        Resource = [aws_athena_workgroup.reader.arn]
      },
      {
        Sid      = "ReadStateMachines"
        Effect   = "Allow"
        Action   = ["states:ListExecutions", "states:DescribeStateMachine"]
        Resource = local.state_machine_arns
      },
      {
        Sid      = "ReadExecutions"
        Effect   = "Allow"
        Action   = ["states:DescribeExecution", "states:GetExecutionHistory"]
        Resource = local.execution_arns
      },
      {
        Sid      = "ReadLogs"
        Effect   = "Allow"
        Action   = ["logs:FilterLogEvents", "logs:GetLogEvents", "logs:DescribeLogStreams"]
        Resource = local.log_group_arns
      },
    ]
  })
}

resource "aws_athena_workgroup" "reader" {
  name          = var.workgroup_name
  description   = "Hosted UI reads of clavesa workspace ${var.workspace_name} (ADR-032)."
  state         = "ENABLED"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff

    result_configuration {
      output_location = "s3://${var.pipeline_bucket}/${local.athena_results_prefix}"
    }
  }

  tags = local.tags
}

# ---------------------------------------------------------------------------
# Forwarding to the hosted bus (only with var.event_bus_arn)
# ---------------------------------------------------------------------------

# S3 sends events to EventBridge only when the bucket says so. This is
# the bucket's whole notification configuration (Terraform owns it), so a
# source module with manage_bucket_notifications on this same bucket would
# fight it; clavesa sources point at their own data buckets.
resource "aws_s3_bucket_notification" "eventbridge" {
  count       = local.forward ? 1 : 0
  bucket      = var.pipeline_bucket
  eventbridge = true
}

resource "aws_cloudwatch_event_rule" "sfn_status" {
  count       = local.forward ? 1 : 0
  name        = "${var.workgroup_name}-sfn"
  description = "Step Functions status changes of clavesa workspace ${var.workspace_name}, to the hosted UI."
  event_pattern = jsonencode({
    source        = ["aws.states"]
    "detail-type" = ["Step Functions Execution Status Change"]
    detail = {
      stateMachineArn = [for a in local.state_machine_arns : { wildcard = a }]
    }
  })
  tags = local.tags
}

resource "aws_cloudwatch_event_rule" "progress" {
  count       = local.forward ? 1 : 0
  name        = "${var.workgroup_name}-progress"
  description = "Run markers (_progress/<run>/_run.json) of clavesa workspace ${var.workspace_name}, to the hosted UI."
  event_pattern = jsonencode({
    source        = ["aws.s3"]
    "detail-type" = ["Object Created"]
    detail = {
      bucket = { name = [var.pipeline_bucket] }
      object = { key = [{ wildcard = "_progress/*/_run.json" }] }
    }
  })
  tags = local.tags
}

# A rule that targets an event bus in another account needs a role that
# may put events on that bus.
resource "aws_iam_role" "forward" {
  count = local.forward ? 1 : 0
  name  = "${var.role_name}-fwd"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = local.tags
}

resource "aws_iam_role_policy" "forward" {
  count = local.forward ? 1 : 0
  name  = "put-hosted-bus"
  role  = aws_iam_role.forward[0].id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "events:PutEvents", Resource = var.event_bus_arn }]
  })
}

resource "aws_cloudwatch_event_target" "sfn_status" {
  count    = local.forward ? 1 : 0
  rule     = aws_cloudwatch_event_rule.sfn_status[0].name
  arn      = var.event_bus_arn
  role_arn = aws_iam_role.forward[0].arn
}

resource "aws_cloudwatch_event_target" "progress" {
  count    = local.forward ? 1 : 0
  rule     = aws_cloudwatch_event_rule.progress[0].name
  arn      = var.event_bus_arn
  role_arn = aws_iam_role.forward[0].arn
}
