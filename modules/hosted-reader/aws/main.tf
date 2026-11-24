# Read-only access to one clavesa workspace for the hosted UI (ADR-032
# section 2). Created in the workspace's own account when clavesa.json
# has a `hosted` block:
#   - the reader role: trusted only by var.reader_principals, with
#     sts:ExternalId = the workspace name, and a policy that reads this
#     workspace's bucket, Glue databases, state machines, log groups and
#     schedule rules;
#     the one write is Athena results under athena-results/clavesa-ui/
#     (with var.lf_tag_key, under athena-results/clavesa-ui/<role>/);
#   - an Athena workgroup with an enforced output location and a
#     per-query scan limit, so a bad query has a capped cost;
#   - with var.event_bus_arn: EventBridge rules that forward the
#     workspace's Step Functions status changes and its _progress run
#     markers to the hosted bus, so the hosted summary refreshes on a run;
#   - for each database in var.lf_databases: Lake Formation grants, for
#     accounts where Lake Formation governs the Glue catalog;
#   - with var.lf_tag_key (column tags, docs/ideas/column-tags.md): one
#     tag grant on what is tagged "open" in place of the grants on all
#     tables, and no read of table files, Delta logs or run logs.
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

  # Column tags: a reader limited by tags keeps its results under its
  # own prefix, so no other role's results (which can hold columns it may
  # not read) are readable to it, and its own are not to others.
  tagged                = var.lf_tag_key != ""
  athena_results_prefix = local.tagged ? "athena-results/clavesa-ui/${var.role_name}/" : "athena-results/clavesa-ui/"

  # What the reader may GetObject. A tagged reader reads only clavesa's
  # own files (the tree copy, deployed.json, run markers) and its Athena
  # results: no table files and no Delta logs, whose statistics hold the
  # values of the first 32 columns. Lake Formation gives Athena the table
  # data for the columns the reader may read.
  readable_objects = local.tagged ? [
    "${local.bucket}/_clavesa/*",
    "${local.bucket}/_progress/*",
    "${local.bucket}/${local.athena_results_prefix}*",
  ] : ["${local.bucket}/*"]

  state_machine_arns = [for p in var.state_machine_patterns : "arn:${local.partition}:states:${local.region}:${local.account}:stateMachine:${p}"]
  # A pipeline's EventBridge rules are its state machine name plus
  # "-schedule" (trigger_schedule) or "-poller" (trigger_batch_window).
  # The bare name matched only a "<prefix>-*" pattern (smoke2), never the
  # exact names of a workspace on legacy aws_names (web-traffic).
  schedule_rule_arns = flatten([for p in var.state_machine_patterns : [
    "arn:${local.partition}:events:${local.region}:${local.account}:rule/${p}-schedule",
    "arn:${local.partition}:events:${local.region}:${local.account}:rule/${p}-poller",
  ]])
  execution_arns = [for p in var.state_machine_patterns : "arn:${local.partition}:states:${local.region}:${local.account}:execution:${p}:*"]
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
    Statement = concat([
      {
        Sid      = "BucketLocation"
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation"]
        Resource = [local.bucket]
      },
      merge({
        Sid      = "ListBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [local.bucket]
        },
        # A tagged reader lists only clavesa's own prefixes: a table's
        # partition directories can name row values.
        local.tagged ? { Condition = { StringLike = { "s3:prefix" = ["_clavesa/*", "_progress/*", "${local.athena_results_prefix}*"] } } } : {},
      ),
      {
        Sid      = "ReadObjects"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = local.readable_objects
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
      ],
      [
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
          # Whether a pipeline's schedule rule is enabled, so the summary
          # does not call a pipeline whose schedule is paused overdue
          # (v2.27.4). Pipeline rules share the state machine names.
          Sid      = "ReadScheduleRules"
          Effect   = "Allow"
          Action   = ["events:DescribeRule"]
          Resource = local.schedule_rule_arns
        },
        {
          # Lake Formation vends the S3 credentials of an LF-registered
          # location to Athena through this call. It has no resource ARN;
          # what the role may read is set by the grants below.
          Sid      = "LakeFormationDataAccess"
          Effect   = "Allow"
          Action   = ["lakeformation:GetDataAccess"]
          Resource = "*"
        },
      ],
      # Run logs: a failed node's log can quote a row, so a reader
      # limited by column tags reads none.
      local.tagged ? [] : [{
        Sid      = "ReadLogs"
        Effect   = "Allow"
        Action   = ["logs:FilterLogEvents", "logs:GetLogEvents", "logs:DescribeLogStreams"]
        Resource = local.log_group_arns
      }],
    )
  })
}

# ---------------------------------------------------------------------------
# Lake Formation grants (one pair per database in var.lf_databases)
# ---------------------------------------------------------------------------

# On an account where Lake Formation governs the Glue catalog, the IAM
# policy above is not enough: the reader also needs LF DESCRIBE on each
# database and SELECT + DESCRIBE on its tables, or Athena fails with
# "Insufficient Lake Formation permission(s)". The same pair the
# pipelines emit for their own roles (orchestration.tf). On an account
# that leaves the catalog to IAM (IAMAllowedPrincipals on each database)
# the grants pass through and change nothing. The deploying principal
# must be a Lake Formation data lake admin, as for a pipeline deploy.
# The reader's grants ignore the provider's read-back of permissions,
# which adds what the principal holds from elsewhere (the database's
# IAM_ALLOWED_PRINCIPALS ALL) and made every plan replace them
# (2026-11-17). They are replaced only when the declared permissions,
# kept in the terraform_data beside each, change.
resource "terraform_data" "reader_db_permissions" {
  input = ["DESCRIBE"]
}

resource "aws_lakeformation_permissions" "reader_db" {
  for_each    = toset(var.lf_databases)
  principal   = aws_iam_role.reader.arn
  permissions = terraform_data.reader_db_permissions.input
  database {
    name = each.value
  }

  lifecycle {
    ignore_changes       = [permissions, permissions_with_grant_option]
    replace_triggered_by = [terraform_data.reader_db_permissions]
  }
}

resource "terraform_data" "reader_tables_permissions" {
  input = ["SELECT", "DESCRIBE"]
}

# Without column tags: SELECT and DESCRIBE on every table of each
# database.
resource "aws_lakeformation_permissions" "reader_tables" {
  for_each    = local.tagged ? toset([]) : toset(var.lf_databases)
  principal   = aws_iam_role.reader.arn
  permissions = terraform_data.reader_tables_permissions.input
  table {
    database_name = each.value
    wildcard      = true
  }

  lifecycle {
    ignore_changes       = [permissions, permissions_with_grant_option]
    replace_triggered_by = [terraform_data.reader_tables_permissions]
  }
}

# With column tags: SELECT and DESCRIBE on the tables and columns tagged
# "open" under the workspace's key. A table or column with another value,
# or with the databases' "unreviewed", is not readable, and Glue GetTable
# leaves its columns out.
resource "terraform_data" "reader_tagged_permissions" {
  input = ["SELECT", "DESCRIBE"]
}

resource "aws_lakeformation_permissions" "reader_tagged" {
  count       = local.tagged ? 1 : 0
  principal   = aws_iam_role.reader.arn
  permissions = terraform_data.reader_tagged_permissions.input
  lf_tag_policy {
    resource_type = "TABLE"
    expression {
      key    = var.lf_tag_key
      values = ["open"]
    }
  }

  lifecycle {
    ignore_changes       = [permissions, permissions_with_grant_option]
    replace_triggered_by = [terraform_data.reader_tagged_permissions]
  }
}

resource "aws_athena_workgroup" "reader" {
  name          = var.workgroup_name
  description   = "Hosted UI reads of clavesa workspace ${var.workspace_name} (ADR-032)."
  state         = "ENABLED"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration = true
    # Metrics on (slice 3 review, finding 2): ProcessedBytes per query,
    # so the hosted UI's alarms can see what its reads scan. Free.
    publish_cloudwatch_metrics_enabled = true
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

# ---------------------------------------------------------------------------
# The admin role (column tags; docs/ideas/column-tags.md, section 2)
# ---------------------------------------------------------------------------

# With column tags the reader above reads only what is tagged "open". The
# hosted UI's admins (Cognito group "all") must read everything to
# administer the workspace, so the Lambda assumes this role for them: the
# read access the reader has without column tags, with named Lake
# Formation grants (every column, untagged tables included), its own
# workgroup and its own result prefix. Same trust as the reader.
locals {
  admin                = local.tagged && var.admin_role_name != ""
  admin_results_prefix = "athena-results/clavesa-ui/${var.admin_role_name}/"
  admin_principals     = [for p in var.reader_principals : p if can(regex(":role/", p))]
}

resource "aws_iam_role" "admin" {
  count                = local.admin ? 1 : 0
  name                 = var.admin_role_name
  description          = "Full read access to clavesa workspace ${var.workspace_name} for the hosted UI's admins (column tags)."
  max_session_duration = 3600
  # Trusted only by the role principals of reader_principals (the hosted
  # Lambda), never by an IAM user added to test what a viewer sees.
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "HostedUIAdmin"
      Effect    = "Allow"
      Principal = { AWS = local.admin_principals }
      Action    = "sts:AssumeRole"
      Condition = { StringEquals = { "sts:ExternalId" = var.workspace_name } }
    }]
  })
  tags = local.tags
}

resource "aws_iam_role_policy" "admin" {
  count = local.admin ? 1 : 0
  name  = "read-workspace-all"
  role  = aws_iam_role.admin[0].id
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
        Resource = ["${local.bucket}/${local.admin_results_prefix}*"]
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
        Resource = [aws_athena_workgroup.admin[0].arn]
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
      {
        Sid      = "ReadScheduleRules"
        Effect   = "Allow"
        Action   = ["events:DescribeRule"]
        Resource = local.schedule_rule_arns
      },
      {
        Sid      = "LakeFormationDataAccess"
        Effect   = "Allow"
        Action   = ["lakeformation:GetDataAccess"]
        Resource = "*"
      },
      # The admins role reads everything, so it must never share anything
      # out or change who reads what: no Resource Access Manager call, no
      # Lake Formation grant, no resource policy or ACL, whatever it is
      # given later (docs/ideas/access-model.md, "Changing grants, hosted
      # UI"). No Condition: the Deny holds in every case.
      {
        Sid    = "NoResourceSharing"
        Effect = "Deny"
        Action = [
          "ram:*",
          "lakeformation:Grant*",
          "lakeformation:BatchGrant*",
          "glue:PutResourcePolicy",
          "s3:PutBucketPolicy",
          "s3:PutObjectAcl",
        ]
        Resource = "*"
      },
    ]
  })
}

resource "aws_lakeformation_permissions" "admin_db" {
  for_each    = local.admin ? toset(var.lf_databases) : toset([])
  principal   = aws_iam_role.admin[0].arn
  permissions = terraform_data.reader_db_permissions.input
  # No grant option, and a grant option added outside Terraform is a
  # change the next deploy undoes: permissions_with_grant_option is not
  # in ignore_changes.
  permissions_with_grant_option = []
  database {
    name = each.value
  }

  lifecycle {
    ignore_changes       = [permissions]
    replace_triggered_by = [terraform_data.reader_db_permissions]
  }
}

# Named grants: every column of every table, whatever its tag.
resource "aws_lakeformation_permissions" "admin_tables" {
  for_each    = local.admin ? toset(var.lf_databases) : toset([])
  principal   = aws_iam_role.admin[0].arn
  permissions = terraform_data.reader_tables_permissions.input
  # No grant option, and a grant option added outside Terraform is a
  # change the next deploy undoes: permissions_with_grant_option is not
  # in ignore_changes.
  permissions_with_grant_option = []
  table {
    database_name = each.value
    wildcard      = true
  }

  lifecycle {
    ignore_changes       = [permissions]
    replace_triggered_by = [terraform_data.reader_tables_permissions]
  }
}

resource "aws_athena_workgroup" "admin" {
  count         = local.admin ? 1 : 0
  name          = var.admin_workgroup_name
  description   = "Hosted UI admin reads of clavesa workspace ${var.workspace_name} (column tags)."
  state         = "ENABLED"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff

    result_configuration {
      output_location = "s3://${var.pipeline_bucket}/${local.admin_results_prefix}"
    }
  }

  tags = local.tags
}
