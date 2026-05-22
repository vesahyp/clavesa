locals {
  all_tags = merge(var.tags, {
    "clavesa:pipeline" = var.pipeline_name
    "clavesa:type"     = "orchestration"
  })

  # ---------------------------------------------------------------------------
  # Graph analysis — successors and predecessors per node
  # ---------------------------------------------------------------------------

  successors = {
    for node_name in keys(var.nodes) :
    node_name => distinct([for e in var.edges : e.to if e.from == node_name])
  }

  predecessors = {
    for node_name in keys(var.nodes) :
    node_name => distinct([for e in var.edges : e.from if e.to == node_name])
  }

  # Entry node: the unique node with no predecessors (DAG start).
  entry_node = [
    for k in keys(var.nodes) :
    k if length(local.predecessors[k]) == 0
  ][0]

  fanout_nodes = toset([for k in keys(var.nodes) : k if length(local.successors[k]) > 1])
  fanin_nodes  = toset([for k in keys(var.nodes) : k if length(local.predecessors[k]) > 1])

  # Nodes interior to a fan-out branch (placed inside a Parallel state, not at top level).
  fanout_branch_nodes = toset([
    for node_name in keys(var.nodes) :
    node_name
    if length(local.predecessors[node_name]) == 1 &&
    contains(local.fanout_nodes, try(local.predecessors[node_name][0], ""))
  ])

  # ---------------------------------------------------------------------------
  # Error handling
  # ---------------------------------------------------------------------------

  error_catch_target = var.error_notification_topic != null ? "PipelineFailedNotify" : "PipelineFailed"

  retry_policy = [
    {
      ErrorEquals     = ["States.TaskFailed"]
      IntervalSeconds = 5
      MaxAttempts     = 3
      BackoffRate     = 2.0
      MaxDelaySeconds = 60
    }
  ]

  catch_policy = [
    {
      ErrorEquals = ["States.ALL"]
      Next        = local.error_catch_target
    }
  ]

  # ---------------------------------------------------------------------------
  # Lambda task parameters per node
  #
  # Every node is a Lambda invocation; the Clavesa runner is the one engine
  # that backs all compute targets (ADR 003 / ADR-012). The payload is the
  # exact event shape runner.handler() consumes.
  # ---------------------------------------------------------------------------

  # The Payload includes three SFN context-object fields ($$.Execution.Id /
  # $$.Execution.StartTime / $$.Execution.Input) so the runner can attribute
  # its node_runs row to a specific Step Functions execution and read the
  # `_trigger` value the start path stamped into the execution input. The
  # whole `$$.Execution.Input` object is forwarded (rather than `._trigger`
  # directly) because a JsonPath into a missing key fails the state at
  # runtime — referencing the context object never does. The runner ignores
  # unknown keys, so this is non-breaking for older runner images.
  node_parameters = {
    for node_name, node_config in var.nodes :
    node_name => {
      FunctionName = node_config.lambda_function_arn
      Payload = {
        inputs                       = node_config.inputs
        outputs                      = node_config.outputs
        "_sf_execution_arn.$"        = "$$.Execution.Id"
        "_sf_execution_started_at.$" = "$$.Execution.StartTime"
        "_execution_input.$"         = "$$.Execution.Input"
      }
    }
  }

  # ---------------------------------------------------------------------------
  # Next state target per node (null = terminal, emit End = true)
  # ---------------------------------------------------------------------------

  node_next = {
    for node_name in keys(var.nodes) :
    node_name => (
      contains(local.fanout_nodes, node_name) ? "${node_name}_Branches" :
      length(local.successors[node_name]) == 1 ? local.successors[node_name][0] :
      null
    )
  }

  # ---------------------------------------------------------------------------
  # Top-level Task states. Non-terminal and terminal nodes are built in
  # separate for-expressions so each is type-homogeneous; jsondecode(jsonencode)
  # then erases types when merged.
  # Fan-out branch nodes live inside Parallel branches — excluded here.
  # ---------------------------------------------------------------------------

  _task_states_with_next = {
    for node_name, node_config in var.nodes :
    node_name => jsondecode(jsonencode({
      Type           = "Task"
      Resource       = "arn:aws:states:::lambda:invoke"
      TimeoutSeconds = node_config.timeout_seconds
      Parameters     = local.node_parameters[node_name]
      ResultPath     = "$.runner_results.${node_name}"
      Retry          = local.retry_policy
      Catch          = local.catch_policy
      Next           = local.node_next[node_name]
    }))
    if local.node_next[node_name] != null && !contains(local.fanout_branch_nodes, node_name)
  }

  _task_states_terminal = {
    for node_name, node_config in var.nodes :
    node_name => jsondecode(jsonencode({
      Type           = "Task"
      Resource       = "arn:aws:states:::lambda:invoke"
      TimeoutSeconds = node_config.timeout_seconds
      Parameters     = local.node_parameters[node_name]
      ResultPath     = "$.runner_results.${node_name}"
      Retry          = local.retry_policy
      Catch          = local.catch_policy
      End            = true
    }))
    if local.node_next[node_name] == null && !contains(local.fanout_branch_nodes, node_name)
  }

  task_states = merge(local._task_states_with_next, local._task_states_terminal)

  # ---------------------------------------------------------------------------
  # Fan-out: Parallel states. One branch per successor; each branch is a single
  # Task state (one level deep). Uncaught branch failures propagate up to the
  # Parallel state's Catch.
  # ---------------------------------------------------------------------------

  _fanout_convergence = {
    for node_name in local.fanout_nodes :
    node_name => [
      for k in keys(var.nodes) :
      k if contains(local.fanin_nodes, k) && anytrue([
        for succ in local.successors[node_name] : contains(local.successors[succ], k)
      ])
    ]
  }

  _fanout_with_next = {
    for node_name in local.fanout_nodes :
    "${node_name}_Branches" => jsondecode(jsonencode({
      Type = "Parallel"
      Branches = [
        for successor in local.successors[node_name] : {
          StartAt = successor
          States = {
            (successor) = jsondecode(jsonencode({
              Type           = "Task"
              Resource       = "arn:aws:states:::lambda:invoke"
              TimeoutSeconds = var.nodes[successor].timeout_seconds
              Parameters     = local.node_parameters[successor]
              Retry          = local.retry_policy
              End            = true
            }))
          }
        }
      ]
      Retry = local.retry_policy
      Catch = local.catch_policy
      Next  = local._fanout_convergence[node_name][0]
    }))
    if length(local._fanout_convergence[node_name]) > 0
  }

  _fanout_terminal = {
    for node_name in local.fanout_nodes :
    "${node_name}_Branches" => jsondecode(jsonencode({
      Type = "Parallel"
      Branches = [
        for successor in local.successors[node_name] : {
          StartAt = successor
          States = {
            (successor) = jsondecode(jsonencode({
              Type           = "Task"
              Resource       = "arn:aws:states:::lambda:invoke"
              TimeoutSeconds = var.nodes[successor].timeout_seconds
              Parameters     = local.node_parameters[successor]
              Retry          = local.retry_policy
              End            = true
            }))
          }
        }
      ]
      Retry = local.retry_policy
      Catch = local.catch_policy
      End   = true
    }))
    if length(local._fanout_convergence[node_name]) == 0
  }

  fanout_parallel_states = merge(local._fanout_with_next, local._fanout_terminal)

  # ---------------------------------------------------------------------------
  # Error handling states
  # ---------------------------------------------------------------------------

  pipeline_failed_state = jsondecode(jsonencode({
    Type  = "Fail"
    Error = "PipelineFailed"
    Cause = "A pipeline node failed. Check CloudWatch Logs for execution details."
  }))

  pipeline_notify_state = var.error_notification_topic != null ? jsondecode(jsonencode({
    Type     = "Task"
    Resource = "arn:aws:states:::sns:publish"
    Parameters = {
      TopicArn    = var.error_notification_topic
      Subject     = "Clavesa Pipeline Failed: ${var.pipeline_name}"
      "Message.$" = "States.Format('Pipeline {} failed in state {}. Error: {}. Cause: {}', '${var.pipeline_name}', $$.State.Name, $.Error, $.Cause)"
    }
    Next = "PipelineFailed"
  })) : null

  asl_states = merge(
    local.task_states,
    local.fanout_parallel_states,
    { "PipelineFailed" = local.pipeline_failed_state },
    var.error_notification_topic != null
      ? { "PipelineFailedNotify" = local.pipeline_notify_state }
      : {}
  )

  asl_definition = jsonencode({
    Comment = "Clavesa pipeline: ${var.pipeline_name}"
    StartAt = local.entry_node
    States  = local.asl_states
  })
}

# ---------------------------------------------------------------------------
# Glue Data Catalog database — metastore for Iceberg tables produced by this
# pipeline's transforms (per ADR-013). Free; tables register here so Athena,
# EMR, Redshift Spectrum, etc. can query the pipeline's outputs natively.
# Database name follows the ADR-016 encoder: `<catalog>__<schema>`. Mirrors
# `internal/identutil.EncodeGlueDatabase` on the Go side and `_glue_db()`
# in the runner (and modules/transform/aws's local.catalog_db). All four
# encoders MUST stay byte-identical so the runner writes to the DB
# terraform created.
# ---------------------------------------------------------------------------

locals {
  _safe_catalog = replace(var.catalog, "-", "_")
  _safe_schema  = replace(var.schema, "-", "_")
  catalog_db    = "${local._safe_catalog}__${local._safe_schema}"
}

resource "aws_glue_catalog_database" "pipeline" {
  name        = local.catalog_db
  description = "Clavesa pipeline output tables — ${var.pipeline_name}"
  tags        = local.all_tags
}

# ---------------------------------------------------------------------------
# CloudWatch Log Group — execution logging (90-day retention)
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "sfn_logs" {
  name              = "/clavesa/${var.pipeline_name}/sfn"
  retention_in_days = 90
  tags              = local.all_tags
}

# ---------------------------------------------------------------------------
# IAM execution role for the Step Functions state machine
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "sfn_assume" {
  statement {
    sid     = "StepFunctionsTrust"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sfn_exec" {
  name               = "clavesa-${var.pipeline_name}-orchestration"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json
  tags               = local.all_tags
}

resource "aws_iam_role_policy" "sfn_exec_policy" {
  name = "clavesa-${var.pipeline_name}-orchestration"
  role = aws_iam_role.sfn_exec.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      [
        {
          Sid      = "LambdaInvoke"
          Effect   = "Allow"
          Action   = ["lambda:InvokeFunction"]
          Resource = "*"
        },
        {
          Sid    = "CloudWatchLogsDelivery"
          Effect = "Allow"
          Action = [
            "logs:CreateLogDelivery",
            "logs:GetLogDelivery",
            "logs:UpdateLogDelivery",
            "logs:DeleteLogDelivery",
            "logs:ListLogDeliveries",
            "logs:PutResourcePolicy",
            "logs:DescribeResourcePolicies",
            "logs:DescribeLogGroups",
          ]
          Resource = "*"
        },
      ],
      var.error_notification_topic != null ? [
        {
          Sid      = "SnsPublishFailure"
          Effect   = "Allow"
          Action   = ["sns:Publish"]
          Resource = [var.error_notification_topic]
        }
      ] : []
    )
  })
}

# ---------------------------------------------------------------------------
# Step Functions state machine (Standard Workflow)
# ---------------------------------------------------------------------------

resource "aws_sfn_state_machine" "pipeline" {
  name     = "clavesa-${var.pipeline_name}"
  role_arn = aws_iam_role.sfn_exec.arn
  type     = "STANDARD"

  definition = local.asl_definition

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn_logs.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  tags = local.all_tags

  depends_on = [aws_iam_role_policy.sfn_exec_policy]
}

# ---------------------------------------------------------------------------
# EventBridge schedule trigger (only when var.schedule is set)
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_event_rule" "schedule" {
  count = var.schedule != null ? 1 : 0

  name                = "clavesa-${var.pipeline_name}-schedule"
  description         = "Scheduled trigger for Clavesa pipeline: ${var.pipeline_name}"
  schedule_expression = var.schedule
  tags                = local.all_tags
}

data "aws_iam_policy_document" "events_assume" {
  count = var.schedule != null ? 1 : 0

  statement {
    sid     = "EventBridgeTrust"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "events_trigger" {
  count = var.schedule != null ? 1 : 0

  name               = "clavesa-${var.pipeline_name}-trigger"
  assume_role_policy = data.aws_iam_policy_document.events_assume[0].json
  tags               = local.all_tags
}

resource "aws_iam_role_policy" "events_trigger_policy" {
  count = var.schedule != null ? 1 : 0

  name = "clavesa-${var.pipeline_name}-trigger"
  role = aws_iam_role.events_trigger[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "StartExecution"
        Effect   = "Allow"
        Action   = ["states:StartExecution"]
        Resource = [aws_sfn_state_machine.pipeline.arn]
      }
    ]
  })
}

resource "aws_cloudwatch_event_target" "schedule" {
  count = var.schedule != null ? 1 : 0

  rule     = aws_cloudwatch_event_rule.schedule[0].name
  arn      = aws_sfn_state_machine.pipeline.arn
  role_arn = aws_iam_role.events_trigger[0].arn

  # _trigger gets parsed by the runs_writer Lambda from detail.input on the
  # Step Functions execution status-change event and written verbatim to
  # `<database>.runs.trigger`. Allowed values (kept in sync with
  # runs_writer/index.py:TRIGGER_VALUES):
  #
  #   "scheduled" — this EventBridge target (cron / rate schedule).
  #   "event"     — SQS poller Lambda (poller.py), fired by source queues.
  #   "manual"    — anything else (CLI run, console click, missing/malformed input).
  #
  # Any new start path must set one of these. Smuggling through the input is
  # far cheaper than a CloudTrail subscription.
  input = jsonencode({
    pipeline = var.pipeline_name
    _trigger = "scheduled"
  })
}

# ---------------------------------------------------------------------------
# SQS poller — Lambda that checks source queues on a schedule and starts the
# state machine when any queue has messages. Only created when both
# trigger_queue_arns and trigger_batch_window are set.
# ---------------------------------------------------------------------------

locals {
  enable_poller = length(var.trigger_queue_arns) > 0 && var.trigger_batch_window != null
}

data "archive_file" "poller" {
  count       = local.enable_poller ? 1 : 0
  type        = "zip"
  source_file = "${path.module}/poller.py"
  output_path = "${path.module}/poller.zip"
}

data "aws_iam_policy_document" "poller_assume" {
  count = local.enable_poller ? 1 : 0

  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "poller" {
  count              = local.enable_poller ? 1 : 0
  name               = "clavesa-${var.pipeline_name}-poller"
  assume_role_policy = data.aws_iam_policy_document.poller_assume[0].json
  tags               = local.all_tags
}

resource "aws_iam_role_policy" "poller" {
  count = local.enable_poller ? 1 : 0
  name  = "clavesa-${var.pipeline_name}-poller"
  role  = aws_iam_role.poller[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "SQSPoll"
        Effect   = "Allow"
        Action   = ["sqs:ReceiveMessage", "sqs:PurgeQueue"]
        Resource = var.trigger_queue_arns
      },
      {
        Sid      = "SFNStart"
        Effect   = "Allow"
        Action   = ["states:StartExecution"]
        Resource = [aws_sfn_state_machine.pipeline.arn]
      },
      {
        Sid      = "Logs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = ["arn:aws:logs:*:*:*"]
      }
    ]
  })
}

resource "aws_lambda_function" "poller" {
  count            = local.enable_poller ? 1 : 0
  function_name    = "clavesa-${var.pipeline_name}-poller"
  role             = aws_iam_role.poller[0].arn
  runtime          = "python3.12"
  handler          = "poller.handler"
  filename         = data.archive_file.poller[0].output_path
  source_code_hash = data.archive_file.poller[0].output_base64sha256
  timeout          = 30

  environment {
    variables = {
      QUEUE_ARNS        = jsonencode(var.trigger_queue_arns)
      STATE_MACHINE_ARN = aws_sfn_state_machine.pipeline.arn
    }
  }

  tags = local.all_tags
}

resource "aws_cloudwatch_event_rule" "poller" {
  count               = local.enable_poller ? 1 : 0
  name                = "clavesa-${var.pipeline_name}-poller"
  description         = "Polls source queues for ${var.pipeline_name} at ${var.trigger_batch_window}"
  schedule_expression = var.trigger_batch_window
  tags                = local.all_tags
}

resource "aws_cloudwatch_event_target" "poller" {
  count = local.enable_poller ? 1 : 0
  rule  = aws_cloudwatch_event_rule.poller[0].name
  arn   = aws_lambda_function.poller[0].arn
}

resource "aws_lambda_permission" "poller" {
  count         = local.enable_poller ? 1 : 0
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.poller[0].function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.poller[0].arn
}

# ---------------------------------------------------------------------------
# runs writer — EventBridge subscription on Step Functions terminal status
# changes. Tiny Python Lambda appends one row per execution to
#   clavesa_<pipeline>.runs
# (Iceberg via Athena INSERT INTO). Pairs with the runner-populated
# node_runs table — joining on sf_execution_arn answers "which nodes ran in
# this execution?" once the runner threads the ARN through (separate slice).
#
# Gated on var.bucket — when null the whole stack is skipped, so existing
# pipelines without a bucket wired in keep working unchanged.
# ---------------------------------------------------------------------------

locals {
  enable_runs_writer = var.bucket != null
}

data "archive_file" "runs_writer" {
  count       = local.enable_runs_writer ? 1 : 0
  type        = "zip"
  source_dir  = "${path.module}/runs_writer"
  output_path = "${path.module}/runs_writer.zip"
}

data "aws_iam_policy_document" "runs_writer_assume" {
  count = local.enable_runs_writer ? 1 : 0

  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "runs_writer" {
  count              = local.enable_runs_writer ? 1 : 0
  name               = "clavesa-${var.pipeline_name}-runs-writer"
  assume_role_policy = data.aws_iam_policy_document.runs_writer_assume[0].json
  tags               = local.all_tags
}

resource "aws_iam_role_policy" "runs_writer" {
  count = local.enable_runs_writer ? 1 : 0
  name  = "clavesa-${var.pipeline_name}-runs-writer"
  role  = aws_iam_role.runs_writer[0].id

  # Athena needs StartQueryExecution + GetQueryExecution to drive DDL/DML;
  # Glue needs full table ops on this pipeline's database (CREATE TABLE
  # via Athena Iceberg mutates the Glue table); S3 needs read+write on the
  # warehouse bucket for Athena query results, the Iceberg manifest, and
  # the data files.
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AthenaQuery"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:GetWorkGroup",
        ]
        Resource = "*"
      },
      {
        Sid    = "GlueCatalog"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:GetPartition",
          "glue:GetPartitions",
        ]
        Resource = [
          "arn:aws:glue:*:*:catalog",
          # System observability DB (workspace-wide, ADR-016 v0.20.0).
          "arn:aws:glue:*:*:database/${replace(var.system_catalog, "-", "_")}__pipelines",
          "arn:aws:glue:*:*:table/${replace(var.system_catalog, "-", "_")}__pipelines/*",
        ]
      },
      {
        Sid    = "S3Warehouse"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [
          "arn:aws:s3:::${var.bucket}",
          "arn:aws:s3:::${var.bucket}/*",
        ]
      },
      {
        Sid      = "Logs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = ["arn:aws:logs:*:*:*"]
      },
    ]
  })
}

resource "aws_lambda_function" "runs_writer" {
  count            = local.enable_runs_writer ? 1 : 0
  function_name    = "clavesa-${var.pipeline_name}-runs-writer"
  role             = aws_iam_role.runs_writer[0].arn
  runtime          = "python3.12"
  handler          = "index.handler"
  filename         = data.archive_file.runs_writer[0].output_path
  source_code_hash = data.archive_file.runs_writer[0].output_base64sha256
  # Athena DDL + INSERT against an empty Iceberg table runs in 5–10s; allow
  # headroom for cold starts and the rare slow query.
  timeout = 60

  environment {
    variables = {
      CLAVESA_PIPELINE         = var.pipeline_name
      # System-catalog observability DB (ADR-016 v0.20.0). Encoding mirrors
      # `<system_catalog>__pipelines` — same `<catalog>__<schema>` shape used
      # for user pipelines. The `pipelines` schema is workspace-wide and
      # multi-writer; every pipeline's runs_writer appends here.
      CLAVESA_DATABASE         = "${replace(var.system_catalog, "-", "_")}__pipelines"
      CLAVESA_WAREHOUSE_BUCKET = var.bucket
    }
  }

  tags = local.all_tags
}

resource "aws_cloudwatch_event_rule" "runs" {
  count       = local.enable_runs_writer ? 1 : 0
  name        = "clavesa-${var.pipeline_name}-runs"
  description = "Captures terminal Step Functions execution events for ${var.pipeline_name} into the runs Iceberg table"

  event_pattern = jsonencode({
    source        = ["aws.states"]
    "detail-type" = ["Step Functions Execution Status Change"]
    detail = {
      stateMachineArn = [aws_sfn_state_machine.pipeline.arn]
      status          = ["SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"]
    }
  })

  tags = local.all_tags
}

resource "aws_cloudwatch_event_target" "runs" {
  count = local.enable_runs_writer ? 1 : 0
  rule  = aws_cloudwatch_event_rule.runs[0].name
  arn   = aws_lambda_function.runs_writer[0].arn
}

resource "aws_lambda_permission" "runs" {
  count         = local.enable_runs_writer ? 1 : 0
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.runs_writer[0].function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.runs[0].arn
}
