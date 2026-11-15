########################################################################
# Summary refresh (ADR-032 section 2).
#
#   workspace account: SFN status change / _progress/*/_run.json
#        └─ rule (emitted by the workspace deploy) ──> bus clavesa-ui ──> Lambda
#   05:00 UTC daily: rule on the default bus ───────────────────────────> Lambda
#
# The bus accepts PutEvents only from the registry's accounts. The
# publisher role lets the Mac write the local workspaces' summaries.
########################################################################

resource "aws_cloudwatch_event_bus" "ui" {
  name = var.pool_name
}

data "aws_iam_policy_document" "bus" {
  count = length(local.workspace_accounts) > 0 ? 1 : 0

  statement {
    sid       = "RegisteredWorkspaceAccounts"
    effect    = "Allow"
    actions   = ["events:PutEvents"]
    resources = [aws_cloudwatch_event_bus.ui.arn]
    principals {
      type        = "AWS"
      identifiers = [for a in local.workspace_accounts : "arn:aws:iam::${a}:root"]
    }
  }
}

resource "aws_cloudwatch_event_bus_policy" "ui" {
  count          = length(local.workspace_accounts) > 0 ? 1 : 0
  event_bus_name = aws_cloudwatch_event_bus.ui.name
  policy         = data.aws_iam_policy_document.bus[0].json
}

resource "aws_cloudwatch_event_rule" "refresh" {
  name           = "${var.pool_name}-refresh"
  description    = "Workspace run events: refresh that workspace's summary (ADR-032)."
  event_bus_name = aws_cloudwatch_event_bus.ui.name
  event_pattern = jsonencode({
    source = ["aws.states", "aws.s3"]
  })
}

resource "aws_cloudwatch_event_target" "refresh" {
  rule           = aws_cloudwatch_event_rule.refresh.name
  event_bus_name = aws_cloudwatch_event_bus.ui.name
  arn            = aws_lambda_function.api.arn
}

resource "aws_lambda_permission" "refresh" {
  statement_id  = "AllowRefreshRule"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.refresh.arn
}

# Daily, so freshness ages stay right on days with no run.
resource "aws_cloudwatch_event_rule" "daily" {
  name                = "${var.pool_name}-daily"
  description         = "Refresh every cloud workspace's summary once a day (ADR-032)."
  schedule_expression = var.daily_refresh_schedule
}

resource "aws_cloudwatch_event_target" "daily" {
  rule = aws_cloudwatch_event_rule.daily.name
  arn  = aws_lambda_function.api.arn
}

resource "aws_lambda_permission" "daily" {
  statement_id  = "AllowDailyRule"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily.arn
}

########################################################################
# The publisher role: a local-warehouse workspace's Mac assumes it to
# write summaries/<name>.json, for the local names in the registry only.
# No local workspace is registered: then the role has no policy and
# can write nothing.
########################################################################

resource "aws_iam_role" "publisher" {
  name        = "${var.pool_name}-publisher"
  description = "Writes the summaries of local-warehouse workspaces to the hosted UI (ADR-032)."
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = var.publisher_principals }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "publisher" {
  count = length(local.local_workspaces) > 0 ? 1 : 0
  name  = "put-local-summaries"
  role  = aws_iam_role.publisher.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "s3:PutObject"
      Resource = [for n in local.local_workspaces : "${aws_s3_bucket.data.arn}/summaries/${n}.json"]
    }]
  })
}
