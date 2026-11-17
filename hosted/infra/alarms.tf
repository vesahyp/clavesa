########################################################################
# Alarms to the phone (ADR-032 section 4): a Lambda error, or a summary
# refresh that failed, pushes through the jeeves-push SNS topic.
########################################################################

resource "aws_cloudwatch_metric_alarm" "api_errors" {
  alarm_name          = "${local.api_name}-errors"
  alarm_description   = "The hosted clavesa UI Lambda failed an invocation (a crash, or an event refresh that failed)."
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = aws_lambda_function.api.function_name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_topic_arn == "" ? [] : [var.alarm_topic_arn]
}

# Every failed refresh logs a line that starts with REFRESH_FAILED
# (internal/hosted.RefreshFailedMarker).
resource "aws_cloudwatch_log_metric_filter" "refresh_failed" {
  name           = "${var.pool_name}-refresh-failed"
  log_group_name = aws_cloudwatch_log_group.api.name
  pattern        = "REFRESH_FAILED"

  metric_transformation {
    name          = "RefreshFailures"
    namespace     = "Clavesa/HostedUI"
    value         = "1"
    default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "refresh_failed" {
  alarm_name          = "${var.pool_name}-refresh-failed"
  alarm_description   = "A hosted clavesa UI summary refresh failed: a workspace card is going stale. See the ${aws_cloudwatch_log_group.api.name} log group."
  namespace           = "Clavesa/HostedUI"
  metric_name         = "RefreshFailures"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_topic_arn == "" ? [] : [var.alarm_topic_arn]
}

# What the hosted UI's reads scan in Athena (slice 3 review, finding 2):
# one alarm per registered cloud workspace whose reader workgroup is in
# this account and region (CloudWatch alarms are per account and
# region). A normal day scans well under 1 GiB (about 50 MB per
# dashboard open); 2 GiB in one hour (about $0.01) means a loop or a
# misuse, so it pushes within the hour rather than the next day.
locals {
  athena_alarm_workspaces = [
    for w in local.workspaces : w.name
    if w.kind == "cloud" && try(w.account, "") == data.aws_caller_identity.current.account_id && try(w.region, "") == var.region
  ]
}

resource "aws_cloudwatch_metric_alarm" "reader_scan" {
  for_each            = toset(local.athena_alarm_workspaces)
  alarm_name          = "${var.pool_name}-athena-scan-${each.value}"
  alarm_description   = "The hosted UI's reads of ${each.value} scanned more than 2 GiB in an hour in Athena workgroup clavesa-ui-${each.value}."
  namespace           = "AWS/Athena"
  metric_name         = "ProcessedBytes"
  dimensions          = { WorkGroup = "clavesa-ui-${each.value}" }
  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 1
  threshold           = 2147483648
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_topic_arn == "" ? [] : [var.alarm_topic_arn]
}
