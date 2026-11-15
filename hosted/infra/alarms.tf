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
