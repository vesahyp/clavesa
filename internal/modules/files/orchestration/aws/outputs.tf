output "state_machine_arn" {
  description = "ARN of the created Step Functions state machine."
  value       = aws_sfn_state_machine.pipeline.arn
}

output "state_machine_name" {
  description = "Name of the state machine (for CLI/console reference)."
  value       = aws_sfn_state_machine.pipeline.name
}

output "execution_role_arn" {
  description = "IAM role ARN used for state machine execution."
  value       = aws_iam_role.sfn_exec.arn
}

output "trigger_rule_arn" {
  description = "EventBridge rule ARN for the schedule trigger. null if var.schedule is not set."
  value       = var.schedule != null ? aws_cloudwatch_event_rule.schedule[0].arn : null
}
