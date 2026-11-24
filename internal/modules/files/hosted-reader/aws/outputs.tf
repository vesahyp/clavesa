output "reader_role_arn" {
  description = "The read-only role the hosted UI assumes, with sts:ExternalId = the workspace name."
  value       = aws_iam_role.reader.arn
}

output "workgroup_name" {
  description = "The Athena workgroup the reader queries through."
  value       = aws_athena_workgroup.reader.name
}

output "athena_output_location" {
  description = "Where the reader's Athena results land (the workgroup enforces it)."
  value       = "s3://${var.pipeline_bucket}/${local.athena_results_prefix}"
}

output "admin_role_arn" {
  description = "Column tags: the role the hosted UI assumes for its admins (group \"all\"), with sts:ExternalId = the workspace name. Empty without column tags."
  value       = local.admin ? aws_iam_role.admin[0].arn : ""
}

output "admin_workgroup_name" {
  description = "The admin role's Athena workgroup. Empty without column tags."
  value       = local.admin ? aws_athena_workgroup.admin[0].name : ""
}
