output "register_role_arn" {
  description = "The role each pipeline registers its <pipeline>/_warehouse/ prefix with."
  value       = aws_iam_role.register.arn
}

output "system_location_arn" {
  description = "The registered _system/ location; the runners and runs writers get DATA_LOCATION_ACCESS on it."
  value       = aws_lakeformation_resource.system.arn
}

output "tag_key" {
  description = "The workspace's Lake Formation tag key; each pipeline tags its database with it."
  value       = aws_lakeformation_lf_tag.workspace.key
}
