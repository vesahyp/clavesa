output "external_location" {
  description = "The external location over the workspace bucket: the admin-created one, the one this module created, or null."
  value       = local.location_name
}

output "catalog" {
  description = "The Unity Catalog catalog clavesa writes into."
  value       = var.create_catalog ? databricks_catalog.workspace[0].name : var.catalog
}

output "iam_role_arn" {
  description = "The IAM role Unity Catalog assumes; null unless this module created the storage."
  value       = local.create_storage ? aws_iam_role.unity_catalog[0].arn : null
}

output "runner_secret_arn" {
  description = "The Secrets Manager secret the runner reads the OAuth secret from; null when the credential already points at an arn:."
  value       = var.create_runner_secret ? aws_secretsmanager_secret.client_secret[0].arn : null
}
