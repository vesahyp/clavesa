output "pipeline_bucket" {
  description = "S3 bucket name shared by all pipelines in this workspace."
  value       = aws_s3_bucket.workspace_bucket.bucket
}

output "athena_workgroup" {
  description = "Athena workgroup name shared by all pipelines in this workspace."
  value       = aws_athena_workgroup.workspace.name
}

output "system_catalog" {
  description = "Workspace-owned observability catalog identifier (ADR-016). Pipelines pass this to their transform + orchestration modules so runs/node_runs/tables writes land in the workspace's system Glue DB."
  value       = var.system_catalog
}

output "system_pipelines_db" {
  description = "Glue database name encoding `<system_catalog>__pipelines`. Direct handle for Athena/Glue queries that want to skip the catalog-schema translation step."
  value       = aws_glue_catalog_database.system_pipelines.name
}
