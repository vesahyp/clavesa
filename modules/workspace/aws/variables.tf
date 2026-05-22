variable "workspace_name" {
  description = "Unique name for this workspace. Used as prefix for the shared S3 bucket and Athena workgroup."
  type        = string
}

variable "tags" {
  description = "Additional tags to apply to all resources created by this module."
  type        = map(string)
  default     = {}
}

variable "force_destroy" {
  description = <<-EOT
    Allow `terraform destroy` to delete the workspace bucket even when it
    contains objects. Defaults to false because the bucket holds the entire
    workspace's Iceberg warehouse, run-history tables, and Athena results —
    losing them is unrecoverable. Set to true only for ephemeral test
    workspaces that are intended to be torn down.
  EOT
  type        = bool
  default     = false
}

variable "system_catalog" {
  description = <<-EOT
    Workspace-owned observability catalog (ADR-016). Hosts the multi-writer
    `pipelines` schema (runs / node_runs / tables) and future system schemas
    (`query.history`, `billing.run_costs`, `access.audit`). Mirrors
    Databricks Unity Catalog's account-level `system` catalog. Glue
    encoding: `<system_catalog>__pipelines` as the database name.
  EOT
  type        = string
}

variable "athena_results_retention_days" {
  description = <<-EOT
    How long to keep Athena query-result objects under `athena-results/`
    and `*/_athena-results/`. The workspace runs one Athena query per
    Catalog page hit, dashboard widget reload, and runs_writer INSERT;
    each writes a result object that's only useful until the caller reads
    it. Default 14 days bounds the steady-state count without making
    debugging-by-replay impossible. Set to 0 to disable the rule (objects
    keep forever, the pre-v0.26 default).
  EOT
  type        = number
  default     = 14
}

variable "kms_key_id" {
  description = <<-EOT
    Optional KMS key ID/ARN for server-side encryption of bucket objects.
    When null (default), the bucket uses SSE-S3 (AES256) with the AWS-
    managed key. Set this to a customer-managed KMS key ARN if your
    organization requires CMK encryption for this workspace's data.
  EOT
  type        = string
  default     = null
}
