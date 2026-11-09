variable "databricks_host" {
  description = "Databricks workspace URL, https and no path."
  type        = string
}

variable "databricks_client_id" {
  description = "The service principal's application ID. Not a secret; the secret comes from DATABRICKS_CLIENT_SECRET."
  type        = string
}

variable "workspace_name" {
  description = "The clavesa workspace name. Prefixes the IAM role, the storage credential, the external location and the runner secret."
  type        = string
}

variable "bucket" {
  description = "The workspace bucket (module.workspace.pipeline_bucket). The external location covers all of it, so every pipeline's tables are under it."
  type        = string
}

variable "catalog" {
  description = "The Unity Catalog catalog (clavesa.json databricks.catalog). Created here only when create_catalog is true."
  type        = string
}

variable "readers" {
  description = "Databricks principals that read the published tables (SQL warehouse users). With create_catalog they get USE CATALOG, USE SCHEMA and SELECT on the catalog; the runner grants SELECT on each table it registers either way."
  type        = list(string)
  default     = []
}

variable "create_catalog" {
  description = "The --create-catalog opt-in (ADR-027 section 1): create the catalog, its managed storage on the workspace bucket and the grants. False: the catalog is the team's."
  type        = bool
  default     = false
}

variable "external_location" {
  description = <<-EOT
    The name of an external location a Databricks admin already created
    over the workspace bucket. With create_catalog and a null value
    (default) this module creates the IAM role, the storage credential and
    the location itself.
  EOT
  type        = string
  default     = null
}

variable "create_runner_secret" {
  description = <<-EOT
    Create the Secrets Manager secret the runner reads the service
    principal's OAuth secret from. True when the credential's secret
    reference is env: or file: (local only): `clavesa deploy` puts the
    value into this secret with PutSecretValue, so it never enters the
    Terraform state. False when the reference is already an arn:.
  EOT
  type        = bool
  default     = true
}

variable "kms_key_arn" {
  description = "The workspace bucket's KMS key ARN when the bucket uses SSE-KMS (module.workspace kms_key_id). Null for SSE-S3."
  type        = string
  default     = null
}

variable "tags" {
  description = "Additional tags for the AWS resources."
  type        = map(string)
  default     = {}
}
