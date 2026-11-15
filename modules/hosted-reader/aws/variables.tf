variable "workspace_name" {
  description = "The clavesa workspace name. It is also the sts:ExternalId the reader principals must send."
  type        = string
}

variable "role_name" {
  description = "Name of the read-only IAM role (clavesa-ui-reader-<workspace>, cut to the IAM limit by clavesa)."
  type        = string
}

variable "workgroup_name" {
  description = "Name of the Athena workgroup the reader queries through (clavesa-ui-<workspace>)."
  type        = string
}

variable "pipeline_bucket" {
  description = "The workspace's pipeline bucket (module.workspace.pipeline_bucket)."
  type        = string
}

variable "glue_database_patterns" {
  description = "Glue database name patterns the reader may read: <catalog>__* and <system_catalog>__*."
  type        = list(string)
}

variable "state_machine_patterns" {
  description = "Step Functions state machine name patterns of the workspace's pipelines (a prefix with * in workspace-scoped naming, exact names otherwise)."
  type        = list(string)
}

variable "log_group_patterns" {
  description = "CloudWatch Logs log group name patterns of the workspace's runner, sidecar and state machine log groups."
  type        = list(string)
}

variable "reader_principals" {
  description = "IAM principal ARNs allowed to assume the reader role (normally the hosted UI Lambda's role)."
  type        = list(string)
}

variable "state_object_patterns" {
  description = "Object key patterns in the pipeline bucket that hold Terraform state. The reader gets an explicit Deny on them."
  type        = list(string)
  default     = ["*.tfstate", "*.tfstate.*"]
}

variable "event_bus_arn" {
  description = "The hosted UI's clavesa-ui event bus. Empty: no forwarding rule."
  type        = string
  default     = ""
}

variable "athena_bytes_scanned_cutoff" {
  description = "Per-query scan limit of the reader's workgroup, in bytes."
  type        = number
  default     = 1073741824
}

variable "tags" {
  description = "Additional tags for every resource."
  type        = map(string)
  default     = {}
}
