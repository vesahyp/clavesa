variable "workspace_name" {
  description = "The clavesa workspace (clavesa.json name)."
  type        = string
}

variable "role_name" {
  description = "Name of the Lake Formation registration role, clavesa-lf-register-<workspace>."
  type        = string
}

variable "pipeline_bucket" {
  description = "The workspace's pipeline bucket (module.workspace.pipeline_bucket)."
  type        = string
}

variable "tags" {
  description = "Extra tags on the role."
  type        = map(string)
  default     = {}
}

variable "tag_key" {
  description = "The workspace's Lake Formation tag key, clavesa_<workspace>."
  type        = string
}

variable "tag_values" {
  description = "The tag values: the \"access\" block's tags plus \"unreviewed\"."
  type        = list(string)

  validation {
    condition     = contains(var.tag_values, "open") && contains(var.tag_values, "unreviewed")
    error_message = "tag_values must include \"open\" and \"unreviewed\"."
  }
}

variable "system_database" {
  description = "The workspace's system database (module.workspace.system_pipelines_db), tagged \"open\"."
  type        = string
}
