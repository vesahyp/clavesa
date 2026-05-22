# --- Common variables (Sub-artifact A) ---

variable "tags" {
  description = "Additional tags merged on top of the module's own `clavesa:*` tags. Workspace-wide tags also apply via the AWS provider's default_tags block."
  type        = map(string)
  default     = {}
}

variable "pipeline_name" {
  description = "Pipeline-level namespace. Used in S3 paths, resource naming, and resource tagging. Must be unique within an AWS account and region."
  type        = string
}

variable "name" {
  description = "Unique node identifier within the pipeline. Used in resource naming and tagging."
  type        = string
}

# --- Destination-specific variables ---

variable "input" {
  description = "Single upstream output reference. Accepts the full output object from an upstream module (module.<name>.outputs[\"<key>\"])."
  type = object({
    table_path    = string
    catalog_db    = string
    catalog_table = string
    schema = optional(object({
      columns = list(object({
        name     = string
        type     = string
        nullable = optional(bool, true)
      }))
      include_rescued_data = optional(bool, false)
    }))
  })
}

variable "bucket" {
  description = "Target S3 bucket where the destination writes its output files."
  type        = string
}

variable "prefix" {
  description = "Target S3 key prefix. Empty string writes directly to the bucket root."
  type        = string
  default     = ""
}

variable "format" {
  description = "Output file format. One of: parquet, csv, json. No auto-detection — always explicit."
  type        = string
  default     = "parquet"
  validation {
    condition     = contains(["parquet", "csv", "json"], var.format)
    error_message = "format must be one of: parquet, csv, json."
  }
}

variable "write_mode" {
  description = "Write mode. 'overwrite' replaces all files under the prefix on each run. 'append' adds files without removing existing ones."
  type        = string
  default     = "overwrite"
  validation {
    condition     = contains(["overwrite", "append"], var.write_mode)
    error_message = "write_mode must be one of: overwrite, append."
  }
}

variable "compression" {
  description = "Compression codec. null uses the format default: snappy for parquet, none for csv and json."
  type        = string
  default     = null
  validation {
    condition     = var.compression == null ? true : contains(["snappy", "gzip", "none"], var.compression)
    error_message = "compression must be one of: snappy, gzip, none (or null to use format default)."
  }
}

