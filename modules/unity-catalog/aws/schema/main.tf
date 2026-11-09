# One clavesa pipeline's schema in Unity Catalog (ADR-027). Called from the
# pipeline's generated orchestration.tf. The runner registers the tables;
# this module owns only the schema. force_destroy stays false: a destroy
# does not drop a schema that still holds tables.

terraform {
  required_version = ">= 1.3"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = ">= 1.50"
    }
  }
}

# Configured here for the same reason as in the parent module: the
# caller's root already has its one required_providers block. The client
# secret comes from DATABRICKS_CLIENT_SECRET, set by `clavesa deploy`.
provider "databricks" {
  host      = var.databricks_host
  client_id = var.databricks_client_id
}

variable "databricks_host" {
  description = "Databricks workspace URL, https and no path."
  type        = string
}

variable "databricks_client_id" {
  description = "The service principal's application ID."
  type        = string
}

variable "catalog" {
  description = "The Unity Catalog catalog (clavesa.json databricks.catalog)."
  type        = string
}

variable "schema" {
  description = "The pipeline's schema identifier (ADR-016). Hyphens become underscores, as on the Glue side."
  type        = string
}

variable "pipeline_name" {
  description = "The clavesa pipeline that owns the schema."
  type        = string
}

resource "databricks_schema" "pipeline" {
  catalog_name = var.catalog
  name         = replace(var.schema, "-", "_")
  comment      = "clavesa pipeline ${var.pipeline_name}: tables written and registered by clavesa. Read them; do not write to them."
  properties = {
    "clavesa.pipeline"   = var.pipeline_name
    "clavesa.managed-by" = "clavesa"
  }
}

output "full_name" {
  description = "<catalog>.<schema>"
  value       = databricks_schema.pipeline.id
}
