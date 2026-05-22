# Fixture: basic destination module
#
# Destinations are pass-through path declarations — the module computes
# the final S3 target_path from var.bucket/prefix/name and exposes it
# for the orchestration emitter to thread into the upstream transform's
# runner outputs. No Lambda, no Glue Job, no IAM role: the upstream
# transform's runner role gets the write permission via the
# orchestration emitter.

terraform {
  required_version = ">= 1.3"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.6"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

module "warehouse" {
  source = "../../"

  pipeline_name = "example-pipeline"
  name          = "warehouse"

  # Upstream transform output. In a real pipeline this is
  # module.<transform>.outputs["<key>"] passed verbatim.
  input = {
    table_path    = "s3://my-pipeline-data/example-pipeline/validate/valid/"
    catalog_db    = "clavesa__example_pipeline"
    catalog_table = "validate__valid"
    schema = {
      columns = [
        { name = "id",     type = "string" },
        { name = "amount", type = "decimal(10,2)", nullable = false },
        { name = "ts",     type = "timestamp" },
      ]
      include_rescued_data = false
    }
  }

  bucket     = "analytics-warehouse"
  prefix     = "clean/events/"
  format     = "parquet"
  write_mode = "overwrite"
}

output "outputs" {
  description = "Always empty — destinations are terminal."
  value       = module.warehouse.outputs
}

output "target_path" {
  description = "Final S3 path the upstream transform writes to. Threaded by the orchestration emitter into the upstream node's outputs."
  value       = module.warehouse.target_path
}

output "metadata" {
  description = "Destination metadata for observability (target_path, format, write_mode, compression)."
  value       = module.warehouse.metadata
}
