# Fixture: single-input SQL transform
#
# Validates that the module creates:
#   - aws_s3_object.logic   (SQL body uploaded; the pipeline-level Lambda
#                            emitted by internal/orchestration/tfgen fetches
#                            this at runtime via _read_text("s3://...")).
#
# Per-transform Lambda / IAM resources were collapsed in v0.30+ — the
# pipeline Lambda handles every node, so this module is config-only
# for `compute = "lambda"`.

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

module "validate" {
  source = "../../"

  pipeline_name = "example-pipeline"
  name          = "validate"
  bucket        = "my-pipeline-data"

  # Three-level namespace (ADR-016). Catalog comes from the workspace
  # (clavesa.json); schema is per-pipeline. system_catalog points
  # at the workspace-owned observability DB (v0.20.0+).
  catalog        = "clavesa"
  schema         = "example_pipeline"
  system_catalog = "clavesa_system"

  inputs = {
    raw = {
      table_path    = "s3://my-source-bucket/example-pipeline/raw_events/"
      catalog_db    = "clavesa__example_pipeline"
      catalog_table = "raw_events__default"
      schema        = null
    }
  }

  language = "sql"
  sql      = "SELECT * FROM raw WHERE amount > 0"

  runner_image = "000000000000.dkr.ecr.us-east-1.amazonaws.com/example/transform-runner:latest"

  output_definitions = {
    "default" = {
      schema = {
        columns = [
          { name = "id",     type = "string" },
          { name = "amount", type = "decimal(10,2)", nullable = false },
          { name = "ts",     type = "timestamp" }
        ]
      }
    }
  }
}

output "outputs" {
  description = "Named output map keyed by output_definitions. Downstream nodes reference module.validate.outputs[\"default\"]."
  value       = module.validate.outputs
}
