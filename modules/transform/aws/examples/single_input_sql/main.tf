# Fixture: single-input SQL transform
#
# Validates that the module creates:
#   - aws_iam_role.lambda_exec
#   - aws_iam_role_policy.lambda_exec_policy
#   - aws_s3_object.logic                  (SQL body uploaded for the runner)
#   - aws_lambda_function.runner           (PySpark runner container)
#
# The runner_image must be a private ECR URI (Lambda image-digest pinning
# requires `aws_ecr_image`, which only resolves private repos). Pre-push
# the runner image to your workspace's ECR — `clavesa workspace init`
# does this for you. The placeholder URI here is enough for
# `terraform validate`; `terraform apply` would need a real one.

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

output "lambda_function_arn" {
  description = "Pass to the orchestration module's nodes[*].lambda_function_arn."
  value       = module.validate.lambda_function_arn
}
