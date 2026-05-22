# Example: linear source → transform → destination pipeline
#
# Validates that the module creates:
#   - aws_sfn_state_machine.pipeline   (Standard Workflow, ASL with Task states)
#   - aws_iam_role.sfn_exec            (execution role + Lambda invoke policy)
#   - aws_iam_role_policy.sfn_exec_policy
#   - aws_cloudwatch_log_group.sfn_logs
#   - aws_cloudwatch_event_rule.schedule[0]    (when var.schedule is set)
#   - aws_cloudwatch_event_target.schedule[0]
#   - aws_glue_catalog_database.pipeline_db    (encoded <catalog>__<schema>)
#
# In a real pipeline, lambda_function_arn values come from
# module.<transform>.lambda_function_arn (transform/aws). Here we pass
# dummy ARNs so the example is self-contained for `terraform validate`.

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

module "pipeline" {
  source = "../../"

  pipeline_name = "basic-etl"

  # Three-level namespace (ADR-016). Catalog comes from the workspace
  # (`clavesa.json`); schema is per-pipeline. system_catalog points
  # at the workspace-owned observability DB (v0.20.0+).
  catalog        = "clavesa"
  schema         = "basic_etl"
  system_catalog = "clavesa_system"

  nodes = {
    validate_orders = {
      lambda_function_arn = "arn:aws:lambda:us-east-1:000000000000:function:basic-etl-validate-orders"
      inputs = {
        raw = "s3://example-source-bucket/orders/"
      }
      outputs = {
        default = "" # empty string → Iceberg auto-table at <catalog>.<schema>.validate_orders__default
      }
      timeout_seconds = 300
    }
  }

  # No edges in a single-node example. With multiple nodes:
  #   edges = [{ from = "extract", from_output = "default", to = "transform" }]
  edges = []

  # Run once a day at 02:00 UTC (optional).
  schedule = "cron(0 2 * * ? *)"

  tags = {
    Environment = "example"
  }
}

output "state_machine_arn" {
  description = "ARN of the Step Functions state machine."
  value       = module.pipeline.state_machine_arn
}

output "state_machine_name" {
  description = "Name of the state machine."
  value       = module.pipeline.state_machine_name
}

output "execution_role_arn" {
  description = "IAM execution role ARN."
  value       = module.pipeline.execution_role_arn
}
