# Fixture: basic source module
#
# Sources are pass-through path declarations — the module creates an SQS
# queue + EventBridge rule for orchestration triggering and exposes the
# user-configured S3 location as outputs[*].table_path. No staging,
# no Glue Job, no Iceberg registration on the source side.

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

variable "pipeline_name" {
  default = "example-pipeline"
}

module "raw_events" {
  source = "../../"

  pipeline_name = var.pipeline_name
  name          = "raw_events"
  bucket        = "example-source-bucket"
  prefix        = "events/"
  format        = "json"
}

output "raw_events_outputs" {
  description = "Named output map keyed by 'default'. Downstream transforms reference module.raw_events.outputs[\"default\"] in their inputs map."
  value       = module.raw_events.outputs
}

output "trigger_queue_arn" {
  description = "Pass to the orchestration module's trigger_queue_arns variable to run the pipeline on new-data events."
  value       = module.raw_events.trigger_queue_arn
}
