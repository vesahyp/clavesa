# Fixture: the ways clavesa calls the unity-catalog module (ADR-027).
#
# `create_catalog` is a workspace connected with --create-catalog: the
# IAM role (read all, write _unity_catalog/ only), the storage credential,
# one external location over the bucket, the catalog with its managed
# location, the EXTERNAL USE grants and the runner's secret.
# `existing_catalog` is the default: the team's catalog, nothing created
# but the runner secret. `schema` is the per-pipeline schema a
# --create-catalog workspace's orchestration.tf adds.
#
# The root declares only aws, like a clavesa workspace main.tf: the
# modules configure the databricks provider themselves. `terraform
# validate` only; no credentials are needed.

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
  region = "us-east-2"
}

module "create_catalog" {
  source = "../../"

  databricks_host      = "https://dbc-00000000-0000.cloud.databricks.com"
  databricks_client_id = "00000000-0000-0000-0000-000000000000"
  workspace_name       = "example-ws"
  bucket               = "example-ws-clavesa"
  catalog              = "example_ws"
  create_catalog       = true
}

module "existing_catalog" {
  source = "../../"

  databricks_host      = "https://dbc-00000000-0000.cloud.databricks.com"
  databricks_client_id = "00000000-0000-0000-0000-000000000000"
  workspace_name       = "example-ws"
  bucket               = "example-ws-clavesa"
  catalog              = "analytics"
  create_runner_secret = false
}

module "schema" {
  source = "../../schema"

  databricks_host      = "https://dbc-00000000-0000.cloud.databricks.com"
  databricks_client_id = "00000000-0000-0000-0000-000000000000"
  catalog              = "example_ws"
  schema               = "web-traffic"
  pipeline_name        = "web-traffic"
}
