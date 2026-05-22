terraform {
  # >= 1.3 required for optional() in type constraints.
  required_version = ">= 1.3"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.6"
    }
  }
}
