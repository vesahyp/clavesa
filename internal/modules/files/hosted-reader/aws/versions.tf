terraform {
  required_version = ">= 1.4" # terraform_data
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.0"
    }
  }
}
