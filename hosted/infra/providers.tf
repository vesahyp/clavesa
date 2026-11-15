terraform {
  # 1.10 is the floor: use_lockfile below is the S3-native state lock.
  # mise.toml pins the exact version.
  required_version = ">= 1.10"

  # The defaults are the first deploy's (ui.clavesa.dev). Your own copy
  # points the state somewhere else at init time:
  #   terraform -chdir=hosted/infra init -reconfigure \
  #     -backend-config=bucket=<your bucket> -backend-config=profile=<profile>
  # (or HOSTED_BACKEND_FLAGS="..." make hosted-init).
  backend "s3" {
    bucket       = "vesa-backup-699166197771"
    key          = "tfstate/clavesa-dev/hosted.tfstate"
    region       = "eu-north-1"
    profile      = "personal"
    encrypt      = true
    use_lockfile = true
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}

# Cognito, SES, S3, Route 53 and the Lambda live in var.region. The
# project tag is the cost-allocation split.
provider "aws" {
  region  = var.region
  profile = var.aws_profile

  default_tags {
    tags = {
      project = "clavesa"
    }
  }
}

# CloudFront requires its ACM certificate in us-east-1.
provider "aws" {
  alias   = "us_east_1"
  region  = "us-east-1"
  profile = var.aws_profile

  default_tags {
    tags = {
      project = "clavesa"
    }
  }
}

data "aws_caller_identity" "current" {}
