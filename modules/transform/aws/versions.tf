terraform {
  # >= 1.3 required for optional() in type constraints.
  # >= 1.9 required to enable cross-variable validation on the `sql` variable
  # (see variables.tf). If you need that validation and are on < 1.9, remove
  # the validation block and enforce the constraint via CI or code review.
  required_version = ">= 1.3"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.6" # open_table_format_input added in 5.6.0
    }
  }
}
