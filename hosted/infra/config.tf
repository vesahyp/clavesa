########################################################################
# Operator config, read from SSM Parameter Store at plan time (jeeves
# practices/infrastructure.md, keitos/infra/config.tf is the pattern).
#
# var.config_parameter: {"allowlist": ["a@b"], "registration_open": false}
#   Who may sign up. Fails shut: a missing key, an unreadable value or
#   config_parameter = "" all mean "nobody new gets in".
# var.google_parameter: {"client_id": "...", "client_secret": "..."}
#   Optional. Looked up by path, so its absence is not an error: no
#   parameter, no Google IdP, no Google button.
########################################################################

data "aws_ssm_parameter" "config" {
  count = var.config_parameter == "" ? 0 : 1
  name  = var.config_parameter
}

# Lists the parameters next to google_parameter, so the stack can tell
# whether it exists without failing the plan when it does not.
data "aws_ssm_parameters_by_path" "google_dir" {
  count           = var.google_parameter == "" ? 0 : 1
  path            = regex("^(.*)/[^/]+$", var.google_parameter)[0]
  recursive       = false
  with_decryption = false
}

locals {
  account_id = data.aws_caller_identity.current.account_id

  cognito_domain_prefix = coalesce(var.cognito_domain_prefix, "${var.pool_name}-${local.account_id}")
  spa_bucket_name       = coalesce(var.spa_bucket_name, "${var.pool_name}-spa-${local.account_id}")

  # nonsensitive() on the way in so the registration flag plans readably;
  # the allowlist is re-marked sensitive where it is used (auth.tf), so
  # no plan, output or log ever shows an address.
  config = try(
    jsondecode(nonsensitive(data.aws_ssm_parameter.config[0].value)),
    {},
  )
  registration_open = try(local.config.registration_open, false) == true
  allowlist         = distinct([for e in try(local.config.allowlist, []) : lower(trimspace(e))])

  google_exists = var.google_parameter != "" && contains(
    try(data.aws_ssm_parameters_by_path.google_dir[0].names, []),
    var.google_parameter,
  )
}

data "aws_ssm_parameter" "google" {
  count = local.google_exists ? 1 : 0
  name  = var.google_parameter
}

locals {
  google = try(jsondecode(nonsensitive(data.aws_ssm_parameter.google[0].value)), {})
  # The client id is not a secret; the secret is re-marked.
  google_client_id     = try(local.google.client_id, "")
  google_client_secret = sensitive(try(local.google.client_secret, ""))
  google_enabled       = local.google_client_id != ""
}
