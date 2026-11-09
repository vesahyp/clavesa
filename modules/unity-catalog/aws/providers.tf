# The Databricks provider is configured here, not in the caller, because
# the caller is a clavesa-generated root whose main.tf already holds the
# one `required_providers` block a module may have. Authenticates as the
# service principal (OAuth M2M); the client secret comes from the
# DATABRICKS_CLIENT_SECRET environment variable, which `clavesa deploy`
# sets from the credential. The secret is never a variable, so it never
# enters the plan or the state. (A module with its own provider block
# cannot be called with count or for_each; clavesa never does.)
provider "databricks" {
  host      = var.databricks_host
  client_id = var.databricks_client_id
}
