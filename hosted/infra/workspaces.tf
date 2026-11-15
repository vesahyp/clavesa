########################################################################
# The workspace registry (ADR-032 section 2, "How a new workspace joins").
# One entry per workspace the hosted UI shows. Every signed-in user sees
# every workspace: the per-workspace viewers list of the ADR is left out
# until there is a second viewer.
#
#   name             the workspace name (clavesa.json "name"); /w/<name>/
#                    and summaries/<name>.json
#   kind             "cloud": the Lambda reads it through its reader role
#                    and refreshes the summary on its run events.
#                    "local": the Mac publishes the summary through the
#                    publisher role (clavesa workspace summary --publish).
#   account, region  where the workspace lives
#   reader_role_arn  cloud only: clavesa-ui-reader-<name> (the workspace
#                    deploy creates it; `clavesa workspace hosted show`)
#   bucket           cloud only: the workspace's pipeline bucket
#
# The list reaches the Lambda as JSON (REGISTRY), lets the event bus
# accept the accounts, and lets the publisher role write the local names.
########################################################################

locals {
  workspaces = [
    {
      name            = "smoke2"
      kind            = "cloud"
      account         = "699166197771"
      region          = "eu-north-1"
      reader_role_arn = "arn:aws:iam::699166197771:role/clavesa-ui-reader-smoke2"
      bucket          = "smoke2-clavesa"
    },
  ]

  workspace_accounts = distinct([for w in local.workspaces : w.account])
  local_workspaces   = [for w in local.workspaces : w.name if w.kind == "local"]

  # A local entry may leave reader_role_arn and bucket out; they reach
  # the Lambda as "".
  registry_json = jsonencode([for w in local.workspaces : {
    name            = w.name
    kind            = w.kind
    account         = w.account
    region          = w.region
    reader_role_arn = try(w.reader_role_arn, "")
    bucket          = try(w.bucket, "")
  }])
}
