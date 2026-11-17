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
#   account, region  cloud only: where the workspace lives (a local
#                    entry is just name and kind; its summary names its
#                    region)
#   reader_role_arn  cloud only: clavesa-ui-reader-<name> (the workspace
#                    deploy creates it; `clavesa workspace hosted show`)
#   bucket           cloud only: the workspace's pipeline bucket
#   personal_data    true when the workspace's raw data holds personal
#                    data (visitor IPs, user agents): the Lambda then
#                    refuses raw row reads of it, and shows table rows of
#                    gold databases only (internal/hosted/personal.go)
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
    {
      name            = "ucpub"
      kind            = "cloud"
      account         = "699166197771"
      region          = "us-east-2"
      reader_role_arn = "arn:aws:iam::699166197771:role/clavesa-ui-reader-ucpub"
      bucket          = "ucpub-clavesa"
    },
    {
      name            = "ucguide"
      kind            = "cloud"
      account         = "699166197771"
      region          = "us-east-2"
      reader_role_arn = "arn:aws:iam::699166197771:role/clavesa-ui-reader-ucguide"
      bucket          = "ucguide-clavesa"
    },
    # The webbaa account's web traffic analytics (aabbew/monorepo
    # analytics/web-traffic). Its reader role came with monorepo PR 341.
    {
      name            = "web-traffic"
      kind            = "cloud"
      account         = "471112936790"
      region          = "eu-north-1"
      reader_role_arn = "arn:aws:iam::471112936790:role/clavesa-ui-reader-web-traffic"
      bucket          = "web-traffic-clavesa"
      personal_data   = true # bronze and silver hold visitor IPs
    },
    # The nightly site-traffic pipelines on the main Mac (local warehouse).
    # Each run publishes summaries/<name>.json through the publisher role
    # (hosted/README.md, "Adding a local workspace").
    { name = "site-traffic", kind = "local" },
    { name = "omatienoo-traffic", kind = "local" },
    { name = "podbay-traffic", kind = "local" },
    { name = "kivikko-traffic", kind = "local" },
    { name = "keitos-traffic", kind = "local" },
    { name = "vinkki-traffic", kind = "local" },
    { name = "rakka-traffic", kind = "local" },
  ]

  # Only cloud workspaces send events, so only their accounts may put
  # events on the bus.
  workspace_accounts = distinct([for w in local.workspaces : w.account if w.kind == "cloud"])
  local_workspaces   = [for w in local.workspaces : w.name if w.kind == "local"]

  # A local entry leaves account, region, reader_role_arn and bucket out;
  # they reach the Lambda as "".
  registry_json = jsonencode([for w in local.workspaces : {
    name            = w.name
    kind            = w.kind
    account         = try(w.account, "")
    region          = try(w.region, "")
    reader_role_arn = try(w.reader_role_arn, "")
    bucket          = try(w.bucket, "")
    personal_data   = try(w.personal_data, false)
  }])
}
