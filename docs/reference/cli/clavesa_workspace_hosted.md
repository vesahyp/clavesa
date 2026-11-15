# clavesa workspace hosted

Join or leave the hosted read-only UI

Join or leave a hosted, read-only clavesa UI (ADR-032).

When the workspace is hosted, the workspace stack gets a read-only IAM
role (clavesa-ui-reader-<workspace>) that only the hosted UI's principal
may assume, with the workspace name as its ExternalId; an Athena workgroup
(clavesa-ui-<workspace>) with a 1 GB scan limit per query; and, with an
event bus, rules that forward this workspace's run events to it. Every
deploy also uploads a read-only copy of the workspace tree (clavesa.json,
the pipeline .tf files and deployed.json, never Terraform state,
.env files or credentials) to s3://<pipeline_bucket>/_clavesa/tree/.

A local-warehouse workspace has no stack and no reader role. Its hosted
block names the hosted UI's publisher role and data bucket instead, and
every clavesa pipeline run ends by publishing the workspace summary to
summaries/<workspace>.json through that role (turn it off for one run
with --no-publish or CLAVESA_NO_PUBLISH=1). The name must be registered
as a local workspace in the hosted UI's registry first.

  clavesa workspace hosted enable --url https://ui.clavesa.dev
  clavesa workspace hosted show
  clavesa workspace hosted disable

## Usage

```
clavesa workspace hosted
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## Subcommands

- [clavesa workspace hosted disable](clavesa_workspace_hosted_disable.md) — Remove the hosted block from clavesa.json
- [clavesa workspace hosted enable](clavesa_workspace_hosted_enable.md) — Write the hosted block into clavesa.json
- [clavesa workspace hosted show](clavesa_workspace_hosted_show.md) — Show the hosted block

## See also

- [clavesa workspace](clavesa_workspace.md) — Manage workspaces
- [Command index](README.md)
