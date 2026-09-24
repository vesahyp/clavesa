# clavesa workspace

Manage workspaces

Manage workspaces (init, plan, deploy, destroy).

A workspace is a directory that contains one or more pipeline subdirectories.

Examples:
  clavesa workspace init my-project
  clavesa workspace init my-project --cloud aws
  clavesa workspace plan
  clavesa workspace deploy

## Usage

```
clavesa workspace
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## Subcommands

- [clavesa workspace backend](clavesa_workspace_backend.md) — Show the workspace's configured Terraform backend
- [clavesa workspace deploy](clavesa_workspace_deploy.md) — terraform init -upgrade → plan -out=tfplan → apply tfplan, with preflight
- [clavesa workspace destroy](clavesa_workspace_destroy.md) — terraform destroy on the workspace (sweeping system-catalog Glue tables first)
- [clavesa workspace init](clavesa_workspace_init.md) — Initialize a new workspace
- [clavesa workspace migrate-state](clavesa_workspace_migrate-state.md) — Move the workspace's Terraform state onto the configured remote backend
- [clavesa workspace plan](clavesa_workspace_plan.md) — Run terraform plan on the workspace
- [clavesa workspace set-backend](clavesa_workspace_set-backend.md) — Configure (or clear) the workspace's remote Terraform backend
- [clavesa workspace tables](clavesa_workspace_tables.md) — List Delta tables in the workspace catalog
- [clavesa workspace upgrade](clavesa_workspace_upgrade.md) — Upgrade the workspace shell and every pipeline to the binary's module version
- [clavesa workspace use](clavesa_workspace_use.md) — Switch the current workspace, or set its warehouse / AWS profile

## See also

- [clavesa](clavesa.md) — Visual ETL for Terraform pipelines
- [Command index](README.md)
