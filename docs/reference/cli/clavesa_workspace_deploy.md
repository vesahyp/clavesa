# clavesa workspace deploy

terraform init -upgrade → plan -out=tfplan → apply tfplan, with preflight

Run the substantive deploy lifecycle against the workspace's terraform.

Preflight refuses to invoke terraform unless clavesa.json is present and
the local runner image's clavesa.runner_sha label matches the embedded
runner files (catches the stale-image-pushed-silently case). The flow saves
the plan to ./tfplan and pauses for a 'yes' confirmation before applying;
the plan is cleaned up on success.

Use --yes to skip the confirmation prompt (for CI / scripted use).
Use --plan-only to stop after plan without applying.

## Usage

```
clavesa workspace deploy [flags]
```

## Flags

```
      --plan-only   stop after terraform plan (don't apply)
  -y, --yes         skip the interactive 'Apply this plan?' confirmation
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa workspace](clavesa_workspace.md) — Manage workspaces
- [Command index](README.md)
