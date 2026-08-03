# clavesa workspace upgrade

Upgrade the workspace shell and every pipeline to the binary's module version

Upgrade the workspace shell AND every pipeline in it to match the
running `clavesa` binary's ModuleVersion, then rebuild the local
runner image. One shot — no need to walk pipelines by hand.

Mechanics:
  - Re-extracts the embedded Terraform modules to .clavesa/modules/<version>/
    (idempotent; skips when the SHA stamp already matches).
  - Rewrites the workspace's `module "workspace"` source line to
    the new version with the leading "./" prefix Terraform 1.x requires.
  - Bumps `runner_version`'s default in variables.tf so the next
    deploy pushes the matching runner image.
  - Upgrades every pipeline (rewrites module sources, strips deprecated
    module arguments, re-syncs orchestration.tf). Continue-on-error: a
    pipeline that fails is reported and the rest still run.
  - Rebuilds the local Docker runner image from the embedded runner
    sources, tagging both :latest and :<version>. The build runs every
    time; docker's layer cache makes a no-change rebuild a fast cache hit.

Pass --shell-only to upgrade just the workspace shell and skip the
pipeline walk — the pre-one-shot behaviour.

Does NOT touch clavesa.json or the rest of main.tf — your provider
blocks and any extra resources are preserved.

Run this after upgrading `clavesa` itself (`brew upgrade clavesa` or
swapping the binary).

## Usage

```
clavesa workspace upgrade [flags]
```

## Flags

```
      --shell-only   upgrade only the workspace shell; skip the per-pipeline walk
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa workspace](clavesa_workspace.md) — Manage workspaces
- [Command index](README.md)
