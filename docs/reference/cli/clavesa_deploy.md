# clavesa deploy

Apply the workspace infra and every pipeline in one pass

Deploy the whole workspace: apply the workspace terraform first, then
every pipeline in it.

The workspace is applied before any pipeline because each pipeline reads
data.terraform_remote_state.workspace from ../terraform.tfstate, so a
pipeline can't plan against a workspace that hasn't been applied. If the
workspace apply fails the run aborts before touching any pipeline.

Each pipeline first regenerates its orchestration.tf from this binary's
emitter (orchestration.tf is a generated file; manual edits don't survive),
then runs its own init → plan → apply lifecycle. A pipeline failure is
reported and the run continues to the next pipeline; the command exits
non-zero if any pipeline failed.

Re-running is a cheap no-op. Terraform's plan and the ECR image digest
decide what actually changes; there's no hand-rolled staleness check, so a
no-change workspace and pipelines just re-plan and apply nothing.

Use --yes to skip the per-target confirmation prompts (for CI / scripted use).
Use --plan-only to stop after plan without applying (same as `clavesa plan`).

## Usage

```
clavesa deploy [flags]
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

- [clavesa](clavesa.md) — Visual ETL for Terraform pipelines
- [Command index](README.md)
