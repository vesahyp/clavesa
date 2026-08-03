# clavesa pipeline deploy

terraform init -upgrade → plan -out=tfplan → apply tfplan for a pipeline

Run the substantive deploy lifecycle against the pipeline's terraform.

Preflight checks clavesa.json at the workspace root (the pipeline's
parent), then regenerates orchestration.tf from the current pipeline
graph and this binary's emitter — deploy always applies the orchestration
shape the installed version produces, so emitter fixes reach deployed
pipelines without a separate sync step. orchestration.tf is a generated
file; manual edits to it do not survive a deploy.

Saves the plan to <pipeline>/tfplan and pauses for a 'yes' confirmation
before applying; the plan is cleaned up on success.

The runner image isn't re-checked here — pipeline deploys pin the Lambda
to an ECR digest and don't push, so a stale local image isn't a risk.
That preflight runs as part of `workspace deploy`.

Pipeline directory:
  Pass the pipeline directory as the first argument, relative to the
  workspace root (e.g. "my-pipeline") or as an absolute path. Omit it to
  use the current directory, which is handy once you have cd'd into the
  pipeline. Run outside any pipeline with no argument and the command
  reports a clear error.

## Usage

```
clavesa pipeline deploy <pipeline-dir> [flags]
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

- [clavesa pipeline](clavesa_pipeline.md) — Manage pipelines
- [Command index](README.md)
