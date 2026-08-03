# clavesa plan

Plan the workspace infra and every pipeline (no apply)

Plan the whole workspace: terraform plan the workspace and every pipeline,
without applying anything. Equivalent to `clavesa deploy --plan-only`.

If the workspace hasn't been deployed yet (no ../terraform.tfstate), the
per-pipeline plans can't resolve data.terraform_remote_state.workspace, so
they're skipped with a note rather than reported as failures. The workspace
plan still runs and is the useful output in that case. Run `clavesa deploy`
once to lay down the workspace state, then `clavesa plan` shows pipeline
diffs too.

Re-running is a cheap no-op: terraform's plan decides what would change;
there's no hand-rolled staleness check.

## Usage

```
clavesa plan
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa](clavesa.md) — Visual ETL for Terraform pipelines
- [Command index](README.md)
