# clavesa pipeline destroy

terraform destroy on a pipeline (sweeping runtime-created Glue tables first)

Run terraform destroy after deleting Glue tables that the runner created
at execution time. Without the sweep, terraform destroy refuses on
`aws_glue_catalog_database.pipeline` with "database is not empty"
because runner-created Delta tables aren't in terraform state.

The sweep targets the pipeline's own Glue DB (default:
<workspace_catalog>__sanitize(<pipeline_name>)). Pass --glue-db <name>
if the pipeline's var.schema was overridden from its default.

Workspace system-DB row cleanup (runs / node_runs / tables rows where
pipeline = <this pipeline>) is not done here — those rows live inside
shared Delta tables and need an Athena DELETE through the workspace
workgroup. They stay around after destroy as historical context.

--skip-sweep skips the sweep step (faster when you know the DB is already
empty); the sweep itself asks for explicit 'yes' confirmation before
deleting anything.
Use --yes to skip both the sweep confirmation and the terraform destroy
prompt (for CI / scripted use). The pipeline name + workspace path are
still echoed to stderr before any AWS calls.

Pipeline directory:
  Pass the pipeline directory as the first argument, relative to the
  workspace root (e.g. "my-pipeline") or as an absolute path. Omit it to
  use the current directory, which is handy once you have cd'd into the
  pipeline. Run outside any pipeline with no argument and the command
  reports a clear error.

## Usage

```
clavesa pipeline destroy <pipeline-dir> [flags]
```

## Flags

```
      --glue-db string   explicit Glue DB to sweep (default: <catalog>__sanitize(<pipeline>))
      --skip-sweep       skip the Glue-table sweep preflight
  -y, --yes              skip the interactive confirmation prompt
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa pipeline](clavesa_pipeline.md) — Manage pipelines
- [Command index](README.md)
