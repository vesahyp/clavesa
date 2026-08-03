# clavesa workspace destroy

terraform destroy on the workspace (sweeping system-catalog Glue tables first)

Run terraform destroy after deleting Glue tables that the runner + the
runs_writer Lambda created at runtime against the workspace-wide system
catalog. The system DB holds runs / node_runs / tables — workspace-shared
Delta tables, multi-writer across every pipeline in the workspace
(ADR-016 v0.20.0). They aren't in terraform state, so without the sweep,
`aws_glue_catalog_database.system_pipelines` refuses to destroy
with "database is not empty".

Tear down individual pipelines first via `clavesa pipeline destroy`
— this command does not chain into per-pipeline destroys. The sweep
targets the system DB only (`<system_catalog>__pipelines` per ADR-016).

Workspace destroy also pre-empties the versioned workspace S3 bucket
and drains the Athena workgroup with RecursiveDeleteOption=true so
terraform destroy doesn't 409 on bucket / workgroup state.

--skip-sweep bypasses the sweep step; the sweep itself asks for explicit
'yes' confirmation before deleting anything.
Use --yes to skip both the sweep confirmation and the terraform destroy
prompt (for CI / scripted use). The workspace name + path are still
echoed to stderr before any AWS calls.

## Usage

```
clavesa workspace destroy [flags]
```

## Flags

```
      --skip-sweep   skip the Glue-table sweep preflight
  -y, --yes          skip the interactive confirmation prompt
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa workspace](clavesa_workspace.md) — Manage workspaces
- [Command index](README.md)
