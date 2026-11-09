# clavesa pipeline databricks-run

Show the Unity Catalog tables a run wrote and the Databricks jobs it started

Show the Unity Catalog tables a run wrote and the Databricks jobs it
started, with links. <run-id> is the run's execution ARN, as `pipeline run`
prints it, or its short ID as `pipeline runs` lists it.

## Usage

```
clavesa pipeline databricks-run <pipeline> <run-id> [flags]
```

## Flags

```
      --json   output as JSON
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa pipeline](clavesa_pipeline.md) — Manage pipelines
- [Command index](README.md)
