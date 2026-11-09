# clavesa pipeline databricks-jobs

Show or set the Databricks jobs started after every successful run

Show or set the Databricks jobs this pipeline starts after every
successful run (ADR-027, option A). Each job starts once, after the whole
run succeeded; the run page links to the Databricks run.

  clavesa pipeline databricks-jobs orders --set 263121218945262
  clavesa pipeline databricks-jobs orders --clear

Takes effect at the next `clavesa pipeline deploy`. The service principal
needs CAN MANAGE RUN on each job.

## Usage

```
clavesa pipeline databricks-jobs <pipeline> [flags]
```

## Flags

```
      --clear            start no jobs
      --json             output as JSON
      --set int64Slice   job IDs, comma-separated (replaces the list) (default [])
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa pipeline](clavesa_pipeline.md) — Manage pipelines
- [Command index](README.md)
