# clavesa bundle destroy

Remove a bundle target's jobs from AWS (terraform destroy of their pipelines)

For each job of the target (or the --job ones): terraform destroy of its
clavesa pipeline, after the runner-created Glue tables of the pipeline
are swept, as clavesa pipeline destroy does. The tables the job's code
wrote in its own Glue databases, and the shadow tables in Unity Catalog,
stay: they are data, not infrastructure; the command lists the shadow
tables with the SQL that drops them. The job on Databricks is not
touched (databricks bundle destroy -t <target> does that).

  clavesa bundle destroy -t aws --job daily_sales --yes

## Usage

```
clavesa bundle destroy [flags]
```

## Flags

```
      --bundle string     the bundle folder (the one with databricks.yml) (default ".")
      --job stringArray   destroy only this job (repeatable; default: every job of the target)
  -t, --target string     the bundle target (default: the target with default: true)
      --var stringArray   set a bundle variable, name=value (repeatable); wins over BUNDLE_VAR_<name> and the target
  -y, --yes               skip the confirmations
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa bundle](clavesa_bundle.md) — Deploy Databricks bundle (DAB) jobs to clavesa (ADR-031)
- [Command index](README.md)
