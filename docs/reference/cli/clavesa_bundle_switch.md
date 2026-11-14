# clavesa bundle switch

Print the steps and the exact SQL that switch a job's tables from Databricks to clavesa (runs nothing)

Print, table by table and in order, the switch of a job from its Databricks
target to its clavesa target: the history copy, pausing the Databricks job,
the name swap for managed tables, the grants, and the bundle change
(shadow: false, replaces_job) with the deploy. Clavesa runs nothing in
Databricks; a person who can run SQL there does those steps.

  clavesa bundle switch -t aws --job daily_sales --databricks-target prod

## Usage

```
clavesa bundle switch [flags]
```

## Flags

```
      --bundle string              the bundle folder (the one with databricks.yml) (default ".")
      --databricks-target string   the bundle target that deploys the job to Databricks (named in the steps)
      --job string                 the job
  -t, --target string              the bundle target (default: the target with default: true)
      --var stringArray            set a bundle variable, name=value (repeatable); wins over BUNDLE_VAR_<name> and the target
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa bundle](clavesa_bundle.md) — Deploy Databricks bundle (DAB) jobs to clavesa (ADR-031)
- [Command index](README.md)
