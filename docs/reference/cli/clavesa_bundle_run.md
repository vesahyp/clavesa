# clavesa bundle run

Run one job of a bundle target on clavesa

Plan the bundle target, generate the job's clavesa pipeline into the
workspace (bundles/<bundle>_<job>/, read-only: the bundle is the source) and
run it. The plan must be ready (clavesa bundle plan says Ready: yes).

Job parameters take their defaults from the bundle; --param overrides one.
Databricks secrets the code reads come from the shell for a local run:
export CLAVESA_DBX_SECRET__<scope>__<key>.

  clavesa bundle run -t aws-local --job daily_sales --compute local
  clavesa bundle run -t aws-local --job daily_sales --param run_date=2026-11-12
  clavesa bundle run -t aws --job daily_sales --wait     # the deployed job

## Usage

```
clavesa bundle run [flags]
```

## Flags

```
      --bundle string       the bundle folder (the one with databricks.yml) (default ".")
      --compute string      local: the docker runner on the local warehouse; cloud (the default on a cloud warehouse): the deployed job
      --job string          the job to run (its key under resources.jobs); may be left out when the bundle has one job
      --json                output as JSON
      --param stringArray   a job parameter, name=value (repeatable)
  -t, --target string       the bundle target (default: the target with default: true)
      --var stringArray     set a bundle variable, name=value (repeatable); wins over BUNDLE_VAR_<name> and the target
      --wait                with the deployed job: block until the run ends
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa bundle](clavesa_bundle.md) — Deploy Databricks bundle (DAB) jobs to clavesa (ADR-031)
- [Command index](README.md)
