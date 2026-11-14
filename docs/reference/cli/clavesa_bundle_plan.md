# clavesa bundle plan

Show, task by task, where each job of a bundle target runs and what has to change

Resolve the bundle for the target the way the Databricks CLI does (includes,
variables, target overrides, presets) and report every task of every job:
its type, where it runs (clavesa or databricks), and ok, warn or refuse
with the reason. A refuse makes the plan exit 1. It needs no Databricks
login and makes no network call, so it runs in CI.

  clavesa bundle plan -t aws
  clavesa bundle plan --bundle ./my-bundle -t aws --json

## Usage

```
clavesa bundle plan [flags]
```

## Flags

```
      --bundle string     the bundle folder (the one with databricks.yml) (default ".")
      --json              output as JSON
  -t, --target string     the bundle target (default: the target with default: true)
      --var stringArray   set a bundle variable, name=value (repeatable); wins over BUNDLE_VAR_<name> and the target
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa bundle](clavesa_bundle.md) — Deploy Databricks bundle (DAB) jobs to clavesa (ADR-031)
- [Command index](README.md)
