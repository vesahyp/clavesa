# clavesa sql

SparkSQL tooling (parse-check, lint)

SparkSQL tooling. Subcommands work against the workspace's
warm Spark worker (the same JVM that powers the Catalog UI's
ad-hoc query runner), so a parse-check is a single in-JVM call
without paying the Spark cold-start cost.

## Usage

```
clavesa sql
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## Subcommands

- [clavesa sql lint](clavesa_sql_lint.md) — Parse-check a SparkSQL file; exits non-zero on parse failure

## See also

- [clavesa](clavesa.md) — Visual ETL for Terraform pipelines
- [Command index](README.md)
