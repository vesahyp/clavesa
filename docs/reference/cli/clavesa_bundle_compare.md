# clavesa bundle compare

Compare the tables a job writes on Databricks with clavesa's shadow tables (PASS or FAIL)

For every table the job writes, compare the Databricks job's table with the
table clavesa writes next to it in shadow mode (<table>_clavesa): rows and
the sum of each column in clavesa.compare.<table>.sums, per value of
clavesa.compare.<table>.partition, and the columns. The job passes when
every table does. Exit status 1 on FAIL, so a daily cron job can alert.

The target must have catalog: unity and shadow mode, and the job must be
deployed (clavesa bundle deploy): the deployed runner reads both tables
(a managed one through vended credentials), so it costs no DBUs.

  clavesa bundle compare -t aws --job daily_sales --days 7

## Usage

```
clavesa bundle compare [flags]
```

## Flags

```
      --bundle string     the bundle folder (the one with databricks.yml) (default ".")
      --days int          compare only the last N days of each table's partition column (0: all)
      --job string        the job (may be left out when the bundle has one job)
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
