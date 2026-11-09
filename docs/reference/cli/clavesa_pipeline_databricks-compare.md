# clavesa pipeline databricks-compare

Compare the replaced Databricks step's table with clavesa's shadow table, per partition

Compare the table the Databricks step writes (--existing) with the table
clavesa writes next to it in shadow mode (--shadow): rows and the sum of
each --sum column, per --partition value, and the columns. It passes only
when both tables have the same partitions and every number is equal.
Exit status 0 is pass, 1 is fail, so a daily cron job can alert on it.

The check runs in the local runner container against the cloud, with the
deployed pipeline's Databricks connection: Spark reads both Delta tables
directly (a managed table through credentials Unity Catalog vends), so it
costs no DBUs. Sums are exact (each value is cast to decimal(38,6)).

  clavesa pipeline databricks-compare events \
    --existing analytics.sales.daily_events --shadow analytics.sales.daily_events_clavesa \
    --partition event_date --sum amount,quantity \
    --where "event_date >= date_sub(current_date(), 7)"

See docs/cookbook/replace-a-databricks-step.md.

## Usage

```
clavesa pipeline databricks-compare <pipeline> [flags]
```

## Flags

```
      --existing string    the table the Databricks step writes, <catalog>.<schema>.<table>
      --json               output as JSON
      --partition string   the column both tables are partitioned by, e.g. event_date
      --shadow string      the table clavesa writes next to it, <catalog>.<schema>.<table>
      --sum strings        key columns to sum per partition (numeric); repeat or comma-separate
      --where string       Spark SQL predicate applied to both tables, e.g. "event_date >= date_sub(current_date(), 7)"
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa pipeline](clavesa_pipeline.md) — Manage pipelines
- [Command index](README.md)
