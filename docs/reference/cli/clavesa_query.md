# clavesa query

Run an ad-hoc SQL query against the workspace catalog

Run a free-form SQL query against the workspace catalog, dispatched
by the workspace warehouse (the same routing the UI's /query page uses):

  - warehouse = local  →  the warm Spark container against the local
                          Hive metastore catalog. Full SparkSQL dialect.
  - warehouse = cloud  →  Athena over the deployed Glue catalog. Athena
                          speaks Trino — your SparkSQL is transpiled
                          automatically, so you author one dialect either
                          way (ADR-023).

The warehouse defaults to "local" and is set with `clavesa workspace use --warehouse`.
Pass --warehouse local|cloud to override it for this query only.

Tables address as <workspace>__<schema>.<table> on both warehouses
(ADR-016/ADR-018; the v1.x `clavesa.` Iceberg-catalog prefix is gone).

Reads SQL from the first positional arg, or from STDIN when none given.

Examples:
  clavesa query "SHOW DATABASES"
  clavesa query "SELECT * FROM clavesa_demo__demo.trips LIMIT 5"
  clavesa query "SELECT count(*) FROM clavesa_demo__demo.trips" --warehouse cloud
  echo "SELECT count(*) FROM clavesa_demo__demo.trips" | clavesa query --json

## Usage

```
clavesa query [SQL] [flags]
```

## Flags

```
      --json               Emit columns + rows JSON
      --warehouse string   override the workspace warehouse for this query: local | cloud
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa](clavesa.md) — Visual ETL for Terraform pipelines
- [Command index](README.md)
