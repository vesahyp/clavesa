# clavesa workspace connect databricks

Write this workspace's tables into a Databricks Unity Catalog

Write this workspace's tables into Databricks Unity Catalog, so clavesa
can replace steps of existing Databricks pipelines (ADR-027).

  clavesa credential register dbx-sp --kind databricks-oauth \
    --client-id <application id> --secret env:DATABRICKS_CLIENT_SECRET
  clavesa workspace connect databricks \
    --host https://dbc-1234abcd-5678.cloud.databricks.com \
    --catalog analytics --credential dbx-sp

Each pipeline output is the table <catalog>.<schema>.<table>, where the
schema is the pipeline's unity_catalog_schema (default: its clavesa
schema) and the table is the output's clavesa table name. When that
table already exists as an external table, clavesa writes into it in
place; a managed table is refused. A table that does not exist yet is
registered as a new external table in clavesa's bucket (needs
--external-location, or --create-catalog).

By default --catalog is an existing catalog, and clavesa creates no
catalog and no schema. --create-catalog is for a Databricks workspace
with no real catalog: the next workspace deploy creates the catalog, its
storage on the workspace bucket and the pipelines' schemas.

The command checks every step a Databricks admin must do first and prints
the exact fix for each one that is missing. It records the connection in
clavesa.json only when no check fails. It never changes anything in
Databricks: the next deploy does that.

--publish-tables=false and --push-lineage=false turn off the table
registration or the lineage push. Both APIs are Public Preview.

## Usage

```
clavesa workspace connect databricks [flags]
```

## Flags

```
      --catalog string             Unity Catalog catalog to publish into
      --create-catalog             create --catalog, its storage and the pipelines' schemas at the next deploy (for a workspace with no real catalog)
      --credential string          name of a kind=databricks-oauth credential
      --external-location string   admin-created external location over the workspace bucket, for new tables
      --host string                Databricks workspace URL (https://dbc-....cloud.databricks.com)
      --json                       output as JSON
      --publish-tables             register each output table in Unity Catalog (default true)
      --push-lineage               push pipeline lineage into Unity Catalog (default true)
      --reader strings             Databricks user, group or service principal that reads the tables (from a SQL warehouse); gets SELECT on every table clavesa registers. Repeat or comma-separate
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa workspace connect](clavesa_workspace_connect.md) — Connect the workspace to an outside catalog
- [Command index](README.md)
