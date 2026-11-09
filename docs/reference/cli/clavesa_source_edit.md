# clavesa source edit

Edit a registered source

Update fields of an already-registered source.

Only the flags you pass change; every other field keeps its current
value. The source name is fixed — pipelines reference sources by name,
so a rename is a delete + re-register, not an edit.

  clavesa source edit trips --from https://example.com/trips-v2.parquet
  clavesa source edit logs  --prefix events/2024/ --start-from now
  clavesa source edit trips --credentials ""    # clear the credential

Editing a kind=s3 source does not re-sync pipelines already attached to
it — re-run 'source attach' to propagate. kind=http edits take effect on
the next run automatically.

## Usage

```
clavesa source edit <name> [flags]
```

## Flags

```
      --bucket string                S3 bucket name (kind=s3)
      --credentials string           name of a registered credential; pass "" to clear
      --endpoint string              kind=kinesis: endpoint URL override (LocalStack); pass "" to clear
      --format string                data format (parquet, csv, json, tsv)
      --from string                  URL shorthand: https://… for kind=http, s3://… for kind=s3, uc://catalog.schema.table for kind=unity-catalog, kinesis://<stream> for kind=kinesis
      --kind string                  source kind (http, s3, unity-catalog, kinesis)
      --manage-notifications         kind=s3: have terraform manage the bucket's EventBridge notification config
      --no-trigger                   kind=s3: new files do not start runs; the pipeline runs on its schedule and drains the queue then
      --partitions strings           kind=s3: comma-separated Hive partition keys; pass "" to clear
      --prefix string                S3 key prefix (kind=s3); auto-suffixed with /
      --read-option stringToString   repeatable Spark read option for delimited text (format=tsv/csv): delimiter, comment, header, columns (default [])
      --region string                Kinesis stream region (kind=kinesis); pass "" for the deploy region
      --start-from string            kind=s3 with --partitions: watermark seed ("all" | "now" | "<cursor>")
      --starting-position string     kind=kinesis: TRIM_HORIZON or LATEST; pass "" for the default
      --stream string                Kinesis data stream name (kind=kinesis)
      --table string                 Unity Catalog table <catalog>.<schema>.<table> (kind=unity-catalog)
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa source](clavesa_source.md) — Manage workspace input sources (registry)
- [Command index](README.md)
