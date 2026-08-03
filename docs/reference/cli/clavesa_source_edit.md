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
      --format string                data format (parquet, csv, json, tsv)
      --from string                  URL shorthand: https://… for kind=http, s3://… for kind=s3
      --kind string                  source kind (http, s3)
      --manage-notifications         kind=s3: have terraform manage the bucket's EventBridge notification config
      --partitions strings           kind=s3: comma-separated Hive partition keys; pass "" to clear
      --prefix string                S3 key prefix (kind=s3); auto-suffixed with /
      --read-option stringToString   repeatable Spark read option for delimited text (format=tsv/csv): delimiter, comment, header, columns (default [])
      --start-from string            kind=s3 with --partitions: watermark seed ("all" | "now" | "<cursor>")
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa source](clavesa_source.md) — Manage workspace input sources (registry)
- [Command index](README.md)
