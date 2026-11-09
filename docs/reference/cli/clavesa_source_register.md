# clavesa source register

Register a new source in the workspace registry

Register a source under the workspace's registry.

Two shorthands cover the common cases — pass --from with the URL form:

  clavesa source register trips --from https://example.com/trips.parquet
  clavesa source register logs  --from s3://my-bucket/events/2024/

For s3, you can also pass kind/bucket/prefix explicitly:

  clavesa source register logs --kind s3 --bucket my-bucket --prefix events/ --format json

A table a Databricks step wrote (Unity Catalog) takes a uc:// URL, or
--table. It needs a Databricks connection (clavesa workspace connect
databricks) and reads in the cloud only:

  clavesa source register orders --from uc://main.sales.orders

--format is inferred from the trailing filename when omitted. Pass
--attach <pipeline-dir> --to <transform> [--as <alias>] to attach the
new source to a pipeline in one step.

Incremental ingest: a deployed s3 source reads only the newly-arrived
files each run by draining the bucket's notification queue (the SQS queue
fed by S3 Object Created events); no per-run bucket listing. Declare
Hive-style partition keys with --partitions year,month,day so the output
table recovers its partition columns. --start-from controls the first run
before the queue takes over: "all" (default; read pre-existing files),
"now" (skip them), or a literal "/"-joined cursor like "2024-01-01".
Local runs fall back to listing (no queue), with identical results.

## Usage

```
clavesa source register <name> [flags]
```

## Flags

```
      --as string                    input alias (default: source name) when --attach is set
      --attach string                also attach to a pipeline in this workspace (pipeline dir)
      --bucket string                S3 bucket name (kind=s3)
      --credentials string           name of a registered credential (slice 2: header auth)
      --format string                data format (parquet, csv, json, tsv); inferred from filename if omitted
      --from string                  URL shorthand: https://… for kind=http, s3://… for kind=s3, uc://catalog.schema.table for kind=unity-catalog
      --kind string                  source kind (http, s3, unity-catalog); inferred from --from when omitted
      --manage-notifications         kind=s3: have terraform manage the bucket's EventBridge notification config (authoritative — replaces existing notification config). Default off; only enable when clavesa owns the source bucket
      --no-trigger                   kind=s3: new files do not start runs; the pipeline runs on its schedule and drains the queue then
      --partitions strings           kind=s3: comma-separated Hive partition keys (e.g. year,month,day) for partition-column recovery
      --prefix string                S3 key prefix (kind=s3); auto-suffixed with /
      --read-option stringToString   repeatable Spark read option for delimited text (format=tsv/csv): delimiter, comment, header, columns (e.g. --read-option delimiter=$'\t' --read-option header=false) (default [])
      --start-from string            kind=s3 with --partitions: first-run seed before queue-drain takes over ("all" | "now" | "<cursor>")
      --table string                 Unity Catalog table <catalog>.<schema>.<table> (kind=unity-catalog; needs clavesa workspace connect databricks)
      --to string                    transform node id (when --attach is set)
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa source](clavesa_source.md) — Manage workspace input sources (registry)
- [Command index](README.md)
