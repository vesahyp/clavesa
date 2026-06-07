# Bulk-read an S3 bucket

> **When you have one:** an existing bucket of historical data — exports from another system, a static archive, a one-time dump from a vendor — and you want it loaded into Clavesa as a Delta table you can query.

This recipe is the one-shot case: read every file in the prefix, write it to a table, done. If new files arrive over time and you want them flowing in automatically, see [s3-trigger](s3-trigger.md).

## What you'll end up with

- One Delta table at `clavesa_<workspace>__<pipeline>.<node>` in Glue Data Catalog.
- Rows readable from Athena, the Clavesa Catalog, or any Delta-aware query engine.
- A pipeline you can re-run on demand to refresh the table against the latest contents of the bucket.

## Prerequisites

- A workspace from `clavesa workspace init` (see the [main README quick-start](../../README.md#quick-start)).
- AWS credentials that can read the source bucket. Local-compute pipelines pick these up from your shell (`AWS_PROFILE`, `~/.aws/credentials`); cloud-compute pipelines get read permission grafted on by the source module at deploy time.
- A bucket with files in **Parquet, JSON, or CSV**. Files don't need to be partitioned for this recipe — the runner walks every key under the prefix.

## The recipe

```bash
# 1. Register the bucket as a source. Clavesa sniffs the s3:// URL
#    and promotes it to kind=s3 with bucket + prefix derived.
bin/clavesa source register orders \
  --from s3://your-bucket/exports/orders/ \
  --format parquet

# 2. Create a pipeline and add a transform.
bin/clavesa pipeline create sales
bin/clavesa node add sales --type transform --name orders_raw

# 3. The transform's SQL just SELECTs from the source — bulk-read means
#    every file under the prefix becomes one stream the runner reads.
bin/clavesa node edit sales orders_raw \
  --set "sql=SELECT * FROM orders"

# 4. Wire and run.
bin/clavesa source attach sales orders --to orders_raw --as orders
bin/clavesa pipeline run sales
```

## What you should see

- `pipeline run` reports `orders_raw` as `ok` after Spark cold-start (~30s) plus however long the read takes.
- `/` (Catalog) shows a new table `orders_raw` under `clavesa_<workspace>__sales`.
- Click the table: schema inferred from your data, sample rows pulled by the runner.
- `/pipelines/dashboard?dir=sales` shows the run history; click a row → run-detail with the per-node breakdown.

## Re-running

Every `pipeline run sales` re-reads every file under the prefix and **overwrites** the target table — the default output mode is `replace`. Right when:

- You want the table to mirror the bucket exactly. Files removed from S3 disappear from the next run's table.
- The bucket is small enough that the re-read is cheap.

For larger buckets or append-only sources, see [Incremental reads](#incremental-reads) below.

## Incremental reads

If files in the bucket have Hive-style partition keys in the path — `year=2024/month=01/`, `region=us-east-1/`, etc. — you can switch to an incremental shape where each run reads only the newly-arrived files.

Register the source with `--partitions` (and optionally `--start-from`):

```bash
bin/clavesa source register orders \
  --kind s3 --bucket your-bucket --prefix exports/orders/ \
  --format parquet \
  --partitions year,month,day \
  --start-from all
```

Then wire it to an append-mode transform:

```bash
bin/clavesa pipeline create sales
bin/clavesa node add sales --type transform --name orders_raw
bin/clavesa node edit sales orders_raw \
  --set "sql=SELECT * FROM orders" \
  --output-mode append
bin/clavesa source attach sales orders --to orders_raw --as orders
```

The UI equivalent: register the source from `/sources` → expand **Advanced** → set Format to `parquet`, fill Partitions with `year,month,day`, Start from with `all`. Attach it to the transform; set the **Output** mode to `append` on the editor's right panel.

Now a deployed run reads only the new files, drained from the bucket's notification queue (the same mechanism the [event-driven recipe](s3-trigger.md) uses); the partition keys give the output table its partition columns. Local runs fall back to listing new partitions by watermark. Pair this with the cron trigger pattern, or move on to the fully event-driven pattern in [s3-trigger](s3-trigger.md).

**Note:** partitioned reads require `format=parquet` today. The runner's incremental path hardcodes `spark.read.parquet`; CSV/JSON partitioned reads land when the runner grows a format-dispatched branch.

## Troubleshooting

**`AccessDenied` on the first run.** Local-compute pipelines use your shell's AWS credentials. Either `export AWS_PROFILE=<profile>` before launching `clavesa ui`, or switch to `compute = "lambda"` and `clavesa pipeline deploy <pipeline>` (the deployed Lambda role gets read permission on the source bucket automatically).

**Run completes but rows look empty / wrong types.** Confirm `--format` matches the actual file format. CSV needs `--format csv` and assumes a header row. Parquet self-describes. JSON works for **JSONL** (one object per line), not pretty-printed JSON arrays.

**File count in the millions and the run never finishes.** Bulk-read walks every key. Switch to partitioned incremental reads (above) — the runner reads one partition at a time and you can resume from a watermark.

## See also

- [s3-trigger](s3-trigger.md) — same bucket, but new files trigger the pipeline automatically.
- [backfill](backfill.md) — load history once a streaming pipeline is in place, or replay a window after a transform fix.
