# Event-driven S3 source

> **When you have one:** a bucket where new files land continuously — application logs, daily exports from a scraper, partner data drops — and you want each new file to kick off a pipeline run.

Each S3 object-create event hits an EventBridge rule, lands on an SQS queue, and a cron-triggered poller Lambda starts one Step Functions execution per distinct partition. The runner reads only the new partitions since the last watermark and writes the result to a Delta table.

If you also need to load historical files that existed before the pipeline was set up, see [backfill](backfill.md) — staging and reviewing a window is its own flow once the pipeline above is live.

## What you'll end up with

- A deployed pipeline (`compute = "lambda"`) that processes one partition's worth of new files per run.
- An EventBridge rule + SQS queue subscribed to S3 object-create events on the source bucket, draining into a Step Functions execution per partition.
- A Delta table in Glue Data Catalog that grows incrementally as new files arrive.

## Prerequisites

- A deployed workspace. `clavesa workspace init` builds the runner image; `clavesa workspace deploy` provisions the pipeline bucket, ECR repo, and system catalog.
- A source bucket with **Hive-style partitions** in the keys: `year=YYYY/month=MM/day=DD/hour=HH/`, `region=X/dt=Y/`, etc. The trigger fires on every object-create, but the pipeline reads partition-at-a-time.
- The source bucket needs **EventBridge notifications enabled**. Pass `--manage-notifications` on `source register` when clavesa owns the bucket (terraform takes over `aws_s3_bucket_notification`); for buckets you manage elsewhere, leave it off and enable notifications yourself with one `aws s3api put-bucket-notification-configuration … '{"EventBridgeConfiguration":{}}'`.

## The recipe

```bash
# 1. Register the source. --partitions declares the Hive-style keys in
#    the bucket layout; --start-from "all" reads history on first run,
#    "now" skips it. Each subsequent run advances a watermark and reads
#    only newer partitions. --manage-notifications has terraform own the
#    bucket's EventBridge notification config (drop the flag if you
#    manage that bucket elsewhere; the resource is authoritative).
#    Replace event_id with your own natural key.
bin/clavesa source register events \
  --kind s3 --bucket your-source-bucket --prefix events/ \
  --format parquet \
  --partitions year,month,day,hour \
  --start-from all \
  --manage-notifications

# 2. Pipeline + transform on cloud compute.
bin/clavesa pipeline create stream
bin/clavesa node add stream --type transform --name passthrough
bin/clavesa node edit stream passthrough \
  --set compute=lambda \
  --set "sql=SELECT *, current_timestamp() AS ingested_at FROM events"

# 3. Append-mode output with merge_keys — append because each run
#    processes new partitions; merge_keys make the write idempotent
#    under retry. See "Why merge_keys" below.
bin/clavesa node edit stream passthrough \
  --output-mode append \
  --output-merge-keys event_id

# 4. Wire and deploy.
bin/clavesa source attach stream events --to passthrough --as events

bin/clavesa pipeline deploy stream
```

The UI equivalent walks the same surfaces: `/sources` → Register → Advanced → set partitions; `/pipelines` → New → add transform → set compute=lambda + SQL; editor's right panel **Output** → mode `append` + merge keys `event_id`; **Save Output Config**; back to the pipeline page → **Open editor** is where the source attach lives, attaching `events` to `passthrough`.

Apply provisions: the runner Lambda with read permission on `your-source-bucket` (IAM scope auto-derived from the registered source's bucket — v0.22.0+), the SQS queue + EventBridge rule subscribed to object-create events under `events/`, the poller Lambda that drains the queue, and the Step Functions state machine that runs the transform.

## What you should see

Drop a new file in `s3://your-source-bucket/events/year=2026/month=05/day=13/hour=14/<anything>.parquet`. Within ~60 seconds:

- `/pipelines/dashboard?dir=stream` shows a new run with `trigger = "event"`.
- The Delta table grows by however many rows the file contributed.
- The watermark advances to that partition cursor; the next run sees only newer partitions.

If you drop two files in the same partition while a run is in flight, EventBridge deduplicates at the partition level — the runner reads the whole partition's contents in one pass, so file-count fan-out doesn't translate to run-count fan-out.

## Why merge_keys on an "append" output

The pipeline advances its watermark **after** the data write commits. If a run retries (Lambda transient failure, network blip mid-write), the same partition gets re-read on the next attempt. With `mode = "append"` and no `merge_keys`, you'd land the rows twice.

With `merge_keys = ["event_id"]`, the runner upgrades the write to `MERGE INTO target USING staging ON target.event_id = staging.event_id WHEN MATCHED UPDATE * WHEN NOT MATCHED INSERT *`. Matching keys update in place; only genuinely new keys insert. A retry can't dupe.

This is the recommended shape for any event-driven pipeline. Plain `append` is fine only when the source has a strict no-replay guarantee you trust.

## How the moving parts fit together

Three independent pieces, each runnable on its own:

1. **EventBridge rule** on the source bucket → SQS queue. Set up once at deploy.
2. **Poller Lambda** drains the queue every minute (cron-triggered) → starts one Step Functions execution per distinct partition.
3. **Step Functions** invokes the transform Lambda; the runner reads only new partitions since the last watermark.

## Loading historical files

The pipeline above only processes files that land *after* the EventBridge rule is in place. To load anything older — a year of archival logs, last quarter's partner exports — use the [backfill](backfill.md) flow: stage the historical window into a parallel Delta table, review the result side-by-side with the canonical target, then promote.

## Troubleshooting

**Files land in S3 but no run starts.** Check `aws s3api get-bucket-notification-configuration --bucket your-source-bucket` returns `{"EventBridgeConfiguration": {}}`. Without that, the EventBridge rule never receives the object-create event. If clavesa owns the bucket, re-`source register --manage-notifications` and re-apply; otherwise enable notifications with one `aws s3api put-bucket-notification-configuration --bucket … --notification-configuration '{"EventBridgeConfiguration":{}}'`.

**Run starts but reads zero rows.** Cursor format mismatch. The partition values in your bucket keys need to match the `partitions = [...]` list in the source block. If your bucket layout is `dt=2026-05-13/`, change `partitions` to `["dt"]` and the cursor format to `2026-05-13`.

**`AccessDenied` reading from the source bucket.** The runner Lambda's IAM role is scoped to the buckets in `var.source_inputs`. Confirm the registered source's bucket matches the live bucket name (`clavesa source show events`) — if not, re-`clavesa source register` and re-`source attach` so the pipeline `.tf` carries the new bucket, then `clavesa pipeline deploy stream`.

## See also

- [s3-bulk-ingest](s3-bulk-ingest.md) — one-shot full-bucket read without triggers, lower-overhead pattern for static historical data.
- [backfill](backfill.md) — load historical files that landed before this pipeline existed, or replay a window after a transform-logic fix.
- [merge-dim-table](merge-dim-table.md) — the SCD-Type-1 variant of the merge_keys pattern this recipe uses.
