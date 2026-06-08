# Event-driven S3 source

> **When you have one:** a bucket where new files land continuously — application logs, daily exports from a scraper, partner data drops — and you want each new file to kick off a pipeline run.

Each S3 object-create event hits an EventBridge rule and lands on an SQS queue. A cron-triggered poller checks the queue and starts a Step Functions execution when new objects are waiting. The runner then drains the queue for those object keys, reads exactly the new files (no bucket listing), writes the result to a Delta table, and deletes the messages once the write commits.

If you also need to load historical files that existed before the pipeline was set up, see [backfill](backfill.md) — staging and reviewing a window is its own flow once the pipeline above is live.

## What you'll end up with

- A deployed pipeline (`compute = "lambda"`) that reads only the newly-arrived files each run, drained from the queue.
- An EventBridge rule + SQS queue subscribed to S3 object-create events on the source bucket; the runner drains the queue each run instead of listing the bucket.
- A Delta table in Glue Data Catalog that grows incrementally as new files arrive.

## Prerequisites

- A deployed workspace. `clavesa workspace init` builds the runner image; `clavesa workspace deploy` provisions the pipeline bucket, ECR repo, and system catalog.
- A source bucket with **Hive-style partitions** in the keys: `year=YYYY/month=MM/day=DD/hour=HH/`, `region=X/dt=Y/`, etc., so the output table recovers its partition columns. The trigger fires on every object-create and the runner reads exactly the new objects each run. (Flat buckets with no partition keys work too — you just don't get partition columns.)
- The source bucket needs **EventBridge notifications enabled**. Pass `--manage-notifications` on `source register` when clavesa owns the bucket (terraform takes over `aws_s3_bucket_notification`); for buckets you manage elsewhere, leave it off and enable notifications yourself with one `aws s3api put-bucket-notification-configuration … '{"EventBridgeConfiguration":{}}'`.

## The recipe

```bash
# 1. Register the source. --partitions declares the Hive-style keys in
#    the bucket layout (for partition-column recovery); --start-from "all"
#    reads pre-existing files on first run, "now" skips them. After that,
#    each run drains the notification queue and reads only the newly-
#    arrived files. --manage-notifications has terraform own the
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

Apply provisions: the runner Lambda with read permission on `your-source-bucket` (IAM scope auto-derived from the registered source's bucket — v0.22.0+) plus `sqs:ReceiveMessage`/`DeleteMessage` on the trigger queue, the SQS queue + EventBridge rule subscribed to object-create events under `events/` (and a dead-letter queue for objects that repeatedly fail to read), the poller Lambda that checks the queue and starts a run when objects are waiting, and the Step Functions state machine that runs the transform — which drains the queue and reads the new objects.

## What you should see

Drop a new file in `s3://your-source-bucket/events/year=2026/month=05/day=13/hour=14/<anything>.parquet`. Within ~60 seconds:

- `/pipelines/dashboard?dir=stream` shows a new run with `trigger = "event"`.
- The Delta table grows by however many rows the file contributed.
- The consumed queue messages are deleted after the write commits; the next run sees only files that arrived since.

Files arriving between runs are drained together in the next run (bounded by `CLAVESA_MAX_FILES_PER_RUN`, default 1000), so a burst of uploads becomes one run rather than one run per file. A backlog larger than the cap drains over several runs.

## Why merge_keys on an "append" output

The runner deletes the consumed queue messages **after** the data write commits. If a run retries (Lambda transient failure, network blip mid-write), those messages redeliver and the same files get re-read on the next attempt. With `mode = "append"` and no `merge_keys`, you'd land the rows twice.

With `merge_keys = ["event_id"]`, the runner upgrades the write to `MERGE INTO target USING staging ON target.event_id = staging.event_id WHEN MATCHED UPDATE * WHEN NOT MATCHED INSERT *`. Matching keys update in place; only genuinely new keys insert. A retry can't dupe.

This is the recommended shape for any event-driven pipeline. Plain `append` is fine only when the source has a strict no-replay guarantee you trust.

## How the moving parts fit together

Three independent pieces, each runnable on its own:

1. **EventBridge rule** on the source bucket → SQS queue. Set up once at deploy.
2. **Poller Lambda** checks the queue depth every minute (cron-triggered) → starts one Step Functions execution when objects are waiting. It does not consume the queue.
3. **Step Functions** invokes the transform Lambda; the runner drains the queue for the new object keys, reads exactly those files, and deletes the messages after the write commits.

## Loading historical files

The pipeline above only processes files that land *after* the EventBridge rule is in place. To load anything older — a year of archival logs, last quarter's partner exports — use the [backfill](backfill.md) flow: stage the historical window into a parallel Delta table, review the result side-by-side with the canonical target, then promote.

## Troubleshooting

**Files land in S3 but no run starts.** Check `aws s3api get-bucket-notification-configuration --bucket your-source-bucket` returns `{"EventBridgeConfiguration": {}}`. Without that, the EventBridge rule never receives the object-create event. If clavesa owns the bucket, re-`source register --manage-notifications` and re-apply; otherwise enable notifications with one `aws s3api put-bucket-notification-configuration --bucket … --notification-configuration '{"EventBridgeConfiguration":{}}'`.

**Run starts but reads zero rows.** Usually the queue had nothing new and the run was a no-op skip, or the EventBridge rule's key prefix doesn't match where files actually land. Confirm the prefix in `clavesa source show events` matches the object keys. If you also see empty partition columns, the partition values in your bucket keys need to match the `partitions = [...]` list — e.g. for a `dt=2026-05-13/` layout, set `partitions` to `["dt"]`.

**`AccessDenied` reading from the source bucket.** The runner Lambda's IAM role is scoped to the buckets in `var.source_inputs`. Confirm the registered source's bucket matches the live bucket name (`clavesa source show events`) — if not, re-`clavesa source register` and re-`source attach` so the pipeline `.tf` carries the new bucket, then `clavesa pipeline deploy stream`.

## See also

- [s3-bulk-ingest](s3-bulk-ingest.md) — one-shot full-bucket read without triggers, lower-overhead pattern for static historical data.
- [backfill](backfill.md) — load historical files that landed before this pipeline existed, or replay a window after a transform-logic fix.
- [merge-cdf](merge-cdf.md) — the SCD-Type-1 variant of the merge_keys pattern this recipe uses.
