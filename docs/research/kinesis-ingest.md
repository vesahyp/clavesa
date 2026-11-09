# Stream ingest: design note

Design note for [ADR-028](../decisions/028-kinesis-stream-source.md),
2026-11-01, revision 3. No AWS or Databricks resources were created. Each claim
is marked **[docs]** (checked in vendor docs or the connector repository on
2026-11-01) or **[guess]** (my estimate or knowledge, not checked or tested).

Revisions:

1. Land then load. Rejected by Vesa: no staging copy, the data must not be
   duplicated.
2. A hand-written Kinesis reader. Rejected by Vesa: use what already exists
   and is tested.
3. This version: Spark Structured Streaming with an existing source and the
   Delta sink.

## The answer

One always-on Fargate task per stream runs a Spark Structured Streaming query
on the clavesa runtime. The query reads with the AWS Kinesis connector (or
Spark's own Kafka source), and writes with the Delta sink every 120 seconds.
Offsets live in the Spark checkpoint in S3. Exactly-once into Delta comes from
the Delta sink. Clavesa writes no offset or sequence-number code.

Cost is unchanged from revision 2: about **$95 a month** at 2 TB and about
**$185** at a 3x month, against about **$216 to $723** and **$431 to about
$1,000** for an always-on Databricks classic job (all guesses, section 6).

The main risk moves to the Kinesis connector: it is young, and it is not on
Maven Central (section 1).

## Sizing

| | Average | Peak (3x) |
|---|---|---|
| Events | 770/s | 2,310/s |
| Data | 0.8 MB/s | 2.4 MB/s |
| Per day | 67 GB, 66 million events | 3x |
| Kinesis shards for writes (1 MB/s or 1,000 records/s each) | 1 | 3 |

Shard limits are **[guess]**. The stream exists, so its shard count is given.
The design assumes 3 to 4 shards.

## 1. The sources

### Kinesis: `awslabs/spark-sql-kinesis-connector`

Facts from the repository **[docs]**, checked 2026-11-01:

| Item | Finding |
|---|---|
| License | Apache-2.0 |
| Spark support | v2.0.0 (2026-04-10) builds against Spark 4.0.1, Scala 2.13. `main` builds against Spark 4.1.0. Clavesa runs 4.0.2. |
| Distribution | Not on Maven Central (`com.amazon.emr:spark-streaming-sql-kinesis-connector_2.13` returns 404). The README says to build it with `mvn clean install`. |
| Activity | 41 stars, 15 contributors, 26 open issues, last push 2026-10-08. Releases: 1.3.0 (2024-07) to 2.0.0 (2026-04). |
| Consumer types | `GetRecords` polling (default) and `SubscribeToShard` (enhanced fan-out) |
| Offsets | Connector metadata in the checkpoint folder (default) or in DynamoDB (`kinesis.metadataCommitterType`) |
| Reshard | `kinesis.describeShardInterval` lists shards every 1 s. v2.0.0 fixed reading from closed shards (PR #65). |
| Limits | README: no speculative execution, no `Trigger.AvailableNow`, no continuous processing |
| Known bugs | `AT_TIMESTAMP` start reported broken (#67). An EFO loop on idle shards was fixed after 2.0.0 (#92). |
| Data loss | `kinesis.failOnDataLoss` defaults to **false** |
| Batch size | `kinesis.maxFetchRecordsPerShard` defaults to 100,000 per micro-batch |
| Endpoint | `kinesis.endpointUrl` is required and can point at a local mock |

Verdict: usable, with care. AWS maintains it and fixes land, but it is young
on Spark 4 and has a small user base. Clavesa will:

- build the jar in the runner image from a pinned commit on `main` after #92,
  and record the commit in `download_jars.sh`;
- use `GetRecords` polling (shared throughput), which avoids the EFO bugs and
  the EFO charge;
- set `kinesis.failOnDataLoss = true`;
- set `kinesis.maxFetchRecordsPerShard = 300000`. At peak a shard gives about
  92,000 records in 120 s [guess], so the default would leave no room for
  catch-up;
- start new streams at `TRIM_HORIZON` or `LATEST`, never `AT_TIMESTAMP`;
- keep a contract test suite against the pinned commit (section 7).

**What the Databricks Kinesis connector has that this one does not** [docs:
Databricks "Connect to Amazon Kinesis"]:

- `Trigger.AvailableNow` (DBR 13.3 LTS and above). The open connector does not
  support it, so a scheduled "drain and stop" run is not possible with it.
- Authentication through a Unity Catalog service credential or connection.
- Tuning options: `shardsPerTask`, `fetchBufferSize`, `minFetchPeriod`,
  `maxFetchDuration`, and `maxPartitions` (DBR 19+).
- Documented reshard handling "without interrupting or restarting the stream".
- `failOnDataLoss` on by default (DBR 19+).
- On restart after EFO, replay of the last uncommitted batch through polling.
- Vendor support, and use in Lakeflow pipelines.

Both have EFO and polling. The open connector also has a DynamoDB metadata
option and a Kinesis sink.

### Kafka: Spark's built-in source

`org.apache.spark:spark-sql-kafka-0-10_2.13` ships with each Spark release and
is on Maven Central (4.0.2 is there) [docs]. Offsets live in the Spark
checkpoint, and it supports `Trigger.AvailableNow` [guess]. For Amazon MSK with
IAM auth, add `aws-msk-iam-auth` [guess].

## 2. Exactly-once without a staging copy

```python
q = (spark.readStream.format("aws-kinesis")
       .option("kinesis.streamName", stream)
       .option("kinesis.endpointUrl", endpoint)
       .option("kinesis.region", region)
       .option("kinesis.startingPosition", "TRIM_HORIZON")
       .option("kinesis.failOnDataLoss", "true")
       .option("kinesis.maxFetchRecordsPerShard", "300000")
       .load()
       .selectExpr("CAST(data AS STRING) AS raw", "partitionKey", "shardId",
                   "sequenceNumber", "approximateArrivalTimestamp")
       # optional user SQL over the batch, then:
       .writeStream.format("delta")
       .outputMode("append")
       .trigger(processingTime="120 seconds")
       .option("checkpointLocation", f"{table_path}/_checkpoints/stream")
       .partitionBy("dt", "hour")
       .toTable(table))
```

- **The guarantee.** The Spark checkpoint stores the source offsets of each
  batch before the batch runs. The Delta sink records the batch in the Delta
  commit. On restart Spark replays the last batch from the checkpoint, and the
  Delta sink skips it if it already committed. The Delta docs: "The
  transaction log enables Delta Lake to guarantee exactly-once processing"
  [docs]. The source must be replayable, and Kinesis is, within its retention
  [guess].
- **Crash recovery.** ECS restarts the task. The query starts from the same
  checkpoint. No clavesa code runs for this.
- **Reshard.** The connector lists shards every second and reads closed shards
  to their end [docs: options, PR #65]. Clavesa's contract test proves the
  order per key (section 7) [guess that it holds].
- **Late data.** The partition is the arrival time, so late records never
  reopen an old partition. Gold aggregations by event time use `merge` with a
  lookback [guess: a usage pattern].

**What the built-in checkpoint does not cover, and the answer:**

| Gap | Answer |
|---|---|
| Producer duplicates: the same event put twice gets two sequence numbers [docs: Kinesis "Handle duplicate records"] | Declare `record_key`. The query adds `withWatermark` on the arrival time and `dropDuplicatesWithinWatermark(record_key)` [guess: Spark 3.5+ API]. Its state lives in the same checkpoint. |
| The checkpoint is lost, corrupt, or must be reset after a query change that it does not allow | A recovery run: a new checkpoint with a new query id (the Delta docs warn that the old app id makes Delta ignore the new writes [docs]), start at `TRIM_HORIZON`, and write through `foreachBatch` with a MERGE on `(shardId, sequenceNumber)`. The table keeps these two columns for this case. No bookkeeping code; only the columns. |
| The writer is down longer than the stream retention | Not recoverable from the stream. `failOnDataLoss = true` stops the query, and the alarms fire long before. |

## 3. Concurrent writers on S3

**One Spark driver per table.**

1. **ECS.** `desired_count = 1`. The deploy uses
   `deployment_maximum_percent = 100` and
   `deployment_minimum_healthy_percent = 0`, so the old task stops before the
   new one starts [guess].
2. **Commit files.** Every commit uses the DynamoDB LogStore
   (`io.delta.storage.S3DynamoDBLogStore`), so a stale task cannot overwrite a
   commit file [docs: experimental, safe only when every writer uses it].
3. **Checkpoint.** Two queries on one checkpoint from two processes are not
   safe [guess]. Layer 1 prevents it. If it happens anyway, layer 2 keeps the
   Delta log whole, and the Delta sink skips a batch id that is already
   committed.

The revision 2 run lease is dropped. It was custom code, and these three
layers cover the same case. Maintenance runs in the same driver (section 4),
which the Delta docs allow ("must originate from a single Spark driver"
[docs]). No other clavesa node and no Databricks job writes the table.

## 4. Small files, commits and compaction

| | Average | Peak |
|---|---|---|
| Trigger | 120 s | 120 s |
| Raw data per batch | 96 MB | 288 MB |
| Commits per day | 720 | 720 |
| Parquet file per batch per partition | about 20 MB [guess: 5x on JSON] | about 60 MB |
| Checkpoint files per batch | offsets, commits, connector metadata per shard | same |

- **Compaction.** At minute 5 of each hour, a driver thread runs `OPTIMIZE` on
  the previous hour partition: about 30 files to one of about 550 MB [guess].
  `OPTIMIZE` and a blind append do not conflict in Delta [guess].
- **Delta checkpoint** every 10 commits by default [guess]: about 72 a day.
- **VACUUM** once a day, default retention 7 days [guess], about 720 small
  files a day.
- **Spark checkpoint cleanup:** Spark keeps the last 100 batches by default
  (`spark.sql.streaming.minBatchesToRetain`) [guess].
- **Cost of compaction:** no extra compute (the task is sized for it). Small
  files kept for 7 days are about 90 GB, about $2 a month [guess]. Those old
  files serve Delta time travel. The current table does not read them.

## 5. Unity Catalog writes and lineage

Unchanged from revision 2. The Delta sink writes through `UCSingleCatalog` to
an external Delta table with OAuth M2M [docs: Unity REST]. OAuth renews the
token for long jobs [docs]. External tables keep the in-driver `OPTIMIZE`.
Unity Catalog warns against several writer clients on one table [docs], and
section 3 keeps one.

Lineage, pushed at each deploy through the external lineage API [docs: Public
Preview]:

```
external_metadata  kinesis/<stream name>  (or kafka/<topic>)
  -> external_metadata  clavesa/<pipeline>.<stream node>
  -> table  <catalog>.<schema>.<table>
```

The gaps are as before [docs]: no lineage in the system tables, and column
lineage needs a column map.

## 6. Monthly cost

Recheck against revision 2: the compute shape is the same (one always-on
driver), so the cost is the same. Structured Streaming adds checkpoint writes
in S3 and drops the lease. Kinesis and table storage are the same on both
sides and are left out. One month is 730 hours. All rates are **[guess]**
from my knowledge and from third-party guides
([mammoth.io](https://mammoth.io/blog/databricks-pricing/)).

**Clavesa:**

| Line | 2 TB month | 3x month |
|---|---|---|
| Fargate: $0.04048 per vCPU-hour, $0.004445 per GB-hour | 2 vCPU, 8 GB: $85 | 4 vCPU, 16 GB: $170 |
| S3 requests: Delta commit plus Spark checkpoint, about 15 per batch | $2 | $3 |
| DynamoDB LogStore, on demand | under $1 | under $1 |
| CloudWatch logs, alarms, custom metrics | $5 | $7 |
| Small files kept until VACUUM | $2 | $6 |
| **Total** | **about $95** | **about $185** |

The JVM of an always-on Spark driver and the connector buffer fit in 8 GB at
the 288 MB peak batch [guess].

**Databricks, always-on Structured Streaming on classic jobs compute:**

| Setup | 2 TB month | 3x month |
|---|---|---|
| Single node | m5.xlarge: $216 to $241 | m5.2xlarge: $431 to $481 |
| Driver + 2 workers, m5.xlarge | $647 to $723 | about $1,000 with m5.2xlarge workers |
| Of which DBUs | $76 to $303 | $150 to $450 |

Rates: m5.xlarge $0.192 per hour EC2 and 0.69 DBU per hour, m5.2xlarge $0.384
and 1.37 DBU, jobs DBU at $0.15 (Premium) to $0.20 (Enterprise).

**Saving:** about $120 to $630 a month at 2 TB, about $250 to $800 at 3x. Most
of it comes from a smaller machine. Serverless Databricks jobs cannot run an
always-on stream: they support only `AvailableNow` and `Once` [docs:
serverless limitations].

## 7. First slice and its local test

**Slice 1: `stream` mode in the runner, local warehouse only.**

1. Runner image: build the Kinesis connector jar from the pinned commit in a
   Maven build stage, and add `spark-sql-kafka-0-10_2.13:4.0.2` to
   `download_jars.sh`.
2. `runner/stream_query.py`: build the query above from the node config
   (source kind, options, optional SQL, `record_key`, trigger interval), plus
   the hourly `OPTIMIZE` thread.
3. CLI `clavesa stream run <node> [--drain]` against the local Delta
   warehouse. `--drain` calls `processAllAvailable()` and stops, because the
   connector has no `AvailableNow` [guess that `processAllAvailable` ends on
   this source].

**Local test**, in `make test-runner-py`:

- **Kinesis mock: LocalStack first.** The connector uses the AWS SDK v2, which
  speaks CBOR to Kinesis by default. LocalStack's Kinesis supports that
  [guess]. moto may need `aws.cborEnabled=false` on the JVM [guess]. Check
  whether LocalStack's free image still runs without an account before
  committing to it [guess].
- **Kafka:** a single-node Kafka container in KRaft mode [guess].
- **Delta:** the local warehouse, as today.

| Test | Pass when |
|---|---|
| Put 10,000 records on 2 shards, `--drain` | exactly 10,000 rows |
| Kill the JVM at 20 random points, restart each time | exactly 10,000 rows |
| Split a shard in the middle | no lost rows, no duplicates, `sequenceNumber` grows per `partitionKey` |
| Same event put twice, `record_key` set | one row |
| Delete the checkpoint, run the recovery run | exactly 10,000 rows |
| Shard expired while the writer was down | the query fails with a data-loss error |
| The same counting test on Kafka | exactly 10,000 rows |

These tests also pin the connector: a new connector commit must pass them
before the image takes it.

**Slice 2:** `modules/stream/aws` (ECS service, task role, DynamoDB LogStore,
checkpoint prefix, alarms), `terraform validate` in a harness, then
`make smoke-cloud` on a test stream once Vesa agrees to create one.

**Slice 3:** Unity Catalog write and lineage push, and the UI.

## Operations

| Alarm | Metric | Default |
|---|---|---|
| Writer down | ECS running task count | below 1 for 5 minutes |
| Writer behind | Structured Streaming progress: now minus the newest `approximateArrivalTimestamp` committed, from a `StreamingQueryListener` | above 10 minutes |
| Batch too slow | batch duration from the listener | above 120 s for 3 batches |
| Stream about to expire records | `GetRecords.IteratorAgeMilliseconds` on the stream [guess] | above 50% of retention |
| Query failed | task exit, error in the log | any |

**Backpressure.** Kinesis holds the backlog. `maxFetchRecordsPerShard` bounds
each batch, so memory stays bounded in a catch-up.

**UI.** The stream node shows events per second and lag as a live badge. The
node page shows lag and events per second as charts over time, from the
listener's progress events. A stream has no runs; it shows a strip of batch
health, one cell per 10 minutes.
