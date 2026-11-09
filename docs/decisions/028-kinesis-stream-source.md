---
adr: 28
title: A Kinesis stream is a workspace source; a transform drains it with Spark Structured Streaming in each scheduled run
date: 2026-11-09
status: Accepted
deciders: Vesa
---

# ADR 028: Kinesis sources, drained by Spark Structured Streaming

**Status**: Accepted (2026-11-09), revision 4. Revisions 1 to 3 were proposed
on 2026-11-01 as ADR-027 (that number is now Unity Catalog publish). Design
note: [docs/research/kinesis-ingest.md](../research/kinesis-ingest.md).
Recipe: [kinesis-to-unity-catalog](../cookbook/kinesis-to-unity-catalog.md).

## Context

At work, a pipeline of about 2 TB and 2 billion events a month runs on
Databricks. Its first steps read Amazon Kinesis Data Streams into Unity
Catalog tables. Clavesa 2.23 already takes over a step of a Databricks
pipeline (ADR-027): it runs the step on Lambda or Fargate, writes a Delta
table into Unity Catalog under the step's own name, pushes lineage and starts
the Databricks jobs that come next. Kinesis was the missing input.

Sizing: about 770 events/s and 0.8 MB/s on average, 3x at peak (about 1 KB per
event).

Forces:

- **No staging copy.** The data goes from the stream straight into the Delta
  table (Vesa, 2026-11-01).
- **Use what exists and is tested.** No hand-written Kinesis reader (Vesa,
  2026-11-01).
- **One engine** (ADR-012): PySpark 4.0.2 with Delta 4.0.0.
- **One writer per Delta table on S3.** Concurrent writers from two drivers
  can lose data unless every writer uses the DynamoDB LogStore (Delta docs).
- **Cost per billion records** is the north star. An always-on task costs the
  same at 2 a.m. with no data as at peak.
- **The step contract of 2.23** is a run that ends: write, register in Unity
  Catalog, push lineage, start the next Databricks jobs.

## Which revision, and why

| Revision | What | Verdict |
|---|---|---|
| 1 | A Lambda lands each batch in S3, a pipeline loads it | Rejected by Vesa: a staging copy |
| 2 | A hand-written `GetRecords` loop per shard, offsets in the Delta commit | Rejected by Vesa: a tested connector exists |
| 3 | Spark Structured Streaming with the AWS Kinesis connector, exactly-once into Delta, on an always-on Fargate service | **The engine is kept.** The always-on service is not the first cloud shape |
| 4 | Revision 3's engine, run as a drain inside each scheduled pipeline run | **This decision** |

Revision 3 is the only one that meets both of Vesa's constraints: no staging
copy and no hand-written reader. Its engine (the connector, the Spark
checkpoint, Delta's idempotent writes) is what this ADR keeps.

What changes is how it runs. Revision 3 ran one always-on ECS service per
table. That shape does not fit the goal: the goal is to take over *steps* of a
Databricks pipeline, and in 2.23 a step is a run that ends, so it can register
its table and start the jobs that come next. An always-on query never ends, so
it never starts a job. It also costs an always-on task per table, and it can
only run on Fargate. A drain that runs inside the normal scheduled run reuses
every 2.23 part unchanged (schedule, Step Functions, the run lease, the
`node_runs` record, Unity Catalog publish, lineage, the job start), runs on
Lambda as well as Fargate, and costs only while it reads. The price is
freshness: the table is as fresh as the schedule (minutes), against about 2
minutes always-on.

## Decision

1. **A Kinesis stream is a workspace source** (ADR-017):
   `clavesa source register <name> --kind kinesis --stream <name>
   [--region] [--starting-position TRIM_HORIZON|LATEST] [--endpoint]`. A
   transform reads it like any source (`source attach`). The runner role gets
   read access to exactly that stream.
2. **A transform whose input is a Kinesis source drains the stream in each
   run.** The runner starts a Spark Structured Streaming query with the
   `awslabs/spark-sql-kinesis-connector` v2.0.0 source (built into the runner
   image from a pinned commit, polling consumer, `failOnDataLoss = true`),
   runs it until the last two progress events read no rows, and stops it.
   The node's SQL runs on each micro-batch; the input alias is the view it
   reads.
3. **Exactly once into Delta.** Offsets live in the Spark checkpoint under the
   warehouse (`_streams/<table>/gen-<n>`). Each micro-batch is appended in
   `foreachBatch` with Delta's idempotent-write options (`txnAppId` = the
   table and checkpoint generation, `txnVersion` = the batch id), so a batch
   Spark replays after a crash is skipped. `foreachBatch`, not the Delta sink,
   because the recovery run (a lost checkpoint: new generation from the
   earliest record, `MERGE` on the record position) must continue on the
   same checkpoint, and Spark does not allow a sink-type change on one.
4. **Deadlines are soft.** On Lambda the drain stops two minutes before the
   function's limit; elsewhere after 50 minutes. A stopped drain commits what
   it wrote, says `caught_up: false`, and the next run continues.
5. **Shape limits.** The Kinesis input is the transform's only input, the
   transform has one output, and the output appends. The output keeps
   `_source_partition` and `_source_offset` (the record's position).
   Partitioning comes from the output's `partition_by`, else `dt`, `hour`
   when the output has them.
6. **One writer.** The pipeline's run lease already keeps two runs of one
   pipeline from overlapping, so one driver writes a table at a time. No
   DynamoDB LogStore.
7. **Unity Catalog, lineage and the next jobs** work exactly as for a batch
   output (ADR-027): `prepare` before the drain (a new table is created at
   `unity_catalog_location`), `publish` after it, lineage
   `kinesis:<region>:<stream> → clavesa step → table`.
8. **Local mode** (ADR-014) runs the same code in the runner container
   (`clavesa pipeline run`), and `clavesa stream run` stays for the
   standalone and always-on local query, with Kafka.

## Consequences

Gains:

- Tested code owns the hard parts: shard discovery, reshard order, offsets,
  replay and exactly-once into Delta.
- Kinesis steps of a Databricks pipeline move to clavesa with nothing new
  around them: the schedule, the run page, Unity Catalog, lineage and the job
  start are the 2.23 ones.
- Runs on Lambda for small streams and on Fargate for large ones.
- Pay per run, not per hour.

Costs:

- **Freshness is the schedule.** Minutes, not seconds.
- **The schedule must stay shorter than the stream's retention**, or records
  expire unread. The run then fails loudly (`failOnDataLoss`).
- **The Kinesis connector is young** (v2.0.0 of 2026-04-10 is its first
  Spark 4 release), not on Maven Central, has no `Trigger.AvailableNow`
  (hence the drain test on progress events) and a broken `AT_TIMESTAMP`
  start (#67). Clavesa pins a commit and patches one bug (closed-shard
  start, `runner/patches/`).
- **The checkpoint is state outside the table.** A lost checkpoint needs a
  recovery run (`clavesa stream run --recover`; the pipeline form is open).
- **Some SQL changes break the checkpoint** (a stateful change). Row-level
  SQL (parse, cast, filter) does not.
- **Per micro-batch SQL.** An aggregate aggregates one batch; whole-table
  logic goes in the next transform.

## Proven

- Local, `make test-stream` (LocalStack Kinesis and Kafka): exact row counts
  after `kill -9` cycles, a shard split, producer duplicates and a checkpoint
  reset.
- Cloud, `make smoke-kinesis` (2026-11-09, personal AWS account, us-east-2,
  one provisioned shard, the personal Databricks test workspace). Checked from
  a Databricks SQL warehouse as a reader of the table: 1,000 events on
  Fargate; a run stopped mid-drain while 2,500 more arrived had committed
  2,900 of 3,500 rows, and the next run brought the table to exactly 3,500
  rows and 3,500 distinct ids; a run with nothing new added nothing; the
  same pipeline on Lambda continued the checkpoint to exactly 4,000. Sums and
  sample rows matched what was sent; lineage upstream of the table. A Fargate
  run (2 vCPU, 8 GB) took 50 to 72 seconds of container time and peaked at
  1.2 to 1.8 GB.

## Open

- An always-on mode (revision 3's ECS service) for a step that needs a table
  fresher than a few minutes.
- A recovery run from a pipeline (today only `clavesa stream run --recover`).
- Kafka as a pipeline source (the local stream mode reads it already).
- Producer duplicates in the pipeline form (`--record-key` is local only).

## Options not taken

### Land then load (revision 1, rejected by Vesa, 2026-11-01)

A Lambda wrote each Kinesis batch as a file under `_landing/` in S3, and the
S3-triggered pipeline loaded it into Delta. Rejected because it keeps a
staging copy.

### A hand-written Kinesis reader (revision 2, rejected by Vesa, 2026-11-01)

A custom `GetRecords` loop per shard, with the sequence numbers in the Delta
commit. Rejected because a tested connector exists.

### An always-on Fargate service per table (revision 3)

Kept as the next step if freshness of about 2 minutes is needed. Not first,
because a query that never ends cannot start the next Databricks jobs, it
costs an always-on task per table, and it does not run on Lambda.

### Lambda with a Kinesis event source mapping, writing Delta directly

The 6 MB invocation limit closes a batch at about 4 MB: about 15,000 commits a
day of about 1 MB files, and one writer per shard.

### Fargate with delta-rs

A second Delta writer next to the runtime, and the Unity Catalog docs do not
list delta-rs as a supported writer.

### The Databricks Kinesis connector

It runs only in the Databricks Runtime. The point is to leave Databricks
compute.
