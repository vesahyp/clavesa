# Read a Kinesis stream into Unity Catalog

> **When you have one:** events on an Amazon Kinesis Data Stream that a Databricks streaming job reads into a Unity Catalog table all day. Clavesa reads the stream on Fargate or Lambda, on a schedule, appends to a Delta table registered in Unity Catalog under the step's own name, pushes lineage, and starts the Databricks jobs that come next. Nothing is staged: records go from the stream straight into the table ([ADR-028](../decisions/028-kinesis-stream-source.md)).

Do [feed-a-databricks-job](feed-a-databricks-job.md) first: this recipe needs a workspace on the cloud warehouse that is connected to Databricks. The rollout for a step that already runs at work (shadow mode, the daily comparison, the switch) is in the [getting-started guide](../databricks/getting-started.md); a Kinesis step follows it the same way.

## What you'll end up with

- A workspace source for the stream, and a transform that reads it. Every run reads what arrived since the last run, from a Spark checkpoint in the clavesa bucket, and appends it to the table.
- `<catalog>.<schema>.<node>` in Unity Catalog, an external Delta table, with lineage `kinesis stream → clavesa step → table`.
- Exactly once: a run that stops part-way (a crash, a deploy, a time limit) writes nothing twice and loses nothing. The next run continues from the checkpoint.

## How it runs

Each run is a Spark Structured Streaming query that drains the stream: it reads micro-batches until the stream has no new records, then stops. Your SQL runs on each micro-batch. Each batch is appended with Delta's idempotent-write options, so a batch Spark replays after a crash is skipped. Then the run publishes to Unity Catalog and starts the next jobs, like any clavesa step.

- **Freshness** is the schedule: every 15 minutes means the table is at most about 15 minutes behind, plus the run time.
- **The schedule must be shorter than the stream's retention** (24 hours by default). Records that expire before a run reads them are lost, and the run fails loudly (`failOnDataLoss`).
- **Lambda** runs stop about two minutes before the 15-minute limit and leave the rest for the next run. Use `fargate` when one run's backlog is more than Lambda reads in 13 minutes.

## The recipe

```bash
# 1. The stream, as a workspace source. --region defaults to the deploy region.
bin/clavesa source register clicks --kind kinesis --stream <stream name> --region <region>

# 2. The step. --schema is the Unity Catalog schema; the node name is the table name:
#    this writes <catalog>.web.clicks. The SQL reads the input under the alias
#    you attach it as (here: events). Each record's payload is the column `raw`
#    (a string); keep _source_partition and _source_offset, they are the record's
#    position in the stream.
bin/clavesa pipeline create web --schema web
bin/clavesa node add web --type transform --name clicks
bin/clavesa node edit web clicks \
  --set "sql=SELECT CAST(get_json_object(raw, '\$.id') AS BIGINT) AS id, get_json_object(raw, '\$.user') AS user_id, CAST(get_json_object(raw, '\$.amount') AS DECIMAL(12,2)) AS amount, CAST(arrival_ts AS DATE) AS event_date, arrival_ts, _source_partition, _source_offset FROM events" \
  --set compute=fargate \
  --output-mode append --output-partition-by event_date
bin/clavesa source attach web clicks --to clicks --as events

# 3. Size and schedule (every 15 minutes). A small task is enough for a few MB/s.
cat > web/terraform.tfvars <<'EOF'
fargate_cpu       = 2
fargate_memory_gb = 8
trigger_schedule  = "rate(15 minutes)"
EOF

# 4. Deploy and run once by hand.
bin/clavesa pipeline deploy web --yes
bin/clavesa pipeline run web --wait
```

The run ends like a batch step's:

```
Status: SUCCEEDED
Unity Catalog: <catalog>.web.clicks registered  https://<host>/explore/data/<catalog>/web/clicks
```

The run log (`bin/clavesa pipeline logs web`) has one line per run with what it read:

```
[clavesa] stream 'events' -> <catalog>__web.clicks: 1000 rows in 1 batch(es), generation 1, caught up
```

### The input columns

The view your SQL reads has one row per record:

| Column | Type | What it is |
|---|---|---|
| `raw` | string | the record's data, decoded as UTF-8 |
| `_source_partition` | string | the record's partition key |
| `_source_offset` | string | its sequence number |
| `arrival_ts` | timestamp | when Kinesis received it |
| `dt`, `hour` | string, int | `arrival_ts` as a day and an hour |

The output must keep `_source_partition` and `_source_offset`. Without `--output-partition-by`, the table is partitioned by `dt` and `hour` when the output has both, else not at all.

## Verify

```bash
bin/clavesa workspace databricks tables          # <catalog>.web.clicks, "written by clavesa"
bin/clavesa pipeline runs web                    # the run, SUCCEEDED
```

In Databricks (a SQL warehouse or a notebook):

```sql
SELECT count(*), count(DISTINCT _source_partition, _source_offset) FROM <catalog>.web.clicks;
```

The two numbers are equal: no record is in the table twice. The **Lineage** tab of the table in Catalog Explorer shows `clavesa_web_clicks` upstream, with the stream upstream of it.

`make smoke-kinesis` walks this recipe on a real stream: it creates a one-shard stream, puts events on it, runs on Fargate, stops a run in the middle while events arrive, runs again, then on Lambda, checks counts and sample rows in Databricks, and deletes the stream.

## Limits (first version)

- A Kinesis input is the transform's only input, the transform has one output, and the output appends. Joins and aggregates over the whole table belong in a second transform that reads this table.
- The SQL runs per micro-batch: row-level work (parse, cast, filter, rename). An aggregate would aggregate one batch.
- Producer duplicates (the same event sent twice, at two positions) are kept; drop them downstream, or see `clavesa stream run --record-key` for the local stream mode.
- `AT_TIMESTAMP` start is not supported (broken upstream, awslabs/spark-sql-kinesis-connector#67). `--starting-position LATEST` starts a new checkpoint at the end of the stream.
- No always-on mode in the cloud yet: freshness is the schedule. ADR-028 keeps the always-on Fargate service as the next step if a minute matters.
- No backfill: the stream keeps only its retention window.
