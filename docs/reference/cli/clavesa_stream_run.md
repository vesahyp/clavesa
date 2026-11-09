# clavesa stream run

Run one stream node against the local warehouse

Run a Spark Structured Streaming query that reads a Kinesis stream or a
Kafka topic and writes the Delta table <catalog>__<schema>.<name> in the local
warehouse. Nothing is staged: records go from the stream into the table.

Offsets live in the Spark checkpoint under the warehouse
(_streams/<table>/gen-<n>). Each micro-batch is written with Delta's
idempotent-write options, so a batch that Spark replays after a crash is not
written twice. failOnDataLoss is on: a shard or partition that expired before
it was read fails the run.

Without --drain the query runs until it is stopped. With --drain it stops once
every source reports no new data, and prints a summary.

--record-key drops producer duplicates (dropDuplicatesWithinWatermark over the
arrival time). --recover is for a lost or reset checkpoint: it starts a new
checkpoint generation at the earliest retained position and MERGEs on the
record key (or on _source_partition + _source_offset) until drained.

Examples:
  clavesa stream run clicks --kind kinesis --stream clicks \
    --endpoint http://localstack:4566 --network my-net --drain
  clavesa stream run orders --kind kafka --bootstrap-servers kafka:9092 \
    --topic orders --network my-net

## Usage

```
clavesa stream run <name> [flags]
```

## Flags

```
      --bootstrap-servers string     Kafka bootstrap servers
      --container-name string        container name (default clavesa-stream-<workspace>-<name>)
      --drain                        stop once every source reports no new data
      --endpoint string              Kinesis endpoint URL (required by the connector)
      --forward-env strings          env vars forwarded to the runner when set (default [AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,AWS_SESSION_TOKEN,AWS_REGION])
      --json                         print the summary as JSON
      --kind string                  source kind: kinesis or kafka (default "kinesis")
      --network string               docker network the endpoint is on
      --optimize-every-seconds int   OPTIMIZE the previous hour partition at this interval (0 = off); not used with --drain (default 3600)
      --record-key string            comma-separated columns that identify one event; drops producer duplicates
      --recover                      new checkpoint generation from the earliest position, MERGE until drained (needs --drain)
      --region string                Kinesis region (default: from the endpoint)
      --schema string                schema the table lands in (default "streams")
      --sql batch                    optional SQL over the view batch; must keep the source columns
      --starting-offsets string      Kafka start for a new checkpoint: earliest or latest (default "earliest")
      --starting-position string     Kinesis start for a new checkpoint: TRIM_HORIZON or LATEST (default "TRIM_HORIZON")
      --stream string                Kinesis stream name
      --topic string                 Kafka topic
      --trigger-seconds int          micro-batch interval (default 120)
      --watermark string             how long a record key is remembered for duplicate drops (default "10 minutes")
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa stream](clavesa_stream.md) — Run stream nodes (Kinesis, Kafka) into Delta tables
- [Command index](README.md)
