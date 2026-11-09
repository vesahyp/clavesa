"""Test helper for `make test-stream` (ADR-028 slice 1).

Runs inside the runner image on the test docker network, so it has boto3,
PySpark, Delta and the Kafka jars. scripts/test-stream.sh drives it; each
subcommand prints one JSON line.

    kinesis-create --stream S --shards N
    kinesis-put    --stream S --start I --count N [--rate R] [--repeat-from J --repeat-count K]
    kinesis-split  --stream S
    kafka-put      --bootstrap B --topic T --start I --count N
    count          --path P [--order]

Events are JSON: {"event_id": "e<i>", "n": <i>}. The partition key is
"k<i mod 50>", so ordering per key can be checked after a shard split.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

KINESIS_ENDPOINT = os.environ.get("KINESIS_ENDPOINT", "http://localstack:4566")
REGION = os.environ.get("AWS_REGION", "us-east-1")


def _kinesis():
    import boto3  # noqa: PLC0415

    return boto3.client("kinesis", endpoint_url=KINESIS_ENDPOINT, region_name=REGION)


def _event(i: int) -> tuple[bytes, str]:
    return json.dumps({"event_id": f"e{i}", "n": i}).encode(), f"k{i % 50}"


def kinesis_create(a) -> dict:
    k = _kinesis()
    k.create_stream(StreamName=a.stream, ShardCount=a.shards)
    for _ in range(60):
        if k.describe_stream_summary(StreamName=a.stream)["StreamDescriptionSummary"]["StreamStatus"] == "ACTIVE":
            break
        time.sleep(0.5)
    return {"stream": a.stream, "shards": a.shards}


def _put_range(k, stream: str, ids: list[int], rate: float) -> int:
    put = 0
    for off in range(0, len(ids), 250):
        chunk = ids[off : off + 250]
        records = [{"Data": d, "PartitionKey": pk} for d, pk in map(_event, chunk)]
        pending = records
        for _ in range(20):
            resp = k.put_records(StreamName=stream, Records=pending)
            if resp.get("FailedRecordCount", 0) == 0:
                break
            pending = [r for r, res in zip(pending, resp["Records"]) if "ErrorCode" in res]
            time.sleep(0.2)
        else:
            raise SystemExit("put_records kept failing")
        put += len(chunk)
        if rate > 0:
            time.sleep(len(chunk) / rate)
    return put


def kinesis_put(a) -> dict:
    k = _kinesis()
    n = _put_range(k, a.stream, list(range(a.start, a.start + a.count)), a.rate)
    dup = 0
    if a.repeat_count:
        # Producer retries: the same events again, so new sequence numbers.
        dup = _put_range(k, a.stream, list(range(a.repeat_from, a.repeat_from + a.repeat_count)), 0)
    return {"put": n, "repeated": dup}


def kinesis_split(a) -> dict:
    k = _kinesis()
    shards = [s for s in k.list_shards(StreamName=a.stream)["Shards"]
              if "EndingSequenceNumber" not in s["SequenceNumberRange"]]
    s = shards[0]
    lo, hi = int(s["HashKeyRange"]["StartingHashKey"]), int(s["HashKeyRange"]["EndingHashKey"])
    k.split_shard(StreamName=a.stream, ShardToSplit=s["ShardId"], NewStartingHashKey=str((lo + hi) // 2))
    for _ in range(60):
        if k.describe_stream_summary(StreamName=a.stream)["StreamDescriptionSummary"]["StreamStatus"] == "ACTIVE":
            break
        time.sleep(0.5)
    open_after = [x["ShardId"] for x in k.list_shards(StreamName=a.stream)["Shards"]
                  if "EndingSequenceNumber" not in x["SequenceNumberRange"]]
    return {"split": s["ShardId"], "open_shards": open_after}


def _spark():
    from pyspark.sql import SparkSession  # noqa: PLC0415

    return (
        SparkSession.builder.master("local[2]").appName("stream-harness")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )


def kafka_put(a) -> dict:
    spark = _spark()
    rows = [(f"k{i % 50}", json.dumps({"event_id": f"e{i}", "n": i})) for i in range(a.start, a.start + a.count)]
    (
        spark.createDataFrame(rows, "key string, value string")
        .write.format("kafka")
        .option("kafka.bootstrap.servers", a.bootstrap)
        .option("topic", a.topic)
        .save()
    )
    return {"put": len(rows)}


def count(a) -> dict:
    from pyspark.sql import functions as F  # noqa: PLC0415

    spark = _spark()
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.read.format("delta").load(a.path)
    df = df.withColumn("event_id", F.get_json_object("raw", "$.event_id"))
    rows = df.count()
    distinct = df.select("event_id").distinct().count()
    out = {"rows": rows, "distinct_events": distinct, "duplicates": rows - distinct}
    if a.order:
        # Per partition key, the order in which records were committed must
        # follow the stream order. Within one commit order is not defined,
        # so compare commit versions: a record with a higher sequence number
        # must never land in an earlier commit than a lower one.
        cdf = (
            spark.read.format("delta").option("readChangeFeed", "true")
            .option("startingVersion", 0).load(a.path)
            .where("_change_type = 'insert'")
            .select("_source_partition", "_source_offset", "_commit_version")
            .collect()
        )
        by_key: dict[str, list[tuple[int, int]]] = {}
        for r in cdf:
            by_key.setdefault(r["_source_partition"], []).append(
                (int(r["_source_offset"]), int(r["_commit_version"]))
            )
        violations = 0
        for recs in by_key.values():
            recs.sort()
            for (_, v1), (_, v2) in zip(recs, recs[1:]):
                if v2 < v1:
                    violations += 1
        out["order_violations"] = violations
        out["keys_checked"] = len(by_key)
    return out


def main() -> int:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("kinesis-create")
    c.add_argument("--stream", required=True)
    c.add_argument("--shards", type=int, default=2)
    c = sub.add_parser("kinesis-put")
    c.add_argument("--stream", required=True)
    c.add_argument("--start", type=int, default=0)
    c.add_argument("--count", type=int, required=True)
    c.add_argument("--rate", type=float, default=0)
    c.add_argument("--repeat-from", type=int, default=0)
    c.add_argument("--repeat-count", type=int, default=0)
    c = sub.add_parser("kinesis-split")
    c.add_argument("--stream", required=True)
    c = sub.add_parser("kafka-put")
    c.add_argument("--bootstrap", required=True)
    c.add_argument("--topic", required=True)
    c.add_argument("--start", type=int, default=0)
    c.add_argument("--count", type=int, required=True)
    c = sub.add_parser("count")
    c.add_argument("--path", required=True)
    c.add_argument("--order", action="store_true")
    a = p.parse_args()
    fn = {
        "kinesis-create": kinesis_create,
        "kinesis-put": kinesis_put,
        "kinesis-split": kinesis_split,
        "kafka-put": kafka_put,
        "count": count,
    }[a.cmd]
    print(json.dumps(fn(a)), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
