"""Local validation for the v0.12.0 incremental-processing helpers.

Run with: python3 -m pytest tests/runner/test_incremental.py -v
   or:    python3 tests/runner/test_incremental.py

Exercises the new functions in runner/runner.py without any cloud:
  - _list_partition_tree: recursive Hive-style S3 walk
  - _resolve_initial_cursor: "all"/"now"/literal start_from semantics
  - _read_watermark / _write_watermark: S3 JSON round-trip
  - _resolve_input: dict-form descriptors → DataFrame + watermark advance
  - _resolve_output: dict-form descriptors → write spec
  - handler(): skip-on-empty path, watermark-after-success path

boto3 is stubbed with an in-memory fake. Spark is not exercised — that
path requires the runner image; this test covers the partition/cursor
plumbing only.
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

REPO = Path(__file__).resolve().parents[2]
RUNNER = REPO / "runner" / "runner.py"


def _load_runner():
    """Import runner.py with boto3/pyspark/botocore stubbed.

    Avoids the heavy native deps for purely-Python tests. Each test resets
    the fake S3 bucket before running.
    """
    fake_s3 = _FakeS3Backend()
    fake_sqs = _FakeSQSBackend()
    boto3_mod = types.ModuleType("boto3")

    def _client(service, *_a, **_k):
        if service == "sqs":
            return fake_sqs.client()
        return fake_s3.client()

    boto3_mod.client = _client  # type: ignore[attr-defined]

    botocore_mod = types.ModuleType("botocore")
    botocore_exceptions = types.ModuleType("botocore.exceptions")
    class _ClientError(Exception):
        def __init__(self, response):
            self.response = response
            super().__init__(response)
    botocore_exceptions.ClientError = _ClientError
    botocore_mod.exceptions = botocore_exceptions

    pyspark_mod = types.ModuleType("pyspark")
    pyspark_sql = types.ModuleType("pyspark.sql")
    class _DataFrame:
        pass
    pyspark_sql.DataFrame = _DataFrame
    pyspark_sql.SparkSession = object
    pyspark_mod.sql = pyspark_sql

    sys.modules["boto3"] = boto3_mod
    sys.modules["botocore"] = botocore_mod
    sys.modules["botocore.exceptions"] = botocore_exceptions
    sys.modules["pyspark"] = pyspark_mod
    sys.modules["pyspark.sql"] = pyspark_sql

    spec = importlib.util.spec_from_file_location("runner", str(RUNNER))
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    mod._FAKE_S3 = fake_s3  # expose so tests can populate it
    mod._FAKE_SQS = fake_sqs  # notification-drain ingest (#25)
    return mod


class _FakeS3Backend:
    """Tiny dict-backed S3 stand-in. Stores object bodies; supports
    list_objects_v2 with prefix+delimiter (CommonPrefixes), get_object,
    put_object."""

    def __init__(self):
        self.objects: dict[tuple[str, str], bytes] = {}

    def reset(self):
        self.objects.clear()

    def put(self, bucket: str, key: str, body: bytes = b""):
        self.objects[(bucket, key)] = body

    def client(self):
        backend = self

        class _Client:
            def list_objects_v2(self, **kwargs):
                bucket = kwargs["Bucket"]
                prefix = kwargs.get("Prefix", "")
                delim = kwargs.get("Delimiter")
                keys = [k for (b, k) in backend.objects.keys() if b == bucket and k.startswith(prefix)]
                if delim is None:
                    return {"Contents": [{"Key": k} for k in sorted(keys)]}
                # CommonPrefixes simulation: collect distinct prefix-up-to-next-delim.
                common: set[str] = set()
                contents: list[dict] = []
                for k in keys:
                    rest = k[len(prefix):]
                    if delim in rest:
                        common.add(prefix + rest.split(delim, 1)[0] + delim)
                    else:
                        contents.append({"Key": k})
                return {
                    "CommonPrefixes": sorted([{"Prefix": p} for p in common], key=lambda x: x["Prefix"]),
                    "Contents": sorted(contents, key=lambda x: x["Key"]),
                }

            def get_paginator(self, name):
                assert name == "list_objects_v2"
                outer = self

                class _Pag:
                    def paginate(self, **kwargs):
                        yield outer.list_objects_v2(**kwargs)

                return _Pag()

            def get_object(self, **kwargs):
                bucket = kwargs["Bucket"]
                key = kwargs["Key"]
                if (bucket, key) not in backend.objects:
                    from botocore.exceptions import ClientError  # type: ignore
                    raise ClientError({"Error": {"Code": "NoSuchKey"}})
                body = backend.objects[(bucket, key)]
                return {"Body": _StreamingBody(body)}

            def put_object(self, **kwargs):
                backend.objects[(kwargs["Bucket"], kwargs["Key"])] = kwargs["Body"]
                return {}

        return _Client()


class _StreamingBody:
    def __init__(self, body: bytes):
        self._body = body
    def read(self):
        return self._body


class _FakeSQSBackend:
    """In-memory SQS stand-in for notification-drain ingest (#25). Holds a
    list of messages; receive_message pops up to MaxNumberOfMessages, and
    delete_message_batch records the receipt handles deleted (so a test can
    assert delete-after-commit fired on the right handles)."""

    def __init__(self):
        self.messages: list[dict] = []
        self.deleted: list[str] = []
        self.fail_delete = False

    def reset(self):
        self.messages = []
        self.deleted = []
        self.fail_delete = False

    def add_object_event(self, bucket: str, key: str, handle: str):
        """Enqueue an EventBridge `Object Created` event for one S3 object."""
        body = json.dumps({"detail": {"bucket": {"name": bucket}, "object": {"key": key}}})
        self.messages.append({"Body": body, "ReceiptHandle": handle})

    def add_raw(self, body: str, handle: str):
        """Enqueue a raw (possibly un-parseable) message body."""
        self.messages.append({"Body": body, "ReceiptHandle": handle})

    def client(self):
        backend = self

        class _Client:
            def receive_message(self, **kwargs):
                n = kwargs.get("MaxNumberOfMessages", 10)
                batch = backend.messages[:n]
                backend.messages = backend.messages[n:]
                return {"Messages": batch} if batch else {}

            def delete_message_batch(self, **kwargs):
                if backend.fail_delete:
                    raise RuntimeError("simulated delete failure")
                backend.deleted.extend(e["ReceiptHandle"] for e in kwargs["Entries"])
                return {}

        return _Client()


class _FakeSpark:
    """Minimal Spark stand-in: records the keys handed to a reader so a drain
    test can assert exactly which objects were read. `read.option(...).parquet(
    *keys)` returns a sentinel DataFrame and stashes the keys on `.read_keys`."""

    def __init__(self):
        self.read_keys: list[str] | None = None
        self.base_path: str | None = None

    @property
    def read(self):
        spark = self

        class _Reader:
            def option(self, name, value):
                if name == "basePath":
                    spark.base_path = value
                return self

            def parquet(self, *keys):
                spark.read_keys = list(keys)
                return "DF"

            def csv(self, *keys):
                spark.read_keys = list(keys)
                return "DF"

            def json(self, *keys):
                spark.read_keys = list(keys)
                return "DF"

        return _Reader()


# ---------------------------------------------------------------------------
# Test fixtures: a synthetic CloudFront-shaped partition tree.
# ---------------------------------------------------------------------------


def _seed_cloudfront_partitions(s3: _FakeS3Backend, bucket: str, base: str, days_hours: list[tuple[str, str]]):
    """Populate fake S3 with one Parquet 'object' per (day, hour) leaf."""
    s3.reset()
    for day, hour in days_hours:
        leaf = f"{base}day={day}/hour={hour}/part-0000.parquet"
        s3.put(bucket, leaf, b"PAR1")  # pretend Parquet header


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_list_partition_tree_walks_two_levels():
    runner = _load_runner()
    s3 = runner._FAKE_S3
    _seed_cloudfront_partitions(s3, "logs", "year=2026/month=04/", [
        ("2026-04-26", "00"),
        ("2026-04-26", "01"),
        ("2026-04-27", "00"),
    ])
    parts = runner._list_partition_tree("logs", "year=2026/month=04/", ["day", "hour"])
    assert [(c, leaf.split("/", 2)[-1] if "/" in leaf else leaf) for c, leaf in parts] == [
        (("2026-04-26", "00"), "day=2026-04-26/hour=00/"),
        (("2026-04-26", "01"), "day=2026-04-26/hour=01/"),
        (("2026-04-27", "00"), "day=2026-04-27/hour=00/"),
    ]


def test_list_partition_tree_handles_unrelated_keys():
    """Partition walk must skip keys at the same level whose name doesn't
    start with the expected '<partition>=' token."""
    runner = _load_runner()
    s3 = runner._FAKE_S3
    s3.reset()
    s3.put("logs", "year=2026/month=04/day=2026-04-26/hour=00/part.parquet", b"")
    s3.put("logs", "year=2026/month=04/_SUCCESS", b"")  # noise
    s3.put("logs", "year=2026/month=04/day=2026-04-26/hour=00/.checksum", b"")
    parts = runner._list_partition_tree("logs", "year=2026/month=04/", ["day", "hour"])
    assert parts == [(("2026-04-26", "00"), "year=2026/month=04/day=2026-04-26/hour=00/")]


def test_resolve_initial_cursor_modes():
    runner = _load_runner()
    parts = [(("2026-04-26", "00"), "x"), (("2026-04-27", "01"), "y")]

    assert runner._resolve_initial_cursor("all", parts) is None
    assert runner._resolve_initial_cursor("", parts) is None
    assert runner._resolve_initial_cursor("now", parts) == ("2026-04-27", "01")
    assert runner._resolve_initial_cursor("now", []) is None
    assert runner._resolve_initial_cursor("2026-04-26", parts) == ("2026-04-26",)
    # Multi-segment literal — supports e.g. "2026-04-26/14".
    assert runner._resolve_initial_cursor("2026-04-26/14", parts) == ("2026-04-26", "14")


def test_watermark_round_trip():
    runner = _load_runner()
    s3 = runner._FAKE_S3
    s3.reset()
    uri = "s3://wm/pipe/_watermarks/src.json"

    assert runner._read_watermark(uri) is None  # no key yet

    runner._write_watermark(uri, ("2026-04-26", "07"))
    cursor = runner._read_watermark(uri)
    assert cursor == ("2026-04-26", "07")

    payload = json.loads(s3.objects[("wm", "pipe/_watermarks/src.json")])
    assert payload["cursor"] == ["2026-04-26", "07"]
    assert "updated_at" in payload


def test_resolve_input_partitioned_first_run_all():
    """First run with start_from='all' and no watermark reads everything;
    new_cursor is the max."""
    runner = _load_runner()
    s3 = runner._FAKE_S3
    _seed_cloudfront_partitions(s3, "logs", "year=2026/month=04/", [
        ("2026-04-26", "00"),
        ("2026-04-27", "01"),
    ])
    os.environ["CLAVESA_WATERMARKS"] = "s3://wm/pipe/_watermarks/"

    spark = MagicMock()
    spark.read.option.return_value.parquet.return_value = "<df>"

    src = {
        "kind": "partitioned_path",
        "path": "s3://logs/year=2026/month=04/",
        "partitions": ["day", "hour"],
        "start_from": "all",
    }
    df, advance = runner._resolve_input(spark, "cloudfront", src)
    assert df == "<df>"
    args = spark.read.option.return_value.parquet.call_args.args
    assert "s3://logs/year=2026/month=04/day=2026-04-26/hour=00/" in args
    assert "s3://logs/year=2026/month=04/day=2026-04-27/hour=01/" in args
    # basePath must be set so Spark reconstructs partition columns from
    # the leaf paths' tails — without it, multi-path reads drop year/
    # month/day/hour columns and a later append fails INCOMPATIBLE_DATA.
    spark.read.option.assert_called_with("basePath", "s3://logs/year=2026/month=04/")
    assert advance == {"uri": "s3://wm/pipe/_watermarks/cloudfront.json",
                       "new_cursor": ("2026-04-27", "01")}


def test_resolve_input_partitioned_skips_when_no_new():
    """Second run with watermark at max returns (None, None) — handler should
    short-circuit."""
    runner = _load_runner()
    s3 = runner._FAKE_S3
    _seed_cloudfront_partitions(s3, "logs", "year=2026/month=04/", [
        ("2026-04-26", "00"),
        ("2026-04-27", "01"),
    ])
    os.environ["CLAVESA_WATERMARKS"] = "s3://wm/pipe/_watermarks/"
    runner._write_watermark("s3://wm/pipe/_watermarks/cloudfront.json", ("2026-04-27", "01"))

    spark = MagicMock()
    src = {
        "kind": "partitioned_path",
        "path": "s3://logs/year=2026/month=04/",
        "partitions": ["day", "hour"],
        "start_from": "all",
    }
    df, advance = runner._resolve_input(spark, "cloudfront", src)
    assert df is None
    assert advance is None
    spark.read.option.assert_not_called()
    spark.read.parquet.assert_not_called()


def test_resolve_input_partitioned_skip_bypassed_when_forced():
    """`--force` plumbed into _resolve_input(forced=True) — even with the
    watermark already at the max partition, the runner re-reads the full
    source range instead of returning (None, None). Watermark still
    advances on success (pinned to the same value here, a no-op)."""
    runner = _load_runner()
    s3 = runner._FAKE_S3
    _seed_cloudfront_partitions(s3, "logs", "year=2026/month=04/", [
        ("2026-04-26", "00"),
        ("2026-04-27", "01"),
    ])
    os.environ["CLAVESA_WATERMARKS"] = "s3://wm/pipe/_watermarks/"
    runner._write_watermark("s3://wm/pipe/_watermarks/cloudfront.json", ("2026-04-27", "01"))

    spark = MagicMock()
    spark.read.option.return_value.parquet.return_value = "<df>"

    src = {
        "kind": "partitioned_path",
        "path": "s3://logs/year=2026/month=04/",
        "partitions": ["day", "hour"],
        "start_from": "all",
    }
    df, advance = runner._resolve_input(spark, "cloudfront", src, forced=True)
    # Did NOT skip — got back a df and read every partition under the prefix.
    assert df is not None
    assert advance is not None
    spark.read.option.return_value.parquet.assert_called_once()
    paths = spark.read.option.return_value.parquet.call_args.args
    assert len(paths) == 2  # both partitions re-read


def test_resolve_input_partitioned_reads_only_new():
    """Watermark mid-range: read only partitions strictly above it."""
    runner = _load_runner()
    s3 = runner._FAKE_S3
    _seed_cloudfront_partitions(s3, "logs", "year=2026/month=04/", [
        ("2026-04-26", "00"),
        ("2026-04-26", "01"),
        ("2026-04-27", "00"),
    ])
    os.environ["CLAVESA_WATERMARKS"] = "s3://wm/pipe/_watermarks/"
    runner._write_watermark("s3://wm/pipe/_watermarks/cloudfront.json", ("2026-04-26", "01"))

    spark = MagicMock()
    spark.read.option.return_value.parquet.return_value = "<df>"

    src = {
        "kind": "partitioned_path",
        "path": "s3://logs/year=2026/month=04/",
        "partitions": ["day", "hour"],
        "start_from": "all",
    }
    df, advance = runner._resolve_input(spark, "cloudfront", src)
    args = spark.read.option.return_value.parquet.call_args.args
    assert args == ("s3://logs/year=2026/month=04/day=2026-04-27/hour=00/",)
    assert advance["new_cursor"] == ("2026-04-27", "00")


def test_resolve_input_partitioned_start_from_literal():
    """No stored watermark + start_from='2026-04-27' filters out earlier days."""
    runner = _load_runner()
    s3 = runner._FAKE_S3
    _seed_cloudfront_partitions(s3, "logs", "year=2026/month=04/", [
        ("2026-04-26", "00"),
        ("2026-04-27", "00"),
        ("2026-04-27", "01"),
    ])
    os.environ["CLAVESA_WATERMARKS"] = "s3://wm/pipe/_watermarks/"

    spark = MagicMock()
    spark.read.option.return_value.parquet.return_value = "<df>"

    src = {
        "kind": "partitioned_path",
        "path": "s3://logs/year=2026/month=04/",
        "partitions": ["day", "hour"],
        "start_from": "2026-04-27",
    }
    df, advance = runner._resolve_input(spark, "cloudfront", src)
    args = spark.read.option.return_value.parquet.call_args.args
    assert "s3://logs/year=2026/month=04/day=2026-04-26/hour=00/" not in args
    assert ("2026-04-27", "00") < advance["new_cursor"] or advance["new_cursor"] == ("2026-04-27", "01")


def test_resolve_input_partitioned_backfill_window():
    """Backfill mode reads the closed [from_cursor, to_cursor] window and
    returns advance=None (no watermark interaction)."""
    runner = _load_runner()
    s3 = runner._FAKE_S3
    _seed_cloudfront_partitions(s3, "logs", "year=2026/month=04/", [
        ("2026-04-25", "00"),
        ("2026-04-26", "00"),
        ("2026-04-26", "01"),
        ("2026-04-27", "00"),
        ("2026-04-28", "00"),
    ])
    os.environ["CLAVESA_WATERMARKS"] = "s3://wm/pipe/_watermarks/"
    # Pre-stage a watermark — backfill must ignore it.
    runner._write_watermark("s3://wm/pipe/_watermarks/cloudfront.json", ("2026-04-28", "00"))

    spark = MagicMock()
    spark.read.option.return_value.parquet.return_value = "<df>"

    src = {
        "kind": "partitioned_path",
        "path": "s3://logs/year=2026/month=04/",
        "partitions": ["day", "hour"],
        "start_from": "all",
    }
    backfill = {"from_cursor": ["2026-04-26", "00"], "to_cursor": ["2026-04-27", "00"]}
    df, advance = runner._resolve_input(spark, "cloudfront", src, backfill=backfill)
    assert df == "<df>"
    assert advance is None, "backfill must not advance watermarks"
    args = spark.read.option.return_value.parquet.call_args.args
    assert "s3://logs/year=2026/month=04/day=2026-04-25/hour=00/" not in args
    assert "s3://logs/year=2026/month=04/day=2026-04-28/hour=00/" not in args
    assert "s3://logs/year=2026/month=04/day=2026-04-26/hour=00/" in args
    assert "s3://logs/year=2026/month=04/day=2026-04-26/hour=01/" in args
    assert "s3://logs/year=2026/month=04/day=2026-04-27/hour=00/" in args


def test_resolve_input_partitioned_backfill_empty_window():
    """Backfill window with no partitions returns (None, None) — handler
    short-circuits with skipped status, no error."""
    runner = _load_runner()
    s3 = runner._FAKE_S3
    _seed_cloudfront_partitions(s3, "logs", "year=2026/month=04/", [
        ("2026-04-25", "00"),
    ])
    os.environ["CLAVESA_WATERMARKS"] = "s3://wm/pipe/_watermarks/"

    spark = MagicMock()
    src = {
        "kind": "partitioned_path",
        "path": "s3://logs/year=2026/month=04/",
        "partitions": ["day", "hour"],
        "start_from": "all",
    }
    backfill = {"from_cursor": ["2026-05-01", "00"], "to_cursor": ["2026-05-02", "00"]}
    df, advance = runner._resolve_input(spark, "cloudfront", src, backfill=backfill)
    assert df is None
    assert advance is None
    spark.read.option.assert_not_called()


def test_resolve_input_partitioned_backfill_requires_both_cursors():
    """Missing from/to cursor is a configuration error — fail fast."""
    runner = _load_runner()
    s3 = runner._FAKE_S3
    _seed_cloudfront_partitions(s3, "logs", "year=2026/month=04/", [("2026-04-26", "00")])
    os.environ["CLAVESA_WATERMARKS"] = "s3://wm/pipe/_watermarks/"
    spark = MagicMock()
    src = {
        "kind": "partitioned_path",
        "path": "s3://logs/year=2026/month=04/",
        "partitions": ["day", "hour"],
        "start_from": "all",
    }
    try:
        runner._resolve_input(spark, "cloudfront", src, backfill={"from_cursor": ["2026-04-26", "00"]})
    except RuntimeError as e:
        assert "backfill requires both" in str(e)
    else:
        raise AssertionError("expected RuntimeError on missing to_cursor")


def test_resolve_input_string_form_unchanged():
    """Backwards compat: string-form path goes through spark.read.parquet
    with no watermark advance."""
    runner = _load_runner()
    spark = MagicMock()
    spark.read.parquet.return_value = "<df>"

    df, advance = runner._resolve_input(spark, "logs", "s3://b/p/")
    assert df == "<df>"
    assert advance is None
    spark.read.parquet.assert_called_with("s3://b/p/")


def test_resolve_input_string_form_table_id():
    runner = _load_runner()
    spark = MagicMock()
    spark.table.return_value = "<df>"

    # ADR-018: Delta tables resolve via spark_catalog, two-segment
    # `<db>.<table>` — no leading catalog prefix.
    df, advance = runner._resolve_input(spark, "alias", "db.t")
    assert df == "<df>"
    assert advance is None
    spark.table.assert_called_with("db.t")


def test_resolve_output_string_forms():
    runner = _load_runner()
    os.environ["CLAVESA_PIPELINE"] = "p"
    os.environ["CLAVESA_NODE"] = "n"
    os.environ["CLAVESA_CATALOG"] = "clavesa_demo_ws"
    os.environ["CLAVESA_SCHEMA"] = "p"

    s = runner._resolve_output("default", "")
    assert s == {"kind": "delta_table", "target": "clavesa_demo_ws__p.n__default", "mode": "replace", "merge_keys": [], "merge_update": {}, "cluster_by": []}

    s = runner._resolve_output("default", "s3://bucket/dest/")
    assert s == {"kind": "path", "target": "s3://bucket/dest/", "mode": "replace", "merge_keys": [], "merge_update": {}, "cluster_by": []}

    # Bare-string table id is passed through as-is — the runner no longer
    # interprets a "clavesa." prefix (ADR-018: Delta tables resolve via
    # spark_catalog, two-segment `<db>.<table>`).
    s = runner._resolve_output("default", "demo_ws__p.n__default")
    assert s == {"kind": "delta_table", "target": "demo_ws__p.n__default", "mode": "replace", "merge_keys": [], "merge_update": {}, "cluster_by": []}


def test_resolve_output_dict_append():
    runner = _load_runner()
    os.environ["CLAVESA_PIPELINE"] = "p"
    os.environ["CLAVESA_NODE"] = "n"
    os.environ["CLAVESA_CATALOG"] = "clavesa_demo_ws"
    os.environ["CLAVESA_SCHEMA"] = "p"

    s = runner._resolve_output("default", {"kind": "delta_table", "table_id": "", "mode": "append"})
    assert s == {"kind": "delta_table", "target": "clavesa_demo_ws__p.n__default", "mode": "append", "merge_keys": [], "stats": False, "merge_update": {}, "cluster_by": []}

    # stats opt-in flows through to the resolved spec — the call site
    # only reads spec["stats"], so propagation is the whole test.
    s = runner._resolve_output("default", {"kind": "delta_table", "table_id": "", "stats": True})
    assert s["stats"] is True


def test_resolve_output_cluster_by_and_merge_update():
    """cluster_by (liquid clustering) and merge_update (aggregate-aware
    merge) parse off the dict descriptor into the resolved spec."""
    runner = _load_runner()
    os.environ["CLAVESA_PIPELINE"] = "p"
    os.environ["CLAVESA_NODE"] = "n"
    os.environ["CLAVESA_CATALOG"] = "clavesa_demo_ws"
    os.environ["CLAVESA_SCHEMA"] = "p"

    s = runner._resolve_output("default", {
        "kind": "delta_table", "table_id": "db.t", "mode": "merge",
        "merge_keys": ["k"], "merge_update": {"cnt": "additive"},
        "cluster_by": ["event_date"],
    })
    assert s["merge_keys"] == ["k"]
    assert s["merge_update"] == {"cnt": "additive"}
    assert s["cluster_by"] == ["event_date"]

    # Unset → empty containers, not missing keys (the write path reads them).
    s = runner._resolve_output("default", {"kind": "delta_table", "table_id": "db.t"})
    assert s["merge_update"] == {}
    assert s["cluster_by"] == []


def test_merge_set_clause_primitives_and_raw():
    """_merge_set_clause maps keywords to exprs, passes raw through, replaces
    unlisted columns, and skips merge keys."""
    runner = _load_runner()
    clause = runner._merge_set_clause(
        ["k", "cnt", "first_seen", "last_seen", "sketch", "note", "other"],
        ["k"],
        {"cnt": "additive", "first_seen": "min", "last_seen": "max",
         "sketch": "sketch", "note": "concat(target.note, source.note)"},
    )
    assert "target.`k`" not in clause  # merge key never updated
    assert "target.`cnt` = target.`cnt` + source.`cnt`" in clause
    assert "target.`first_seen` = least(target.`first_seen`, source.`first_seen`)" in clause
    assert "target.`last_seen` = greatest(target.`last_seen`, source.`last_seen`)" in clause
    assert "target.`sketch` = hll_union(target.`sketch`, source.`sketch`)" in clause
    assert "target.`note` = concat(target.note, source.note)" in clause
    assert "target.`other` = source.`other`" in clause  # unlisted → replace


def test_resolve_output_rejects_unknown_mode():
    runner = _load_runner()
    try:
        runner._resolve_output("default", {"mode": "upsert"})
    except RuntimeError as e:
        assert "unsupported mode" in str(e)
    else:
        raise AssertionError("expected RuntimeError")


def test_resolve_output_merge_requires_keys():
    """mode=merge without merge_keys is a configuration error — fail fast."""
    runner = _load_runner()
    os.environ["CLAVESA_PIPELINE"] = "p"
    os.environ["CLAVESA_NODE"] = "n"
    os.environ["CLAVESA_CATALOG"] = "clavesa_demo_ws"
    os.environ["CLAVESA_SCHEMA"] = "p"
    try:
        runner._resolve_output("default", {"mode": "merge"})
    except RuntimeError as e:
        assert "merge_keys" in str(e)
    else:
        raise AssertionError("expected RuntimeError for empty merge_keys")


def test_resolve_output_merge_keys_default_mode():
    """merge_keys declared with mode unset defaults to mode=merge."""
    runner = _load_runner()
    os.environ["CLAVESA_PIPELINE"] = "p"
    os.environ["CLAVESA_NODE"] = "n"
    os.environ["CLAVESA_CATALOG"] = "clavesa_demo_ws"
    os.environ["CLAVESA_SCHEMA"] = "p"

    s = runner._resolve_output("default", {"merge_keys": ["event_id"]})
    assert s["mode"] == "merge"
    assert s["merge_keys"] == ["event_id"]


# ---------------------------------------------------------------------------
# Notification-drain ingest (#25): SQS-queue-as-cursor read path. The queue
# (fed by S3 Object Created events) replaces the partition-tree listing — drain
# new keys, read exactly those, delete the messages after the write commits.
# ---------------------------------------------------------------------------

_DRAIN_SRC = {
    "kind": "partitioned_path",
    "path": "s3://logs/cf/",
    "partitions": ["day", "hour"],
    "start_from": "all",
    "queue_url": "https://sqs.local/q",
}


def _arm_drain(runner, alias: str = "raw", cursor: tuple[str, ...] = ("2026-04-01", "00")):
    """Commit the watermark that arms the drain path for a partitioned source.
    The queue takes over only after the first listed run has committed a
    cursor — `start_from` governs the first run (see _resolve_input)."""
    os.environ["CLAVESA_WATERMARKS"] = "s3://wm/pipe/_watermarks/"
    runner._write_watermark(f"s3://wm/pipe/_watermarks/{alias}.json", cursor)


def test_drain_reads_new_keys_and_returns_ack():
    runner = _load_runner()
    _arm_drain(runner)
    sqs = runner._FAKE_SQS
    sqs.reset()
    sqs.add_object_event("logs", "cf/day=2026-04-26/hour=00/a.parquet", "h1")
    sqs.add_object_event("logs", "cf/day=2026-04-26/hour=01/b.parquet", "h2")
    spark = _FakeSpark()
    df, advance = runner._resolve_input(spark, "raw", dict(_DRAIN_SRC))
    assert df == "DF"
    assert spark.read_keys == [
        "s3://logs/cf/day=2026-04-26/hour=00/a.parquet",
        "s3://logs/cf/day=2026-04-26/hour=01/b.parquet",
    ]
    # basePath = the source prefix so Hive partition columns recover identically
    # to a listing-mode read.
    assert spark.base_path == "s3://logs/cf/"
    assert advance == {
        "ack": {"queue_url": "https://sqs.local/q", "handles": ["h1", "h2"], "region": None}
    }


def test_drain_url_decodes_object_keys():
    runner = _load_runner()
    _arm_drain(runner)
    sqs = runner._FAKE_SQS
    sqs.reset()
    # S3 event keys arrive percent-encoded: '+' is a space, %3D is '='.
    sqs.add_object_event("logs", "cf/day%3D2026-04-26/part+1.parquet", "h1")
    spark = _FakeSpark()
    runner._resolve_input(spark, "raw", dict(_DRAIN_SRC))
    assert spark.read_keys == ["s3://logs/cf/day=2026-04-26/part 1.parquet"]


def test_drain_empty_queue_skips_run():
    runner = _load_runner()
    _arm_drain(runner)
    runner._FAKE_SQS.reset()
    spark = _FakeSpark()
    df, advance = runner._resolve_input(spark, "raw", dict(_DRAIN_SRC))
    assert df is None and advance is None
    assert spark.read_keys is None  # nothing read on an empty drain


def test_drain_dedups_object_but_acks_all_handles():
    runner = _load_runner()
    _arm_drain(runner)
    sqs = runner._FAKE_SQS
    sqs.reset()
    # Same object enqueued twice (at-least-once delivery): read once, ack both.
    sqs.add_object_event("logs", "cf/day=2026-04-26/hour=00/a.parquet", "h1")
    sqs.add_object_event("logs", "cf/day=2026-04-26/hour=00/a.parquet", "h2")
    spark = _FakeSpark()
    _, advance = runner._resolve_input(spark, "raw", dict(_DRAIN_SRC))
    assert spark.read_keys == ["s3://logs/cf/day=2026-04-26/hour=00/a.parquet"]
    assert advance["ack"]["handles"] == ["h1", "h2"]


def test_drain_leaves_unparseable_message_on_queue():
    runner = _load_runner()
    _arm_drain(runner)
    sqs = runner._FAKE_SQS
    sqs.reset()
    sqs.add_object_event("logs", "cf/day=2026-04-26/hour=00/a.parquet", "h1")
    sqs.add_raw("not json at all", "bad1")
    sqs.add_raw(json.dumps({"detail": {"no": "object"}}), "bad2")
    spark = _FakeSpark()
    _, advance = runner._resolve_input(spark, "raw", dict(_DRAIN_SRC))
    assert spark.read_keys == ["s3://logs/cf/day=2026-04-26/hour=00/a.parquet"]
    # Only the parseable message is acked; poison ones stay for DLQ redrive.
    assert advance["ack"]["handles"] == ["h1"]


def test_drain_caps_batch_and_leaves_remainder():
    runner = _load_runner()
    _arm_drain(runner)
    sqs = runner._FAKE_SQS
    sqs.reset()
    for i in range(25):
        sqs.add_object_event("logs", f"cf/day=2026-04-26/hour=00/p{i}.parquet", f"h{i}")
    os.environ["CLAVESA_MAX_FILES_PER_RUN"] = "5"
    try:
        spark = _FakeSpark()
        df, _ = runner._resolve_input(spark, "raw", dict(_DRAIN_SRC))
    finally:
        del os.environ["CLAVESA_MAX_FILES_PER_RUN"]
    # The cap bounds the receive loop so a backlog drains over several runs;
    # the remainder stays queued for the next hourly fire.
    assert df == "DF"
    assert 0 < len(spark.read_keys) < 25
    assert len(sqs.messages) > 0


def test_drain_not_consulted_before_first_watermark_start_from_applies():
    """queue_url set but NO committed watermark: the drain must not run —
    `start_from` governs the first run via the listing path. A fresh cloud
    deploy over pre-existing objects has an empty queue (the files predate
    it); draining unconditionally skipped the run forever and `start_from`
    was dead. The queued message must also survive untouched for the next
    run (drain only takes over after the first listed commit)."""
    runner = _load_runner()
    s3 = runner._FAKE_S3
    _seed_cloudfront_partitions(s3, "logs", "cf/", [
        ("2026-04-26", "23"),
        ("2026-04-27", "00"),
    ])
    os.environ["CLAVESA_WATERMARKS"] = "s3://wm/pipe/_watermarks/"
    sqs = runner._FAKE_SQS
    sqs.reset()
    sqs.add_object_event("logs", "cf/day=2026-04-27/hour=00/a.parquet", "h1")

    spark = _FakeSpark()
    src = dict(_DRAIN_SRC, start_from="2026-04-27")
    df, advance = runner._resolve_input(spark, "raw", src)
    assert df == "DF"
    # Listing path, filtered by the start_from cursor — not the drain.
    assert spark.read_keys == ["s3://logs/cf/day=2026-04-27/hour=00/"]
    assert advance == {"uri": "s3://wm/pipe/_watermarks/raw.json",
                       "new_cursor": ("2026-04-27", "00")}
    # Queue untouched: the message is still there for the first drain run.
    assert len(sqs.messages) == 1


def test_drain_consulted_after_watermark_commit():
    """queue_url set + committed watermark + not forced: the drain IS the
    cursor — its non-empty result is returned (ack record, no listing)."""
    runner = _load_runner()
    _arm_drain(runner)
    sqs = runner._FAKE_SQS
    sqs.reset()
    sqs.add_object_event("logs", "cf/day=2026-04-27/hour=01/b.parquet", "h1")
    spark = _FakeSpark()
    df, advance = runner._resolve_input(spark, "raw", dict(_DRAIN_SRC))
    assert df == "DF"
    assert spark.read_keys == ["s3://logs/cf/day=2026-04-27/hour=01/b.parquet"]
    assert advance == {
        "ack": {"queue_url": "https://sqs.local/q", "handles": ["h1"], "region": None}
    }


def test_drain_bypassed_when_forced_full_range_reread():
    """forced=True bypasses the queue entirely and takes the listing branch's
    forced full-range re-read. Queue messages are deliberately NOT consumed —
    the next unforced drain redelivers them (at-least-once, absorbed by
    replace/merge outputs)."""
    runner = _load_runner()
    s3 = runner._FAKE_S3
    _seed_cloudfront_partitions(s3, "logs", "cf/", [
        ("2026-04-26", "00"),
        ("2026-04-27", "00"),
    ])
    # Watermark already at the max partition: an unforced listing run would skip.
    _arm_drain(runner, cursor=("2026-04-27", "00"))
    sqs = runner._FAKE_SQS
    sqs.reset()
    sqs.add_object_event("logs", "cf/day=2026-04-27/hour=00/a.parquet", "h1")

    spark = _FakeSpark()
    df, advance = runner._resolve_input(spark, "raw", dict(_DRAIN_SRC), forced=True)
    # Forced full-range re-read via the listing branch, not the drain.
    assert df == "DF"
    assert spark.read_keys == [
        "s3://logs/cf/day=2026-04-26/hour=00/",
        "s3://logs/cf/day=2026-04-27/hour=00/",
    ]
    assert advance == {"uri": "s3://wm/pipe/_watermarks/raw.json",
                       "new_cursor": ("2026-04-27", "00")}
    # Queue untouched: forced runs don't consume messages.
    assert len(sqs.messages) == 1


def test_drain_flat_s3_source_also_drains():
    runner = _load_runner()
    sqs = runner._FAKE_SQS
    sqs.reset()
    sqs.add_object_event("raw-bkt", "events/x.parquet", "h1")
    spark = _FakeSpark()
    flat = {
        "kind": "s3",
        "bucket": "raw-bkt",
        "prefix": "events/",
        "format": "parquet",
        "queue_url": "https://sqs.local/q",
    }
    _, advance = runner._resolve_input(spark, "raw", flat)
    assert spark.read_keys == ["s3://raw-bkt/events/x.parquet"]
    assert spark.base_path == "s3://raw-bkt/events/"
    assert advance["ack"]["handles"] == ["h1"]


def test_drain_if_configured_returns_none_without_queue_url():
    # Listing-mode fallback (local run / no queue): the gate returns None so the
    # caller takes the existing read path; the queue is never touched.
    runner = _load_runner()
    runner._FAKE_SQS.reset()
    spark = _FakeSpark()
    assert runner._drain_if_configured(spark, "raw", {"format": "parquet"}, "b", "p/") is None


def test_delete_messages_chunks_and_records():
    runner = _load_runner()
    sqs = runner._FAKE_SQS
    sqs.reset()
    handles = [f"h{i}" for i in range(23)]
    runner._delete_sqs_messages("https://sqs.local/q", handles)  # 3 chunks: 10+10+3
    assert sqs.deleted == handles


def test_delete_failure_does_not_raise():
    runner = _load_runner()
    sqs = runner._FAKE_SQS
    sqs.reset()
    sqs.fail_delete = True
    # Best-effort: a delete failure must not raise (message redelivers, dedup absorbs).
    runner._delete_sqs_messages("https://sqs.local/q", ["h1"])
    assert sqs.deleted == []


def test_max_files_per_run_defaults_on_garbage_or_nonpositive():
    runner = _load_runner()
    for bad in ("", "abc", "0", "-3"):
        os.environ["CLAVESA_MAX_FILES_PER_RUN"] = bad
        try:
            assert runner._max_files_per_run() == 1000
        finally:
            del os.environ["CLAVESA_MAX_FILES_PER_RUN"]
    os.environ["CLAVESA_MAX_FILES_PER_RUN"] = "50"
    try:
        assert runner._max_files_per_run() == 50
    finally:
        del os.environ["CLAVESA_MAX_FILES_PER_RUN"]


# ---------------------------------------------------------------------------
# Test runner: prints PASS/FAIL summary when invoked directly.
# ---------------------------------------------------------------------------


def _all_tests():
    g = globals()
    return [(name, fn) for name, fn in sorted(g.items()) if name.startswith("test_") and callable(fn)]


def main():
    passed = 0
    failed: list[tuple[str, str]] = []
    for name, fn in _all_tests():
        try:
            fn()
        except Exception as e:
            failed.append((name, f"{type(e).__name__}: {e}"))
            print(f"FAIL  {name}  →  {type(e).__name__}: {e}")
        else:
            passed += 1
            print(f"PASS  {name}")
    total = passed + len(failed)
    print(f"\n{passed}/{total} passed", "❌" if failed else "✅")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
