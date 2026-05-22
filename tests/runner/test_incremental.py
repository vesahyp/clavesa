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
    boto3_mod = types.ModuleType("boto3")
    boto3_mod.client = lambda *_a, **_k: fake_s3.client()  # type: ignore[attr-defined]

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

    df, advance = runner._resolve_input(spark, "alias", "clavesa.db.t")
    assert df == "<df>"
    assert advance is None
    spark.table.assert_called_with("clavesa.db.t")


def test_resolve_output_string_forms():
    runner = _load_runner()
    os.environ["CLAVESA_PIPELINE"] = "p"
    os.environ["CLAVESA_NODE"] = "n"
    os.environ["CLAVESA_CATALOG"] = "clavesa_demo_ws"
    os.environ["CLAVESA_SCHEMA"] = "p"

    s = runner._resolve_output("default", "")
    assert s == {"kind": "iceberg_table", "target": "clavesa.clavesa_demo_ws__p.n__default", "mode": "replace", "merge_keys": []}

    s = runner._resolve_output("default", "s3://bucket/dest/")
    assert s == {"kind": "path", "target": "s3://bucket/dest/", "mode": "replace", "merge_keys": []}

    s = runner._resolve_output("default", "clavesa.db.t")
    assert s == {"kind": "iceberg_table", "target": "clavesa.db.t", "mode": "replace", "merge_keys": []}


def test_resolve_output_dict_append():
    runner = _load_runner()
    os.environ["CLAVESA_PIPELINE"] = "p"
    os.environ["CLAVESA_NODE"] = "n"
    os.environ["CLAVESA_CATALOG"] = "clavesa_demo_ws"
    os.environ["CLAVESA_SCHEMA"] = "p"

    s = runner._resolve_output("default", {"kind": "iceberg_table", "table_id": "", "mode": "append"})
    assert s == {"kind": "iceberg_table", "target": "clavesa.clavesa_demo_ws__p.n__default", "mode": "append", "merge_keys": [], "stats": False}

    # stats opt-in flows through to the resolved spec — the call site
    # only reads spec["stats"], so propagation is the whole test.
    s = runner._resolve_output("default", {"kind": "iceberg_table", "table_id": "", "stats": True})
    assert s["stats"] is True


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
