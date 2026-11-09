"""Unit tests for runner/stream_query.py (ADR-028 slice 1): the config
validation and the checkpoint-generation state. Pure stdlib, no Spark.

Run with: python3 tests/runner/test_stream_query.py

The Spark parts (sources, foreachBatch writer, drain, recovery MERGE) are
proven end to end by `make test-stream` against LocalStack and Kafka.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "runner"))

import stream_query as sq  # noqa: E402


def _cfg(**over):
    d = {"name": "clicks", "kind": "kinesis", "table": "cat__streams.clicks",
         "kinesis": {"stream_name": "s", "endpoint_url": "http://x"}}
    d.update(over)
    return d


class ConfigTest(unittest.TestCase):
    def _load(self, d):
        os.environ["CLAVESA_STREAM_CONFIG"] = json.dumps(d)
        try:
            return sq.StreamConfig.from_env()
        finally:
            del os.environ["CLAVESA_STREAM_CONFIG"]

    def test_defaults(self):
        os.environ["CLAVESA_WAREHOUSE"] = "/wh"
        try:
            c = self._load(_cfg())
        finally:
            del os.environ["CLAVESA_WAREHOUSE"]
        self.assertEqual(c.trigger_seconds, 120)
        self.assertEqual(c.checkpoint_root, "/wh/_streams/cat__streams.clicks")
        self.assertFalse(c.drain)

    def test_kind_must_be_known(self):
        with self.assertRaises(SystemExit):
            self._load(_cfg(kind="sqs"))

    def test_recover_needs_drain(self):
        with self.assertRaises(SystemExit):
            self._load(_cfg(recover=True))
        self.assertTrue(self._load(_cfg(recover=True, drain=True)).recover)

    def test_merge_keys(self):
        self.assertEqual(self._load(_cfg()).merge_keys(), ["_source_partition", "_source_offset"])
        self.assertEqual(self._load(_cfg(record_key=["event_id"])).merge_keys(), ["event_id"])

    def test_unknown_keys_ignored(self):
        self.assertEqual(self._load(_cfg(future_knob=1)).name, "clicks")


class StateTest(unittest.TestCase):
    def test_generation_round_trip(self):
        with tempfile.TemporaryDirectory() as d:
            c = sq.StreamConfig(name="n", kind="kafka", table="t", checkpoint_root=os.path.join(d, "ck"))
            self.assertEqual(sq.read_state(c), {"generation": 1, "merge": False})
            sq.write_state(c, {"generation": 2, "merge": True})
            self.assertEqual(sq.read_state(c), {"generation": 2, "merge": True})


class HasCommitTest(unittest.TestCase):
    def test_commit_detection(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertFalse(sq._has_commit(d))
            os.makedirs(os.path.join(d, "_delta_log"))
            self.assertFalse(sq._has_commit(d))  # empty log: crash in the first create
            open(os.path.join(d, "_delta_log", "00000000000000000000.json.tmp"), "w").close()
            self.assertFalse(sq._has_commit(d))
            open(os.path.join(d, "_delta_log", "00000000000000000000.json"), "w").close()
            self.assertTrue(sq._has_commit(d))


class PipelineModeTest(unittest.TestCase):
    """ADR-028 slice 2: the pieces a pipeline run uses."""

    def test_partition_default_dt_hour_when_present(self):
        cfg = sq.StreamConfig(name="n", kind="kinesis", table="t", checkpoint_root="/x")
        self.assertEqual(sq.partition_columns(cfg, ["raw", "dt", "hour"]), ["dt", "hour"])
        self.assertEqual(sq.partition_columns(cfg, ["id", "event_date"]), [])

    def test_partition_configured_must_exist(self):
        cfg = sq.StreamConfig(name="n", kind="kinesis", table="t", checkpoint_root="/x",
                              partition_by=["event_date"])
        self.assertEqual(sq.partition_columns(cfg, ["id", "event_date"]), ["event_date"])
        with self.assertRaises(SystemExit):
            sq.partition_columns(cfg, ["id"])

    def test_s3_paths(self):
        self.assertEqual(sq._join("s3://b/w/_streams/t/", "gen-1"), "s3://b/w/_streams/t/gen-1")
        self.assertEqual(sq._split_s3("s3a://b/k/current.json"), ("b", "k/current.json"))
        cfg = sq.StreamConfig(name="n", kind="kinesis", table="t", checkpoint_root="s3://b/w")
        self.assertEqual(sq._state_path(cfg), "s3://b/w/current.json")


if __name__ == "__main__":
    unittest.main()
