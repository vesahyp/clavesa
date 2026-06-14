"""Unit tests for the `_progress_snapshot` helper in runner/runner.py.

Pure / stdlib-only — no Spark, no Docker. Fakes the SparkStageInfo namedtuple
shape (attribute access: numTasks / numActiveTasks / numCompletedTasks /
numFailedTasks / stageId) that the PySpark StatusTracker python wrapper
returns from getStageInfo().

Run with: python3 tests/runner/test_progress.py
   or:    python3 -m unittest tests/runner/test_progress.py
"""

from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from collections import namedtuple
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RUNNER = REPO / "runner" / "runner.py"

# Mirror the PySpark SparkStageInfo namedtuple attribute shape.
StageInfo = namedtuple(
    "StageInfo",
    ["stageId", "numTasks", "numActiveTasks", "numCompletedTasks", "numFailedTasks"],
)


def _load_runner():
    """Import runner.py with boto3/pyspark stubbed — same trick as
    test_is_forced.py / test_parse_sql.py."""
    boto3_mod = types.ModuleType("boto3")
    boto3_mod.client = lambda *_a, **_k: None  # type: ignore[attr-defined]

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

    sys.modules.setdefault("boto3", boto3_mod)
    sys.modules.setdefault("botocore", botocore_mod)
    sys.modules.setdefault("botocore.exceptions", botocore_exceptions)
    sys.modules.setdefault("pyspark", pyspark_mod)
    sys.modules.setdefault("pyspark.sql", pyspark_sql)

    spec = importlib.util.spec_from_file_location("runner", str(RUNNER))
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


class ProgressSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.runner = _load_runner()

    def test_empty_poll(self):
        seen = set()
        snap = self.runner._progress_snapshot([], seen)
        self.assertEqual(
            snap,
            {
                "stages_total": 0,
                "stages_completed": 0,
                "tasks_total": 0,
                "tasks_completed": 0,
                "tasks_failed": 0,
            },
        )

    def test_single_poll_counters(self):
        seen = set()
        active = [
            StageInfo(stageId=0, numTasks=10, numActiveTasks=4,
                      numCompletedTasks=6, numFailedTasks=1),
            StageInfo(stageId=1, numTasks=20, numActiveTasks=2,
                      numCompletedTasks=18, numFailedTasks=0),
        ]
        snap = self.runner._progress_snapshot(active, seen)
        self.assertEqual(snap["stages_total"], 2)
        self.assertEqual(snap["stages_completed"], 0)  # both still active
        self.assertEqual(snap["tasks_total"], 30)
        self.assertEqual(snap["tasks_completed"], 24)
        self.assertEqual(snap["tasks_failed"], 1)
        self.assertEqual(seen, {0, 1})

    def test_stages_total_grows_across_polls(self):
        seen = set()

        # Poll 1: stage 0 active.
        snap1 = self.runner._progress_snapshot(
            [StageInfo(0, 8, 8, 0, 0)], seen
        )
        self.assertEqual(snap1["stages_total"], 1)
        self.assertEqual(snap1["stages_completed"], 0)

        # Poll 2: stage 0 finished (no longer active), stage 1 active.
        # stages_total counts both seen; one is completed (seen - active).
        snap2 = self.runner._progress_snapshot(
            [StageInfo(1, 5, 5, 0, 0)], seen
        )
        self.assertEqual(snap2["stages_total"], 2)
        self.assertEqual(snap2["stages_completed"], 1)
        self.assertEqual(seen, {0, 1})

        # Poll 3: stage 2 + stage 3 active concurrently.
        snap3 = self.runner._progress_snapshot(
            [StageInfo(2, 4, 1, 3, 0), StageInfo(3, 4, 4, 0, 0)], seen
        )
        self.assertEqual(snap3["stages_total"], 4)
        # 4 seen, 2 active -> 2 completed.
        self.assertEqual(snap3["stages_completed"], 2)
        self.assertEqual(snap3["tasks_total"], 8)
        self.assertEqual(snap3["tasks_completed"], 3)

    def test_completed_when_all_drained(self):
        seen = set()
        self.runner._progress_snapshot([StageInfo(0, 4, 0, 4, 0)], seen)
        # Final poll: nothing active -> all seen stages completed.
        snap = self.runner._progress_snapshot([], seen)
        self.assertEqual(snap["stages_total"], 1)
        self.assertEqual(snap["stages_completed"], 1)

    def test_tolerates_plain_object_attrs(self):
        # The formatter reads attributes (never calls), so a plain object
        # with the same attributes works just as well as the namedtuple.
        class FakeStage:
            stageId = 7
            numTasks = 3
            numActiveTasks = 1
            numCompletedTasks = 2
            numFailedTasks = 1

        seen = set()
        snap = self.runner._progress_snapshot([FakeStage()], seen)
        self.assertEqual(snap["stages_total"], 1)
        self.assertEqual(snap["tasks_total"], 3)
        self.assertEqual(snap["tasks_completed"], 2)
        self.assertEqual(snap["tasks_failed"], 1)
        self.assertEqual(seen, {7})


class ProgressTargetTests(unittest.TestCase):
    def setUp(self):
        self.runner = _load_runner()

    # --- s3 branch (deployed run: CLAVESA_SYSTEM_WAREHOUSE is s3://) -------

    def test_s3_resolves_bucket_and_key(self):
        env = {"CLAVESA_SYSTEM_WAREHOUSE": "s3://my-bucket/_system/pipelines/"}
        target = self.runner._progress_target(env, "arn:aws:states:exec/abc", "trips")
        self.assertEqual(
            target,
            ("s3", "my-bucket", "_progress/arn:aws:states:exec/abc/trips.json"),
        )

    def test_s3_precedence_over_file_warehouse(self):
        # A deployed run can have both set; s3 system warehouse wins.
        env = {
            "CLAVESA_SYSTEM_WAREHOUSE": "s3://sys-bucket/_system/pipelines/",
            "CLAVESA_WAREHOUSE": "s3://pipeline-bucket/p/_warehouse/",
        }
        target = self.runner._progress_target(env, "run1", "trips")
        self.assertEqual(target, ("s3", "sys-bucket", "_progress/run1/trips.json"))

    # --- file branch (local / cloud-local against a disk warehouse) -------

    def test_file_resolves_local_path(self):
        env = {"CLAVESA_WAREHOUSE": "/abs/.clavesa/warehouse"}
        target = self.runner._progress_target(env, "run1", "trips")
        self.assertEqual(
            target,
            ("file", "/abs/.clavesa/warehouse/_progress/run1/trips.json"),
        )

    def test_file_branch_when_system_warehouse_absent(self):
        # Local runs never get CLAVESA_SYSTEM_WAREHOUSE — only CLAVESA_WAREHOUSE
        # (a filesystem path) — so the file branch must fire.
        env = {"CLAVESA_WAREHOUSE": "/tmp/ws/.clavesa/warehouse"}
        target = self.runner._progress_target(env, "abc", "revenue")
        self.assertEqual(target[0], "file")
        self.assertTrue(target[1].endswith("/_progress/abc/revenue.json"))

    # --- None cases -------------------------------------------------------

    def test_none_without_any_warehouse(self):
        self.assertIsNone(self.runner._progress_target({}, "arn", "node"))

    def test_none_when_warehouse_is_s3_but_system_absent(self):
        # CLAVESA_WAREHOUSE on s3 with no system warehouse: the file branch
        # rejects an s3 path, so there's no resolvable file sink and we
        # return None rather than mis-routing.
        env = {"CLAVESA_WAREHOUSE": "s3://b/p/_warehouse/"}
        self.assertIsNone(self.runner._progress_target(env, "arn", "node"))

    def test_none_without_run(self):
        env = {"CLAVESA_SYSTEM_WAREHOUSE": "s3://b/_system/pipelines/"}
        self.assertIsNone(self.runner._progress_target(env, "", "node"))

    def test_none_without_node(self):
        env = {"CLAVESA_SYSTEM_WAREHOUSE": "s3://b/_system/pipelines/"}
        self.assertIsNone(self.runner._progress_target(env, "arn", ""))


if __name__ == "__main__":
    unittest.main()
