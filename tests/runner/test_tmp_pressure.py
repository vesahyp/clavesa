"""Unit tests for the warm-container /tmp pressure gate in runner/runner.py (GH #43).

Run with: python3 tests/runner/test_tmp_pressure.py
   or:    python3 -m unittest tests/runner/test_tmp_pressure.py

`_tmp_pressure_exceeded` decides whether pipeline_handler recycles the cached
Spark session before a transform. The critical property is the Lambda gate:
in a local Docker container /tmp sits on the overlay filesystem, so
shutil.disk_usage reports the HOST disk — without the gate, any dev machine
over 50% full would recycle the session before every transform and defeat
warm-session bundling. Stdlib-only, no real Spark.
"""

from __future__ import annotations

import importlib.util
import os
import shutil
import sys
import types
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RUNNER = REPO / "runner" / "runner.py"


def _load_runner():
    """Import runner.py with boto3/pyspark stubbed (same trick as
    test_spark_liveness.py). The pressure helper needs no real Spark."""
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


runner = _load_runner()

_Usage = type(shutil.disk_usage("/"))  # the named tuple shutil returns


class TmpPressureTest(unittest.TestCase):
    def setUp(self):
        self._saved_env = os.environ.pop("AWS_LAMBDA_FUNCTION_NAME", None)
        self._saved_disk_usage = shutil.disk_usage

    def tearDown(self):
        if self._saved_env is not None:
            os.environ["AWS_LAMBDA_FUNCTION_NAME"] = self._saved_env
        else:
            os.environ.pop("AWS_LAMBDA_FUNCTION_NAME", None)
        shutil.disk_usage = self._saved_disk_usage

    def _fake_usage(self, used, total):
        shutil.disk_usage = lambda _p: _Usage(total=total, used=used, free=total - used)

    def test_not_lambda_full_disk_is_false(self):
        # The load-bearing case: a 90%-full host disk under local Docker
        # must NOT trigger a recycle — the gate keys off the Lambda env var.
        self._fake_usage(used=90, total=100)
        self.assertFalse(runner._tmp_pressure_exceeded())

    def test_lambda_over_threshold_is_true(self):
        os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "clavesa-x-runner"
        self._fake_usage(used=60, total=100)
        self.assertTrue(runner._tmp_pressure_exceeded())

    def test_lambda_under_threshold_is_false(self):
        os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "clavesa-x-runner"
        self._fake_usage(used=40, total=100)
        self.assertFalse(runner._tmp_pressure_exceeded())

    def test_lambda_custom_threshold(self):
        os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "clavesa-x-runner"
        self._fake_usage(used=40, total=100)
        self.assertTrue(runner._tmp_pressure_exceeded(threshold=0.3))

    def test_lambda_stat_failure_is_false(self):
        # Best-effort: a stat error must never block the fast warm path.
        os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "clavesa-x-runner"

        def _boom(_p):
            raise OSError("no /tmp")

        shutil.disk_usage = _boom
        self.assertFalse(runner._tmp_pressure_exceeded())


if __name__ == "__main__":
    unittest.main()
