"""Unit tests for the `_is_forced` helper in runner/runner.py.

Run with: python3 tests/runner/test_is_forced.py
   or:    python3 -m unittest tests/runner/test_is_forced.py
"""

from __future__ import annotations

import importlib.util
import os
import sys
import types
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RUNNER = REPO / "runner" / "runner.py"


def _load_runner():
    """Import runner.py with boto3/pyspark stubbed — same trick as
    test_parse_sql.py / test_node_runs_row.py."""
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


class IsForcedTests(unittest.TestCase):
    def setUp(self):
        self.runner = _load_runner()
        # Clean any inherited env state before each case.
        os.environ.pop("CLAVESA_FORCE", None)
        os.environ.pop("CLAVESA_FORCE_NODES", None)

    def tearDown(self):
        os.environ.pop("CLAVESA_FORCE", None)
        os.environ.pop("CLAVESA_FORCE_NODES", None)

    def test_no_force_returns_false(self):
        self.assertFalse(self.runner._is_forced({}, "bronze"))
        self.assertFalse(self.runner._is_forced({"_trigger": "manual"}, "bronze"))

    def test_env_force_all_nodes(self):
        os.environ["CLAVESA_FORCE"] = "1"
        self.assertTrue(self.runner._is_forced({}, "bronze"))
        self.assertTrue(self.runner._is_forced({}, "any-node"))

    def test_env_force_with_node_filter(self):
        os.environ["CLAVESA_FORCE"] = "1"
        os.environ["CLAVESA_FORCE_NODES"] = "bronze,silver"
        self.assertTrue(self.runner._is_forced({}, "bronze"))
        self.assertTrue(self.runner._is_forced({}, "silver"))
        self.assertFalse(self.runner._is_forced({}, "gold"))

    def test_env_force_csv_strips_whitespace(self):
        os.environ["CLAVESA_FORCE"] = "1"
        os.environ["CLAVESA_FORCE_NODES"] = " bronze , silver "
        self.assertTrue(self.runner._is_forced({}, "bronze"))
        self.assertTrue(self.runner._is_forced({}, "silver"))
        self.assertFalse(self.runner._is_forced({}, "gold"))

    def test_event_force_all_nodes(self):
        self.assertTrue(self.runner._is_forced({"_force": True}, "bronze"))

    def test_event_force_with_node_filter(self):
        ev = {"_force": True, "_force_nodes": ["silver"]}
        self.assertTrue(self.runner._is_forced(ev, "silver"))
        self.assertFalse(self.runner._is_forced(ev, "bronze"))

    def test_event_force_via_execution_input(self):
        # Cloud path: SFN execution input lands under _execution_input.
        ev = {"_execution_input": {"_force": True, "_force_nodes": ["bronze"]}}
        self.assertTrue(self.runner._is_forced(ev, "bronze"))
        self.assertFalse(self.runner._is_forced(ev, "silver"))

    def test_event_force_via_execution_input_all_nodes(self):
        ev = {"_execution_input": {"_force": True}}
        self.assertTrue(self.runner._is_forced(ev, "bronze"))

    def test_env_wins_over_event(self):
        os.environ["CLAVESA_FORCE"] = "1"
        # Event has no _force, but env does — env wins (it's the local path).
        self.assertTrue(self.runner._is_forced({"_trigger": "manual"}, "bronze"))

    def test_non_dict_event(self):
        self.assertFalse(self.runner._is_forced(None, "bronze"))
        self.assertFalse(self.runner._is_forced("not-a-dict", "bronze"))


if __name__ == "__main__":
    unittest.main()
