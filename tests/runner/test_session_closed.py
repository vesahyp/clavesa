"""Unit tests for `_is_session_closed` in runner/runner.py.

Run with: python3 tests/runner/test_session_closed.py
   or:    python3 -m unittest tests/runner/test_session_closed.py

`_is_session_closed` is the pure string predicate that decides whether a
failed /query (or /healthz SELECT 1) should trigger the recover-once retry
in the warm worker. It must fire for the Spark Connect "session was reaped"
message forms and stay quiet for ordinary query errors so we never retry a
genuine SQL failure.

The recover-once retry path itself needs a live Connect server and is the
lead's end-to-end gate; only the pure predicate is unit-tested here.
"""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RUNNER = REPO / "runner" / "runner.py"


def _load_runner():
    """Import runner.py with boto3/pyspark stubbed (same trick as
    test_parse_sql.py). `_is_session_closed` is pure string matching and
    needs no Spark at all."""
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


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_real_session_closed_message_matches():
    """The exact message Spark Connect raises when the session was GC'd."""
    runner = _load_runner()
    real = (
        "(org.apache.spark.SparkSQLException) [INVALID_HANDLE.SESSION_CLOSED] "
        "The handle 1a2b3c is invalid. Session was closed."
    )
    assert runner._is_session_closed(Exception(real)) is True


def test_invalid_handle_form_matches():
    runner = _load_runner()
    assert runner._is_session_closed(Exception("[INVALID_HANDLE] bad handle")) is True


def test_session_was_closed_form_matches_case_insensitive():
    runner = _load_runner()
    assert runner._is_session_closed(Exception("SESSION WAS CLOSED by server")) is True
    assert runner._is_session_closed(Exception("session_closed")) is True


def test_parse_error_does_not_match():
    runner = _load_runner()
    parse_err = "[PARSE_SYNTAX_ERROR] Syntax error at or near 'EXCEPT'(line 1, pos 9)"
    assert runner._is_session_closed(Exception(parse_err)) is False


def test_table_not_found_does_not_match():
    runner = _load_runner()
    tbl_err = "[TABLE_OR_VIEW_NOT_FOUND] The table or view `foo` cannot be found."
    assert runner._is_session_closed(Exception(tbl_err)) is False


if __name__ == "__main__":
    test_real_session_closed_message_matches()
    test_invalid_handle_form_matches()
    test_session_was_closed_form_matches_case_insensitive()
    test_parse_error_does_not_match()
    test_table_not_found_does_not_match()
    print("ok")
