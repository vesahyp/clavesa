"""Unit tests for the `_parse_sql` helper in runner/runner.py.

Run with: python3 tests/runner/test_parse_sql.py
   or:    python3 -m unittest tests/runner/test_parse_sql.py

`_parse_sql` is the seam behind POST /parse on the warm worker. The
JVM call (`_SPARK._jsparkSession.sessionState().sqlParser().parsePlan`)
is mocked so the helper can be exercised without a real Spark process.
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
    test_node_runs_row.py). The pure `_parse_sql` helper does not need
    a real Spark — we mock `_SPARK` per-test."""
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


class _FakeSqlParser:
    """Mimics the JVM's SqlParser — parsePlan either succeeds (returns a
    sentinel) or raises. Used to drive _parse_sql through its two
    branches without a real py4j gateway."""

    def __init__(self, raise_with=None):
        self._raise_with = raise_with

    def parsePlan(self, _sql):  # noqa: N802 — match the JVM method
        if self._raise_with is not None:
            raise self._raise_with
        return object()


class _FakeSessionState:
    def __init__(self, parser):
        self._parser = parser

    def sqlParser(self):  # noqa: N802
        return self._parser


class _FakeJSparkSession:
    def __init__(self, parser):
        self._state = _FakeSessionState(parser)

    def sessionState(self):  # noqa: N802
        return self._state


class _FakeSparkSession:
    def __init__(self, parser):
        self._jsparkSession = _FakeJSparkSession(parser)


class _FakePy4JJavaError(Exception):
    """Stand-in for py4j.protocol.Py4JJavaError — carries a
    .java_exception with a .getMessage() the helper extracts."""

    def __init__(self, message):
        super().__init__(message)
        self.java_exception = _FakeJavaException(message)


class _FakeJavaException:
    def __init__(self, message):
        self._message = message

    def getMessage(self):  # noqa: N802
        return self._message


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_parse_sql_valid_returns_ok_true():
    runner = _load_runner()
    runner._SPARK = _FakeSparkSession(_FakeSqlParser())
    out = runner._parse_sql("SELECT 1")
    assert out == {"ok": True}, out


def test_parse_sql_invalid_returns_ok_false_with_parser_message():
    runner = _load_runner()
    parser_msg = (
        "[PARSE_SYNTAX_ERROR] Syntax error at or near 'EXCEPT'(line 1, pos 9)"
    )
    runner._SPARK = _FakeSparkSession(_FakeSqlParser(raise_with=_FakePy4JJavaError(parser_msg)))
    out = runner._parse_sql("SELECT * EXCEPT (foo) FROM bar")
    assert out["ok"] is False, out
    assert out["error"] == parser_msg, out


def test_parse_sql_non_py4j_exception_falls_back_to_str():
    """A non-py4j exception (transient gateway failure, mis-shaped
    Connect call, etc.) must still surface as ok=false with a
    populated error string — the warm worker must never return an
    empty error message."""
    runner = _load_runner()
    runner._SPARK = _FakeSparkSession(_FakeSqlParser(raise_with=RuntimeError("connection lost")))
    out = runner._parse_sql("SELECT 1")
    assert out["ok"] is False, out
    assert "connection lost" in out["error"], out


def test_parse_sql_empty_string_short_circuits_via_http():
    """The HTTP wrapper (do_POST) rejects empty SQL with HTTP 400 before
    calling _parse_sql, so the helper itself never sees an empty
    string in production. Still, calling with empty SQL must not raise
    — the JVM call will reject it, and we surface the message."""
    runner = _load_runner()
    parser_msg = "Empty input"
    runner._SPARK = _FakeSparkSession(_FakeSqlParser(raise_with=_FakePy4JJavaError(parser_msg)))
    out = runner._parse_sql("")
    assert out["ok"] is False, out
    assert out["error"] == parser_msg, out


if __name__ == "__main__":
    test_parse_sql_valid_returns_ok_true()
    test_parse_sql_invalid_returns_ok_false_with_parser_message()
    test_parse_sql_non_py4j_exception_falls_back_to_str()
    test_parse_sql_empty_string_short_circuits_via_http()
    print("ok")
