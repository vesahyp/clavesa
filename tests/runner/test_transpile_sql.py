"""Unit tests for the `_transpile_sql` helper in runner/runner.py.

Run with: python3 tests/runner/test_transpile_sql.py
   or:    python3 -m unittest tests/runner/test_transpile_sql.py

`_transpile_sql` is the seam behind POST /transpile on the long-lived,
non-Spark transpile server. It imports sqlglot lazily, so the host does
not need sqlglot installed — the helper picks up a fake `sqlglot` module
stubbed into sys.modules, the same trick test_parse_sql.py uses for
pyspark/boto3.
"""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RUNNER = REPO / "runner" / "runner.py"


# ---------------------------------------------------------------------------
# Fake sqlglot — mimics just the surface _transpile_sql touches:
#   sqlglot.transpile(sql, read, write, unsupported_level) -> [str]
#   sqlglot.ErrorLevel.RAISE
#   sqlglot.errors.ParseError / .UnsupportedError
# ---------------------------------------------------------------------------


class _ParseError(Exception):
    def __init__(self, message, errors=None):
        super().__init__(message)
        self.errors = errors or []


class _UnsupportedError(Exception):
    pass


def _make_fake_sqlglot(transpile_impl):
    """Build a fake `sqlglot` module whose .transpile delegates to
    transpile_impl(sql, read, write, unsupported_level)."""
    sqlglot_mod = types.ModuleType("sqlglot")

    errors_mod = types.ModuleType("sqlglot.errors")
    errors_mod.ParseError = _ParseError
    errors_mod.UnsupportedError = _UnsupportedError

    error_level = types.SimpleNamespace(RAISE="RAISE")

    sqlglot_mod.transpile = transpile_impl
    sqlglot_mod.ErrorLevel = error_level
    sqlglot_mod.errors = errors_mod
    return sqlglot_mod, errors_mod


def _load_runner(transpile_impl):
    """Import runner.py with boto3/pyspark stubbed (same trick as
    test_parse_sql.py) AND a fake `sqlglot` whose .transpile is
    transpile_impl, so the lazy `import sqlglot` inside _transpile_sql
    (and at run_transpile_server startup) resolves to the fake."""
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

    # The fake sqlglot is rebuilt per test (different transpile_impl), so
    # overwrite rather than setdefault.
    sqlglot_mod, errors_mod = _make_fake_sqlglot(transpile_impl)
    sys.modules["sqlglot"] = sqlglot_mod
    sys.modules["sqlglot.errors"] = errors_mod

    spec = importlib.util.spec_from_file_location("runner", str(RUNNER))
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_transpile_success_returns_trino():
    def fake_transpile(sql, read, write, unsupported_level):
        assert read == "spark", read
        assert write == "athena", write
        assert unsupported_level == "RAISE", unsupported_level
        return ["SELECT DATE_DIFF('day', d1, d2) AS n FROM t"]

    runner = _load_runner(fake_transpile)
    out = runner._transpile_sql("SELECT datediff(d2, d1) AS n FROM t")
    assert out == {"ok": True, "trino": "SELECT DATE_DIFF('day', d1, d2) AS n FROM t"}, out


def test_transpile_parse_error_extracts_line_col():
    def fake_transpile(sql, read, write, unsupported_level):
        raise _ParseError(
            "Invalid expression / Unexpected token. Line 1, Col: 9.",
            errors=[{"line": 1, "col": 9, "description": "Invalid expression"}],
        )

    runner = _load_runner(fake_transpile)
    out = runner._transpile_sql("SELECT * EXCEPT FROM bar")
    assert out["ok"] is False, out
    assert out["error"], out
    assert out["line"] == 1, out
    assert out["col"] == 9, out


def test_transpile_parse_error_missing_errors_list():
    """A ParseError with no .errors entries must not raise — line/col
    fall back to None and the message still surfaces."""
    def fake_transpile(sql, read, write, unsupported_level):
        raise _ParseError("syntax error", errors=[])

    runner = _load_runner(fake_transpile)
    out = runner._transpile_sql("SELECT bogus(")
    assert out["ok"] is False, out
    assert "syntax error" in out["error"], out
    assert out["line"] is None, out
    assert out["col"] is None, out


def test_transpile_unsupported_error_line_col_none():
    def fake_transpile(sql, read, write, unsupported_level):
        raise _UnsupportedError("Cannot map Spark construct to Athena")

    runner = _load_runner(fake_transpile)
    out = runner._transpile_sql("SELECT some_spark_only_thing() FROM t")
    assert out["ok"] is False, out
    assert "Cannot map" in out["error"], out
    assert out["line"] is None, out
    assert out["col"] is None, out


def test_transpile_generic_exception_never_raises():
    def fake_transpile(sql, read, write, unsupported_level):
        raise RuntimeError("unexpected sqlglot internal failure")

    runner = _load_runner(fake_transpile)
    out = runner._transpile_sql("SELECT 1")
    assert out["ok"] is False, out
    assert out["error"], out
    assert "unexpected sqlglot internal failure" in out["error"], out
    assert out["line"] is None, out
    assert out["col"] is None, out


if __name__ == "__main__":
    test_transpile_success_returns_trino()
    test_transpile_parse_error_extracts_line_col()
    test_transpile_parse_error_missing_errors_list()
    test_transpile_unsupported_error_line_col_none()
    test_transpile_generic_exception_never_raises()
    print("ok")
