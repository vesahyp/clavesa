"""Unit tests for the py4j session self-heal helpers in runner/runner.py (#23).

Run with: python3 tests/runner/test_spark_liveness.py
   or:    python3 -m unittest tests/runner/test_spark_liveness.py

`_is_spark_session_dead` is the cheap liveness probe (one py4j round-trip to
SparkContext.isStopped(), no Spark job) and `_reset_spark_session` drops the
cached session. Both are pure-Python with a fake session here — no real Spark.
The end-to-end heal (kill the cached session mid-bundle, next node rebuilds)
is the lead's docker integration gate.
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
    test_session_closed.py). The liveness helpers need no real Spark."""
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
# Fakes
# ---------------------------------------------------------------------------


class _FakeSc:
    """Stands in for the JVM-side SparkContext (session.sparkContext._jsc.sc())."""

    def __init__(self, *, stopped=False, raise_exc=None):
        self._stopped = stopped
        self._raise_exc = raise_exc

    def isStopped(self):  # noqa: N802 — mirrors the Java method name
        if self._raise_exc is not None:
            raise self._raise_exc
        return self._stopped


class _FakeJsc:
    def __init__(self, sc):
        self._sc = sc

    def sc(self):
        return self._sc


class _FakeSparkContext:
    def __init__(self, sc):
        self._jsc = _FakeJsc(sc)


class _FakeSession:
    def __init__(self, *, stopped=False, raise_exc=None):
        self.sparkContext = _FakeSparkContext(_FakeSc(stopped=stopped, raise_exc=raise_exc))


class _RecordingSession:
    def __init__(self, *, stop_raises=False):
        self.stop_called = False
        self._stop_raises = stop_raises

    def stop(self):
        self.stop_called = True
        if self._stop_raises:
            raise RuntimeError("stop failed on a dead handle")


# ---------------------------------------------------------------------------
# _is_spark_session_dead
# ---------------------------------------------------------------------------


def test_alive_session_not_dead():
    runner = _load_runner()
    session = _FakeSession(stopped=False)
    assert runner._is_spark_session_dead(session) is False


def test_stopped_session_is_dead():
    runner = _load_runner()
    session = _FakeSession(stopped=True)
    assert runner._is_spark_session_dead(session) is True


def test_connection_refused_is_dead():
    runner = _load_runner()
    session = _FakeSession(raise_exc=ConnectionRefusedError("driver gone"))
    assert runner._is_spark_session_dead(session) is True


def test_generic_exception_is_dead():
    runner = _load_runner()
    session = _FakeSession(raise_exc=ValueError("unexpected"))
    assert runner._is_spark_session_dead(session) is True


# ---------------------------------------------------------------------------
# _reset_spark_session
# ---------------------------------------------------------------------------


def test_reset_stops_and_clears():
    runner = _load_runner()
    fake = _RecordingSession()
    runner._SPARK = fake
    runner._reset_spark_session()
    assert fake.stop_called is True
    assert runner._SPARK is None


def test_reset_swallows_stop_error_and_still_clears():
    runner = _load_runner()
    fake = _RecordingSession(stop_raises=True)
    runner._SPARK = fake
    runner._reset_spark_session()  # must not raise
    assert fake.stop_called is True
    assert runner._SPARK is None


if __name__ == "__main__":
    test_alive_session_not_dead()
    test_stopped_session_is_dead()
    test_connection_refused_is_dead()
    test_generic_exception_is_dead()
    test_reset_stops_and_clears()
    test_reset_swallows_stop_error_and_still_clears()
    print("ok")
