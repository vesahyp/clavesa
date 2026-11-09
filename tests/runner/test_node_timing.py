"""Unit tests for per-phase node timing in runner/runner.py: the pure
_timing_object builder, the _PhaseTimer accumulator, the stderr summary
line, and the node_runs `timing` column.

Run with: python3 tests/runner/test_node_timing.py

Stdlib only: no Spark. The timer's event-log slices are disabled
(spark_slices=False) or fed directly through add_spark().
"""

from __future__ import annotations

import importlib.util
import json
import sys
import types
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RUNNER = REPO / "runner" / "runner.py"


def _load_runner():
    """Import runner.py with boto3/pyspark stubbed (same trick as
    test_incremental.py). Heavy native deps aren't needed for the pure
    helpers we're exercising here.
    """
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




_REAL_MONOTONIC = None


def _fake_clock(runner, clock):
    """Point runner's time.monotonic at a manual clock. runner.time is the
    shared stdlib module, so main() restores the real one after each test."""
    global _REAL_MONOTONIC
    if _REAL_MONOTONIC is None:
        _REAL_MONOTONIC = runner.time.monotonic
    runner.time.monotonic = lambda: clock[0]  # type: ignore[assignment]


def _restore_clock():
    if _REAL_MONOTONIC is not None:
        import time as _t
        _t.monotonic = _REAL_MONOTONIC


PHASES = (
    "spark_session", "catalog", "inputs", "transform", "write",
    "glue_sync", "stats", "table_state", "system",
)
SPARK_KEYS = (
    "num_stages", "num_tasks", "input_bytes", "input_records",
    "shuffle_read_bytes", "shuffle_write_bytes", "memory_spilled_bytes",
    "disk_spilled_bytes", "executor_run_time_ms",
)


def test_timing_object_all_keys_present_and_zero_by_default():
    runner = _load_runner()
    t = runner._timing_object({})
    assert set(t) == {"phases", "inputs", "outputs", "spark"}, t
    assert tuple(t["phases"]) == PHASES, t["phases"]
    assert all(v == 0 for v in t["phases"].values()), t["phases"]
    assert t["inputs"] == {} and t["outputs"] == {} and t["spark"] == {}


def test_timing_object_skipped_phases_stay_zero():
    """A node that failed in inputs: later phases keep 0, unknown keys drop."""
    runner = _load_runner()
    t = runner._timing_object(
        {"spark_session": 3, "inputs": 840, "bogus": 9},
        inputs={"src_trips": 840},
    )
    assert t["phases"]["inputs"] == 840
    assert t["phases"]["spark_session"] == 3
    for p in ("transform", "write", "glue_sync", "stats", "table_state", "system"):
        assert t["phases"][p] == 0, (p, t["phases"])
    assert "bogus" not in t["phases"]
    assert t["inputs"] == {"src_trips": 840}


def test_timing_object_spark_slices_omitted_when_empty():
    runner = _load_runner()
    full = {k: None for k in SPARK_KEYS}
    full.update({"num_stages": 4, "num_tasks": 40, "input_bytes": 1234,
                 "executor_run_time_ms": 18000, "peak_execution_memory_mb": 7})
    t = runner._timing_object(
        {"write": 20450},
        outputs={"default": 20450},
        spark={
            "write": full,
            "inputs": {"num_stages": 0, "num_tasks": 0},
            "transform": {k: None for k in SPARK_KEYS},
            # Not a Spark-capable phase: dropped even with stages.
            "glue_sync": {"num_stages": 2},
        },
    )
    assert list(t["spark"]) == ["write"], t["spark"]
    w = t["spark"]["write"]
    assert tuple(w) == SPARK_KEYS, w
    assert w["num_stages"] == 4 and w["num_tasks"] == 40
    assert w["input_bytes"] == 1234 and w["executor_run_time_ms"] == 18000
    assert w["shuffle_read_bytes"] == 0  # None -> 0
    assert t["outputs"] == {"default": 20450}


def test_timing_object_system_fill_in():
    runner = _load_runner()
    t = runner._timing_object({"write": 5, "system": 1}, system_ms=1200)
    assert t["phases"]["system"] == 1200
    assert runner._timing_object({"system": 7})["phases"]["system"] == 7


def test_phase_timer_nesting_subtracts_child():
    """catalog inside a write is counted once: write gets its own time only."""
    runner = _load_runner()
    clock = [0.0]
    _fake_clock(runner, clock)
    timer = runner._PhaseTimer(spark_slices=False)
    w = timer.start("write", detail="outputs", key="default")
    clock[0] += 1.0
    with timer.measure("catalog"):
        clock[0] += 0.25
    clock[0] += 2.0
    timer.stop(w)
    with timer.measure("inputs", detail="inputs", key="a"):
        clock[0] += 0.5
    with timer.measure("inputs", detail="inputs", key="b"):
        clock[0] += 0.3
    t = timer.to_dict(system_ms=12)
    assert t["phases"]["write"] == 3000, t
    assert t["phases"]["catalog"] == 250, t
    assert t["outputs"] == {"default": 3000}, t
    assert t["phases"]["inputs"] == 800, t
    assert t["inputs"] == {"a": 500, "b": 300}, t
    assert t["phases"]["system"] == 12
    assert t["phases"]["transform"] == 0


def test_phase_timer_close_open_records_failed_phase():
    """An exception leaves a phase open; close_open still records its time
    (and that of any open child) and later phases stay 0."""
    runner = _load_runner()
    clock = [10.0]
    _fake_clock(runner, clock)
    timer = runner._PhaseTimer(spark_slices=False)
    timer.start("write", detail="outputs", key="o")
    clock[0] += 1.0
    timer.start("catalog")
    clock[0] += 0.5
    timer.close_open()
    t = timer.to_dict()
    assert t["phases"]["catalog"] == 500, t
    assert t["phases"]["write"] == 1000, t
    assert t["phases"]["stats"] == 0 and t["phases"]["table_state"] == 0
    # Stopping an already-closed token is a no-op.
    timer.close_open()
    assert timer.to_dict() == t


def test_phase_timer_add_spark_sums_slices():
    runner = _load_runner()
    timer = runner._PhaseTimer(spark_slices=False)
    timer.add_spark("write", {"num_stages": 2, "num_tasks": 10, "input_bytes": 5})
    timer.add_spark("write", {"num_stages": 1, "num_tasks": 3, "input_bytes": None})
    timer.add_spark("stats", {"num_stages": None, "num_tasks": None})
    t = timer.to_dict()
    assert t["spark"]["write"]["num_stages"] == 3, t
    assert t["spark"]["write"]["num_tasks"] == 13
    assert t["spark"]["write"]["input_bytes"] == 5
    assert "stats" not in t["spark"]


def test_timing_log_line_is_one_line():
    runner = _load_runner()
    t = runner._timing_object({"inputs": 840, "transform": 95, "write": 20500, "table_state": 310})
    line = runner._timing_log_line("trips", t)
    assert "\n" not in line
    assert line == (
        "[clavesa] timing trips: inputs=0.8s transform=0.1s write=20.5s "
        "stats=0s table_state=0.3s (spark_session=0s catalog=0s)"
    ), line


def _row(runner, **kw):
    return runner._build_node_run_row(
        run_id="r", started_ms=0, ended_ms=10, status="ok",
        error_class=None, error_msg=None, cold_start=False, context=None,
        env={"CLAVESA_PIPELINE": "p", "CLAVESA_NODE": "n"}, **kw,
    )


def test_build_row_timing_compact_json_with_system_zero():
    runner = _load_runner()
    timing = runner._timing_object(
        {"inputs": 840, "write": 20450, "system": 999},
        inputs={"src": 840},
        spark={"write": {"num_stages": 4, "num_tasks": 40}},
    )
    row = _row(runner, timing=timing)
    raw = row["timing"]
    assert isinstance(raw, str), raw
    assert " " not in raw, f"expected compact JSON: {raw}"
    parsed = json.loads(raw)
    assert parsed["phases"]["system"] == 0, parsed
    assert parsed["phases"]["write"] == 20450
    assert parsed["inputs"] == {"src": 840}
    assert parsed["spark"]["write"]["num_stages"] == 4
    # The caller's object is not mutated.
    assert timing["phases"]["system"] == 999


def test_build_row_timing_null_when_absent():
    """No timing (a cascade-skip or a pre-timing caller) is SQL NULL."""
    runner = _load_runner()
    row = _row(runner)
    assert "timing" in row and row["timing"] is None, row


# ---------------------------------------------------------------------------
# Test runner — same pattern as test_node_runs_row.py.
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
            _restore_clock()
        except Exception as e:
            _restore_clock()
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
