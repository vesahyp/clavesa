"""Unit tests for the Spark event-log / resource-metric helpers in
runner/runner.py.

Run with: python3 tests/runner/test_event_log.py
   or:    python3 -m unittest tests/runner/test_event_log.py

Pure stdlib — no Spark, no docker, no /proc dependency (the peak-RSS parser
is fed sample text directly). The impure wrappers (_read_spark_metrics,
_event_log_offset) that touch the filesystem are exercised by the
docker-gated integration suite; here we test the pure core that does the
aggregation, which is the warm-worker-critical part.
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
    test_node_runs_row.py). The helpers under test are pure stdlib."""
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
# Event-log line builders — shape mirrors Spark's JsonProtocol output.
# ---------------------------------------------------------------------------


def _task_end_line(
    *,
    reason="Success",
    peak_exec_mem=0,
    memory_spilled=0,
    disk_spilled=0,
    remote_read=0,
    local_read=0,
    shuffle_written=0,
    input_bytes=0,
    input_records=0,
    jvm_gc=0,
    cpu_ns=0,
    run_ms=0,
    launch_time=0,
    finish_time=0,
) -> str:
    ev = {
        "Event": "SparkListenerTaskEnd",
        "Task End Reason": {"Reason": reason},
        "Task Info": {"Launch Time": launch_time, "Finish Time": finish_time},
        "Task Metrics": {
            "Peak Execution Memory": peak_exec_mem,
            "Memory Bytes Spilled": memory_spilled,
            "Disk Bytes Spilled": disk_spilled,
            "JVM GC Time": jvm_gc,
            "Executor CPU Time": cpu_ns,
            "Executor Run Time": run_ms,
            "Shuffle Read Metrics": {
                "Remote Bytes Read": remote_read,
                "Local Bytes Read": local_read,
            },
            "Shuffle Write Metrics": {"Shuffle Bytes Written": shuffle_written},
            "Input Metrics": {
                "Bytes Read": input_bytes,
                "Records Read": input_records,
            },
        },
    }
    return json.dumps(ev)


def _stage_completed_line() -> str:
    return json.dumps({"Event": "SparkListenerStageCompleted"})


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_read_peak_rss_mb():
    runner = _load_runner()
    status = (
        "Name:\tjava\n"
        "VmPeak:\t 4194304 kB\n"
        "VmHWM:\t 2097152 kB\n"
        "VmRSS:\t 1048576 kB\n"
    )
    # 2097152 kB / 1024 = 2048 MB.
    assert runner._read_peak_rss_mb(status) == 2048


def test_read_peak_rss_mb_tab_and_spaces():
    runner = _load_runner()
    # Exact format from the contract: "VmHWM:\t  123456 kB".
    assert runner._read_peak_rss_mb("VmHWM:\t  123456 kB\n") == 123456 // 1024


def test_read_peak_rss_mb_missing_returns_none():
    runner = _load_runner()
    assert runner._read_peak_rss_mb("Name:\tjava\nVmRSS:\t 100 kB\n") is None


def test_read_peak_rss_mb_malformed_returns_none():
    runner = _load_runner()
    assert runner._read_peak_rss_mb("VmHWM:\tnot-a-number kB\n") is None


def test_aggregate_event_log():
    runner = _load_runner()
    lines = [
        _task_end_line(
            peak_exec_mem=10 * 1048576,   # 10 MB
            memory_spilled=100,
            disk_spilled=200,
            remote_read=1000,
            local_read=500,
            shuffle_written=300,
            input_bytes=10_000,
            input_records=50,
            jvm_gc=5,
            cpu_ns=2_000_000,             # 2 ms
            run_ms=40,
            launch_time=1000,
            finish_time=1100,             # 100 ms duration
        ),
        _task_end_line(
            peak_exec_mem=30 * 1048576,   # 30 MB (max)
            memory_spilled=300,
            disk_spilled=400,
            remote_read=2000,
            local_read=0,
            shuffle_written=700,
            input_bytes=20_000,
            input_records=75,
            jvm_gc=10,
            cpu_ns=3_000_000,             # 3 ms
            run_ms=60,
            launch_time=2000,
            finish_time=2250,             # 250 ms duration (max)
        ),
        _task_end_line(
            reason="ExecutorLostFailure",  # failed task
            peak_exec_mem=5 * 1048576,
            memory_spilled=0,
            disk_spilled=0,
            input_bytes=0,
            input_records=0,
            cpu_ns=1_000_000,             # 1 ms
            run_ms=20,
            launch_time=3000,
            finish_time=3050,             # 50 ms duration
        ),
        _stage_completed_line(),
    ]

    out = runner._aggregate_event_log(lines)

    # peak execution memory: max over tasks, bytes → MB.
    assert out["peak_execution_memory_mb"] == 30
    # spill / shuffle / input: sums.
    assert out["memory_spilled_bytes"] == 100 + 300 + 0
    assert out["disk_spilled_bytes"] == 200 + 400 + 0
    assert out["shuffle_read_bytes"] == (1000 + 500) + (2000 + 0) + 0
    assert out["shuffle_write_bytes"] == 300 + 700 + 0
    assert out["input_bytes"] == 10_000 + 20_000 + 0
    assert out["input_records"] == 50 + 75 + 0
    # counts.
    assert out["num_stages"] == 1
    assert out["num_tasks"] == 3
    assert out["num_failed_tasks"] == 1
    # time sums / conversions.
    assert out["jvm_gc_time_ms"] == 5 + 10 + 0
    # cpu ns → ms: (2e6 + 3e6 + 1e6) / 1e6 = 6.
    assert out["executor_cpu_time_ms"] == 6
    assert out["executor_run_time_ms"] == 40 + 60 + 20
    # max task duration over tasks.
    assert out["max_task_duration_ms"] == 250


def test_aggregate_event_log_empty_all_none():
    """No relevant events → every key present and None."""
    runner = _load_runner()
    out = runner._aggregate_event_log([])
    for k in runner._SPARK_METRIC_KEYS:
        assert k in out
        assert out[k] is None


def test_aggregate_event_log_ignores_malformed():
    runner = _load_runner()
    lines = [
        "",
        "   ",
        "not json at all",
        "{not: valid}",
        _task_end_line(input_bytes=5, input_records=1, run_ms=10),
    ]
    out = runner._aggregate_event_log(lines)
    assert out["num_tasks"] == 1
    assert out["input_bytes"] == 5
    assert out["executor_run_time_ms"] == 10


def test_aggregate_event_log_defensive_missing_nested_keys():
    """Events missing Task Metrics / Task Info / Task End Reason don't crash;
    a task with no metrics still counts toward num_tasks."""
    runner = _load_runner()
    lines = [
        json.dumps({"Event": "SparkListenerTaskEnd"}),  # no nested keys
    ]
    out = runner._aggregate_event_log(lines)
    assert out["num_tasks"] == 1
    # Missing Task End Reason → not "Success" → counts as failed.
    assert out["num_failed_tasks"] == 1
    assert out["input_bytes"] == 0
    assert out["max_task_duration_ms"] == 0


def test_aggregate_event_log_offset_scoping():
    """WARM-WORKER REGRESSION GUARD. Under session reuse the event log
    accumulates across invocations; the runner seeks past the prior tail and
    parses only the new bytes. Build invocation-1 lines, record their byte
    length as the offset, append invocation-2 lines, then aggregate only
    text[offset:] — the result must reflect invocation 2 alone, not 1+2."""
    runner = _load_runner()

    inv1 = "\n".join([
        _task_end_line(input_bytes=1000, input_records=10, run_ms=100),
        _task_end_line(input_bytes=1000, input_records=10, run_ms=100),
    ]) + "\n"
    offset = len(inv1.encode("utf-8"))

    inv2 = "\n".join([
        _task_end_line(input_bytes=7, input_records=3, run_ms=42),
    ]) + "\n"

    full = inv1 + inv2

    # Mirror _read_spark_metrics: seek past the prior tail by byte offset.
    tail = full.encode("utf-8")[offset:].decode("utf-8")
    out = runner._aggregate_event_log(tail.splitlines())

    # Only invocation 2's single task is counted.
    assert out["num_tasks"] == 1
    assert out["input_bytes"] == 7
    assert out["input_records"] == 3
    assert out["executor_run_time_ms"] == 42


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
