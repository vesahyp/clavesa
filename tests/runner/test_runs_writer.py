"""Unit tests for the runs_writer Lambda's pure helpers.

ADR-018 (v2.0.0): runs_writer used to be a stand-alone Python zip
Lambda at internal/orchestration/sidecar/runs_writer/index.py that
INSERTed via Athena. That path is gone — Athena's Delta support is
read-only, so we bundled runs_writer into the runner image and the
helpers now live inside runner.py alongside the transform handler.
These tests exercise those helpers directly against runner.py.

Run with: python3 tests/runner/test_runs_writer.py

The Spark + Delta round-trip (`_record_run` itself) needs the full
runner container; cover that via `make test-runner` rather than here.
This file covers the pure helpers + the no-stub-needed parts of
`runs_writer_handler`.
"""

from __future__ import annotations

import datetime as _dt
import importlib.util
import json
import os
import sys
import types
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RUNNER = REPO / "runner" / "runner.py"


def _load_runner():
    """Import runner.py with PySpark stubbed so the module loads
    without a real JVM. Only the runs_writer helpers we exercise here
    avoid Spark; the rest of runner.py (transform handler, record_run)
    stays importable but uninvoked.
    """
    # Stub out the heavyweight imports so module top-level executes
    # without a JVM. PySpark itself isn't imported at top level (lazy
    # via _spark()), so we don't need to stub it.
    os.environ.setdefault("CLAVESA_PIPELINE", "test_pipeline")
    os.environ.setdefault("CLAVESA_SYSTEM_CATALOG", "test_sys")

    # spark_conf is imported lazily inside _spark() — same story; the
    # helpers we exercise never call _spark.

    spec = importlib.util.spec_from_file_location("runner_under_test", str(RUNNER))
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# _runs_writer_extract_trigger — read `_trigger` smuggled through the SFN
# exec input. Default to "manual" when missing / malformed / unknown.
# ---------------------------------------------------------------------------


def test_extract_trigger_scheduled():
    r = _load_runner()
    payload = json.dumps({"pipeline": "p", "_trigger": "scheduled"})
    assert r._runs_writer_extract_trigger(payload) == "scheduled"


def test_extract_trigger_event():
    r = _load_runner()
    assert r._runs_writer_extract_trigger(json.dumps({"_trigger": "event"})) == "event"


def test_extract_trigger_manual_when_input_empty():
    """Manual runs via console / CLI / `clavesa pipeline run-cloud` typically
    pass an empty input (or omit _trigger). Default to 'manual' rather than
    leaving the column NULL — keeps queries on `runs.trigger` total."""
    r = _load_runner()
    assert r._runs_writer_extract_trigger("") == "manual"
    assert r._runs_writer_extract_trigger(None) == "manual"
    assert r._runs_writer_extract_trigger("{}") == "manual"


def test_extract_trigger_malformed_input():
    """SFN can pass arbitrary input; a non-JSON string or non-dict shouldn't
    crash the writer — degrade to 'manual'."""
    r = _load_runner()
    assert r._runs_writer_extract_trigger("not json at all") == "manual"
    assert r._runs_writer_extract_trigger(json.dumps([1, 2, 3])) == "manual"


def test_extract_trigger_already_parsed_dict():
    """Some test paths and direct invocations may hand us the dict already
    parsed; the function should accept that without re-parsing."""
    r = _load_runner()
    assert r._runs_writer_extract_trigger({"_trigger": "scheduled"}) == "scheduled"


def test_extract_trigger_backfill():
    """Backfill executions get trigger='backfill' so the runs history surfaces
    them separately from regular scheduled/event runs."""
    r = _load_runner()
    assert r._runs_writer_extract_trigger({"_trigger": "backfill"}) == "backfill"
    assert r._runs_writer_extract_trigger({"_trigger": "backfill-direct"}) == "backfill-direct"


def test_extract_trigger_unknown_value():
    """Unknown trigger values degrade to 'manual' so the column stays clean."""
    r = _load_runner()
    assert r._runs_writer_extract_trigger({"_trigger": "lambda-warm-shot"}) == "manual"


# ---------------------------------------------------------------------------
# _runs_writer_extract_target_table — picks the staging table id from a
# backfill payload.
# ---------------------------------------------------------------------------


def test_extract_target_table_from_backfill_input():
    """Backfill stamps `_backfill.target_outputs` into the SFN input — the
    runs row carries the staging table id so the UI can join target → staging."""
    r = _load_runner()
    bf_input = {
        "_trigger": "backfill",
        "_backfill": {
            "node": "passthrough",
            "target_outputs": {
                "default": "clavesa.db.passthrough__default__backfill__run123",
            },
        },
    }
    assert (
        r._runs_writer_extract_target_table(bf_input)
        == "clavesa.db.passthrough__default__backfill__run123"
    )
    # JSON string form (the EventBridge payload shape) parses too.
    assert (
        r._runs_writer_extract_target_table(json.dumps(bf_input))
        == "clavesa.db.passthrough__default__backfill__run123"
    )


def test_extract_target_table_none_when_no_backfill():
    """Regular runs leave target_table NULL."""
    r = _load_runner()
    assert r._runs_writer_extract_target_table(None) is None
    assert r._runs_writer_extract_target_table("") is None
    assert r._runs_writer_extract_target_table({"_trigger": "manual"}) is None
    assert r._runs_writer_extract_target_table({"_backfill": "not-a-dict"}) is None


# ---------------------------------------------------------------------------
# _runs_writer_parse_cause — best-effort error_msg extraction.
# ---------------------------------------------------------------------------


def test_parse_cause_lambda_json():
    r = _load_runner()
    cause = '{"errorMessage": "Table not found", "errorType": "AnalysisException"}'
    msg, step = r._runs_writer_parse_cause(cause)
    assert msg == "Table not found"
    assert step == ""


def test_parse_cause_plain_text():
    r = _load_runner()
    msg, step = r._runs_writer_parse_cause("State machine failed: timeout")
    assert msg == "State machine failed: timeout"
    assert step == ""


def test_parse_cause_truncates_huge():
    r = _load_runner()
    huge = "X" * 10_000
    msg, _ = r._runs_writer_parse_cause(huge)
    assert len(msg) <= 4096
    assert msg.endswith("...")


def test_parse_cause_empty():
    r = _load_runner()
    msg, step = r._runs_writer_parse_cause("")
    assert msg == "" and step == ""


# ---------------------------------------------------------------------------
# _runs_writer_build_row — the EventBridge detail → runs-row mapping.
# Returns typed values (datetime, int, None) for direct Spark createDataFrame
# consumption — no Athena value rendering anymore.
# ---------------------------------------------------------------------------


def test_build_row_succeeded():
    r = _load_runner()
    detail = {
        "executionArn": "arn:aws:states:us-east-1:1:execution:clavesa-x:abc-123",
        "stateMachineArn": "arn:aws:states:us-east-1:1:stateMachine:clavesa-x",
        "status": "SUCCEEDED",
        "startDate": 1778157045000,
        "stopDate": 1778157050000,
    }
    row = r._runs_writer_build_row(detail)
    assert row["run_id"] == "abc-123"
    assert row["pipeline"] == "test_pipeline"
    assert row["sf_execution_arn"] == detail["executionArn"]
    assert row["status"] == "SUCCEEDED"
    assert row["duration_ms"] == 5000
    assert row["error_class"] == ""
    assert row["error_msg"] is None
    assert row["failed_step"] == ""
    # Typed datetime values, not strings — runner._record_run writes via
    # spark.createDataFrame so it expects native types.
    assert isinstance(row["started_at"], _dt.datetime)
    assert isinstance(row["ended_at"], _dt.datetime)
    assert row["started_at"].tzinfo is not None
    assert row["ended_at"].tzinfo is not None


def test_build_row_failed_carries_error():
    r = _load_runner()
    detail = {
        "executionArn": "arn:aws:states:us-east-1:1:execution:sm:exec",
        "status": "FAILED",
        "startDate": 1778157045000,
        "stopDate": 1778157046000,
        "error": "States.TaskFailed",
        "cause": '{"errorMessage": "boom", "errorType": "RuntimeError"}',
    }
    row = r._runs_writer_build_row(detail)
    assert row["status"] == "FAILED"
    assert row["error_class"] == "States.TaskFailed"
    assert row["error_msg"] == "boom"
    assert row["duration_ms"] == 1000


def test_build_row_running_no_stop_leaves_duration_null():
    r = _load_runner()
    detail = {
        "executionArn": "arn:aws:states:us-east-1:1:execution:sm:exec",
        "status": "RUNNING",  # not terminal — handler skips, but builder still works
        "startDate": 1778157045000,
        # stopDate absent
    }
    row = r._runs_writer_build_row(detail)
    assert row["duration_ms"] is None
    assert row["ended_at"] is None


def test_build_row_negative_duration_clamped():
    r = _load_runner()
    detail = {
        "executionArn": "arn:aws:states:us-east-1:1:execution:sm:exec",
        "status": "SUCCEEDED",
        "startDate": 1778157045010,
        "stopDate": 1778157045005,  # clock skew
    }
    row = r._runs_writer_build_row(detail)
    assert row["duration_ms"] == 0


def test_build_row_populates_trigger_from_input():
    r = _load_runner()
    detail = {
        "executionArn": "arn:aws:states:us-east-1:1:execution:sm:exec",
        "status": "SUCCEEDED",
        "startDate": 1778157045000,
        "stopDate": 1778157050000,
        "input": json.dumps({"pipeline": "p", "_trigger": "scheduled"}),
    }
    row = r._runs_writer_build_row(detail)
    assert row["trigger"] == "scheduled"


def test_build_row_defaults_trigger_to_manual():
    r = _load_runner()
    detail = {
        "executionArn": "arn:aws:states:us-east-1:1:execution:sm:exec",
        "status": "SUCCEEDED",
        "startDate": 1778157045000,
        "stopDate": 1778157050000,
        # No input field — manual run via CLI/console.
    }
    row = r._runs_writer_build_row(detail)
    assert row["trigger"] == "manual"


# ---------------------------------------------------------------------------
# runs_writer_handler — short-circuit on non-terminal status.
# Real terminal-status round-trips need Spark; cover via make test-runner.
# ---------------------------------------------------------------------------


def test_handler_skips_running():
    r = _load_runner()
    out = r.runs_writer_handler({"detail": {"status": "RUNNING"}}, None)
    assert out.get("skipped")


def test_handler_skips_unknown_status():
    r = _load_runner()
    out = r.runs_writer_handler({"detail": {"status": "PENDING"}}, None)
    assert out.get("skipped")


def test_handler_skips_empty_detail():
    r = _load_runner()
    out = r.runs_writer_handler({}, None)
    assert out.get("skipped")


# ---------------------------------------------------------------------------
# _runs_writer_truncate — string truncation helper.
# ---------------------------------------------------------------------------


def test_truncate_under_limit():
    r = _load_runner()
    assert r._runs_writer_truncate("hello") == "hello"


def test_truncate_at_limit():
    r = _load_runner()
    s = "x" * 4096
    assert r._runs_writer_truncate(s) == s


def test_truncate_over_limit_ellipsis():
    r = _load_runner()
    s = "x" * 5000
    out = r._runs_writer_truncate(s)
    assert len(out) == 4096
    assert out.endswith("...")


# ---------------------------------------------------------------------------
# Test runner — same script style as the other runner tests.
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
            print(f"FAIL  {name}  -  {type(e).__name__}: {e}")
        else:
            passed += 1
            print(f"PASS  {name}")
    total = passed + len(failed)
    print(f"\n{passed}/{total} passed", "FAILED" if failed else "OK")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
