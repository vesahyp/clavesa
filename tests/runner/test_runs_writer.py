"""Unit tests for the runs-writer Lambda's pure helpers.

Run with: python3 tests/runner/test_runs_writer.py

Exercises the row construction + cause parsing + Athena value rendering
for modules/orchestration/aws/runs_writer/index.py. The Athena round-trip
(StartQueryExecution/GetQueryExecution polling) needs real AWS or a
moto-server stub; cover that in cloud validation, not here.
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import types
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
WRITER = REPO / "modules" / "orchestration" / "aws" / "runs_writer" / "index.py"


def _load_writer():
    """Import index.py with boto3 stubbed to avoid an AWS profile lookup
    at import time. The handler module reads three env vars at import,
    so the test sets minimal placeholders before loading.
    """
    boto3_mod = types.ModuleType("boto3")
    boto3_mod.client = lambda *_a, **_k: None  # type: ignore[attr-defined]
    sys.modules.setdefault("boto3", boto3_mod)

    os.environ.setdefault("CLAVESA_PIPELINE", "test_pipeline")
    os.environ.setdefault("CLAVESA_DATABASE", "clavesa_test_pipeline")
    os.environ.setdefault("CLAVESA_WAREHOUSE_BUCKET", "clavesa-bucket")

    spec = importlib.util.spec_from_file_location("runs_writer", str(WRITER))
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# _format_athena_ts — millisecond precision, UTC, Athena literal shape.
# ---------------------------------------------------------------------------


def test_format_ts_basic():
    w = _load_writer()
    # 2026-05-07T12:30:45.500Z = 1778157045500 ms
    out = w._format_athena_ts(1778157045500)
    assert out == "2026-05-07 12:30:45.500"


def test_format_ts_handles_none_and_non_int():
    w = _load_writer()
    assert w._format_athena_ts(None) is None
    assert w._format_athena_ts("123") is None
    assert w._format_athena_ts(0) is None  # treat 0 as "missing"


# ---------------------------------------------------------------------------
# _parse_cause — best-effort error_msg extraction.
# ---------------------------------------------------------------------------


def test_parse_cause_lambda_json():
    w = _load_writer()
    cause = '{"errorMessage": "Table not found", "errorType": "AnalysisException"}'
    msg, step = w._parse_cause(cause)
    assert msg == "Table not found"
    assert step == ""


def test_parse_cause_plain_text():
    w = _load_writer()
    msg, step = w._parse_cause("State machine failed: timeout")
    assert msg == "State machine failed: timeout"
    assert step == ""


def test_parse_cause_truncates_huge():
    w = _load_writer()
    huge = "X" * 10_000
    msg, _ = w._parse_cause(huge)
    assert len(msg) <= 4096
    assert msg.endswith("...")


def test_parse_cause_empty():
    w = _load_writer()
    msg, step = w._parse_cause("")
    assert msg == "" and step == ""


# ---------------------------------------------------------------------------
# _build_row — the EventBridge detail → Iceberg row mapping.
# ---------------------------------------------------------------------------


def test_build_row_succeeded():
    w = _load_writer()
    detail = {
        "executionArn": "arn:aws:states:us-east-1:1:execution:clavesa-x:abc-123",
        "stateMachineArn": "arn:aws:states:us-east-1:1:stateMachine:clavesa-x",
        "status": "SUCCEEDED",
        "startDate": 1778157045000,
        "stopDate": 1778157050000,
    }
    row = w._build_row(detail)
    assert row["run_id"] == "abc-123"
    assert row["pipeline"] == "test_pipeline"
    assert row["sf_execution_arn"] == detail["executionArn"]
    assert row["status"] == "SUCCEEDED"
    assert row["duration_ms"] == 5000
    assert row["error_class"] == ""
    assert row["error_msg"] == ""
    assert row["failed_step"] == ""
    assert row["started_at"] == "2026-05-07 12:30:45.000"
    assert row["ended_at"] == "2026-05-07 12:30:50.000"


def test_build_row_failed_carries_error():
    w = _load_writer()
    detail = {
        "executionArn": "arn:aws:states:us-east-1:1:execution:sm:exec",
        "status": "FAILED",
        "startDate": 1778157045000,
        "stopDate": 1778157046000,
        "error": "States.TaskFailed",
        "cause": '{"errorMessage": "boom", "errorType": "RuntimeError"}',
    }
    row = w._build_row(detail)
    assert row["status"] == "FAILED"
    assert row["error_class"] == "States.TaskFailed"
    assert row["error_msg"] == "boom"
    assert row["duration_ms"] == 1000


def test_build_row_running_no_stop_leaves_duration_null():
    w = _load_writer()
    detail = {
        "executionArn": "arn:aws:states:us-east-1:1:execution:sm:exec",
        "status": "RUNNING",  # not terminal — handler skips, but builder still works
        "startDate": 1778157045000,
        # stopDate absent
    }
    row = w._build_row(detail)
    assert row["duration_ms"] is None
    assert row["ended_at"] is None


def test_build_row_negative_duration_clamped():
    w = _load_writer()
    detail = {
        "executionArn": "arn:aws:states:us-east-1:1:execution:sm:exec",
        "status": "SUCCEEDED",
        "startDate": 1778157045010,
        "stopDate": 1778157045005,  # clock skew
    }
    row = w._build_row(detail)
    assert row["duration_ms"] == 0


# ---------------------------------------------------------------------------
# _extract_trigger — read `_trigger` smuggled through the SFN exec input
# (orchestration emitter sets it on every known automated start path; manual
# runs default to "manual").
# ---------------------------------------------------------------------------


def test_extract_trigger_scheduled():
    w = _load_writer()
    payload = json.dumps({"pipeline": "p", "_trigger": "scheduled"})
    assert w._extract_trigger(payload) == "scheduled"


def test_extract_trigger_event():
    w = _load_writer()
    assert w._extract_trigger(json.dumps({"_trigger": "event"})) == "event"


def test_extract_trigger_manual_when_input_empty():
    """Manual runs via console / CLI / `clavesa pipeline run-cloud` typically
    pass an empty input (or omit _trigger). Default to 'manual' rather than
    leaving the column NULL — keeps queries on `runs.trigger` total."""
    w = _load_writer()
    assert w._extract_trigger("") == "manual"
    assert w._extract_trigger(None) == "manual"
    assert w._extract_trigger("{}") == "manual"


def test_extract_trigger_malformed_input():
    """SFN can pass arbitrary input; a non-JSON string or non-dict shouldn't
    crash the writer — degrade to 'manual'."""
    w = _load_writer()
    assert w._extract_trigger("not json at all") == "manual"
    assert w._extract_trigger(json.dumps([1, 2, 3])) == "manual"


def test_extract_trigger_already_parsed_dict():
    """Some test paths and direct invocations may hand us the dict already
    parsed; the function should accept that without re-parsing."""
    w = _load_writer()
    assert w._extract_trigger({"_trigger": "scheduled"}) == "scheduled"


def test_extract_trigger_backfill():
    """Backfill executions get trigger='backfill' so the runs history surfaces
    them separately from regular scheduled/event runs."""
    w = _load_writer()
    assert w._extract_trigger({"_trigger": "backfill"}) == "backfill"
    assert w._extract_trigger({"_trigger": "backfill-direct"}) == "backfill-direct"


def test_extract_target_table_from_backfill_input():
    """Backfill stamps `_backfill.target_outputs` into the SFN input — the
    runs row carries the staging table id so the UI can join target → staging."""
    w = _load_writer()
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
        w._extract_target_table(bf_input)
        == "clavesa.db.passthrough__default__backfill__run123"
    )
    # JSON string form (the EventBridge payload shape) parses too.
    assert (
        w._extract_target_table(json.dumps(bf_input))
        == "clavesa.db.passthrough__default__backfill__run123"
    )


def test_extract_target_table_none_when_no_backfill():
    """Regular runs leave target_table NULL."""
    w = _load_writer()
    assert w._extract_target_table(None) is None
    assert w._extract_target_table("") is None
    assert w._extract_target_table({"_trigger": "manual"}) is None
    assert w._extract_target_table({"_backfill": "not-a-dict"}) is None


def test_build_row_populates_trigger_from_input():
    w = _load_writer()
    detail = {
        "executionArn": "arn:aws:states:us-east-1:1:execution:sm:exec",
        "status": "SUCCEEDED",
        "startDate": 1778157045000,
        "stopDate": 1778157050000,
        "input": json.dumps({"pipeline": "p", "_trigger": "scheduled"}),
    }
    row = w._build_row(detail)
    assert row["trigger"] == "scheduled"


def test_build_row_defaults_trigger_to_manual():
    w = _load_writer()
    detail = {
        "executionArn": "arn:aws:states:us-east-1:1:execution:sm:exec",
        "status": "SUCCEEDED",
        "startDate": 1778157045000,
        "stopDate": 1778157050000,
        # No input field — manual run via CLI/console.
    }
    row = w._build_row(detail)
    assert row["trigger"] == "manual"


# ---------------------------------------------------------------------------
# _render_value — INSERT VALUES literal rendering, including quote escape.
# ---------------------------------------------------------------------------


def test_render_value_string_escapes_quote():
    w = _load_writer()
    assert w._render_value("error_msg", "it's broken") == "'it''s broken'"


def test_render_value_timestamp():
    w = _load_writer()
    assert (
        w._render_value("started_at", "2026-05-07 12:30:45.500")
        == "TIMESTAMP '2026-05-07 12:30:45.500'"
    )


def test_render_value_bigint():
    w = _load_writer()
    assert w._render_value("duration_ms", 1500) == "1500"


def test_render_value_null_for_none():
    w = _load_writer()
    assert w._render_value("error_msg", None) == "NULL"
    assert w._render_value("started_at", None) == "NULL"
    assert w._render_value("duration_ms", None) == "NULL"


# ---------------------------------------------------------------------------
# handler — short-circuit on non-terminal status.
# ---------------------------------------------------------------------------


def test_handler_skips_running():
    w = _load_writer()
    out = w.handler({"detail": {"status": "RUNNING"}}, None)
    assert out.get("skipped")


def test_handler_skips_unknown_status():
    w = _load_writer()
    out = w.handler({"detail": {"status": "PENDING"}}, None)
    assert out.get("skipped")


def test_handler_skips_empty_detail():
    w = _load_writer()
    out = w.handler({}, None)
    assert out.get("skipped")


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
            print(f"FAIL  {name}  →  {type(e).__name__}: {e}")
        else:
            passed += 1
            print(f"PASS  {name}")
    total = passed + len(failed)
    print(f"\n{passed}/{total} passed", "❌" if failed else "✅")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
