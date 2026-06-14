"""Unit tests for runner.pipeline_handler — the bundle-mode entry point
that runs every transform in a pipeline sequentially in one Spark
session.

Run with: python3 tests/runner/test_pipeline_handler.py
   or:    python3 -m unittest tests/runner/test_pipeline_handler.py

Stubbed handler() so we can drive per-transform outcomes without Spark.
"""

from __future__ import annotations

import importlib.util
import io
import json
import sys
import types
from contextlib import redirect_stdout
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RUNNER = REPO / "runner" / "runner.py"


def _load_runner():
    """Import runner.py with boto3/pyspark stubbed. Mirrors
    test_node_runs_row's bootstrap; pipeline_handler doesn't touch
    Spark directly so the stubs are enough."""
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


def _drive(mod, transforms, handler_responses):
    """Invoke pipeline_handler with a stubbed handler() that maps node →
    next response. Returns (result_dict, list_of_stdout_event_dicts).
    """
    call_log: list[str] = []
    def fake_handler(event, context):
        import os as _os
        node = _os.environ.get("CLAVESA_NODE", "")
        call_log.append(node)
        resp = handler_responses.get(node, {"status": "ok"})
        if isinstance(resp, BaseException):
            raise resp
        return resp

    orig_handler = mod.handler
    mod.handler = fake_handler  # type: ignore[assignment]
    try:
        buf = io.StringIO()
        with redirect_stdout(buf):
            result = mod.pipeline_handler({
                "_pipeline_run": True,
                "run_id": "test-run",
                "transforms": transforms,
                "_sf_execution_arn": "test-run",
                "_trigger": "manual",
            }, None)
        out_lines = [ln for ln in buf.getvalue().splitlines() if ln.strip()]
        events = [json.loads(ln) for ln in out_lines]
        return result, events, call_log
    finally:
        mod.handler = orig_handler  # type: ignore[assignment]


def _drive_event(mod, event, handler_responses):
    """Like _drive, but takes the full pipeline_handler event so tests can
    shape the cloud (nested _execution_input) vs local (flat) envelopes.
    Captures each sub_event the stubbed handler() receives."""
    sub_events: list[dict] = []

    def fake_handler(sub_event, context):
        import os as _os
        node = _os.environ.get("CLAVESA_NODE", "")
        sub_events.append(dict(sub_event))
        resp = handler_responses.get(node, {"status": "ok"})
        if isinstance(resp, BaseException):
            raise resp
        return resp

    orig_handler = mod.handler
    mod.handler = fake_handler  # type: ignore[assignment]
    try:
        buf = io.StringIO()
        with redirect_stdout(buf):
            result = mod.pipeline_handler(event, None)
        return result, sub_events
    finally:
        mod.handler = orig_handler  # type: ignore[assignment]


_FORCE_TRANSFORMS = [
    {"node": "a", "language": "sql", "logic_path": "/tmp/a.txt", "inputs": {}, "outputs": {"default": ""}, "parents": []},
    {"node": "b", "language": "sql", "logic_path": "/tmp/b.txt", "inputs": {"a": "..."}, "outputs": {"default": ""}, "parents": ["a"]},
]


def test_force_and_trigger_from_nested_execution_input():
    """Cloud shape: the SFN single-Task ASL nests the CLI's StartExecution
    input under _execution_input — no top-level _force/_trigger. Every
    sub_event must still carry the force flag and trigger."""
    mod = _load_runner()
    result, sub_events = _drive_event(mod, {
        "_pipeline_run": True,
        "run_id": "test-run",
        "transforms": _FORCE_TRANSFORMS,
        "_sf_execution_arn": "arn:test",
        "_execution_input": {"_trigger": "manual", "_force": True},
    }, {})

    assert result["status"] == "ok"
    assert len(sub_events) == 2, sub_events
    for se in sub_events:
        assert se.get("_force") is True, f"sub_event missing _force: {se}"
        assert se.get("_trigger") == "manual", f"sub_event trigger wrong: {se}"


def test_force_nodes_from_nested_execution_input():
    """Cloud shape with scoped force: _force_nodes nested under
    _execution_input must be forwarded into every sub_event."""
    mod = _load_runner()
    result, sub_events = _drive_event(mod, {
        "_pipeline_run": True,
        "run_id": "test-run",
        "transforms": _FORCE_TRANSFORMS,
        "_sf_execution_arn": "arn:test",
        "_execution_input": {"_force": True, "_force_nodes": ["a"]},
    }, {})

    assert result["status"] == "ok"
    assert len(sub_events) == 2, sub_events
    for se in sub_events:
        assert se.get("_force") is True, f"sub_event missing _force: {se}"
        assert se.get("_force_nodes") == ["a"], f"sub_event force_nodes wrong: {se}"


def test_force_from_top_level_still_works():
    """Local bundle shape: _force/_trigger flat on the event (no
    _execution_input) keeps working unchanged."""
    mod = _load_runner()
    result, sub_events = _drive_event(mod, {
        "_pipeline_run": True,
        "run_id": "test-run",
        "transforms": _FORCE_TRANSFORMS,
        "_sf_execution_arn": "test-run",
        "_trigger": "manual",
        "_force": True,
    }, {})

    assert result["status"] == "ok"
    assert len(sub_events) == 2, sub_events
    for se in sub_events:
        assert se.get("_force") is True, f"sub_event missing _force: {se}"
        assert se.get("_trigger") == "manual", f"sub_event trigger wrong: {se}"


def test_three_transforms_all_succeed():
    mod = _load_runner()
    transforms = [
        {"node": "a", "language": "sql", "logic_path": "/tmp/a.txt", "inputs": {}, "outputs": {"default": ""}, "parents": []},
        {"node": "b", "language": "sql", "logic_path": "/tmp/b.txt", "inputs": {"a": "..."}, "outputs": {"default": ""}, "parents": ["a"]},
        {"node": "c", "language": "sql", "logic_path": "/tmp/c.txt", "inputs": {"b": "..."}, "outputs": {"default": ""}, "parents": ["b"]},
    ]
    responses = {
        "a": {"status": "ok", "output_rows": 10},
        "b": {"status": "ok", "output_rows": 20},
        "c": {"status": "ok", "output_rows": 30},
    }
    result, events, calls = _drive(mod, transforms, responses)

    assert calls == ["a", "b", "c"], f"unexpected call order {calls}"
    assert result["status"] == "ok"
    assert result["failed_node"] is None
    assert [t["node"] for t in result["transforms"]] == ["a", "b", "c"]
    # output_rows still threads through into the returned aggregate.
    assert [t.get("output_rows") for t in result["transforms"]] == [10, 20, 30]

    # The bundle no longer streams per-node progress on stdout — per-node
    # progress is now written to the _progress file tree by handler() (stubbed
    # out here, so nothing is written). The local warehouse here isn't s3, so
    # the run-lock path emits nothing either: stdout carries no _event lines.
    assert events == [], f"expected no stdout _event lines, got: {events}"


def test_cascade_skip_when_all_parents_skip():
    mod = _load_runner()
    transforms = [
        {"node": "a", "language": "sql", "logic_path": "/tmp/a.txt", "inputs": {}, "outputs": {"default": ""}, "parents": []},
        {"node": "b", "language": "sql", "logic_path": "/tmp/b.txt", "inputs": {"a": "..."}, "outputs": {"default": ""}, "parents": ["a"]},
    ]
    responses = {
        "a": {"status": "skipped", "reason": "no new partitions"},
        # b should never be invoked — cascade-skipped via parents.
    }
    result, events, calls = _drive(mod, transforms, responses)

    assert calls == ["a"], f"b should not be invoked, calls={calls}"
    assert result["status"] == "ok"  # skipped is not a failure
    assert result["transforms"][0]["status"] == "skipped"
    assert result["transforms"][1]["status"] == "skipped"
    assert result["transforms"][1]["note"] == "all upstreams skipped"


def test_failure_stops_pipeline():
    mod = _load_runner()
    transforms = [
        {"node": "a", "language": "sql", "logic_path": "/tmp/a.txt", "inputs": {}, "outputs": {"default": ""}, "parents": []},
        {"node": "b", "language": "sql", "logic_path": "/tmp/b.txt", "inputs": {"a": "..."}, "outputs": {"default": ""}, "parents": ["a"]},
        {"node": "c", "language": "sql", "logic_path": "/tmp/c.txt", "inputs": {"b": "..."}, "outputs": {"default": ""}, "parents": ["b"]},
    ]
    responses = {
        "a": {"status": "ok"},
        "b": RuntimeError("boom"),
        # c should never be invoked.
    }
    result, events, calls = _drive(mod, transforms, responses)

    assert calls == ["a", "b"], f"c should not be invoked, calls={calls}"
    assert result["status"] == "failed"
    assert result["failed_node"] == "b"
    assert len(result["transforms"]) == 2
    assert result["transforms"][1]["status"] == "failed"
    assert "boom" in result["transforms"][1]["error_msg"]
    assert result["transforms"][1]["error_class"] == "RuntimeError"

    # The failure is carried in the returned aggregate, not a stdout _event
    # line (those were removed with the stdout progress channel). handler()
    # writes the failed terminal marker to the _progress tree; it's stubbed
    # here, so stdout stays clean.
    assert events == [], f"expected no stdout _event lines, got: {events}"


def test_partial_skip_then_continue():
    """A node skipping doesn't cascade unless ALL its parents skipped.
    If a downstream node has multiple parents and only some skipped,
    it should still run."""
    mod = _load_runner()
    transforms = [
        {"node": "a", "language": "sql", "logic_path": "/tmp/a.txt", "inputs": {}, "outputs": {"default": ""}, "parents": []},
        {"node": "b", "language": "sql", "logic_path": "/tmp/b.txt", "inputs": {}, "outputs": {"default": ""}, "parents": []},
        # c has TWO parents — a (skipped) and b (ok). Should still run.
        {"node": "c", "language": "sql", "logic_path": "/tmp/c.txt", "inputs": {"a": "...", "b": "..."}, "outputs": {"default": ""}, "parents": ["a", "b"]},
    ]
    responses = {
        "a": {"status": "skipped", "reason": "no input"},
        "b": {"status": "ok", "output_rows": 5},
        "c": {"status": "ok", "output_rows": 1},
    }
    result, events, calls = _drive(mod, transforms, responses)

    assert calls == ["a", "b", "c"]
    assert result["transforms"][0]["status"] == "skipped"
    assert result["transforms"][1]["status"] == "ok"
    assert result["transforms"][2]["status"] == "ok"


def test_failure_reraises_under_lambda():
    """GH #2: under Lambda (AWS_LAMBDA_FUNCTION_NAME set), pipeline_handler
    must raise after building the failed-status payload so the SFN task
    fails — otherwise the cross-pipeline EventBridge rule (filtered on
    detail.status = SUCCEEDED) fires downstream pipelines on a hidden
    failure. Local mode (env unset) still returns the dict so
    `clavesa pipeline run` parses it.
    """
    import os as _os
    mod = _load_runner()
    transforms = [
        {"node": "a", "language": "sql", "logic_path": "/tmp/a.txt", "inputs": {}, "outputs": {"default": ""}, "parents": []},
    ]
    responses = {"a": RuntimeError("boom")}

    prev = _os.environ.get("AWS_LAMBDA_FUNCTION_NAME")
    _os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "clavesa-test-runner"
    try:
        raised = False
        try:
            _drive(mod, transforms, responses)
        except RuntimeError as exc:
            raised = True
            assert "a" in str(exc), f"raised message should name the failed node: {exc}"
            assert "RuntimeError" in str(exc) or "boom" in str(exc), str(exc)
        assert raised, "expected pipeline_handler to raise under Lambda env"
    finally:
        if prev is None:
            _os.environ.pop("AWS_LAMBDA_FUNCTION_NAME", None)
        else:
            _os.environ["AWS_LAMBDA_FUNCTION_NAME"] = prev


def test_reorders_misordered_payload():
    """GH #6 defensive guard: a payload where the consumer arrives BEFORE its
    parent must still execute the parent first. The Go emitter normally
    topo-orders the transforms list; this proves the runner self-corrects if it
    doesn't (e.g. a hand-built or legacy event), so the consumer never reads a
    not-yet-produced sibling table."""
    mod = _load_runner()
    transforms = [
        # consumer "b" listed first, parent "a" second.
        {"node": "b", "language": "sql", "logic_path": "/tmp/b.txt", "inputs": {"a": "..."}, "outputs": {"default": ""}, "parents": ["a"]},
        {"node": "a", "language": "sql", "logic_path": "/tmp/a.txt", "inputs": {}, "outputs": {"default": ""}, "parents": []},
    ]
    responses = {
        "a": {"status": "ok", "output_rows": 1},
        "b": {"status": "ok", "output_rows": 1},
    }
    result, events, calls = _drive(mod, transforms, responses)

    # Parent runs first despite being last in the payload.
    assert calls == ["a", "b"], f"expected parent-first order, got {calls}"
    assert [t["node"] for t in result["transforms"]] == ["a", "b"], result["transforms"]
    assert result["status"] == "ok"


def test_topo_sort_ignores_unknown_parents():
    """Parents naming nodes not present in the bundle are ignored (they resolve
    as already-materialised tables, not ordering constraints) — the sort must
    not drop or reorder the lone node."""
    mod = _load_runner()
    transforms = [{"node": "a", "parents": ["external_not_in_bundle"]}]
    ordered = mod._topo_sort_transforms(transforms)
    assert [t["node"] for t in ordered] == ["a"], ordered


def test_topo_sort_cycle_falls_back_to_input_order():
    """A dependency cycle returns the original order unchanged rather than
    raising, so the runner still attempts execution."""
    mod = _load_runner()
    transforms = [
        {"node": "a", "parents": ["b"]},
        {"node": "b", "parents": ["a"]},
    ]
    ordered = mod._topo_sort_transforms(transforms)
    assert ordered == transforms, ordered


if __name__ == "__main__":
    test_force_and_trigger_from_nested_execution_input()
    test_force_nodes_from_nested_execution_input()
    test_force_from_top_level_still_works()
    test_three_transforms_all_succeed()
    test_cascade_skip_when_all_parents_skip()
    test_failure_stops_pipeline()
    test_partial_skip_then_continue()
    test_failure_reraises_under_lambda()
    test_reorders_misordered_payload()
    test_topo_sort_ignores_unknown_parents()
    test_topo_sort_cycle_falls_back_to_input_order()
    print("PASS")
