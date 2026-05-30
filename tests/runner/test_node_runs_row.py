"""Unit tests for the node_runs row builder in runner/runner.py.

Run with: python3 tests/runner/test_node_runs_row.py
   or:    python3 -m unittest tests/runner/test_node_runs_row.py

The row builder is pure (no Spark, no boto3, no AWS). The Spark write
path (`_record_node_run`, `_node_runs_schema`) is exercised end-to-end
by the docker-gated integration suite — see tests/runner/runner_test.go.
"""

from __future__ import annotations

import datetime as _dt
import importlib.util
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


class _FakeContext:
    """Minimal stand-in for AWS Lambda context — just enough to exercise
    the attribute fallbacks the row builder uses."""

    def __init__(self, memory_limit_in_mb=None, aws_request_id=None):
        if memory_limit_in_mb is not None:
            self.memory_limit_in_mb = memory_limit_in_mb
        if aws_request_id is not None:
            self.aws_request_id = aws_request_id


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_build_row_success_lambda():
    runner = _load_runner()
    env = {
        "CLAVESA_PIPELINE": "my-pipeline",
        "CLAVESA_NODE": "filter_complete",
        "AWS_LAMBDA_FUNCTION_NAME": "my-pipeline-filter_complete",
    }
    ctx = _FakeContext(memory_limit_in_mb=3008, aws_request_id="req-abc-123")

    row = runner._build_node_run_row(
        run_id="run-xyz",
        started_ms=1_700_000_000_000,
        ended_ms=1_700_000_002_500,
        status="ok",
        error_class=None,
        error_msg=None,
        cold_start=True,
        context=ctx,
        sf_execution_arn="arn:aws:states:eu-north-1:1:execution:clavesa-my-pipeline:abc",
        env=env,
    )

    assert row["run_id"] == "run-xyz"
    assert row["pipeline"] == "my-pipeline"
    assert row["node"] == "filter_complete"
    assert row["status"] == "ok"
    assert row["compute_target"] == "lambda"
    assert row["memory_mb"] == 3008
    assert row["cold_start"] is True
    assert row["lambda_request_id"] == "req-abc-123"
    assert (
        row["sf_execution_arn"]
        == "arn:aws:states:eu-north-1:1:execution:clavesa-my-pipeline:abc"
    )
    assert row["error_class"] is None
    assert row["error_msg"] is None
    assert row["duration_ms"] == 2500
    assert isinstance(row["started_at"], _dt.datetime)
    assert row["started_at"].tzinfo is _dt.timezone.utc


def test_build_row_no_sf_execution_arn_defaults_to_empty():
    """Local CLI runs and ad-hoc Lambda invocations don't carry an SFN
    parent — the row builder must not crash and must not emit None
    (Iceberg's StringType is non-nullable in our schema for this field
    in practice, since we always join on it)."""
    runner = _load_runner()
    row = runner._build_node_run_row(
        run_id="r",
        started_ms=1,
        ended_ms=2,
        status="ok",
        error_class=None,
        error_msg=None,
        cold_start=False,
        context=None,
        env={},
    )
    assert row["sf_execution_arn"] == ""


def test_build_row_failed_carries_exception_info():
    runner = _load_runner()
    env = {"CLAVESA_PIPELINE": "p", "CLAVESA_NODE": "n"}

    row = runner._build_node_run_row(
        run_id="r",
        started_ms=1_700_000_000_000,
        ended_ms=1_700_000_000_100,
        status="failed",
        error_class="AnalysisException",
        error_msg="Table or view not found: orders",
        cold_start=False,
        context=None,
        env=env,
    )

    assert row["status"] == "failed"
    assert row["error_class"] == "AnalysisException"
    assert row["error_msg"] == "Table or view not found: orders"
    # No Lambda context → request id is None, memory is None.
    assert row["lambda_request_id"] is None
    assert row["memory_mb"] is None
    # Without AWS_LAMBDA_FUNCTION_NAME set, compute_target is "local".
    assert row["compute_target"] == "local"


def test_build_row_skipped_status():
    runner = _load_runner()
    row = runner._build_node_run_row(
        run_id="r",
        started_ms=1,
        ended_ms=2,
        status="skipped",
        error_class=None,
        error_msg=None,
        cold_start=False,
        context=None,
        env={},
    )
    assert row["status"] == "skipped"
    assert row["pipeline"] == "default"  # falls back when env unset
    assert row["node"] == "node"


def test_build_row_truncates_long_error_msg():
    """Lambda traces can be huge; the row builder caps error_msg so a
    runaway log line doesn't blow up the Iceberg manifest."""
    runner = _load_runner()
    huge = "X" * 10_000

    row = runner._build_node_run_row(
        run_id="r",
        started_ms=1_700_000_000_000,
        ended_ms=1_700_000_000_001,
        status="failed",
        error_class="Exception",
        error_msg=huge,
        cold_start=False,
        context=None,
        env={},
    )

    assert row["error_msg"] is not None
    assert len(row["error_msg"]) <= 4096
    assert row["error_msg"].endswith("...")


def test_build_row_handles_non_int_memory():
    runner = _load_runner()

    class _Ctx:
        memory_limit_in_mb = "not-a-number"
        aws_request_id = ""

    row = runner._build_node_run_row(
        run_id="r",
        started_ms=1,
        ended_ms=2,
        status="ok",
        error_class=None,
        error_msg=None,
        cold_start=False,
        context=_Ctx(),
        env={},
    )

    # Bad memory value falls back to None instead of crashing the write.
    assert row["memory_mb"] is None
    # Empty request_id is treated as None rather than the empty string —
    # easier to filter in Athena.
    assert row["lambda_request_id"] is None


def test_build_row_negative_duration_clamped():
    """Clock skew on container start has been observed to make ended_ms
    appear before started_ms by a millisecond or two. We clamp to zero
    rather than emit a negative duration that breaks downstream maths."""
    runner = _load_runner()
    row = runner._build_node_run_row(
        run_id="r",
        started_ms=1_700_000_000_010,
        ended_ms=1_700_000_000_005,
        status="ok",
        error_class=None,
        error_msg=None,
        cold_start=False,
        context=None,
        env={},
    )
    assert row["duration_ms"] == 0


# ---------------------------------------------------------------------------
# _node_runs_table_id derives the destination table from env vars.
# ---------------------------------------------------------------------------


def test_build_row_carries_triage_envs_when_set():
    """CLAVESA_RUNNER_IMAGE_DIGEST + CLAVESA_MODULE_VERSION are stamped
    onto every row when set. Lambda gets digest from data.aws_ecr_image at
    deploy time; the module version is baked into the image at build time
    via the Dockerfile ARG. Empty when an old runner produces a row — a
    deliberately benign degradation that keeps cross-version joins clean.
    """
    runner = _load_runner()
    row = runner._build_node_run_row(
        run_id="r",
        started_ms=0,
        ended_ms=0,
        status="ok",
        error_class=None,
        error_msg=None,
        cold_start=False,
        context=None,
        env={
            "CLAVESA_PIPELINE": "p",
            "CLAVESA_NODE": "n",
            "CLAVESA_RUNNER_IMAGE_DIGEST": "sha256:deadbeef",
            "CLAVESA_MODULE_VERSION": "v0.13.0",
        },
    )
    assert row["runner_image_digest"] == "sha256:deadbeef"
    assert row["module_version"] == "v0.13.0"


def test_build_row_triage_envs_default_empty():
    runner = _load_runner()
    row = runner._build_node_run_row(
        run_id="r",
        started_ms=0,
        ended_ms=0,
        status="ok",
        error_class=None,
        error_msg=None,
        cold_start=False,
        context=None,
        env={"CLAVESA_PIPELINE": "p", "CLAVESA_NODE": "n"},
    )
    assert row["runner_image_digest"] == ""
    assert row["module_version"] == ""


def test_build_row_output_rows_default_none():
    """Path-mode-only runs and skipped runs leave output_rows null —
    distinguishes "no Iceberg outputs" from "wrote 0 rows" downstream."""
    runner = _load_runner()
    row = runner._build_node_run_row(
        run_id="r",
        started_ms=0,
        ended_ms=0,
        status="ok",
        error_class=None,
        error_msg=None,
        cold_start=False,
        context=None,
        env={"CLAVESA_PIPELINE": "p", "CLAVESA_NODE": "n"},
    )
    assert row["output_rows"] is None


def test_build_row_output_rows_passes_through():
    runner = _load_runner()
    row = runner._build_node_run_row(
        run_id="r",
        started_ms=0,
        ended_ms=0,
        status="ok",
        error_class=None,
        error_msg=None,
        cold_start=False,
        context=None,
        env={"CLAVESA_PIPELINE": "p", "CLAVESA_NODE": "n"},
        output_rows=42,
    )
    assert row["output_rows"] == 42


def test_build_row_resource_metrics_default_none():
    """When peak_rss_mb / spark_metrics aren't passed, every new column is
    present on the row and None — older callers and capture failures leave
    the columns null rather than absent."""
    runner = _load_runner()
    row = runner._build_node_run_row(
        run_id="r",
        started_ms=0,
        ended_ms=0,
        status="ok",
        error_class=None,
        error_msg=None,
        cold_start=False,
        context=None,
        env={"CLAVESA_PIPELINE": "p", "CLAVESA_NODE": "n"},
    )
    assert "peak_rss_mb" in row
    assert row["peak_rss_mb"] is None
    for k in runner._SPARK_METRIC_KEYS:
        assert k in row, f"missing column {k}"
        assert row[k] is None


def test_build_row_resource_metrics_pass_through():
    """peak_rss_mb and each spark_metrics key are splatted onto the row.
    Keys absent from the spark_metrics dict still land as None."""
    runner = _load_runner()
    spark_metrics = {
        "peak_execution_memory_mb": 256,
        "memory_spilled_bytes": 1024,
        "disk_spilled_bytes": 2048,
        "shuffle_read_bytes": 4096,
        "shuffle_write_bytes": 8192,
        "input_bytes": 100,
        "input_records": 10,
        "num_stages": 3,
        "num_tasks": 12,
        "num_failed_tasks": 1,
        "jvm_gc_time_ms": 55,
        "executor_cpu_time_ms": 999,
        "executor_run_time_ms": 1500,
        # max_task_duration_ms deliberately omitted → should land None.
    }
    row = runner._build_node_run_row(
        run_id="r",
        started_ms=0,
        ended_ms=0,
        status="ok",
        error_class=None,
        error_msg=None,
        cold_start=False,
        context=None,
        env={"CLAVESA_PIPELINE": "p", "CLAVESA_NODE": "n"},
        peak_rss_mb=2048,
        spark_metrics=spark_metrics,
    )
    assert row["peak_rss_mb"] == 2048
    assert row["peak_execution_memory_mb"] == 256
    assert row["num_tasks"] == 12
    assert row["num_failed_tasks"] == 1
    assert row["executor_cpu_time_ms"] == 999
    # Omitted key splats to None, not KeyError.
    assert row["max_task_duration_ms"] is None


def test_build_row_spark_metrics_none_is_safe():
    """spark_metrics=None must not crash — every spark key becomes None."""
    runner = _load_runner()
    row = runner._build_node_run_row(
        run_id="r",
        started_ms=0,
        ended_ms=0,
        status="ok",
        error_class=None,
        error_msg=None,
        cold_start=False,
        context=None,
        env={"CLAVESA_PIPELINE": "p", "CLAVESA_NODE": "n"},
        peak_rss_mb=None,
        spark_metrics=None,
    )
    for k in runner._SPARK_METRIC_KEYS:
        assert row[k] is None


def test_glue_db_three_level_encoding():
    """Post-ADR-016 workspace: both CLAVESA_CATALOG and CLAVESA_SCHEMA
    set. The runner emits ``<catalog>__<schema>`` with double-underscore
    boundary. Mirrors internal/identutil.EncodeGlueDatabase on the Go
    side — encoders must stay byte-identical so the catalog handler
    finds what the runner writes."""
    runner = _load_runner()
    import os

    saved = {k: os.environ.get(k) for k in ("CLAVESA_PIPELINE", "CLAVESA_CATALOG", "CLAVESA_SCHEMA")}
    os.environ["CLAVESA_PIPELINE"] = "cloudfront-pipeline"
    os.environ["CLAVESA_CATALOG"] = "clavesa_demo_ws"
    os.environ["CLAVESA_SCHEMA"] = "cloudfront"
    try:
        assert runner._glue_db() == "clavesa_demo_ws__cloudfront"
        # Slice 3: single-default-output drops the ``__default`` suffix.
        assert runner._table_id_for("default", {"default": None}) == "clavesa_demo_ws__cloudfront.node"
        assert runner._table_id_for("default") == "clavesa_demo_ws__cloudfront.node__default"
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def test_v2_layout_local_strips_db_suffix():
    """ADR-019 Slice 4 on-disk layout: ``_ensure_database`` stamps a
    ``LOCATION`` of ``<base>/<catalog>/<schema>`` (no ``.db`` suffix) so
    local warehouses store tables at the V2 namespace shape Slice 5's
    cloud Glue catalog cutover will mirror."""
    runner = _load_runner()
    assert runner._v2_layout_path("/tmp/wh", "clavesa_demo_ws__bronze") == \
        "/tmp/wh/clavesa_demo_ws/bronze"
    assert runner._v2_layout_path("/tmp/wh", "clavesa_demo_ws_system__pipelines") == \
        "/tmp/wh/clavesa_demo_ws_system/pipelines"
    # Cloud (s3://) keeps the legacy ``.db`` suffix — Glue's Hive client
    # expects the DB location at ``<warehouse>/<db_name>.db/`` and
    # Slice 4 doesn't touch the cloud catalog tree.
    assert runner._v2_layout_path("s3://bucket/key", "clavesa_demo_ws__bronze") == \
        "s3://bucket/key/clavesa_demo_ws__bronze.db"
    # Malformed input (no ``__`` boundary) falls back to legacy ``.db``.
    assert runner._v2_layout_path("/tmp/wh", "stray") == "/tmp/wh/stray.db"


def test_glue_db_dashes_sanitized_at_both_sides():
    """Defensive sanitization: a hand-edited pipeline .tf could carry a
    dashed schema. The runner sanitizes on both sides of the `__`
    boundary so the writer doesn't blow up on a perfectly-loadable
    input. Catalog defensively sanitized too even though Init writes
    sanitized identifiers."""
    runner = _load_runner()
    import os

    saved = {k: os.environ.get(k) for k in ("CLAVESA_PIPELINE", "CLAVESA_CATALOG", "CLAVESA_SCHEMA")}
    os.environ["CLAVESA_PIPELINE"] = "x"
    os.environ["CLAVESA_CATALOG"] = "clavesa-demo-ws"
    os.environ["CLAVESA_SCHEMA"] = "marketing-domain"
    try:
        assert runner._glue_db() == "clavesa_demo_ws__marketing_domain"
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


# ---------------------------------------------------------------------------
# Test runner — same pattern as test_incremental.py.
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
