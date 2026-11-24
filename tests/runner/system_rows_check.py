"""Runs inside the runner image (tests/runner/system_rows_test.go).

Checks that runner._system_df (Arrow) writes the same system rows as the
classic spark.createDataFrame(<list>, schema) for node_runs, runs, tables and
column_stats: the same schema with the same nullability, and the same values
after a Delta write and read back, including naive and aware timestamps, the
top_10 ArrayType(struct) column, None in nullable fields, NaN and large
longs. A None in a non-nullable field must fail on both paths. The Arrow path
must start no Python worker. Prints "SYSTEM_ROWS_OK" when all checks pass.
"""

from __future__ import annotations

import datetime as dt
import math
import sys
import types

sys.path.insert(0, "/var/task")
import runner  # noqa: E402

UTC = dt.timezone.utc


def python_workers() -> int:
    import glob  # noqa: PLC0415

    n = 0
    for f in glob.glob("/proc/[0-9]*/comm"):
        try:
            if open(f).read().strip() == "python3":
                n += 1
        except OSError:
            pass
    return n


def norm(v):
    """A comparable form: NaN equals NaN, Rows become dicts."""
    if isinstance(v, float) and math.isnan(v):
        return "NaN"
    if hasattr(v, "asDict"):
        return {k: norm(x) for k, x in v.asDict().items()}
    if isinstance(v, list):
        return [norm(x) for x in v]
    return v


def rows_of(df):
    return sorted((tuple(norm(r[c]) for c in df.columns) for r in df.collect()), key=repr)


def node_runs_rows():
    ctx = types.SimpleNamespace(aws_request_id="req-1", memory_limit_in_mb=3008,
                                function_name="clavesa-gold-runner", log_group_name="/aws/lambda/x",
                                log_stream_name="2026/11/24/[$LATEST]abc")
    env = {"CLAVESA_PIPELINE": "gold", "CLAVESA_NODE": "fact_events", "AWS_LAMBDA_FUNCTION_NAME": "clavesa-gold-runner",
           "AWS_LAMBDA_FUNCTION_MEMORY_SIZE": "3008", "CLAVESA_MODULE_VERSION": "v0.0.0"}
    ok = runner._build_node_run_row(
        run_id="run-1", started_ms=1_795_000_000_123, ended_ms=1_795_000_042_456, status="ok",
        error_class=None, error_msg=None, cold_start=True, context=ctx,
        sf_execution_arn="arn:aws:states:eu-north-1:1:execution:clavesa-gold:abc", env=env,
        output_rows=2**40, peak_rss_mb=2321,
        jvm_heap={"heap_old_after_gc_mb": 363, "heap_committed_mb": 1024, "heap_max_mb": 1024})
    failed = runner._build_node_run_row(
        run_id="run-2", started_ms=1_795_000_100_000, ended_ms=1_795_000_100_001, status="failed",
        error_class="AnalysisException", error_msg="Täällä ü — [TABLE_OR_VIEW_NOT_FOUND]", cold_start=False,
        context=None, env=env)
    return [ok, failed]


def runs_rows():
    ok = runner._runs_writer_build_row({"executionArn": "arn:aws:states:eu-north-1:1:execution:clavesa-gold:r1",
                                        "status": "SUCCEEDED", "startDate": 1_795_000_000_000,
                                        "stopDate": 1_795_000_358_090})
    failed = runner._runs_writer_build_row({"executionArn": "arn:aws:states:eu-north-1:1:execution:clavesa-gold:r2",
                                            "status": "FAILED", "startDate": 1_795_000_000_000, "stopDate": None,
                                            "error": "States.TaskFailed", "cause": '{"errorMessage": "boom"}'})
    return [ok, failed]


def tables_rows():
    return [
        {"pipeline": "gold", "node": "fact_events", "output_key": "default", "table_name": "fact_events",
         "table_id": "clavesa_web_traffic__gold.fact_events", "snapshot_id": 284,
         # naive, as a Spark collect of Delta history returns it
         "snapshot_ts": dt.datetime(2026, 11, 23, 6, 13, 46, 203283), "row_count": 6669,
         "file_count": 12, "total_bytes": 2**33, "last_writer_run_id": "run-1"},
        {"pipeline": "gold", "node": "dim_session", "output_key": "default", "table_name": "dim_session",
         "table_id": "clavesa_web_traffic__gold.dim_session", "snapshot_id": None, "snapshot_ts": None,
         "row_count": None, "file_count": None, "total_bytes": None, "last_writer_run_id": None},
    ]


def column_stats_rows():
    base = {"run_id": "run-1", "pipeline": "gold", "node": "fact_events", "output_key": "default",
            "table_identifier": "clavesa_web_traffic__gold.fact_events", "snapshot_id": 284,
            "snapshot_ts": dt.datetime(2026, 11, 23, 6, 13, 46, 203283),
            "computed_at": dt.datetime(2026, 11, 24, 6, 19, 0, 123456, tzinfo=UTC)}
    return [
        dict(base, column_name="website_domain", column_type="string", row_count=6669, null_count=0, null_pct=0.0,
             approx_count_distinct=3, min_value="a.fi", max_value="ö.fi", approx_p50=None, approx_p95=None,
             top_10=[{"value": "heineli.fi", "count": 6000}, {"value": None, "count": 2}, {"value": "ü", "count": 1}]),
        dict(base, column_name="ttfb", column_type="double", row_count=6669, null_count=12, null_pct=12 / 6669,
             approx_count_distinct=4000, min_value="0.001", max_value="9.5", approx_p50=float("nan"),
             approx_p95=0.731, top_10=[]),
        dict(base, column_name="x_edge_request_id", column_type="string", row_count=6669, null_count=None,
             null_pct=None, approx_count_distinct=None, min_value=None, max_value=None, approx_p50=None,
             approx_p95=None, top_10=None, snapshot_id=None, snapshot_ts=None),
    ]


CASES = [
    ("node_runs", runner._node_runs_schema, node_runs_rows),
    ("runs", runner._runs_schema, runs_rows),
    ("tables", runner._tables_schema, tables_rows),
    ("column_stats", runner._column_stats_schema, column_stats_rows),
]


def main() -> None:
    spark = runner._spark()
    base_workers = python_workers()
    failures = []

    # Arrow first, so a worker count above the start means the Arrow path made one.
    arrow = {}
    for name, schema_fn, rows_fn in CASES:
        df = runner._system_df(spark, rows_fn(), schema_fn())
        df.write.format("delta").mode("overwrite").save(f"/tmp/sysrows/{name}_arrow")
        arrow[name] = df
    if python_workers() != base_workers:
        failures.append(f"arrow path started python workers: {python_workers()} vs {base_workers}")

    for name, schema_fn, rows_fn in CASES:
        schema = schema_fn()
        classic = spark.createDataFrame(rows_fn(), schema=schema)
        classic.write.format("delta").mode("overwrite").save(f"/tmp/sysrows/{name}_classic")
        if classic.schema != arrow[name].schema:
            failures.append(f"{name}: in-memory schema differs\n  classic {classic.schema}\n  arrow   {arrow[name].schema}")
        a = spark.read.format("delta").load(f"/tmp/sysrows/{name}_arrow")
        c = spark.read.format("delta").load(f"/tmp/sysrows/{name}_classic")
        if a.schema != c.schema:
            failures.append(f"{name}: written schema differs\n  classic {c.schema}\n  arrow   {a.schema}")
        if rows_of(a) != rows_of(c):
            failures.append(f"{name}: written rows differ\n  classic {rows_of(c)}\n  arrow   {rows_of(a)}")
        if rows_of(arrow[name]) != rows_of(classic):
            failures.append(f"{name}: in-memory rows differ")

        # A None in each non-nullable field fails on both paths.
        for f in schema.fields:
            if f.nullable:
                continue
            bad = [dict(rows_fn()[0], **{f.name: None})]
            for label, build in (("classic", lambda: spark.createDataFrame(bad, schema=schema).collect()),
                                 ("arrow", lambda: runner._system_df(spark, bad, schema).collect())):
                try:
                    build()
                    failures.append(f"{name}.{f.name}: {label} accepted None in a non-nullable field")
                except Exception:  # noqa: BLE001
                    pass

    if failures:
        print("SYSTEM_ROWS_FAILED")
        for f in failures:
            print(f)
        sys.exit(1)
    print("SYSTEM_ROWS_OK", ", ".join(f"{n}={arrow[n].count()}" for n, _, _ in CASES))


if __name__ == "__main__":
    main()
