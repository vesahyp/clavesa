"""Clavesa runs-writer Lambda.

Receives Step Functions execution status-change events from EventBridge and
appends one row per terminal execution to clavesa_<pipeline>.runs (Iceberg
via Athena). Pairs with the runner's per-invocation node_runs writes:

    runs       — one row per Step Functions execution (this Lambda)
    node_runs  — one row per Lambda invocation (the runner itself)

Joining on sf_execution_arn answers "which nodes ran in this execution?"
once the runner threads the ARN through (separate slice).

Bootstrapping. CREATE TABLE IF NOT EXISTS runs every invocation. Athena
DDL is cheap (no bytes scanned), idempotent, and avoids needing a separate
deployment-time migration step. First event after deploy pays ~3s extra
to create the table; afterwards it's a no-op.

Implementation. boto3-only — no PyIceberg, no PySpark. Athena's
INSERT INTO Iceberg works against the same Glue catalog the runner writes
into; reusing it keeps the writer image-less (zip Lambda).
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import time
from typing import Any

import boto3

athena = boto3.client("athena")

# All env vars are required — Terraform sets them from the orchestration
# module's variables. Missing means a misconfigured deployment, fail fast.
PIPELINE = os.environ["CLAVESA_PIPELINE"]
# CLAVESA_DATABASE points at the workspace's system observability DB
# (`<system_catalog>__pipelines`) as of v0.20.0 — multi-writer, every
# pipeline's runs_writer Lambda appends to the same shared `runs` table
# and the `pipeline` column carries the per-pipeline filter.
DATABASE = os.environ["CLAVESA_DATABASE"]
WAREHOUSE_BUCKET = os.environ["CLAVESA_WAREHOUSE_BUCKET"]
WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")
# Per-pipeline Athena results dir is fine (just query result dumps), but
# the table LOCATION must be workspace-wide so writers from any pipeline
# converge on the same Iceberg data path — whichever pipeline runs first
# can't be allowed to pin the table to its prefix.
ATHENA_OUTPUT = f"s3://{WAREHOUSE_BUCKET}/{PIPELINE}/_athena-results/"
RUNS_LOCATION = f"s3://{WAREHOUSE_BUCKET}/_system/pipelines/runs/"

TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"}

# Allowed values for runs.trigger. Each start path stamps one of these into
# the SFN execution input under the `_trigger` key; runs_writer reads it
# back via _extract_trigger and writes it to the runs table. Keep this set
# in sync with:
#   - main.tf : aws_cloudwatch_event_target.schedule       → "scheduled"
#   - poller.py : sfn.start_execution(..., input=…)         → "event"
#   - manual / CLI / console runs                           → "manual" (default fallback)
# Any new start path must pick one of these or extend the set in one place.
TRIGGER_VALUES = frozenset({"manual", "scheduled", "event", "backfill", "backfill-direct"})

# Cap how long we'll wait for an Athena DDL/DML query — both should complete
# in under 5 seconds; anything longer is a sign of throttling or a service
# issue the Lambda should surface as a failure.
ATHENA_POLL_INTERVAL_S = 0.5
ATHENA_MAX_ATTEMPTS = 60


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    detail = event.get("detail") or {}
    status = detail.get("status")
    if status not in TERMINAL_STATUSES:
        # RUNNING events fire too — ignore. Only terminal states get a row;
        # the "currently running" view comes from SFN ListExecutions.
        return {"skipped": f"non-terminal status: {status!r}"}

    row = _build_row(detail)
    _ensure_table()
    _insert_row(row)
    return {"ok": True, "run_id": row["run_id"], "status": status}


def _build_row(detail: dict[str, Any]) -> dict[str, Any]:
    sf_execution_arn = str(detail.get("executionArn") or "")
    # Run id is the trailing segment of the execution ARN — already unique per
    # execution and matches what the SFN console displays.
    run_id = sf_execution_arn.rsplit(":", 1)[-1] if sf_execution_arn else ""

    started_ms = detail.get("startDate")
    stopped_ms = detail.get("stopDate")
    duration_ms: int | None = None
    if isinstance(started_ms, int) and isinstance(stopped_ms, int):
        duration_ms = max(stopped_ms - started_ms, 0)

    failed_step = ""
    error_class = ""
    error_msg = ""
    status = str(detail.get("status") or "")
    if status != "SUCCEEDED":
        error_class = _truncate(str(detail.get("error") or ""))
        error_msg, failed_step = _parse_cause(str(detail.get("cause") or ""))

    return {
        "run_id": run_id,
        "pipeline": PIPELINE,
        "sf_execution_arn": sf_execution_arn,
        "status": status,
        "trigger": _extract_trigger(detail.get("input")),
        "target_table": _extract_target_table(detail.get("input")),
        "started_at": _format_athena_ts(started_ms),
        "ended_at": _format_athena_ts(stopped_ms),
        "duration_ms": duration_ms,
        "failed_step": failed_step,
        "error_class": error_class,
        "error_msg": error_msg,
    }


def _extract_trigger(raw_input: Any) -> str:
    """Read `_trigger` from the SFN execution input. The orchestration emitter
    smuggles a value into `_trigger` from each known start-execution path
    (scheduled via EventBridge target, event via the SQS poller); manual runs
    via console / CLI / `clavesa pipeline run-cloud` either set it
    explicitly or leave it absent — we default the latter to "manual" so the
    column never reads as a missing-data NULL.

    SFN's EventBridge payload presents `input` as a JSON-encoded string. Old
    runs from before this slice (or runs with malformed input) gracefully
    fall back to "manual".
    """
    if not raw_input:
        return "manual"
    try:
        parsed = json.loads(raw_input) if isinstance(raw_input, str) else raw_input
    except (ValueError, TypeError):
        return "manual"
    if not isinstance(parsed, dict):
        return "manual"
    trig = parsed.get("_trigger")
    if not isinstance(trig, str) or not trig.strip():
        return "manual"
    value = trig.strip()
    if value not in TRIGGER_VALUES:
        # Unknown value — record it but don't pollute the column with arbitrary
        # strings. Callers introducing a new trigger should update TRIGGER_VALUES.
        return "manual"
    return value


def _extract_target_table(raw_input: Any) -> str | None:
    """Pick the staging table id out of `_backfill.target_outputs` if present.
    Backfill runs route each output to a parallel `<target>__backfill__<run_id>`
    table; we record the staging id on the runs row so the UI/CLI can find it
    later. v1: single-output backfills are the only shape, so we report the
    first staging table when there are multiple."""
    if not raw_input:
        return None
    try:
        parsed = json.loads(raw_input) if isinstance(raw_input, str) else raw_input
    except (ValueError, TypeError):
        return None
    if not isinstance(parsed, dict):
        return None
    bf = parsed.get("_backfill")
    if not isinstance(bf, dict):
        return None
    targets = bf.get("target_outputs")
    if isinstance(targets, dict) and targets:
        # Deterministic across keys so re-emits don't churn the column.
        for k in sorted(targets):
            v = targets[k]
            if isinstance(v, str) and v:
                return v
    return None


def _format_athena_ts(ms: Any) -> str | None:
    """Format epoch ms as Athena TIMESTAMP literal (millisecond precision).

    Treats 0/None/non-int as "missing" — SFN never emits epoch 0 timestamps,
    so a falsy int is far more likely a placeholder than a real value.
    """
    if not isinstance(ms, int) or ms <= 0:
        return None
    return (
        _dt.datetime.fromtimestamp(ms / 1000, tz=_dt.timezone.utc)
        .strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    )


def _parse_cause(cause: str) -> tuple[str, str]:
    """Best-effort extraction of (error_msg, failed_step) from an SFN cause.

    SFN's cause for Lambda task failures is a JSON object with errorMessage,
    errorType, and a trace; for state-machine-level failures it can be plain
    text. We don't have access to the failed step name from the EventBridge
    payload itself — it's discoverable via GetExecutionHistory but the extra
    SFN call adds latency and cost; leave failed_step empty for now and let
    callers query history when they need it.
    """
    if not cause:
        return "", ""
    try:
        parsed = json.loads(cause)
        if isinstance(parsed, dict):
            msg = parsed.get("errorMessage") or parsed.get("Cause") or cause
            return _truncate(str(msg)), ""
    except (ValueError, TypeError):
        pass
    return _truncate(cause), ""


def _truncate(s: str, limit: int = 4096) -> str:
    if len(s) <= limit:
        return s
    return s[: limit - 3] + "..."


# ---------------------------------------------------------------------------
# Athena
# ---------------------------------------------------------------------------


def _athena_run(sql: str) -> None:
    resp = athena.start_query_execution(
        QueryString=sql,
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        WorkGroup=WORKGROUP,
    )
    qid = resp["QueryExecutionId"]
    for _ in range(ATHENA_MAX_ATTEMPTS):
        info = athena.get_query_execution(QueryExecutionId=qid)
        status = info["QueryExecution"]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            return
        if state in ("FAILED", "CANCELLED"):
            reason = status.get("StateChangeReason", "")
            raise RuntimeError(
                f"Athena query {qid} {state}: {reason} (sql: {sql[:200]})"
            )
        time.sleep(ATHENA_POLL_INTERVAL_S)
    raise RuntimeError(f"Athena query {qid} timed out (sql: {sql[:200]})")


def _ensure_table() -> None:
    """Idempotent Iceberg table creation. CREATE TABLE IF NOT EXISTS is a
    Glue UpdateTable on the second call — cheap, no scan, no commit."""
    sql = (
        f"CREATE TABLE IF NOT EXISTS {DATABASE}.runs (\n"
        "  run_id           string,\n"
        "  pipeline         string,\n"
        "  sf_execution_arn string,\n"
        "  status           string,\n"
        "  trigger          string,\n"
        "  target_table     string,\n"
        "  started_at       timestamp,\n"
        "  ended_at         timestamp,\n"
        "  duration_ms      bigint,\n"
        "  failed_step      string,\n"
        "  error_class      string,\n"
        "  error_msg        string\n"
        ")\n"
        f"LOCATION '{RUNS_LOCATION}'\n"
        "TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet')"
    )
    _athena_run(sql)
    # Pre-v0.21 tables were created without target_table — widen the
    # column set in place. ADD COLUMN is metadata-only on Iceberg (no
    # data rewrite). Re-running on a table that already has the column
    # is a no-op error we swallow so the writer stays idempotent.
    try:
        _athena_run(
            f"ALTER TABLE {DATABASE}.runs ADD COLUMNS (target_table string)"
        )
    except RuntimeError as e:
        msg = str(e).lower()
        if "already exists" not in msg and "duplicate" not in msg:
            raise


# Order must match the column order in _ensure_table.
COLUMNS = (
    "run_id", "pipeline", "sf_execution_arn", "status", "trigger",
    "target_table", "started_at", "ended_at", "duration_ms", "failed_step",
    "error_class", "error_msg",
)
_TIMESTAMP_COLUMNS = {"started_at", "ended_at"}
_BIGINT_COLUMNS = {"duration_ms"}


def _insert_row(row: dict[str, Any]) -> None:
    parts = [_render_value(c, row.get(c)) for c in COLUMNS]
    cols_csv = ", ".join(COLUMNS)
    vals_csv = ", ".join(parts)
    sql = f"INSERT INTO {DATABASE}.runs ({cols_csv}) VALUES ({vals_csv})"
    _athena_run(sql)


def _render_value(col: str, val: Any) -> str:
    if val is None:
        return "NULL"
    if col in _BIGINT_COLUMNS:
        return str(int(val))
    if col in _TIMESTAMP_COLUMNS:
        # Already formatted as 'YYYY-MM-DD HH:MM:SS.fff' by _format_athena_ts.
        return f"TIMESTAMP '{val}'"
    # String column. Single-quote escape per SQL standard.
    s = str(val).replace("'", "''")
    return f"'{s}'"
