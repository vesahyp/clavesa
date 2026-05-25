"""Clavesa notebook REPL subprocess body.

One REPL = one Python process = one Spark Connect session_id. The supervisor
(`run_query_server` in `runner.py`) spawns this script per open notebook and
talks to it over stdin/stdout as JSON-line messages. Notebook globals live in
this process's `_NS` dict; they persist across cells the way a Jupyter kernel's
namespace does.

This module is intentionally self-contained — no import from `runner.py` — so
it can be launched with `python /var/task/notebook_repl.py <session_id>` and
nothing else. The supervisor passes the session_id (UUID) as the only CLI
argument; everything else is read from env.

Wire protocol: line-delimited JSON in both directions. Each request from the
supervisor is one JSON object on a single line of stdin:

    {"op": "cell",   "cell_run_id": "<uuid>", "language": "python", "source": "..."}
    {"op": "cancel", "cell_run_id": "<uuid>"}
    {"op": "ping"}
    {"op": "stop"}

Each response is one JSON object on a single line of stdout:

    {"ok": true, "result": {...CellResult...}}
    {"ok": true, "result": {"pong": true}}
    {"ok": false, "error": "..."}

CellResult mirrors the nbformat outputs[] entries the Go service layer will
unpack into the .ipynb:

    {
      "status": "ok" | "error" | "cancelled",
      "duration_ms": <int>,
      "stdout": "<str>",
      "stderr": "<str>",
      "display": {                            # only on status=ok
        "type":     "table" | "text" | "none",
        "columns":  [...],                    # for table
        "column_types": [...],                # for table
        "rows":     [[...]],                  # for table, max 1000 rows
        "truncated": <bool>,
        "text_repr": "<str>"                  # always present (fallback)
      },
      "error": {                              # only on status=error
        "ename":     "...",
        "evalue":    "...",
        "traceback": [...]
      }
    }

Cancellation lifecycle (Slice 1 limitation): when the supervisor sends a
`cancel` op, this process sets a flag the cell-executing thread checks
between statements AND calls `session.interruptTag(cell_run_id)` on the
Spark Connect session to abort any in-flight Spark job. Pure-Python tight
loops without Spark calls (`while True: pass`) aren't preemptable from
another Python thread without signals — those need the "Stop session"
escape hatch which kills this whole subprocess.
"""

from __future__ import annotations

import ast
import contextlib
import io
import json
import os
import sys
import threading
import time
import traceback
import uuid
from typing import Any

# Cap on output payloads so a runaway `df.show(10**6)` can't blow the .ipynb
# up to gigabytes. The Spark DataFrame path applies a separate row limit
# before collecting, so this only bites stdout/stderr.
_STREAM_CAP_BYTES = 256 * 1024
_DATAFRAME_ROW_CAP = 1000


_NS: dict[str, Any] = {}             # the REPL's persistent globals dict
_CANCELLED: set[str] = set()         # cell_run_ids the supervisor asked to cancel
_CANCEL_LOCK = threading.Lock()      # protects _CANCELLED across the cell + cancel threads


def _build_connect_session(session_id: str):
    """Connect to the warm container's Spark Connect plugin with our session_id.

    Same as runner._connect_session but parameterized — each notebook gets its
    own session_id (UUID5 of the notebook id) so temp views are per-notebook
    and `spark.stop()` in one notebook doesn't tear down catalog or other
    notebooks.
    """
    from pyspark.sql.connect.session import SparkSession  # noqa: PLC0415

    port = os.environ.get("CLAVESA_CONNECT_PORT", "15002")
    return (
        SparkSession.builder
        .remote(f"sc://localhost:{port}/;session_id={session_id}")
        .getOrCreate()
    )


_STDOUT_LOCK = threading.Lock()


def _emit(payload: dict) -> None:
    """Write one JSON-line response to the supervisor's stdout pipe.

    Uses os.write(1, …) so the cell worker thread's ``contextlib.redirect_stdout``
    (which swaps ``sys.stdout`` for the cell's StringIO buffer) can't capture
    a /cancel ack emitted from the main thread mid-cell. Without this the
    cancel ack JSON line ends up inside the cell's stdout output, the
    supervisor times out waiting for the tagged response, and the cell's
    captured stdout has a stray JSON blob in it.

    Lock guards interleaving across threads — file-descriptor writes that
    span buffer boundaries can interleave bytes from concurrent emitters.
    """
    line = (json.dumps(payload, default=str) + "\n").encode("utf-8")
    with _STDOUT_LOCK:
        os.write(1, line)


def _cap(s: str) -> tuple[str, bool]:
    """Truncate s to _STREAM_CAP_BYTES; return (capped, was_truncated)."""
    if len(s) <= _STREAM_CAP_BYTES:
        return s, False
    return s[:_STREAM_CAP_BYTES] + "\n…[truncated]…\n", True


def _parse_magic(source: str) -> tuple[str, str]:
    """Return (language, body) by inspecting the cell's first non-blank line.

    %%sql    → ("sql",    rest of source)
    %%python → ("python", rest of source)
    (no magic) → ("python", source)

    Unknown %%foo magics raise ValueError so the cell errors cleanly.
    """
    stripped = source.lstrip("\n\r ")
    if not stripped.startswith("%%"):
        return "python", source
    first_line, _, rest = stripped.partition("\n")
    magic = first_line[2:].strip()
    if magic == "sql":
        return "sql", rest
    if magic == "python":
        return "python", rest
    raise ValueError(f"unknown cell magic %%{magic} (allowed: %%sql, %%python)")


def _ast_rewrite_last_expr(source: str) -> tuple[ast.Module, bool]:
    """If the cell's last statement is an expression and the source doesn't
    end with `;`, rewrite to `__clv_last = <expr>` so we can introspect it.

    Returns (compiled_tree, has_display) — has_display is True when we did
    the rewrite (so the caller knows to fetch __clv_last after exec).
    """
    tree = ast.parse(source, mode="exec")
    if not tree.body:
        return tree, False
    last = tree.body[-1]
    if not isinstance(last, ast.Expr):
        return tree, False
    if source.rstrip().endswith(";"):
        return tree, False
    # Replace with `__clv_last = <expr>` preserving line/col so tracebacks
    # point at the user's code, not our rewrite.
    assign = ast.Assign(
        targets=[ast.Name(id="__clv_last", ctx=ast.Store())],
        value=last.value,
    )
    ast.copy_location(assign, last)
    ast.fix_missing_locations(assign)
    tree.body[-1] = assign
    return tree, True


def _render_value(spark, value: Any) -> dict:
    """Turn the cell's last-expression value into a CellResult `display` dict.

    The supervisor receives this back and the Go service translates it to a
    nbformat `execute_result` output bundle.
    """
    if value is None:
        return {"type": "none", "text_repr": ""}

    # Spark Connect DataFrame — detected by duck-typing because the class
    # hierarchy differs between Connect and classic PySpark.
    if hasattr(value, "toPandas") and hasattr(value, "columns") and hasattr(value, "schema"):
        truncated = False
        try:
            row_count = value.count()
        except Exception:  # noqa: BLE001
            row_count = None
        # Limit BEFORE collect so big DataFrames don't OOM the REPL.
        if row_count is None or row_count > _DATAFRAME_ROW_CAP:
            truncated = True
            df = value.limit(_DATAFRAME_ROW_CAP)
        else:
            df = value
        columns = list(df.columns)
        column_types = [f.dataType.simpleString() for f in df.schema.fields]
        pdf = df.toPandas()
        records = json.loads(pdf.to_json(orient="records", date_format="iso"))
        rows = [[r.get(c) for c in columns] for r in records]
        text_repr = pdf.to_string(index=False)
        text_repr, _ = _cap(text_repr)
        return {
            "type": "table",
            "columns": columns,
            "column_types": column_types,
            "rows": rows,
            "truncated": truncated,
            "text_repr": text_repr,
        }

    # Pandas DataFrame — same shape, no Spark roundtrip.
    try:
        import pandas as pd  # noqa: PLC0415
        if isinstance(value, pd.DataFrame):
            truncated = len(value) > _DATAFRAME_ROW_CAP
            df = value.head(_DATAFRAME_ROW_CAP) if truncated else value
            columns = [str(c) for c in df.columns]
            column_types = [str(t) for t in df.dtypes]
            records = json.loads(df.to_json(orient="records", date_format="iso"))
            rows = [[r.get(c) for c in columns] for r in records]
            text_repr, _ = _cap(df.to_string(index=False))
            return {
                "type": "table",
                "columns": columns,
                "column_types": column_types,
                "rows": rows,
                "truncated": truncated,
                "text_repr": text_repr,
            }
    except ImportError:
        pass

    # Everything else: repr() it. Capped so a giant nested dict doesn't
    # flood the notebook file.
    text_repr, _ = _cap(repr(value))
    return {"type": "text", "text_repr": text_repr}


class _Cancelled(BaseException):
    """Raised in the cell-executing thread when the cancel flag flips.

    Inherits BaseException (not Exception) so user `except Exception:` clauses
    don't swallow the cancellation by accident. Mirrors how Jupyter handles
    KeyboardInterrupt.
    """


def _check_cancel(cell_run_id: str) -> None:
    """Raise _Cancelled if the supervisor asked us to cancel this cell."""
    with _CANCEL_LOCK:
        if cell_run_id in _CANCELLED:
            _CANCELLED.discard(cell_run_id)
            raise _Cancelled()


def _run_cell(spark, cell_run_id: str, language_hint: str, source: str) -> dict:
    """Execute one cell, return a CellResult dict."""
    start = time.monotonic()
    stdout_buf = io.StringIO()
    stderr_buf = io.StringIO()

    # Tag every Spark op this cell launches so /cancel can interrupt them.
    try:
        spark.addTag(cell_run_id)
    except Exception:  # noqa: BLE001
        # Older Connect clients don't have addTag; cancellation falls back
        # to the Python-side _CANCELLED flag only.
        pass

    def _finish_ok(display: dict) -> dict:
        return {
            "status": "ok",
            "duration_ms": int((time.monotonic() - start) * 1000),
            "stdout": _cap(stdout_buf.getvalue())[0],
            "stderr": _cap(stderr_buf.getvalue())[0],
            "display": display,
        }

    def _finish_error(exc: BaseException) -> dict:
        ename = type(exc).__name__
        evalue = str(exc)
        tb = traceback.format_exception(type(exc), exc, exc.__traceback__)
        return {
            "status": "error",
            "duration_ms": int((time.monotonic() - start) * 1000),
            "stdout": _cap(stdout_buf.getvalue())[0],
            "stderr": _cap(stderr_buf.getvalue())[0],
            "error": {"ename": ename, "evalue": evalue, "traceback": tb},
        }

    def _finish_cancelled() -> dict:
        return {
            "status": "cancelled",
            "duration_ms": int((time.monotonic() - start) * 1000),
            "stdout": _cap(stdout_buf.getvalue())[0],
            "stderr": _cap(stderr_buf.getvalue())[0],
        }

    try:
        with contextlib.redirect_stdout(stdout_buf), contextlib.redirect_stderr(stderr_buf):
            language, body = _parse_magic(source)
            # Honor an explicit language_hint if the supervisor disagrees with
            # our parse — gives the UI an out for "show this as SQL even
            # without %%sql" if we ever add that affordance. Today the hint
            # is just informational.
            _ = language_hint  # noqa: F841 — reserved

            if language == "sql":
                _check_cancel(cell_run_id)
                df = spark.sql(body)
                display = _render_value(spark, df)
                return _finish_ok(display)

            # Python path.
            _check_cancel(cell_run_id)
            tree, has_display = _ast_rewrite_last_expr(body)
            code = compile(tree, "<cell>", "exec")
            # Make `spark` available in the cell namespace, like Jupyter
            # makes `__name__` available — but don't clobber a user-defined
            # one (`spark = something` in cell 1 stays sticky).
            _NS.setdefault("spark", spark)
            exec(code, _NS)  # noqa: S102 — cell execution is the whole point
            _check_cancel(cell_run_id)
            if has_display:
                value = _NS.pop("__clv_last", None)
                display = _render_value(spark, value)
            else:
                display = {"type": "none", "text_repr": ""}
            return _finish_ok(display)

    except _Cancelled:
        return _finish_cancelled()
    except BaseException as exc:  # noqa: BLE001 — we want literally any error
        return _finish_error(exc)


def _run_cell_in_thread(spark, cell_run_id: str, language: str, source: str) -> None:
    """Worker body: run the cell, emit one tagged response, never propagate.

    Tagged with `for: "cell:<cell_run_id>"` so the supervisor's stdout
    dispatcher can match the response to the originating /cell request even
    when a /cancel response gets emitted between op submission and cell
    completion.
    """
    try:
        result = _run_cell(spark, cell_run_id, language, source)
        _emit({"ok": True, "for": f"cell:{cell_run_id}", "result": result})
    except BaseException as exc:  # noqa: BLE001 — protocol-level failures shouldn't kill the REPL
        _emit({"ok": False, "for": f"cell:{cell_run_id}", "error": f"runner error: {exc}"})


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: notebook_repl.py <session_id>", file=sys.stderr)
        return 2
    session_id = sys.argv[1]
    try:
        uuid.UUID(session_id)
    except ValueError:
        print(f"session_id {session_id!r} must be a valid UUID", file=sys.stderr)
        return 2

    spark = _build_connect_session(session_id)
    # Round-trip a trivial query to fail fast if the Connect server isn't up.
    spark.sql("SELECT 1").collect()
    _emit({"ok": True, "for": "ready", "result": {"ready": True, "session_id": session_id}})

    # Main loop dispatches ops. Cell ops launch a worker thread and return
    # immediately so the loop can process a subsequent /cancel while the
    # cell runs. Cancel ops execute inline (set the Python-side flag, fire
    # spark.interruptTag) so they take effect even mid-cell.
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except json.JSONDecodeError as exc:
            _emit({"ok": False, "for": "parse", "error": f"bad json: {exc}"})
            continue

        op = req.get("op")
        if op == "ping":
            _emit({"ok": True, "for": "ping", "result": {"pong": True}})
        elif op == "stop":
            _emit({"ok": True, "for": "stop", "result": {"stopping": True}})
            return 0
        elif op == "cancel":
            cell_run_id = req.get("cell_run_id", "")
            note = ""
            if cell_run_id:
                with _CANCEL_LOCK:
                    _CANCELLED.add(cell_run_id)
                try:
                    spark.interruptTag(cell_run_id)
                except Exception as exc:  # noqa: BLE001
                    note = str(exc)
            _emit({
                "ok": True,
                "for": f"cancel:{cell_run_id}",
                "result": {"cancelled": True, "note": note},
            })
        elif op == "cell":
            cell_run_id = req.get("cell_run_id") or str(uuid.uuid4())
            language = req.get("language", "python")
            source = req.get("source", "")
            threading.Thread(
                target=_run_cell_in_thread,
                args=(spark, cell_run_id, language, source),
                daemon=True,
            ).start()
        else:
            _emit({"ok": False, "for": "unknown", "error": f"unknown op: {op!r}"})

    return 0


if __name__ == "__main__":
    sys.exit(main())
