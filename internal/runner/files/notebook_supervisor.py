"""Clavesa notebook supervisor — REPL subprocess pool inside the warm worker.

Slice 1 of the notebooks feature. Runs inside the same Python process as the
CLAVESA_QUERY_SERVER mode (extends its HTTP handler with /repl/* routes), so
the warm container hosts: (1) the Spark Connect plugin in its JVM, (2) the
catalog query path via a Connect client, (3) this supervisor which spawns
one notebook_repl.py subprocess per open notebook and proxies cell ops to it.

Each REPL is a Python process started with `python notebook_repl.py
<session_id>`, talking back to us via JSON-line stdin/stdout. The supervisor:

  * holds a stdin write lock per REPL (cells + cancels serialize against each
    other on the way IN to the REPL — the REPL itself processes cancels even
    while a cell runs because cells go to a daemon thread there)
  * runs one stdout-reader thread per REPL that dispatches each JSON line to
    the request waiting for that `for: "<tag>"` correlation
  * blocks the HTTP handler thread on a per-request threading.Event until the
    matching response arrives (the supervisor's caller is the Go service
    layer, which already runs each cell/cancel HTTP request on its own
    goroutine — blocking inside one of those goroutines is fine)

HTTP shape (mounted under whatever HTTP server runner.py's run_query_server
runs):

    POST /repl/spawn      body: {"notebook_id": "<filename-safe>"}
                          → 200 {"repl_id": "<uuid>", "session_id": "<uuid>"}

    POST /repl/<rid>/cell body: {"cell_run_id": "<uuid>", "language": "sql"|"python",
                                "source": "..."}
                          → 200 {<CellResult, see notebook_repl.py>}

    POST /repl/<rid>/cancel body: {"cell_run_id": "<uuid>"}
                            → 200 {"cancelled": true}

    DELETE /repl/<rid>    → 200 {"stopped": true}

    GET /repls            → 200 {"repls": [{"repl_id", "notebook_id",
                                           "session_id", "age_ms", "rss_mb"}]}

Eviction is supervisor-driven: an idle reaper goroutine on the GO side calls
DELETE /repl/<id> after the timeout. Python doesn't do auto-eviction here so
the Go process is the single source of truth on lifecycle.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
import uuid
from typing import Any


# Per-REPL request/response timeout. Long enough for a 60s Spark cold cell;
# the supervisor's caller (Go service layer) has its own ~120s HTTP timeout.
_CELL_TIMEOUT_S = 600
_CANCEL_TIMEOUT_S = 5
_SPAWN_READY_TIMEOUT_S = 30


_REPLS_LOCK = threading.Lock()
_REPLS: dict[str, "REPLProcess"] = {}  # keyed by repl_id (uuid str)


def _notebook_session_id(notebook_id: str) -> str:
    """Stable UUID5 session_id for a notebook — Spark Connect requires UUID."""
    return str(uuid.uuid5(uuid.NAMESPACE_OID, f"_clavesa_nb_{notebook_id}"))


class REPLProcess:
    """One notebook REPL subprocess plus the IPC plumbing to talk to it.

    Created via spawn(). Holds the Popen handle, a stdin write lock, and a
    stdout dispatcher thread. Pending requests register their `for: "<tag>"`
    in `_pending` and wait on a threading.Event.
    """

    def __init__(self, repl_id: str, notebook_id: str, session_id: str,
                 proc: subprocess.Popen):
        self.repl_id = repl_id
        self.notebook_id = notebook_id
        self.session_id = session_id
        self.proc = proc
        self.started_at = time.monotonic()
        self._stdin_lock = threading.Lock()
        self._pending_lock = threading.Lock()
        # Maps `for: <tag>` → (Event, dict-slot). Resolved by the dispatcher.
        self._pending: dict[str, tuple[threading.Event, list[dict]]] = {}
        self._stopped = False
        self._stderr_buf: list[str] = []
        threading.Thread(target=self._dispatch_stdout, daemon=True).start()
        threading.Thread(target=self._drain_stderr, daemon=True).start()

    # ---- IPC helpers --------------------------------------------------------

    def _send(self, payload: dict) -> None:
        if self.proc.stdin is None or self.proc.poll() is not None:
            raise RuntimeError(f"REPL {self.repl_id[:8]} is not running (rc={self.proc.returncode})")
        line = json.dumps(payload) + "\n"
        with self._stdin_lock:
            self.proc.stdin.write(line.encode("utf-8"))
            self.proc.stdin.flush()

    def _register(self, tag: str) -> tuple[threading.Event, list[dict]]:
        """Pre-register a waiter for the response carrying `for: <tag>`.

        Must be called BEFORE _send so a fast REPL response doesn't arrive
        before the dispatcher sees the waiter (which would drop it).
        """
        slot: list[dict] = []
        evt = threading.Event()
        with self._pending_lock:
            self._pending[tag] = (evt, slot)
        return evt, slot

    def _dispatch_stdout(self) -> None:
        """Read JSON lines from the REPL's stdout, dispatch to waiters by `for`."""
        try:
            for raw in self.proc.stdout:  # type: ignore[union-attr]
                try:
                    msg = json.loads(raw.decode("utf-8").strip())
                except Exception:  # noqa: BLE001
                    continue
                tag = msg.get("for", "")
                with self._pending_lock:
                    waiter = self._pending.pop(tag, None)
                if waiter is None:
                    # Orphan response (e.g. request gave up + popped its slot).
                    # Drop quietly — supervisor doesn't need to log every one.
                    continue
                evt, slot = waiter
                slot.append(msg)
                evt.set()
        finally:
            # REPL exited or stdout closed — wake every pending waiter so
            # they fail fast instead of hitting their timeout.
            self._stopped = True
            with self._pending_lock:
                pending = list(self._pending.items())
                self._pending.clear()
            for _, (evt, slot) in pending:
                slot.append({"ok": False, "error": f"repl {self.repl_id[:8]} exited"})
                evt.set()

    def _drain_stderr(self) -> None:
        """Capture REPL stderr (small ring) so spawn failures are diagnosable."""
        try:
            for raw in self.proc.stderr:  # type: ignore[union-attr]
                text = raw.decode("utf-8", errors="replace").rstrip()
                self._stderr_buf.append(text)
                # Bound the buffer — runaway stderr (e.g. Spark warning storm)
                # shouldn't grow unbounded in this process's RAM.
                if len(self._stderr_buf) > 500:
                    self._stderr_buf = self._stderr_buf[-500:]
        except Exception:  # noqa: BLE001
            pass

    def stderr_tail(self, n: int = 50) -> str:
        return "\n".join(self._stderr_buf[-n:])

    # ---- Public ops ---------------------------------------------------------

    def wait_ready(self, timeout_s: float = _SPAWN_READY_TIMEOUT_S) -> dict:
        evt, slot = self._register("ready")
        if not evt.wait(timeout_s):
            with self._pending_lock:
                self._pending.pop("ready", None)
            return {"ok": False, "error": f"timed out waiting for REPL ready ({timeout_s}s). stderr tail:\n{self.stderr_tail()}"}
        return slot[0]

    def run_cell(self, cell_run_id: str, language: str, source: str) -> dict:
        tag = f"cell:{cell_run_id}"
        evt, slot = self._register(tag)
        try:
            self._send({"op": "cell", "cell_run_id": cell_run_id,
                        "language": language, "source": source})
        except Exception as exc:  # noqa: BLE001
            with self._pending_lock:
                self._pending.pop(tag, None)
            return {"ok": False, "error": f"send failed: {exc}"}
        if not evt.wait(_CELL_TIMEOUT_S):
            with self._pending_lock:
                self._pending.pop(tag, None)
            return {"ok": False, "error": f"cell timed out after {_CELL_TIMEOUT_S}s"}
        return slot[0]

    def cancel(self, cell_run_id: str) -> dict:
        tag = f"cancel:{cell_run_id}"
        evt, slot = self._register(tag)
        try:
            self._send({"op": "cancel", "cell_run_id": cell_run_id})
        except Exception as exc:  # noqa: BLE001
            with self._pending_lock:
                self._pending.pop(tag, None)
            return {"ok": False, "error": f"send failed: {exc}"}
        if not evt.wait(_CANCEL_TIMEOUT_S):
            with self._pending_lock:
                self._pending.pop(tag, None)
            return {"ok": False, "error": f"cancel timed out after {_CANCEL_TIMEOUT_S}s"}
        return slot[0]

    def stop(self) -> None:
        if self._stopped:
            return
        try:
            self._send({"op": "stop"})
        except Exception:  # noqa: BLE001
            pass
        try:
            self.proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=2)
        self._stopped = True

    def info(self) -> dict[str, Any]:
        return {
            "repl_id": self.repl_id,
            "notebook_id": self.notebook_id,
            "session_id": self.session_id,
            "age_ms": int((time.monotonic() - self.started_at) * 1000),
            "alive": self.proc.poll() is None,
        }


# ---- Spawning -----------------------------------------------------------------

# Path the supervisor execs to start a REPL. /var/task is where the Dockerfile
# COPYs runner files; falls back to the same dir as this module for tests
# that import notebook_supervisor outside the container.
_REPL_SCRIPT = "/var/task/notebook_repl.py"
if not os.path.exists(_REPL_SCRIPT):
    _REPL_SCRIPT = os.path.join(os.path.dirname(__file__), "notebook_repl.py")


def _spawn_repl(notebook_id: str) -> REPLProcess:
    """Fork notebook_repl.py for a fresh notebook session.

    Inherits the parent's env (CLAVESA_CONNECT_PORT etc.) so the REPL hits
    the same Connect server the catalog client uses.
    """
    repl_id = str(uuid.uuid4())
    session_id = _notebook_session_id(notebook_id)
    proc = subprocess.Popen(  # noqa: S603 — fixed script, fixed args
        [sys.executable, _REPL_SCRIPT, session_id],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=os.environ.copy(),
        bufsize=0,
    )
    return REPLProcess(repl_id, notebook_id, session_id, proc)


# ---- HTTP entry point ----------------------------------------------------------

def handle_repl_request(method: str, path: str, body_bytes: bytes) -> tuple[int, dict]:
    """Route a /repl* HTTP request to the right pool operation.

    Returns (status_code, json-serializable dict). The caller (runner.py's
    HTTP Handler) serializes + writes the response.

    Path conventions:
      POST   /repl/spawn         body: {"notebook_id": "<name>"}
      POST   /repl/<id>/cell     body: cell req
      POST   /repl/<id>/cancel   body: cancel req
      DELETE /repl/<id>
      GET    /repls

    Status codes: 200 always for ops that succeeded protocol-wise (cell
    errors land inside the response body as status="error"); 4xx for
    bad input; 5xx only for supervisor-side failures.
    """
    try:
        body = json.loads(body_bytes.decode("utf-8")) if body_bytes else {}
    except Exception as exc:  # noqa: BLE001
        return 400, {"error": f"bad request body: {exc}"}

    if method == "GET" and path == "/repls":
        with _REPLS_LOCK:
            return 200, {"repls": [r.info() for r in _REPLS.values()]}

    if method == "POST" and path == "/repl/spawn":
        notebook_id = body.get("notebook_id", "").strip()
        if not notebook_id:
            return 400, {"error": "notebook_id is required"}
        proc = _spawn_repl(notebook_id)
        ready = proc.wait_ready()
        if not ready.get("ok"):
            proc.stop()
            return 500, {"error": f"REPL failed to come ready: {ready.get('error', 'unknown')}\nstderr tail:\n{proc.stderr_tail()}"}
        with _REPLS_LOCK:
            _REPLS[proc.repl_id] = proc
        return 200, {
            "repl_id": proc.repl_id,
            "notebook_id": notebook_id,
            "session_id": proc.session_id,
        }

    # Routes that need a repl_id in the path.
    parts = [p for p in path.split("/") if p]  # ["repl", "<id>", "<verb>?"]
    if len(parts) >= 2 and parts[0] == "repl":
        repl_id = parts[1]
        verb = parts[2] if len(parts) >= 3 else ""
        with _REPLS_LOCK:
            repl = _REPLS.get(repl_id)
        if repl is None:
            return 404, {"error": f"repl {repl_id} not found"}

        if method == "POST" and verb == "cell":
            cell_run_id = body.get("cell_run_id") or str(uuid.uuid4())
            language = body.get("language", "python")
            source = body.get("source", "")
            resp = repl.run_cell(cell_run_id, language, source)
            if not resp.get("ok"):
                return 500, {"error": resp.get("error", "unknown")}
            return 200, resp.get("result", {})

        if method == "POST" and verb == "cancel":
            cell_run_id = body.get("cell_run_id", "")
            if not cell_run_id:
                return 400, {"error": "cell_run_id is required"}
            resp = repl.cancel(cell_run_id)
            if not resp.get("ok"):
                return 500, {"error": resp.get("error", "unknown")}
            return 200, resp.get("result", {})

        if method == "DELETE" and verb == "":
            repl.stop()
            with _REPLS_LOCK:
                _REPLS.pop(repl_id, None)
            return 200, {"stopped": True}

    return 404, {"error": f"no route: {method} {path}"}


def stop_all() -> None:
    """Tear down every tracked REPL. Used on container shutdown."""
    with _REPLS_LOCK:
        items = list(_REPLS.values())
        _REPLS.clear()
    for r in items:
        r.stop()
