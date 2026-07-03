"""
Clavesa transform runner — PySpark.

One engine, identical semantics across local, Lambda, Fargate, EMR Serverless.
The same transform script that runs in preview mode runs unchanged in production.

Preview mode (CLAVESA_PREVIEW=1):
  - Reads input rows from CLAVESA_PREVIEW_INPUT_<ALIAS> env vars (JSON arrays).
  - Each input is registered as a temp view named <alias>.
  - One of:
      * CLAVESA_SQL — a SparkSQL query string. Single output, key "default".
      * CLAVESA_PYTHON_SCRIPT — a script defining
            transform(spark, inputs) -> dict[str, DataFrame]
        where inputs is dict[str, pyspark.sql.DataFrame].
  - Writes {"<output>": [rows...], ...} JSON to stdout.

Production mode: handler() is the Lambda entry point.
  Event shape (also used for local CLI invocation):
      {
        "inputs":  {"alias": "s3://bucket/path/" | "/local/path/", ...},
        "outputs": {"key":   "s3://bucket/path/" | "/local/path/", ...}
      }
  Reads logic from CLAVESA_LOGIC_S3_PATH (s3:// or local), reads each input
  as Parquet, runs the transform, writes outputs as Parquet. Returns
  {"status": "ok", "outputs": {key: path}}.

S3 vs local: any path that starts with "s3://" routes through boto3; anything
else is treated as a local filesystem path. This lets the same handler() back
both Lambda invocations and local `clavesa pipeline run` commands.
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import sys
import threading
import time
import types
import uuid
from typing import Any, Iterable

# Directory Spark writes its event log into (see spark_conf.py:
# spark.eventLog.dir). The runner tails the single log file in here after
# each transform to aggregate per-invocation task metrics onto node_runs.
# Created in _spark() right before the session builds.
EVENTLOG_DIR = "/tmp/clavesa-eventlog"

# Spark task-metric column names appended to node_runs (after output_rows).
# Order matches _node_runs_schema(); _aggregate_event_log() returns a dict
# keyed by exactly these names. Kept as a module constant so the schema,
# the aggregator, and the all-None failure paths can't drift apart.
_SPARK_METRIC_KEYS = (
    "peak_execution_memory_mb",
    "memory_spilled_bytes",
    "disk_spilled_bytes",
    "shuffle_read_bytes",
    "shuffle_write_bytes",
    "input_bytes",
    "input_records",
    "num_stages",
    "num_tasks",
    "num_failed_tasks",
    "jvm_gc_time_ms",
    "executor_cpu_time_ms",
    "executor_run_time_ms",
    "max_task_duration_ms",
)

# Module-level SparkSession so warm starts (UI preview server reusing the
# container, Lambda warm invocations) skip the ~3-5s JVM boot.
_SPARK = None


def _progress_snapshot(active_stage_infos, seen_stage_ids):
    """Fold the currently-active Spark stages into a flat counter dict.

    ``active_stage_infos`` is the list of stage-info objects for the stages
    Spark reports as active right now (from
    ``statusTracker().getStageInfo(sid)`` per ``getActiveStageIds()``).
    Each object is read by ATTRIBUTE — ``.numTasks``, ``.numActiveTasks``,
    ``.numCompletedTasks``, ``.numFailedTasks``, ``.stageId`` — because the
    PySpark ``StatusTracker`` python wrapper returns ``SparkStageInfo``
    namedtuples, not py4j JavaObjects with method accessors. Reading
    attributes (never calling) also tolerates a plain object/class fake in
    tests.

    ``seen_stage_ids`` is a running set the caller threads across polls; this
    function MUTATES it to include every active stage id seen so far so that
    ``stages_total`` is monotonic even after a stage finishes and drops out
    of the active list. Returns all-int counters.
    """
    active = list(active_stage_infos or [])
    for info in active:
        seen_stage_ids.add(int(getattr(info, "stageId")))
    tasks_total = sum(int(getattr(i, "numTasks")) for i in active)
    tasks_completed = sum(int(getattr(i, "numCompletedTasks")) for i in active)
    tasks_failed = sum(int(getattr(i, "numFailedTasks")) for i in active)
    return {
        "stages_total": len(seen_stage_ids),
        "stages_completed": len(seen_stage_ids) - len(active),
        "tasks_total": tasks_total,
        "tasks_completed": tasks_completed,
        "tasks_failed": tasks_failed,
    }


class _ProgressPoller(threading.Thread):
    """Daemon thread that polls the Spark statusTracker while a single node's
    transform runs and feeds per-poll snapshots to an ``emit`` callback.

    Best-effort: the whole poll body is wrapped so a py4j hiccup (or Spark
    not yet built) never surfaces — the poller exists only to enrich
    progress output, it must never affect the transform outcome. Daemon so
    it can't block process exit. Scoped to one ``node`` with its own ``seen``
    set; ``pipeline_handler`` creates a fresh poller per node.
    """

    _IDLE_SLEEP = 0.5  # waiting for the transform to build _SPARK
    _POLL_SLEEP = 1.5  # between live polls

    def __init__(self, node, emit):
        super().__init__(daemon=True)
        self._node = node
        self._emit = emit
        # NB: must NOT be named ``_stop`` — that shadows threading.Thread._stop
        # (a CPython internal). threading._after_fork() calls thread._stop() on
        # every thread after an os.fork(); with the name shadowed by an Event,
        # that raises "'Event' object is not callable" inside Spark's worker
        # fork and corrupts the transform.
        self._stop_event = threading.Event()
        self._seen = set()

    def _poll_once(self):
        # Don't build Spark from the poller thread — wait for the transform
        # to populate the module singleton.
        if _SPARK is None:
            return False
        tracker = _SPARK.sparkContext.statusTracker()
        active_ids = tracker.getActiveStageIds() or []
        infos = []
        for sid in active_ids:
            info = tracker.getStageInfo(sid)
            if info is not None:
                infos.append(info)
        snapshot = _progress_snapshot(infos, self._seen)
        self._emit({"node": self._node, **snapshot})
        return True

    def run(self):
        while not self._stop_event.is_set():
            polled = False
            try:
                polled = self._poll_once()
            except Exception:  # noqa: BLE001 — best-effort, never crash
                polled = False
            self._stop_event.wait(self._POLL_SLEEP if polled else self._IDLE_SLEEP)

    def stop(self):
        self._stop_event.set()
        if self.is_alive():
            self.join(timeout=2.0)

# Module-level Spark Connect client used by the warm-worker query server
# (CLAVESA_QUERY_SERVER mode). Lazily built once the embedded Connect plugin
# is up; reused for every /query request. Independent of _SPARK — the two
# sessions can coexist in the same process (one py4j driver hosting the
# plugin, one Connect client talking to it over localhost gRPC).
_CONNECT = None
# Spark Connect session id for the warm worker's client session. Starts as a
# stable UUID; rotated to a fresh one by _reset_connect_session after a session
# is closed, because Spark Connect tombstones a reaped session id (reconnecting
# with the same id fails again with SESSION_CLOSED).
_CONNECT_SESSION_ID = None


def _spark():
    """Lazy py4j SparkSession used by Lambda / preview / run / one-shot query.

    The warm-worker mode (CLAVESA_QUERY_SERVER) does NOT use this anymore —
    it goes through a Spark Connect client in run_query_server(). All other
    modes still use the in-process py4j driver: they're one-shot, single-tenant
    container invocations where Connect would buy us nothing.

    Config is shared via spark_conf.clavesa_spark_conf so the py4j path here
    and the Connect-server launch (CLAVESA_CONNECT_SERVER=1) pin the same
    Iceberg catalog + S3A wiring.
    """
    global _SPARK
    # #23 self-heal: if a cached session is dead (driver JVM gone, py4j gateway
    # closed, SparkContext stopped) — e.g. after a GC pause tripped the
    # heartbeat in a long shuffle-heavy bundle run — drop it so the build block
    # below rebuilds a fresh one. SKIP this when acting as the Spark Connect
    # host (CLAVESA_QUERY_SERVER / CLAVESA_CONNECT_SERVER): _SPARK there is the
    # plugin-hosting driver assigned in _run_connect_host, and resetting it
    # would tear down the live Connect server out from under its clients.
    _connect_host = (
        os.environ.get("CLAVESA_QUERY_SERVER") == "1"
        or os.environ.get("CLAVESA_CONNECT_SERVER") == "1"
    )
    if _SPARK is not None and not _connect_host and _is_spark_session_dead(_SPARK):
        _reset_spark_session()
    if _SPARK is None:
        from pyspark.sql import SparkSession  # noqa: PLC0415
        from spark_conf import clavesa_spark_conf, spark_master  # noqa: PLC0415

        master = spark_master()
        builder = (
            SparkSession.builder.appName("clavesa-runner").master(master)
        )
        for k, v in clavesa_spark_conf().items():
            builder = builder.config(k, v)
        # NB: clavesa_spark_conf() creates EVENTLOG_DIR and only then enables
        # spark.eventLog — so every session-build path (handler, preview, warm
        # query/Connect servers) gets the dir created before SparkContext init,
        # which otherwise throws FileNotFoundException on a missing log dir.
        _SPARK = builder.getOrCreate()
        _SPARK.sparkContext.setLogLevel("ERROR")
        # #24: surface the resolved master so a misrouted run (e.g. an
        # unintended local[1] or a stale CLAVESA_SPARK_MASTER) is visible in
        # the runner stderr/log stream instead of being silently inferred.
        print(f"[clavesa] spark master = {master}", file=sys.stderr, flush=True)
    return _SPARK


def _is_spark_session_dead(session) -> bool:
    """Cheapest reliable liveness probe for a cached py4j SparkSession (#23).

    One py4j round-trip to the JVM-side SparkContext.isStopped() — NO Spark job
    (no parallelize/count), because this runs once per node in a bundle. A dead
    driver / closed gateway raises a py4j network error or an OSError on that
    round-trip; we treat any failure, and isStopped()==True, as dead so the
    caller rebuilds. py4j has no session-id tombstoning problem (unlike
    Connect), so a plain rebuild is enough."""
    class _NoPy4J(Exception):
        """Placeholder so the except tuple stays valid when py4j is absent
        (stubbed in unit tests) — it can never actually be raised here."""

    try:
        from py4j.java_gateway import Py4JNetworkError  # noqa: PLC0415
        from py4j.protocol import Py4JError  # noqa: PLC0415
    except Exception:  # noqa: BLE001 — py4j missing (stubbed in unit tests)
        Py4JNetworkError = _NoPy4J  # type: ignore[assignment]
        Py4JError = _NoPy4J  # type: ignore[assignment]
    try:
        return bool(session.sparkContext._jsc.sc().isStopped())
    except (Py4JNetworkError, Py4JError, ConnectionRefusedError, OSError):
        return True
    except Exception:  # noqa: BLE001 — any unexpected failure ⇒ treat as dead
        return True


def _reset_spark_session() -> None:
    """Drop the cached py4j session so the next _spark() rebuilds it (#23).

    Best-effort stop of the stale handle first (it may already be dead), then
    clear the module global. No session-id rotation: py4j has no tombstoning
    problem, so a fresh getOrCreate() is a clean rebuild."""
    global _SPARK
    old = _SPARK
    _SPARK = None
    if old is not None:
        try:
            old.stop()
        except Exception:  # noqa: BLE001 — stale handle; nothing useful to do
            pass


def _tmp_pressure_exceeded(threshold: float = 0.5) -> bool:
    """True when running on Lambda AND /tmp is more than ``threshold``
    (default 50%) full.

    Lambda-only on purpose: there /tmp is a dedicated ephemeral volume, so
    the ratio measures real spill-space pressure. In a local Docker container
    /tmp sits on the overlay filesystem and ``disk_usage`` reports the HOST
    disk — a half-full laptop would recycle the session before every
    transform and defeat warm-session bundling entirely. Local containers
    are one-shot per `pipeline run` anyway, so there is nothing to clean.

    SparkContext.stop() triggers Spark's own shutdown hooks which delete the
    blockmgr and spill directories — so recycling the session is the right
    lever here rather than hand-rolling an rm sweep. Known gap: the recycle
    does NOT free /tmp/clavesa-src-* (the persistent http download cache) or
    finalized event logs; with 10 GB of ephemeral storage those are noise.
    Best-effort: any stat failure returns False so the fast warm path is
    never accidentally skipped. GH #43.
    """
    if not os.environ.get("AWS_LAMBDA_FUNCTION_NAME"):
        return False
    try:
        import shutil  # noqa: PLC0415
        usage = shutil.disk_usage("/tmp")
        return usage.used / usage.total > threshold
    except Exception:  # noqa: BLE001
        return False


def _load_script(source: str) -> types.ModuleType:
    mod = types.ModuleType("_user_transform")
    exec(compile(source, "<clavesa_script>", "exec"), mod.__dict__)  # noqa: S102
    return mod


# ---------------------------------------------------------------------------
# Resource / Spark-metric capture (observability — node_runs columns)
#
# Two layers: pure stdlib parsers (testable without Spark / /proc) and thin
# impure wrappers that read /proc and the event-log file. The pure layer is
# the warm-worker-critical part: _aggregate_event_log must scope to exactly
# one invocation's events, so the impure wrapper seeks past the prior tail.
# ---------------------------------------------------------------------------


def _read_peak_rss_mb(status_text: str | None = None) -> int | None:
    """Process peak RSS (VmHWM) in MB, or None when unavailable.

    Parses the ``VmHWM:`` line of ``/proc/self/status`` (Linux only). VmHWM
    is the process-lifetime high-water mark, so under warm-worker reuse it is
    monotonic across invocations rather than per-invocation. Off-Linux (no
    /proc) or on any parse failure returns None — the column is nullable.

    ``status_text`` lets tests pass the file body directly; when None the
    function reads ``/proc/self/status``.
    """
    if status_text is None:
        try:
            with open("/proc/self/status", "r", encoding="utf-8") as f:
                status_text = f.read()
        except Exception:  # noqa: BLE001
            return None
    try:
        for line in status_text.splitlines():
            if line.startswith("VmHWM:"):
                # "VmHWM:\t  123456 kB" → kB int → MB.
                kb = int(line.split()[1])
                return kb // 1024
    except Exception:  # noqa: BLE001
        return None
    return None


def _aggregate_event_log(lines: Iterable[str]) -> dict[str, int | None]:
    """Aggregate Spark task metrics from newline-delimited event-log JSON.

    Pure / stdlib-only / side-effect-free — the warm-worker-critical core.
    Callers pass only the slice of event-log lines belonging to the current
    invocation (see _read_spark_metrics seeking past the prior tail) so the
    sums scope to one transform run under session reuse.

    Returns a dict with every key in ``_SPARK_METRIC_KEYS`` present. Values
    are None when no relevant event was seen (e.g. an empty input that
    launched no tasks); otherwise the aggregate per the column contract:
      - bytes summed in bytes; memory_mb fields are bytes ÷ 1048576
      - cpu time converted ns → ms; run time already ms
      - max_task_duration_ms = max(FinishTime - LaunchTime) over tasks
    Malformed / blank lines are ignored. Nested keys are read defensively.
    """
    saw_task = False
    num_stages = 0
    num_tasks = 0
    num_failed_tasks = 0

    peak_exec_mem_bytes = 0  # max over tasks
    memory_spilled = 0
    disk_spilled = 0
    shuffle_read = 0
    shuffle_write = 0
    input_bytes = 0
    input_records = 0
    jvm_gc_time = 0
    executor_cpu_ns = 0
    executor_run_ms = 0
    max_task_duration = 0  # max over tasks

    for raw in lines:
        if not raw or not raw.strip():
            continue
        try:
            ev = json.loads(raw)
        except Exception:  # noqa: BLE001
            continue
        if not isinstance(ev, dict):
            continue
        event = ev.get("Event")
        if event == "SparkListenerStageCompleted":
            num_stages += 1
            continue
        if event != "SparkListenerTaskEnd":
            continue

        saw_task = True
        num_tasks += 1

        reason = (ev.get("Task End Reason") or {})
        if not isinstance(reason, dict) or reason.get("Reason") != "Success":
            num_failed_tasks += 1

        tm = ev.get("Task Metrics") or {}
        if isinstance(tm, dict):
            pem = tm.get("Peak Execution Memory") or 0
            if isinstance(pem, (int, float)) and pem > peak_exec_mem_bytes:
                peak_exec_mem_bytes = int(pem)
            memory_spilled += int(tm.get("Memory Bytes Spilled") or 0)
            disk_spilled += int(tm.get("Disk Bytes Spilled") or 0)
            jvm_gc_time += int(tm.get("JVM GC Time") or 0)
            executor_cpu_ns += int(tm.get("Executor CPU Time") or 0)
            executor_run_ms += int(tm.get("Executor Run Time") or 0)

            srm = tm.get("Shuffle Read Metrics") or {}
            if isinstance(srm, dict):
                shuffle_read += int(srm.get("Remote Bytes Read") or 0)
                shuffle_read += int(srm.get("Local Bytes Read") or 0)
            swm = tm.get("Shuffle Write Metrics") or {}
            if isinstance(swm, dict):
                shuffle_write += int(swm.get("Shuffle Bytes Written") or 0)
            im = tm.get("Input Metrics") or {}
            if isinstance(im, dict):
                input_bytes += int(im.get("Bytes Read") or 0)
                input_records += int(im.get("Records Read") or 0)

        ti = ev.get("Task Info") or {}
        if isinstance(ti, dict):
            launch = ti.get("Launch Time")
            finish = ti.get("Finish Time")
            if isinstance(launch, (int, float)) and isinstance(finish, (int, float)):
                dur = int(finish) - int(launch)
                if dur > max_task_duration:
                    max_task_duration = dur

    if not saw_task and num_stages == 0:
        return dict.fromkeys(_SPARK_METRIC_KEYS, None)

    return {
        "peak_execution_memory_mb": (peak_exec_mem_bytes // 1048576) if saw_task else None,
        "memory_spilled_bytes": memory_spilled if saw_task else None,
        "disk_spilled_bytes": disk_spilled if saw_task else None,
        "shuffle_read_bytes": shuffle_read if saw_task else None,
        "shuffle_write_bytes": shuffle_write if saw_task else None,
        "input_bytes": input_bytes if saw_task else None,
        "input_records": input_records if saw_task else None,
        "num_stages": num_stages,
        "num_tasks": num_tasks,
        "num_failed_tasks": num_failed_tasks if saw_task else None,
        "jvm_gc_time_ms": jvm_gc_time if saw_task else None,
        "executor_cpu_time_ms": (executor_cpu_ns // 1_000_000) if saw_task else None,
        "executor_run_time_ms": executor_run_ms if saw_task else None,
        "max_task_duration_ms": max_task_duration if saw_task else None,
    }


def _event_log_data_file() -> str | None:
    """Path to Spark's current event-log DATA file, or None if not written yet.

    Spark 4 writes the *rolling v2* layout: a per-application directory
    ``eventlog_v2_<appId>/`` holding an ``appstatus_<appId>.inprogress``
    marker plus one or more ``events_<n>_<appId>`` data files. Single-file
    mode (older Spark / ``spark.eventLog.rolling.enabled=false``) writes a
    flat ``<appId>(.inprogress)`` file directly in EVENTLOG_DIR. Handle both:
    prefer the rolling layout, and within it the newest ``events_*`` file
    (the one currently being appended). Best-effort — returns None on any
    error so metric capture never trips the node_runs write.
    """
    import glob  # noqa: PLC0415

    try:
        v2_dirs = sorted(
            (d for d in glob.glob(os.path.join(EVENTLOG_DIR, "eventlog_v2_*"))
             if os.path.isdir(d)),
            key=os.path.getmtime,
        )
        if v2_dirs:
            events = [
                p for p in glob.glob(os.path.join(v2_dirs[-1], "events_*"))
                if os.path.isfile(p)
            ]
            if events:
                return max(events, key=os.path.getmtime)
        flat = glob.glob(os.path.join(EVENTLOG_DIR, "*.inprogress"))
        if not flat:
            flat = [
                p for p in glob.glob(os.path.join(EVENTLOG_DIR, "*"))
                if os.path.isfile(p)
            ]
        return flat[0] if flat else None
    except Exception:  # noqa: BLE001
        return None


def _event_log_offset() -> int:
    """Byte size of the current event-log data file, captured at handler entry.

    The post-run read seeks past everything a prior warm invocation already
    wrote, scoping the metric aggregate to this invocation. Best-effort: any
    error returns 0 (the whole tail is then read, which over-counts only on
    the first invocation, when there is nothing prior anyway).
    """
    path = _event_log_data_file()
    if not path:
        return 0
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def _read_spark_metrics(offset: int) -> dict[str, int | None]:
    """Read the event-log tail from ``offset`` to EOF and aggregate it.

    Impure wrapper around the pure _aggregate_event_log. Seeks past the bytes
    a prior invocation wrote, reads the new tail, and aggregates. Any failure
    (no file yet, read error) returns an all-None dict so the node_runs write
    never trips on metric capture.
    """
    path = _event_log_data_file()
    if not path:
        return dict.fromkeys(_SPARK_METRIC_KEYS, None)
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            f.seek(offset)
            tail = f.read()
        return _aggregate_event_log(tail.splitlines())
    except Exception:  # noqa: BLE001
        return dict.fromkeys(_SPARK_METRIC_KEYS, None)


# ---------------------------------------------------------------------------
# Preview-mode helpers (in-memory rows ↔ DataFrames)
# ---------------------------------------------------------------------------


def _df_to_rows(df) -> list[dict[str, Any]]:
    pdf = df.toPandas()
    return json.loads(pdf.to_json(orient="records", date_format="iso"))


def _normalise_output(value) -> list[dict[str, Any]]:
    from pyspark.sql import DataFrame  # noqa: PLC0415

    if isinstance(value, DataFrame):
        return _df_to_rows(value)
    if isinstance(value, list):
        return value
    raise TypeError(
        f"transform() output values must be a Spark DataFrame or list of dicts, got {type(value)}"
    )


def _register_inputs(spark, inputs: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    dataframes: dict[str, Any] = {}
    for alias, rows in inputs.items():
        if not rows:
            df = spark.createDataFrame([], schema="struct<>")
        else:
            df = spark.createDataFrame(rows)
        df.createOrReplaceTempView(alias)
        dataframes[alias] = df
    return dataframes


def run_preview() -> None:
    prefix = "CLAVESA_PREVIEW_INPUT_"
    inputs: dict[str, list] = {}
    for key, val in os.environ.items():
        if key.startswith(prefix):
            alias = key[len(prefix):].lower()
            inputs[alias] = json.loads(val)

    sql = os.environ.get("CLAVESA_SQL", "").strip()
    script = os.environ.get("CLAVESA_PYTHON_SCRIPT", "")

    if not sql and not script:
        print("{}", flush=True)
        return

    spark = _spark()
    dataframes = _register_inputs(spark, inputs)

    if sql:
        result_df = spark.sql(sql)
        result = {"default": _df_to_rows(result_df)}
    else:
        mod = _load_script(script)
        if not hasattr(mod, "transform"):
            raise RuntimeError(
                "User script must define a top-level 'transform(spark, inputs)' function."
            )
        raw = mod.transform(spark, dataframes)
        if not isinstance(raw, dict):
            raise TypeError(f"transform() must return a dict, got {type(raw)}")
        result = {k: _normalise_output(v) for k, v in raw.items()}

    print(json.dumps(result), flush=True)


# ---------------------------------------------------------------------------
# Production-mode helpers (S3/local I/O)
# ---------------------------------------------------------------------------


def _is_s3(path: str) -> bool:
    return path.startswith("s3://")


def _split_s3(path: str) -> tuple[str, str]:
    """s3://bucket/key/parts → ('bucket', 'key/parts'). Trailing slash kept."""
    assert _is_s3(path), f"not an s3 path: {path}"
    rest = path[len("s3://"):]
    bucket, _, key = rest.partition("/")
    return bucket, key


def _read_text(path: str) -> str:
    if _is_s3(path):
        import boto3  # noqa: PLC0415

        bucket, key = _split_s3(path)
        body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
        return body.decode("utf-8")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


_S3_CLIENT = None


def _s3_client():
    """Lazily-built, reused boto3 S3 client for the cloud progress sink.

    Reused across progress PUTs within one Lambda invocation so the poller
    doesn't pay client-construction cost on every 1.5s tick.
    """
    global _S3_CLIENT
    if _S3_CLIENT is None:
        import boto3  # noqa: PLC0415

        _S3_CLIENT = boto3.client("s3")
    return _S3_CLIENT


def _progress_target(env, run, node):
    """Resolve where the per-node progress sink writes, backend-neutral, or None.

    Returns a small descriptor the writer (``_write_progress``) dispatches on:

      - ``("s3", bucket, "_progress/<run>/<node>.json")`` when
        CLAVESA_SYSTEM_WAREHOUSE is an ``s3://`` URI. The bucket is the
        workspace pipeline bucket the Go CloudProvider lists back via its
        workspace S3 client; the key is ``_progress/<run>/<node>.json`` at
        the bucket root. Derived from the SYSTEM warehouse (not
        CLAVESA_WAREHOUSE) because that's the bucket the reader watches and
        real cloud already works this way.
      - ``("file", "<CLAVESA_WAREHOUSE>/_progress/<run>/<node>.json")`` when
        the warehouse is a non-empty, non-s3 filesystem path (local mode /
        cloud-local against a disk warehouse). The container mounts that
        path at the same location, so the Go side reads the same tree.
      - ``None`` when neither warehouse resolves or ``run``/``node`` is empty,
        so the caller skips the sink without a live progress channel.

    The system-warehouse s3 branch takes precedence: a deployed run always
    has CLAVESA_SYSTEM_WAREHOUSE set to ``s3://…`` and we want the s3 sink.

    Pure (no boto3, no env reads of its own) so it's unit-testable.
    """
    env = env or {}
    if not run or not node:
        return None
    system_warehouse = env.get("CLAVESA_SYSTEM_WAREHOUSE", "") or ""
    if system_warehouse and _is_s3(system_warehouse):
        bucket, _ = _split_s3(system_warehouse)
        if bucket:
            return ("s3", bucket, f"_progress/{run}/{node}.json")
        return None
    warehouse = env.get("CLAVESA_WAREHOUSE", "") or ""
    if warehouse and not _is_s3(warehouse):
        path = os.path.join(warehouse, "_progress", run, f"{node}.json")
        return ("file", path)
    return None


def _write_progress(target, payload):
    """Write a progress JSON payload to the resolved target, best-effort.

    ``target`` is a descriptor from ``_progress_target``. Never raises and
    never blocks the transform: a PUT / file-write failure is swallowed and
    logged to stderr only. The ``file`` backend writes atomically (temp file
    in the same directory + os.replace) so a concurrent reader never sees a
    half-written JSON document.
    """
    if not target:
        return
    try:
        body = json.dumps(payload).encode("utf-8")
        kind = target[0]
        if kind == "s3":
            _, bucket, key = target
            _s3_client().put_object(Bucket=bucket, Key=key, Body=body)
        elif kind == "file":
            _, path = target
            os.makedirs(os.path.dirname(path), exist_ok=True)
            tmp = f"{path}.{os.getpid()}.tmp"
            with open(tmp, "wb") as f:
                f.write(body)
            os.replace(tmp, path)
    except Exception as exc:  # noqa: BLE001 — sink is enrichment-only
        print(
            f"[clavesa] progress write failed for {target!r}: {exc!r}",
            file=sys.stderr,
        )


def _looks_like_path(s: str) -> bool:
    """Heuristic: paths have slashes; Iceberg table identifiers don't."""
    return "/" in s or s.startswith("s3://")


# ---------------------------------------------------------------------------
# Incremental processing — partition listing + watermarks (v0.12+)
# ---------------------------------------------------------------------------


def _list_partition_tree(bucket: str, prefix: str, partition_names: list[str]) -> list[tuple[tuple[str, ...], str]]:
    """Walk an S3 partition tree (Hive-style) and return (cursor, full_prefix)
    pairs for every leaf partition.

    For partition_names = ["day", "hour"] under prefix
    "logs/year=2026/month=04/", recursively lists:
      logs/year=2026/month=04/day=*/hour=*/
    Returns e.g. [(("2026-04-26", "00"), "logs/.../day=2026-04-26/hour=00/"), ...].

    Ordering: lexicographic on the cursor tuple. Matches Python tuple
    comparison so callers can do `cursor > watermark` directly.
    """
    import boto3  # noqa: PLC0415

    s3 = boto3.client("s3")

    def walk(cur_prefix: str, remaining: list[str]) -> list[tuple[tuple[str, ...], str]]:
        if not remaining:
            return [((), cur_prefix)]
        head, *tail = remaining
        out: list[tuple[tuple[str, ...], str]] = []
        token = f"{head}="
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=cur_prefix, Delimiter="/"):
            for cp in page.get("CommonPrefixes", []) or []:
                sub = cp["Prefix"]
                last = sub.rstrip("/").rsplit("/", 1)[-1]
                if not last.startswith(token):
                    continue
                value = last[len(token):]
                for cur, leaf in walk(sub, tail):
                    out.append(((value,) + cur, leaf))
        return out

    return sorted(walk(prefix, partition_names), key=lambda x: x[0])


def _watermark_uri(alias: str) -> str:
    """Resolve the watermark URI for a (consumer, input) pair. Pipeline-shared
    so transforms reading the same source see the same cursor.

    Cloud (s3:// base): a per-alias JSON object under the pipeline bucket.
    Local (file:// or plain path): the local pipeline-run flow sets a
    filesystem path so `pipeline run` doesn't need S3 to track watermarks
    against transform upstreams. ADR-014 local-cloud parity.
    """
    base = os.environ.get("CLAVESA_WATERMARKS", "")
    if not base:
        raise RuntimeError("CLAVESA_WATERMARKS env var not set")
    if not base.endswith("/"):
        base += "/"
    return f"{base}{alias}.json"


def _read_watermark(uri: str) -> tuple[str, ...] | None:
    """Returns the stored cursor as a tuple, or None when no watermark exists.
    Treats any read error other than NoSuchKey / FileNotFoundError as fatal:
    better to fail loud than reprocess silently."""
    if uri.startswith("s3://"):
        import boto3  # noqa: PLC0415
        from botocore.exceptions import ClientError  # noqa: PLC0415

        bucket, key = _split_s3(uri)
        try:
            body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
                return None
            raise
        payload = json.loads(body)
    else:
        path = uri[len("file://"):] if uri.startswith("file://") else uri
        try:
            with open(path, "rb") as f:
                payload = json.loads(f.read())
        except FileNotFoundError:
            return None
    cursor = payload.get("cursor")
    if not isinstance(cursor, list):
        return None
    return tuple(str(x) for x in cursor)


def _write_watermark(uri: str, cursor: tuple[str, ...]) -> None:
    import datetime as _dt  # noqa: PLC0415

    payload = {
        "cursor": list(cursor),
        "updated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
    }
    body = json.dumps(payload).encode("utf-8")
    if uri.startswith("s3://"):
        import boto3  # noqa: PLC0415

        bucket, key = _split_s3(uri)
        boto3.client("s3").put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
        return
    path = uri[len("file://"):] if uri.startswith("file://") else uri
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(body)


def _max_files_per_run() -> int:
    """Per-run batch cap for notification-drain ingest. Bounds how many SQS
    object-created messages one run consumes so a backlog drains over several
    runs instead of one unbounded read."""
    raw = os.environ.get("CLAVESA_MAX_FILES_PER_RUN", "")
    try:
        val = int(raw)
    except (TypeError, ValueError):
        return 1000
    return val if val > 0 else 1000


def _drain_source_queue(queue_url: str, max_keys: int, region: str | None = None) -> tuple[list[str], list[str]]:
    """Drain an SQS queue fed by S3 ``Object Created`` events (Auto-Loader-style
    file-notification ingest). Returns ``(keys, handles)`` where ``keys`` is the
    list of ``s3://<bucket>/<key>`` URIs to read this run and ``handles`` is the
    matching list of SQS receipt handles to delete *after* outputs commit.

    Each message body is the EventBridge ``Object Created`` event JSON; the
    bucket + object key live under ``detail.bucket.name`` / ``detail.object.key``
    (the key arrives percent-encoded, so URL-decode it). Keys are deduped within
    the batch.

    Posture on un-parseable bodies: a body that isn't a recognisable
    ObjectCreated event is logged to stderr and skipped, and its handle is NOT
    collected — so it stays on the queue and the DLQ redrive policy handles the
    poison message rather than this run silently swallowing it. Only handles for
    messages we successfully turned into a key are returned for deletion.

    Drains to empty: loops ``ReceiveMessage`` until a genuinely empty receive
    OR ``max_keys`` keys are collected, never on a partial (<10) batch. SQS
    routinely returns fewer than ``MaxNumberOfMessages`` even when the queue
    holds thousands more, so a short read is a sampling artifact, not a drained
    queue (GH #52: treating <10 as "drained" capped ingestion at one batch per
    trigger regardless of arrival rate). The 20s long poll makes the
    terminating empty read authoritative rather than a near-empty short-poll
    miss, and a backlog larger than ``max_keys`` finishes draining on the next
    poller tick (the per-pipeline run lock serializes those executions).
    """
    import boto3  # noqa: PLC0415
    import urllib.parse  # noqa: PLC0415

    sqs = boto3.client("sqs", region_name=region) if region else boto3.client("sqs")

    keys: list[str] = []
    handles: list[str] = []
    seen: set[str] = set()
    skipped = 0
    while len(keys) < max_keys:
        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,
        )
        messages = resp.get("Messages") or []
        if not messages:
            break
        for msg in messages:
            receipt = msg.get("ReceiptHandle")
            try:
                body = json.loads(msg.get("Body") or "")
                detail = body["detail"]
                bucket = detail["bucket"]["name"]
                raw_key = detail["object"]["key"]
                key = urllib.parse.unquote_plus(str(raw_key))
                if not bucket or not key:
                    raise ValueError("empty bucket or key")
            except Exception as exc:  # noqa: BLE001
                skipped += 1
                print(
                    f"[clavesa] drain {queue_url!r}: skipping un-parseable message "
                    f"(left on queue for DLQ redrive): {exc!r}",
                    file=sys.stderr,
                )
                continue
            uri = f"s3://{bucket}/{key}"
            # Always collect the handle for a parsed message so a duplicate
            # event (same object enqueued twice) still gets deleted; only the
            # first occurrence contributes a key to read.
            if receipt:
                handles.append(receipt)
            if uri not in seen:
                seen.add(uri)
                keys.append(uri)
    if skipped:
        print(
            f"[clavesa] drain {queue_url!r}: skipped {skipped} un-parseable message(s)",
            file=sys.stderr,
        )
    return keys, handles


def _delete_sqs_messages(queue_url: str, handles: list[str], region: str | None = None) -> None:
    """Best-effort batch delete of consumed SQS messages after outputs commit.
    Chunks into ≤10-entry ``DeleteMessageBatch`` calls. A delete failure logs to
    stderr but never raises — the message redelivers and downstream dedup absorbs
    the duplicate, same at-least-once posture as watermark advance."""
    if not handles:
        return
    import boto3  # noqa: PLC0415

    sqs = boto3.client("sqs", region_name=region) if region else boto3.client("sqs")
    for start in range(0, len(handles), 10):
        chunk = handles[start:start + 10]
        entries = [{"Id": str(i), "ReceiptHandle": h} for i, h in enumerate(chunk)]
        try:
            resp = sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries)
            failed = resp.get("Failed") or []
            if failed:
                print(
                    f"[clavesa] drain {queue_url!r}: {len(failed)} message(s) failed to delete "
                    f"(will redeliver): {failed!r}",
                    file=sys.stderr,
                )
        except Exception as exc:  # noqa: BLE001
            print(
                f"[clavesa] drain {queue_url!r}: delete_message_batch failed "
                f"(messages will redeliver): {exc!r}",
                file=sys.stderr,
            )


def _resolve_initial_cursor(start_from: str, partitions: list[tuple[tuple[str, ...], str]]) -> tuple[str, ...] | None:
    """Translate a start_from declaration into an effective watermark when none
    is stored yet.

    "all"    → None (no filter; first run reads everything).
    "now"    → the current max partition (skip backfill).
    literal  → tuple-encoded literal (e.g. "2026-04-26" → ("2026-04-26",)).
    """
    if start_from in ("", "all"):
        return None
    if start_from == "now":
        return partitions[-1][0] if partitions else None
    # Literal: take "/"-separated segments. start_from = "2026-04-26" gives
    # the tuple ("2026-04-26",); "2026-04-26/14" gives ("2026-04-26", "14").
    return tuple(start_from.split("/"))


def _download_http_to_tmp(url: str, headers: dict | None = None) -> str:
    """Download an http(s) URL into a unique /tmp file and return the path.

    ADR-017 slice 1: stdlib only (no extra dep), no caching. Slice 2
    adds optional headers for credentialed fetches.

    Filename component preserved so Spark's format dispatch (which keys
    off path extension for some formats) still works. Hash prefix
    incorporates the headers so an authed fetch and an unauthed fetch
    of the same URL don't share a cache entry — typically they return
    different bytes.
    """
    import hashlib  # noqa: PLC0415
    import urllib.request  # noqa: PLC0415

    base = url.rsplit("/", 1)[-1] or "data"
    cache_key = url
    if headers:
        # Sort so dict ordering doesn't change the digest. Header values
        # may be sensitive; only the digest hits disk.
        cache_key += "\n" + "\n".join(f"{k}={headers[k]}" for k in sorted(headers))
    digest = hashlib.sha256(cache_key.encode("utf-8")).hexdigest()[:12]
    dest = f"/tmp/clavesa-src-{digest}-{base}"
    if not os.path.exists(dest):
        tmp = dest + ".part"
        req = urllib.request.Request(url, headers=headers or {})  # noqa: S310
        with urllib.request.urlopen(req) as resp, open(tmp, "wb") as f:  # noqa: S310
            while True:
                chunk = resp.read(64 * 1024)
                if not chunk:
                    break
                f.write(chunk)
        os.replace(tmp, dest)
    return dest


def _resolve_http_headers(credential: dict | None) -> dict | None:
    """Resolve a credential descriptor (inlined by the orchestration
    emitter) into the headers dict the request needs.

    Descriptor shape (slice 2):
        {
          "kind": "header",
          "header_name": "Authorization",
          "value_prefix": "Bearer ",
          "secret": "env:STRIPE_KEY"           # or file:/abs/path or arn:aws:secretsmanager:...
        }

    Returns None when credential is None or empty (anonymous fetch).
    Raises RuntimeError on backend resolution failures — the runner
    fails the run rather than fetching with a bad header (which would
    produce a confusing downstream error).
    """
    if not credential:
        return None
    kind = credential.get("kind", "")
    if kind != "header":
        raise RuntimeError(f"unsupported credential kind {kind!r} (slice 2: only header)")
    secret_value = _resolve_secret(credential.get("secret", ""))
    header_name = credential.get("header_name", "")
    if not header_name:
        raise RuntimeError("credential missing header_name")
    value_prefix = credential.get("value_prefix", "") or ""
    return {header_name: value_prefix + secret_value}


def _resolve_secret(ref: str) -> str:
    """Translate a slice-2 secret reference into the actual secret value.

    Backends:
      env:VAR              — read os.environ[VAR]
      file:<absolute-path> — read the file's full contents (rstripped of
                             trailing whitespace, since editors love
                             adding trailing newlines)
      arn:aws:secretsmanager:... — fetch via boto3 secretsmanager:GetSecretValue

    Cloud-only backends (arn:) require boto3 and IAM. Local-only backends
    (env:/file:) are rejected at orchestration emit time for cloud
    pipelines (see service.SyncOrchestration); the runner doesn't
    re-validate, just fetches.
    """
    if ref.startswith("env:"):
        var = ref[len("env:"):]
        if var not in os.environ:
            raise RuntimeError(f"credential env var {var!r} is not set")
        return os.environ[var]
    if ref.startswith("file:"):
        path = ref[len("file:"):]
        with open(path, "r", encoding="utf-8") as f:
            return f.read().rstrip("\n\r")
    if ref.startswith("arn:aws:secretsmanager:"):
        import boto3  # noqa: PLC0415

        # ARN like arn:aws:secretsmanager:eu-north-1:123:secret:foo
        # boto3 accepts the full ARN as SecretId.
        client = boto3.client("secretsmanager")
        resp = client.get_secret_value(SecretId=ref)
        # SecretString first; fall back to SecretBinary for non-text secrets.
        if "SecretString" in resp:
            return resp["SecretString"]
        return resp["SecretBinary"].decode("utf-8")
    raise RuntimeError(f"unknown secret backend in reference {ref!r}")


def _is_hive_partitioned(path: str) -> bool:
    """True when the immediate children of a prefix-style `path` are
    Hive-style partition directories (`name=value/`).

    Such a layout must be read with Spark's default partition discovery
    so the partition keys surface as columns — `recursiveFileLookup`
    finds the files but silently drops those columns (e.g. CloudFront
    logs laid out `…/day=26/hour=NN/` lose `hour`). Best-effort: any
    listing error returns False, falling back to recursiveFileLookup,
    which at least still finds the files.
    """
    try:
        if _is_s3(path):
            import boto3  # noqa: PLC0415

            bucket, key = _split_s3(path)
            if key and not key.endswith("/"):
                key += "/"
            resp = boto3.client("s3").list_objects_v2(
                Bucket=bucket, Prefix=key, Delimiter="/", MaxKeys=100
            )
            children = [
                cp["Prefix"].rstrip("/").rsplit("/", 1)[-1]
                for cp in resp.get("CommonPrefixes", [])
            ]
        else:
            children = [e.name for e in os.scandir(path) if e.is_dir()]
        return any("=" in c for c in children)
    except Exception:  # noqa: BLE001
        return False


def _read_path_format(spark, path: str, fmt: str):
    """Dispatch a Spark read by source format. Defaults match what most users
    expect for ad-hoc CSV/JSON: header on, schema inferred. Anything more
    specific (custom delimiters, schema enforcement) should ride the source's
    HCL `read_options` once we thread that through — for now, sane defaults.

    recursiveFileLookup is on for prefix-style paths (anything ending in
    "/") so a registered s3 source pointing at e.g. `s3://b/events/`
    picks up `events/2024/jan.json` without users having to encode the
    partition layout in the prefix. It is held OFF for Hive-partitioned
    prefixes (`day=26/hour=NN/…`) so Spark's default partition discovery
    runs and the partition keys surface as columns. Single-file paths
    bypass both so Spark's per-extension dispatch still works.
    """
    recurse = path.endswith("/") and not _is_hive_partitioned(path)
    if fmt in ("parquet", ""):
        reader = spark.read
        if recurse:
            reader = reader.option("recursiveFileLookup", "true")
        return reader.parquet(path)
    if fmt == "csv":
        reader = spark.read.option("header", "true").option("inferSchema", "true")
        if recurse:
            reader = reader.option("recursiveFileLookup", "true")
        return reader.csv(path)
    if fmt == "json" or fmt == "ndjson":
        reader = spark.read
        if recurse:
            reader = reader.option("recursiveFileLookup", "true")
        return reader.json(path)
    raise RuntimeError(f"unsupported source format {fmt!r} for path {path!r}")


def _read_keys_format(spark, keys: list[str], fmt: str, base_path: str):
    """Read an explicit list of object URIs by source format, deriving Hive
    partition columns from ``base_path`` so a flat or partitioned layout yields
    the same schema a full-prefix read would. Mirrors _read_path_format's format
    dispatch but over a concrete key set (the notification-drain path) instead of
    a single prefix. basePath is what tells Spark to recover partition columns
    (year=/month=/…) from each key's tail relative to the source prefix; without
    it, multi-path reads drop the partition columns and the output schema
    diverges from a listing-mode read (same INCOMPATIBLE_DATA_FOR_TABLE trap the
    partitioned_path branch guards against)."""
    reader = spark.read.option("basePath", base_path)
    if fmt in ("parquet", ""):
        return reader.parquet(*keys)
    if fmt == "csv":
        return reader.option("header", "true").option("inferSchema", "true").csv(*keys)
    if fmt == "json" or fmt == "ndjson":
        return reader.json(*keys)
    raise RuntimeError(f"unsupported source format {fmt!r} for drain keys under {base_path!r}")


def _is_forced(event: Any, node_id: str) -> bool:
    """Decide whether this node's incremental-skip checks are bypassed for
    this run.

    Sources of truth (in priority order):
      - Cloud: ``event["_force"]`` truthy. Optional ``event["_force_nodes"]``
        narrows the bypass to a subset; empty/missing means "every node".
      - Local: ``CLAVESA_FORCE=1`` env var. Optional ``CLAVESA_FORCE_NODES``
        CSV narrows the bypass; empty means "every node".

    Returns False when neither is set — normal incremental dispatch.

    Forced runs still advance watermarks on success. The bypass is one-off;
    the next unforced run resumes normal incremental behavior.
    """
    if os.environ.get("CLAVESA_FORCE") == "1":
        nodes_csv = os.environ.get("CLAVESA_FORCE_NODES", "")
        if not nodes_csv:
            return True
        return node_id in [n.strip() for n in nodes_csv.split(",") if n.strip()]
    if isinstance(event, dict):
        force = event.get("_force")
        force_nodes = event.get("_force_nodes") or []
        if not force:
            # Cloud path: SFN execution input lands under _execution_input
            # on the per-Lambda event (see tfgen.go:519).
            exec_input = event.get("_execution_input")
            if isinstance(exec_input, dict):
                force = exec_input.get("_force")
                if not force_nodes:
                    force_nodes = exec_input.get("_force_nodes") or []
        if force:
            if not force_nodes:
                return True
            return node_id in force_nodes
    return False


def _drain_if_configured(spark, alias: str, src: dict[str, Any], bucket: str, prefix: str):
    """Notification-drain ingest (Auto-Loader style), shared by the partitioned
    and flat s3 read paths. When the descriptor carries a non-empty `queue_url`,
    the source's SQS queue (fed by S3 Object Created events) is the cursor: drain
    it for new object keys, read exactly those objects, and delete the messages
    after the Delta write commits (post-commit ack hook in _run_transform). No
    partition-tree listing, no watermark — the queue replaces both, and it works
    the same for flat and partitioned layouts (a flat source otherwise re-reads
    its whole prefix every run, so it benefits most). Semantics are at-least-once:
    a crash before delete redelivers the message and downstream dedup absorbs it.

    Returns None when no `queue_url` is set — the caller falls back to listing.
    Otherwise returns the (df, advance) pair for _resolve_input to return:
    (None, None) when the queue is empty (skips the run), else (df, ack-record).
    """
    queue_url = str(src.get("queue_url") or "")
    if not queue_url:
        return None
    if not prefix.endswith("/"):
        prefix += "/"
    region = src.get("region") or None
    keys, handles = _drain_source_queue(queue_url, _max_files_per_run(), region=region)
    if not keys:
        print(f"[clavesa] input {alias!r}: drain queue empty; skipping run", file=sys.stderr)
        return None, None
    print(
        f"[clavesa] input {alias!r}: draining {len(keys)} new object(s) from queue",
        file=sys.stderr,
    )
    # basePath = the source prefix so Hive partition columns derive identically
    # to a full-prefix read, even when drained keys sit at varying depths.
    fmt = str(src.get("format") or "parquet").lower()
    base_path = f"s3://{bucket}/{prefix}"
    df = _read_keys_format(spark, keys, fmt, base_path)
    return df, {"ack": {"queue_url": queue_url, "handles": handles, "region": region}}


def _resolve_input(spark, alias: str, src: Any, backfill: dict[str, Any] | None = None, forced: bool = False) -> tuple[Any, dict[str, Any] | None]:
    """Returns (DataFrame, watermark_advance_record).

    watermark_advance_record is None when the input doesn't track a watermark
    (string-form path, Iceberg table, or empty partitioned source). When set,
    it carries {"uri": ..., "new_cursor": (...,)} for the runner to commit
    after all outputs succeed.

    `backfill`, when set, switches the partitioned_path branch to read the
    closed [from_cursor, to_cursor] window instead of the watermark-based
    incremental window. The watermark is NEITHER read nor advanced — backfill
    runs go to a parallel staging table that the user inspects + promotes
    separately, leaving production state untouched.

    `forced=True` (set by the caller when the node is in this run's force
    set) bypasses the no-new-data skip on both incremental kinds
    (``partitioned_path`` cursor + ``delta_table_cdf`` version). A forced run
    re-reads the full source range; watermarks still advance on success.
    """
    if isinstance(src, str):
        if _looks_like_path(src):
            return spark.read.parquet(src), None
        return spark.table(src), None

    if not isinstance(src, dict):
        raise TypeError(f"input {alias!r}: unsupported descriptor type {type(src).__name__}")

    kind = src.get("kind", "")
    if kind == "http":
        # ADR-017 slice 1+2: workspace source registry, http kind. Slice 2
        # adds optional header auth resolved at request time from the
        # workspace credentials registry. The orchestration emitter
        # inlines header_name + value_prefix; the runner resolves the
        # secret reference (env: / file: / arn:) and assembles the
        # header value before issuing the request.
        url = src["url"]
        fmt = str(src.get("format") or "parquet").lower()
        headers = _resolve_http_headers(src.get("credentials"))
        local_path = _download_http_to_tmp(url, headers=headers)
        return _read_path_format(spark, local_path, fmt), None

    if kind == "s3":
        # ADR-017 slice 3: same-account S3 reads via Spark's S3A.
        # The Spark builder in `_spark()` already maps the s3:// scheme
        # onto S3AFileSystem with DefaultAWSCredentialsProviderChain,
        # so the credentials picked up here come from (in order) env
        # vars, ~/.aws, EC2 / Lambda instance role. Cross-account reads
        # via assume-role land in a later slice.
        bucket = src["bucket"]
        prefix = str(src.get("prefix") or "")

        # A flat s3 source re-reads its whole prefix every run; notification-drain
        # (when the descriptor carries a queue_url) turns that into "read only the
        # new objects", the single biggest re-read this eliminates.
        drained = _drain_if_configured(spark, alias, src, bucket, prefix)
        if drained is not None:
            return drained

        path = f"s3://{bucket}/{prefix}" if prefix else f"s3://{bucket}/"
        fmt = str(src.get("format") or "parquet").lower()
        return _read_path_format(spark, path, fmt), None

    if kind == "path":
        # `clavesa pipeline run` emits this when the upstream source
        # declares a non-Parquet format. Routes the read by the source's
        # declared `format` attr — without this, every CSV/JSON source would
        # fail at footer-read time pretending to be Parquet.
        path = src["path"]
        fmt = str(src.get("format") or "parquet").lower()
        return _read_path_format(spark, path, fmt), None

    if kind == "delta_table_cdf":
        # v2.0.0 (ADR-018): CDF-based incremental read on a Delta upstream.
        # Same watermark machinery as `partitioned_path` (best-effort advance
        # after outputs commit); the cursor is a Delta commit version
        # (int, JSON-serialised as string) instead of an Iceberg snapshot id.
        #
        # Descriptor shape:
        #   {"kind": "delta_table_cdf",
        #    "table": "<db>.<table>",
        #    "alias": "<consumer_node>__<input_alias>",
        #    "merge_keys": ["k1", "k2"]}  # optional
        #
        # alias scopes the watermark file so two consumers reading the
        # same upstream don't share state.
        from pyspark.sql import functions as F  # noqa: PLC0415

        table = src["table"]
        alias_key = str(src.get("alias") or alias)
        merge_keys = [str(k) for k in (src.get("merge_keys") or [])]

        history_df = spark.sql(f"DESCRIBE HISTORY {table}")
        head_row = (
            history_df.orderBy(F.col("version").desc()).select("version").limit(1).first()
        )
        if head_row is None:
            print(
                f"[clavesa] input {alias!r}: upstream {table} has no commits yet; skipping run",
                file=sys.stderr,
            )
            return None, None
        current_version = int(head_row[0])
        watermark_uri = _watermark_uri(alias_key)
        stored = _read_watermark(watermark_uri)
        last_version: int | None = None
        if stored is not None and len(stored) == 1:
            try:
                last_version = int(stored[0])
            except ValueError:
                last_version = None
        if last_version is None:
            # First run for this (consumer, upstream) pair: full read,
            # advance watermark to current_version.
            print(
                f"[clavesa] input {alias!r}: first incremental run on {table}; reading full snapshot at version {current_version}",
                file=sys.stderr,
            )
            return spark.table(table), {
                "uri": watermark_uri,
                "new_cursor": (str(current_version),),
            }
        if last_version == current_version:
            if forced:
                print(
                    f'[clavesa] node "{os.environ.get("CLAVESA_NODE", "")}": force-run, ignoring incremental cursor (Delta upstream {table} unchanged at version {current_version}); re-reading full snapshot',
                    file=sys.stderr,
                    flush=True,
                )
                # Full re-read; watermark stays pinned to current_version
                # (advancing to the same value is a no-op but the writer
                # tolerates it — keeps the post-success path uniform).
                return spark.table(table), {
                    "uri": watermark_uri,
                    "new_cursor": (str(current_version),),
                }
            print(
                f"[clavesa] input {alias!r}: upstream {table} unchanged since version {current_version}; skipping run",
                file=sys.stderr,
            )
            return None, None
        if last_version > current_version:
            # Upstream was rewritten (DROP + CREATE resets Delta's version
            # counter to zero). Delta versions are monotonic linear
            # integers — no parent-chain DAG to walk, just compare. Fall
            # back to full read + watermark reset, same semantic as the
            # Iceberg-side "orphan snapshot" fallback.
            print(
                f"[clavesa] input {alias!r}: upstream {table} was rewritten "
                f"(watermark version {last_version} > current {current_version}); "
                f"falling back to full read and re-stamping watermark",
                file=sys.stderr,
            )
            return spark.table(table), {
                "uri": watermark_uri,
                "new_cursor": (str(current_version),),
            }
        # CDF read over (last_version, current_version]. Delta's
        # readChangeFeed `startingVersion` is INCLUSIVE; offset by +1 to
        # get open-closed semantics that match the prior Iceberg snapshot
        # range and don't re-emit rows already consumed at last_version.
        df = (
            spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", last_version + 1)
            .option("endingVersion", current_version)
            .table(table)
        )
        # Keep post-image rows only. For mode=merge upstreams, MERGE emits
        # update_preimage + update_postimage pairs; we want the post-image.
        # For mode=append upstreams, only `insert` rows surface. DELETE
        # rows would surface for explicit DELETE upstreams but clavesa
        # doesn't author those today.
        df = df.where(F.col("_change_type").isin("insert", "update_postimage"))
        if merge_keys:
            # Dedupe to latest row per merge key within the CDF range. The
            # same key may have been updated multiple times across the
            # range (e.g. cloudfront-analytics silver re-MERGEs the same
            # request_id when subsequent log lines mutate the row);
            # row_number() over _commit_version DESC keeps the freshest.
            # Delta's commit ordering is the natural tie-breaker; the
            # v1.x `recency_column` design is obsolete under CDF.
            from pyspark.sql import Window  # noqa: PLC0415

            w = Window.partitionBy(*merge_keys).orderBy(F.col("_commit_version").desc())
            df = (
                df.withColumn("__clavesa_rn", F.row_number().over(w))
                .where("__clavesa_rn = 1")
                .drop("__clavesa_rn")
            )
            print(
                f"[clavesa] input {alias!r}: deduped CDF range on {merge_keys} by _commit_version",
                file=sys.stderr,
            )
        # Strip CDF metadata columns so the user transform sees the
        # natural table shape, not the CDC envelope.
        df = df.drop("_change_type", "_commit_version", "_commit_timestamp")
        print(
            f"[clavesa] input {alias!r}: reading {table} CDF range ({last_version}, {current_version}]",
            file=sys.stderr,
        )
        return df, {
            "uri": watermark_uri,
            "new_cursor": (str(current_version),),
        }

    if kind == "partitioned_path":
        path = src["path"]
        partition_names = list(src.get("partitions") or [])
        start_from = str(src.get("start_from") or "all")
        if not path.startswith("s3://"):
            raise RuntimeError(f"partitioned_path inputs must be s3://...; got {path!r}")
        bucket, prefix = _split_s3(path)
        if not prefix.endswith("/"):
            prefix += "/"

        # Backfill overrides the live read entirely: read the explicit
        # [from, to] partition window by listing the tree. This must run
        # BEFORE the notification-drain check — a backfill against a
        # trigger-enabled source must not short-circuit on an empty drain
        # queue (the historical window is independent of the live queue).
        if backfill is not None:
            all_partitions = _list_partition_tree(bucket, prefix, partition_names)
            from_cursor = tuple(backfill.get("from_cursor") or ())
            to_cursor = tuple(backfill.get("to_cursor") or ())
            if not from_cursor or not to_cursor:
                raise RuntimeError(f"input {alias!r}: backfill requires both from_cursor and to_cursor")
            new_partitions = [(c, p) for c, p in all_partitions if from_cursor <= c <= to_cursor]
            if not new_partitions:
                print(f"[clavesa] input {alias!r}: 0 partitions in backfill window [{from_cursor!r}, {to_cursor!r}]; skipping run", file=sys.stderr)
                return None, None
            paths = [f"s3://{bucket}/{leaf}" for _, leaf in new_partitions]
            print(f"[clavesa] input {alias!r}: backfill reading {len(paths)} partitions ({new_partitions[0][0]} → {new_partitions[-1][0]})", file=sys.stderr)
            df = spark.read.option("basePath", f"s3://{bucket}/{prefix}").parquet(*paths)
            return df, None

        # The queue takes over only AFTER the first listed run has committed
        # a watermark — `start_from` governs the first run. Objects that
        # predate the queue never produced S3 events, so a fresh deploy that
        # drained unconditionally would skip forever on an empty queue and
        # never ingest the pre-existing files. A forced run also bypasses the
        # queue and falls through to the listing branch's full-range re-read;
        # it deliberately does NOT consume queue messages — the next unforced
        # drain redelivers those keys (at-least-once, absorbed by replace/
        # merge outputs, same documented semantics as watermark re-reads).
        watermark_uri = _watermark_uri(alias)
        cursor = _read_watermark(watermark_uri)
        if cursor is not None and not forced:
            drained = _drain_if_configured(spark, alias, src, bucket, prefix)
            if drained is not None:
                return drained

        all_partitions = _list_partition_tree(bucket, prefix, partition_names)

        if cursor is None:
            cursor = _resolve_initial_cursor(start_from, all_partitions)

        if cursor is None:
            new_partitions = all_partitions
        else:
            new_partitions = [(c, p) for c, p in all_partitions if c > cursor]

        if not new_partitions:
            if forced and all_partitions:
                print(
                    f'[clavesa] node "{os.environ.get("CLAVESA_NODE", "")}": force-run, ignoring incremental cursor (0 new partitions since {cursor!r}); re-reading full source range',
                    file=sys.stderr,
                    flush=True,
                )
                # Re-read everything under the prefix; watermark advances to
                # the latest known partition so the next unforced run resumes
                # normally from there. If there are literally zero partitions
                # at all we still skip (nothing to read).
                new_partitions = all_partitions
            else:
                print(f"[clavesa] input {alias!r}: 0 new partitions since cursor {cursor!r}; skipping run", file=sys.stderr)
                return None, None

        new_max = new_partitions[-1][0]
        paths = [f"s3://{bucket}/{leaf}" for _, leaf in new_partitions]
        print(f"[clavesa] input {alias!r}: reading {len(paths)} partitions ({new_partitions[0][0]} → {new_max})", file=sys.stderr)
        # basePath tells Spark to derive the Hive partition columns
        # (year/month/day/hour/…) from each leaf path's tail relative to
        # the prefix. Without it, multi-path reads drop the partition
        # columns entirely and the output table schema diverges from a
        # full-prefix read — appending to a table created in full-read
        # mode then fails with INCOMPATIBLE_DATA_FOR_TABLE.
        df = spark.read.option("basePath", f"s3://{bucket}/{prefix}").parquet(*paths)
        return df, {"uri": watermark_uri, "new_cursor": new_max}

    raise RuntimeError(f"input {alias!r}: unknown kind {kind!r}")


_MERGE_PRIMITIVES = {
    "additive": "target.`{c}` + source.`{c}`",
    "min": "least(target.`{c}`, source.`{c}`)",
    "max": "greatest(target.`{c}`, source.`{c}`)",
    "sketch": "hll_union(target.`{c}`, source.`{c}`)",
}


def _merge_set_clause(source_cols: list[str], merge_keys: list[str], merge_update: dict[str, str]) -> str:
    """Build the `WHEN MATCHED THEN UPDATE SET ...` assignments for an
    aggregate-aware merge. Each source column (except merge keys) is set:
    columns named in merge_update use their primitive expr (additive / min /
    max / sketch) or a raw SparkSQL expression; all others replace from
    source. Uses the `target` / `source` MERGE aliases.

    Contract for additive schema evolution (GH #61): the iteration is over
    SOURCE columns, so a newly-added source column not named in merge_update
    still gets a `target.c = source.c` assignment. The write loop ALTERs the
    target to add missing columns BEFORE the MERGE — Delta resolves explicit
    assignments against the current target schema, so without the ALTER a
    new column fails analysis (DELTA_MERGE_UNRESOLVED_EXPRESSION); `MERGE
    WITH SCHEMA EVOLUTION` does not rescue it."""
    keys = set(merge_keys)
    parts = []
    for c in source_cols:
        if c in keys:
            continue
        spec = merge_update.get(c)
        if spec in _MERGE_PRIMITIVES:
            expr = _MERGE_PRIMITIVES[spec].format(c=c)
        elif spec:
            expr = spec  # raw SparkSQL expression, references target./source.
        else:
            expr = f"source.`{c}`"
        parts.append(f"target.`{c}` = {expr}")
    return ", ".join(parts)


_MERGE_BOUND_IN_THRESHOLD = 200

# Delta liquid clustering accepts at most 4 clustering columns. A merge key
# wider than that (e.g. a 5-column utm_* composite) is capped to its first 4,
# which still co-locates the MERGE target well. The MERGE scan-bound uses the
# same cap: columns beyond the first 4 of the effective clustering are never
# skipping columns, so bounding them costs a collect job and prunes nothing.
_MAX_CLUSTER_COLS = 4


def _sql_lit(v):
    """Render a Python value as a Spark-SQL literal, or None if the type is
    one we won't safely render (caller then skips bounding on that column).

    Pure: stdlib + the passed-in value only, no Spark."""
    import decimal  # noqa: PLC0415
    import math  # noqa: PLC0415

    if v is None:
        return None
    # bool BEFORE int (bool is an int subclass).
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    # datetime BEFORE date (datetime is a date subclass).
    if isinstance(v, _dt.datetime):
        return "TIMESTAMP '" + v.isoformat(sep=" ") + "'"
    if isinstance(v, _dt.date):
        return "DATE '" + v.isoformat() + "'"
    if isinstance(v, str):
        # Escape backslashes BEFORE doubling quotes: Spark's default parser
        # (escapedStringLiterals=false) processes C-style backslash escapes
        # inside string literals, so an unescaped '\' would render a literal
        # that is NOT equal to the value — the bound would then exclude a
        # genuinely matching target row and the MERGE would silently
        # duplicate it (GH #70).
        return "'" + v.replace("\\", "\\\\").replace("'", "''") + "'"
    if isinstance(v, float):
        return repr(v) if math.isfinite(v) else None
    if isinstance(v, decimal.Decimal):
        # Non-finite decimals str() to bare tokens (NaN, Infinity) that Spark
        # parses as column references — skip them like non-finite floats.
        return str(v) if v.is_finite() else None
    return None


def _bound_predicate_sql(col, values, *, threshold=_MERGE_BOUND_IN_THRESHOLD, force_between=False):
    """Build a static target-only scan-bound predicate for one column from the
    distinct NON-NULL source values already collected to the driver.

    - If ANY value fails to render (_sql_lit() -> None) -> return None (skip
      the column entirely). A partially rendered bound is unsound: e.g. a NaN
      float key CAN match in the un-bounded MERGE (Spark treats NaN = NaN as
      true), so an IN list built from only the finite siblings would exclude
      that target row and silently duplicate it. A wider bound is always
      correct; a partial one is not (GH #70).
    - If there are no values at all -> return None (never emit IN ()).
    - If force_between is False and count <= threshold ->
      "target.`col` IN (lit, lit, ...)".
    - Else -> "target.`col` BETWEEN <min-lit> AND <max-lit>" using min()/max()
      over the python values (compare values, not strings). If min/max can't
      be computed (mixed/untypeable) -> None.

    `force_between` lets the caller pass the table-batch's TRUE (min, max) when
    the collected sample was truncated, so the BETWEEN bound is not derived
    from a partial sample. Pure: stdlib + values only, no Spark."""
    values = list(values)
    lits = []
    for v in values:
        lit = _sql_lit(v)
        if lit is None:
            return None
        lits.append(lit)
    if not lits:
        return None
    quoted = "`" + col + "`"
    if not force_between and len(lits) <= threshold:
        return f"target.{quoted} IN (" + ", ".join(lits) + ")"
    try:
        mn = min(values)
        mx = max(values)
    except TypeError:
        return None
    mn_lit = _sql_lit(mn)
    mx_lit = _sql_lit(mx)
    if mn_lit is None or mx_lit is None:
        return None
    return f"target.{quoted} BETWEEN {mn_lit} AND {mx_lit}"


def _merge_bound_cols(merge_keys, cluster_by, bound_by=None):
    """Columns to apply the static MERGE scan-bound on.

    Tier 1: merge_keys that are also skipping columns. For a merge output the
    effective clustering is `cluster_by or merge_keys` (the runner clusters a
    merge table by its merge_keys when cluster_by is unset — see the write
    loop's `cluster_cols`), capped at _MAX_CLUSTER_COLS to match what
    _create_delta_table actually clusters — columns beyond the cap can never
    prune, so bounding them would only cost collect jobs.

    Tier 2: every column in `bound_by` is appended unconditionally (even if it
    isn't in cluster_by) — it's the author's explicit assertion that the column
    is functionally determined by the merge keys. If it isn't actually a
    clustering/skipping column it just won't prune (still safe).

    Tier-1 columns come first (in merge_keys order), then the extra bound_by
    columns (in declared order). De-dup; preserve order. Pure: no Spark."""
    skipping = set((cluster_by or merge_keys)[:_MAX_CLUSTER_COLS])
    seen = set()
    out = []
    for k in merge_keys:
        if k in skipping and k not in seen:
            seen.add(k)
            out.append(k)
    for b in bound_by or []:
        if b not in seen:
            seen.add(b)
            out.append(b)
    return out


def _schema_drifted(existing_schema, new_schema) -> bool:
    """True when two Spark schemas differ in ordered (name, type) shape —
    added/removed/renamed columns, type changes, or reorders. Nullability is
    deliberately ignored: user output tables are created from DataFrame
    writes and stay nullable, so a nullability-only delta never needs a
    schema overwrite. Pure: works on any iterable of objects exposing
    ``.name`` and ``.dataType.simpleString()`` (no Spark import)."""

    def shape(schema):
        return [(f.name, f.dataType.simpleString()) for f in schema]

    return shape(existing_schema) != shape(new_schema)


def _resolve_output(key: str, dest: Any, all_outputs: dict | None = None) -> dict[str, Any]:
    """Returns {kind: "path"|"delta_table", target: str, mode: "replace"|"append"|"merge", merge_keys: [...], merge_update: {...}, cluster_by: [...], bound_by: [...]}.

    String forms map to the existing semantics:
      "" or "<id>" → delta_table, mode=replace
      "/path" or "s3://..." → path, mode=replace
    Dict form (v0.12+) carries an explicit mode. `mode = "merge"` requires
    a non-empty `merge_keys` list naming the columns that uniquely identify
    a row in the target.

    `all_outputs` is the full output dict from the transform — used to detect
    the single-output-default case so `_table_id_for` can drop the `__default`
    suffix (ADR-019).
    """
    if isinstance(dest, str):
        if dest and _looks_like_path(dest):
            return {"kind": "path", "target": dest, "mode": "replace", "merge_keys": [], "merge_update": {}, "cluster_by": []}
        target = dest if dest else _table_id_for(key, all_outputs)
        return {"kind": "delta_table", "target": target, "mode": "replace", "merge_keys": [], "merge_update": {}, "cluster_by": []}

    if not isinstance(dest, dict):
        raise TypeError(f"output {key!r}: unsupported descriptor type {type(dest).__name__}")

    kind = dest.get("kind", "delta_table")
    merge_keys = list(dest.get("merge_keys") or [])
    # When merge_keys is declared and mode is unset, default to merge —
    # saves users from picking the right semantics every time.
    mode = dest.get("mode") or ("merge" if merge_keys else "replace")
    if mode not in ("replace", "append", "merge"):
        raise RuntimeError(f"output {key!r}: unsupported mode {mode!r}")
    if mode == "merge" and not merge_keys:
        raise RuntimeError(f"output {key!r}: mode='merge' requires non-empty merge_keys")
    target = dest.get("target") or dest.get("table_id") or dest.get("path") or ""
    if kind == "delta_table" and not target:
        target = _table_id_for(key, all_outputs)
    stats = bool(dest.get("stats"))
    cluster_by = list(dest.get("cluster_by") or [])
    bound_by = list(dest.get("bound_by") or [])
    merge_update = dict(dest.get("merge_update") or {})
    return {
        "kind": kind,
        "target": target,
        "mode": mode,
        "merge_keys": merge_keys,
        "merge_update": merge_update,
        "stats": stats,
        "cluster_by": cluster_by,
        "bound_by": bound_by,
    }


def _glue_db() -> str:
    """Encode the (workspace_catalog, pipeline_schema) env pair into the
    flat Glue Data Catalog database name the runner writes to.

    Mirrors `internal/identutil.EncodeGlueDatabase` on the Go side — the
    Python and Go encoders MUST stay byte-identical, since the runner
    writes to the same Glue DB the catalog handler reads from.

    Both CLAVESA_CATALOG and CLAVESA_SCHEMA are required as of v0.18.0.
    CLAVESA_SCHEMA falls back to a sanitized CLAVESA_PIPELINE only as a
    defensive last resort — orchestration always sets both.

    ADR-019's three-level native ``<catalog>.<schema>.<table>`` addressing
    is blocked on Delta 4.0's session-only ``DeltaCatalog``; Slice 4
    instead moves the local on-disk layout to ``<warehouse>/<catalog>/
    <schema>/<table>/`` while keeping this flat encoded form as the
    in-metastore DB name.
    """
    catalog = os.environ["CLAVESA_CATALOG"]
    schema = os.environ.get("CLAVESA_SCHEMA") or os.environ.get("CLAVESA_PIPELINE", "default")
    return f"{catalog.replace('-', '_')}__{schema.replace('-', '_')}"


def _sanitize_ident(name: str) -> str:
    """Mirror of internal/identutil.Sanitize — dashes become underscores
    so Glue's ``[A-Za-z_][A-Za-z0-9_]*`` constraint is satisfied at the
    table-name segment of the Spark identifier ``_table_id_for`` builds.
    Catalog / schema parts are sanitized by ``_glue_db`` separately."""
    return name.replace("-", "_")




# Workspace system observability schema (ADR-016 "Workspace system
# catalog"). Hard-coded here because the schema name is a domain-grouping
# convention, not a per-pipeline knob — every pipeline writes to the
# same `pipelines` schema under the system catalog, distinguished by the
# `pipeline` column on each row. Future system schemas (`query`,
# `billing`, `access`) introduce their own constants when they ship.
_SYSTEM_SCHEMA = "pipelines"


def _system_glue_db() -> str:
    """Encode the workspace system catalog into its Glue DB name —
    `<system_catalog>__pipelines`. Mirrors the user-DB encoder above so
    Glue's flat-namespace flavor of the three-level address matches what
    the workspace module creates (`aws_glue_catalog_database.system_pipelines`).

    Falls back to `<catalog>__pipelines` if CLAVESA_SYSTEM_CATALOG is
    unset — defensive only; orchestration always sets it for v0.20+.
    """
    system_catalog = os.environ.get("CLAVESA_SYSTEM_CATALOG")
    if not system_catalog:
        system_catalog = os.environ["CLAVESA_CATALOG"] + "_system"
    return f"{system_catalog.replace('-', '_')}__{_SYSTEM_SCHEMA}"


def _ensure_database(spark, db_part: str) -> None:
    """``CREATE DATABASE IF NOT EXISTS`` with a ``LOCATION`` clause that
    pins on-disk layout.

    Hive metastore federation to Glue (sub-slice 15) registers a new DB
    with an empty LOCATION when ``CREATE DATABASE IF NOT EXISTS <db>``
    runs without an explicit LOCATION; the subsequent ``saveAsTable``
    then trips ``IllegalArgumentException: Can not create a Path from
    an empty string`` while computing the table's default path under
    the DB's warehouse dir. Pinning the LOCATION at create time avoids
    that.

    ADR-019 Slice 4 moves the local-mode on-disk layout from
    ``<base>/<catalog>__<schema>.db/`` to ``<base>/<catalog>/<schema>/``
    while keeping the in-metastore database name as the flat
    ``<catalog>__<schema>`` form (Delta's V2 multi-catalog support is
    blocked on a Delta 4.0 limitation — see spark_conf.py). The two
    pieces meet at LOCATION: the metastore still names the DB
    ``<catalog>__<schema>`` but every table inside it lands at the new
    nested path. Slice 5's cloud Glue V2 cutover then encodes the same
    shape in Glue's catalog tree.

    Cloud (``s3://`` warehouse) keeps the legacy ``.db`` suffix so the
    Glue Hive client's ``GetDatabase`` lookup, which expects
    ``<base>/<db>.db/``, still resolves.

    Idempotent: if the DB already exists, ``IF NOT EXISTS`` skips the
    create entirely and the LOCATION clause is ignored.
    """
    system_db = _system_glue_db()
    if db_part == system_db:
        base = os.environ.get("CLAVESA_SYSTEM_WAREHOUSE") or os.environ.get(
            "CLAVESA_WAREHOUSE", ""
        )
    else:
        base = os.environ.get("CLAVESA_WAREHOUSE", "")
    base = base.rstrip("/")
    if base:
        location = _v2_layout_path(base, db_part)
        spark.sql(
            f"CREATE DATABASE IF NOT EXISTS {db_part} LOCATION '{location}'"
        )
    else:
        # Preview / no warehouse configured: spark.sql.warehouse.dir is
        # the fallback and Hive's local-warehouse resolution kicks in.
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_part}")


def _create_delta_table(df, table_id: str, cluster_by: list[str]) -> None:
    """Create a managed Delta table. When ``cluster_by`` is non-empty the
    table is created with Delta liquid clustering on those columns via
    ``CREATE TABLE ... CLUSTER BY (...) AS SELECT`` (the OSS-Delta path;
    DataFrameWriterV2.clusterBy translates to an unsupported partition
    transform). Clustering is set at creation and preserved by later
    overwrite/append/MERGE writes. Empty ``cluster_by`` falls back to a
    plain overwrite create."""
    keys = cluster_by[:_MAX_CLUSTER_COLS]
    if keys:
        if len(cluster_by) > _MAX_CLUSTER_COLS:
            print(
                f"[clavesa] {table_id}: {len(cluster_by)} merge keys exceed Delta's "
                f"{_MAX_CLUSTER_COLS}-column clustering limit; clustering by the first "
                f"{_MAX_CLUSTER_COLS}: {keys}",
                file=sys.stderr,
                flush=True,
            )
        spark = df.sparkSession
        view = "__create_src_" + table_id.replace(".", "_").replace("-", "_")
        df.createOrReplaceTempView(view)
        cols = ", ".join(f"`{c}`" for c in keys)
        spark.sql(
            f"CREATE TABLE {table_id} USING delta CLUSTER BY ({cols}) "
            f"AS SELECT * FROM {view}"
        )
    else:
        df.write.format("delta").mode("overwrite").saveAsTable(table_id)


def _sync_glue_table_schema(table_id: str, schema, location: str | None = None) -> None:
    """Write the real Delta schema into a Glue table's ``StorageDescriptor.Columns``.

    Spark's ``saveAsTable`` registers the Glue table with a generic
    datasource stub (one ``col array<string>`` column) because the real
    schema lives in the Delta ``_delta_log/``, not in Glue. The stub
    schema is what Athena's table browser, autocomplete,
    ``information_schema.columns``, external/BI consumers, and Lake
    Formation all read. AWS docs say a columns-populated Delta
    registration relies on ``spark.sql.sources.provider=delta`` (which
    Spark already sets) and should NOT carry ``table_type`` — so this
    helper writes the genuine columns and strips any stale ``table_type``
    parameter a prior run left behind.

    No-op unless the warehouse is an ``s3://`` path (cloud / Glue mode);
    local Hadoop-catalog runs never touch Glue. ``table_id`` is the
    two-segment ``<glue_db>.<table>`` Spark identifier; ``<glue_db>`` is
    the Glue DatabaseName and the last segment the Glue table Name.
    ``schema`` is the written PySpark ``StructType``;
    ``dataType.simpleString()`` yields Hive/Glue-compatible type strings
    (``decimal(p,s)``, ``timestamp``, ``date``, nested ``array<…>`` /
    ``map<…>`` / ``struct<…>``).

    Best-effort: idempotent (skips the update when Glue's columns already
    equal the computed ones), preserves the existing TableInput
    (StorageDescriptor location + serde + format, PartitionKeys,
    parameters, …), and never raises — a sync failure logs to stderr and
    the next write retries it. Region comes from the default boto3 session
    (Lambda provides it).

    ``location`` (cloud system-table callers only): when not None, the
    sync also REPAIRS the registration's ``StorageDescriptor.Location`` to
    this path and stamps ``spark.sql.sources.provider=delta`` /
    ``spark.sql.partitionProvider=catalog``. This fixes the orphaned
    system-table registration: ``.option("path", …)`` through the Glue
    catalog leaves the SD.Location at an empty ``…-__PLACEHOLDER__`` stub
    and a null provider, so Athena / the catalog UI (which read SD.Location)
    can't find the Delta log even though Spark/Delta reads resolve via Delta
    metadata. With ``location`` set the early-return short-circuit ALSO
    requires Location and provider to already match, so a later run still
    repairs them even when the columns already line up.
    ``location=None`` (the user-output-table callers) preserves the
    existing Location/provider untouched.
    """
    warehouse = os.environ.get("CLAVESA_WAREHOUSE", "")
    if not warehouse.startswith("s3://"):
        return
    try:
        import boto3  # noqa: PLC0415

        database = table_id.rsplit(".", 1)[0]
        name = table_id.rsplit(".", 1)[1]
        columns = [
            {"Name": f.name, "Type": f.dataType.simpleString()}
            for f in schema.fields
        ]

        glue = boto3.client("glue")
        table = glue.get_table(DatabaseName=database, Name=name)["Table"]

        existing_sd = dict(table.get("StorageDescriptor") or {})
        existing_cols = existing_sd.get("Columns") or []
        # Compare by ordered (Name, Type) pairs — ignore Comment/Parameters
        # keys Glue may attach to each column.
        existing_pairs = [(c.get("Name"), c.get("Type")) for c in existing_cols]
        computed_pairs = [(c["Name"], c["Type"]) for c in columns]
        columns_match = existing_pairs == computed_pairs
        if location is None:
            if columns_match:
                return  # Glue already carries the real schema — nothing to do
        else:
            # System-table repair path: only short-circuit when columns AND
            # Location AND the delta provider are all already in place —
            # otherwise a later run must still fix an orphaned registration.
            existing_provider = (table.get("Parameters") or {}).get(
                "spark.sql.sources.provider"
            )
            if (
                columns_match
                and existing_sd.get("Location") == location
                and existing_provider == "delta"
            ):
                return

        new_sd = dict(existing_sd)
        new_sd["Columns"] = columns
        if location is not None:
            new_sd["Location"] = location

        # Drop table_type: the columns-populated Delta registration uses
        # spark.sql.sources.provider=delta (set by Spark), and AWS docs say
        # NOT to set table_type when Glue carries real columns.
        params = {
            k: v
            for k, v in (table.get("Parameters") or {}).items()
            if k != "table_type"
        }
        if location is not None:
            # The orphaned system-table registration carries a null provider
            # and Hive-default SerDe; stamp the Delta provider so Athena /
            # the catalog UI resolve the Delta log at the repaired Location.
            params["spark.sql.sources.provider"] = "delta"
            params.setdefault("spark.sql.partitionProvider", "catalog")

        # Carry forward every settable field of the existing table so the
        # update only rewrites columns + parameters, never clobbering the
        # rest of the registration.
        table_input: dict[str, Any] = {
            "Name": name,
            "StorageDescriptor": new_sd,
            "Parameters": params,
        }
        for field in (
            "Description",
            "Owner",
            "Retention",
            "PartitionKeys",
            "ViewOriginalText",
            "ViewExpandedText",
            "TableType",
            "TargetTable",
        ):
            if field in table:
                table_input[field] = table[field]
        glue.update_table(DatabaseName=database, TableInput=table_input)
    except Exception as exc:  # noqa: BLE001 — best-effort, never crash
        print(
            f"[clavesa] glue schema sync failed for {table_id!r}: {exc!r}",
            file=sys.stderr,
        )


def _v2_layout_path(base: str, db_part: str) -> str:
    """ADR-019 Slice 4 on-disk layout for a Hive-style ``<catalog>__<schema>``
    DB: ``<base>/<catalog>/<schema>`` (no ``.db`` suffix) for local
    warehouses, falling back to the legacy ``<base>/<db_part>.db`` for
    cloud / unsplittable inputs.

    Cloud keeps the ``.db`` suffix because Glue's Hive client expects
    its DB LOCATION to live at ``<warehouse>/<db_name>.db/`` — changing
    it there is out of scope for Slice 4 and tied to Glue catalog
    provisioning in Slice 5.
    """
    if base.startswith("s3://"):
        return f"{base}/{db_part}.db"
    catalog, schema, ok = _split_catalog_schema(db_part)
    if not ok:
        return f"{base}/{db_part}.db"
    return f"{base}/{catalog}/{schema}"


def _split_catalog_schema(db_part: str):
    """Best-effort decode of ``<catalog>__<schema>`` into its parts. The
    ``__`` boundary is the convention `EncodeGlueDatabase` writes. Returns
    (_, _, False) when the boundary isn't present so callers can fall
    back to the legacy single-segment layout."""
    idx = db_part.find("__")
    if idx < 0:
        return "", "", False
    return db_part[:idx], db_part[idx + 2:], True


def _table_id_for(output_key: str, all_outputs: dict | None = None) -> str:
    """Auto-generated Delta table identifier for a transform output.

    Two-segment Spark identifier ``<glue_db>.<table>``. ``<glue_db>``
    comes from ``_glue_db()`` (ADR-016's flat-encoded
    ``<catalog>__<schema>``). The table part is ``<node>`` for the
    single-default-output case (ADR-019 Slice 3) and
    ``<node>__<output_key>`` otherwise.

    The three-level native ``<catalog>.<schema>.<table>`` shape ADR-019
    targets requires Delta V2 multi-catalog support, which Delta 4.0
    doesn't ship outside the session catalog (see spark_conf.py). The
    on-disk layout still moves to the V2 tree via ``_ensure_database``'s
    LOCATION clause, so reads + writes only need to agree on the flat
    DB name in the Hive metastore.
    """
    node = _sanitize_ident(os.environ.get("CLAVESA_NODE", "node"))
    if (
        output_key == "default"
        and isinstance(all_outputs, dict)
        and list(all_outputs.keys()) == ["default"]
    ):
        return f"{_glue_db()}.{node}"
    return f"{_glue_db()}.{node}__{output_key}"


def _evolve_target_schema(spark, target: str, staging: str) -> list[str]:
    """ALTER TABLE target ADD COLUMN for every column in staging missing
    from target. Returns the list of added column names (empty when the
    schemas already match by name).

    Iceberg supports schema evolution natively — added columns read back
    NULL on existing target rows, populate from the staging values on
    matched / inserted rows. Type widening and column drops are NOT
    handled here; those surface loudly from the downstream MERGE / INSERT
    instead of silently corrupting data.
    """
    target_fields = {f.name: f.dataType for f in spark.table(target).schema}
    staging_schema = spark.table(staging).schema
    added: list[str] = []
    for field in staging_schema:
        if field.name in target_fields:
            continue
        spark.sql(
            f"ALTER TABLE {target} ADD COLUMN {field.name} {field.dataType.simpleString()}"
        )
        added.append(field.name)
    if added:
        print(
            f"[clavesa] backfill_promote: evolved {target} with {len(added)} new column(s): {added}",
            file=sys.stderr,
        )
    return added


def _run_operation(event: dict[str, Any]) -> dict[str, Any]:
    """Execute a non-transform control-plane operation against Iceberg.

    Operations:
      backfill_promote: MERGE INTO target USING staging ON <merge_keys> ...
                       (mode=merge|append + opts.{force_dedup,allow_duplicates})
                       ALTER TABLE target ADD COLUMN for any staging-only
                       columns before the merge — Iceberg schema evolution.
                       Drops staging on success.
      backfill_discard: DROP TABLE staging.
      optimize:      OPTIMIZE table [ZORDER BY (cols)]; compact + cluster.
      cluster_alter: ALTER TABLE table CLUSTER BY (cols) then OPTIMIZE.
      vacuum:        VACUUM table [RETAIN n HOURS]; purge stale files.

    All operations run via SparkSQL — same engine that wrote the staging
    table in the first place, same MERGE semantics the runner already
    uses for `mode = "merge"` outputs in the transform path.
    """
    op = event["_operation"]
    spark = _spark()

    if op == "backfill_discard":
        staging = event["staging"]
        spark.sql(f"DROP TABLE IF EXISTS {staging}")
        return {"status": "ok", "operation": op, "staging_dropped": staging}

    if op == "backfill_promote":
        backfill_props = {
            "clavesa.trigger": "backfill",
            "clavesa.run-id": event.get("_sf_execution_arn") or event.get("run_id") or "",
        }
        _apply_snapshot_props(backfill_props)

        staging = event["staging"]
        target = event["target"]
        mode = event.get("mode", "merge")
        merge_keys = list(event.get("merge_keys") or [])
        force_dedup = event.get("force_dedup", "")
        allow_dupes = bool(event.get("allow_duplicates"))

        # First backfill for this node: no canonical target exists yet (the
        # diff advertises "first backfill creates target"). There's nothing
        # to MERGE into or schema-evolve — create the target from staging
        # the same way the transform path writes a Delta output, then drop
        # staging. Skips the merge/append branches below.
        if not spark.catalog.tableExists(target):
            print(
                f"[clavesa] backfill_promote: target {target} does not exist; creating it from staging",
                file=sys.stderr,
            )
            # overwriteSchema: staging is the new truth for the target,
            # schema included. Normally inert (the tableExists guard means
            # this is a create), but it keeps the promote correct if the
            # guard misfires against a cold metastore and an old-schema
            # table is actually present (GH #39).
            spark.table(staging).write.format("delta").mode("overwrite").option(
                "overwriteSchema", "true"
            ).saveAsTable(target)
            _sync_glue_table_schema(target, spark.table(target).schema)
            spark.sql(f"DROP TABLE IF EXISTS {staging}")
            return {
                "status": "ok",
                "operation": op,
                "target": target,
                "staging_dropped": staging,
                "columns_added": [],
                "created_target": True,
            }

        # Evolve the target schema to absorb any new columns the user added
        # to the transform between the canonical run and the backfill —
        # otherwise SparkSQL's `MERGE … UPDATE SET *` silently drops
        # columns it can't resolve on the target side, and positional
        # `INSERT INTO target SELECT *` errors with arity mismatch.
        # Iceberg supports `ALTER TABLE … ADD COLUMN` natively; existing
        # rows in target read back NULL for the added columns.
        columns_added = _evolve_target_schema(spark, target, staging)

        if mode == "merge":
            if not merge_keys:
                raise RuntimeError("backfill_promote merge: merge_keys required")
            on = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)
            sql = (
                f"MERGE INTO {target} t USING {staging} s ON {on} "
                f"WHEN MATCHED THEN UPDATE SET * "
                f"WHEN NOT MATCHED THEN INSERT *"
            )
            spark.sql(sql)
        elif mode == "append":
            if force_dedup:
                on = f"t.{force_dedup} = s.{force_dedup}"
                sql = (
                    f"MERGE INTO {target} t USING {staging} s ON {on} "
                    f"WHEN MATCHED THEN UPDATE SET * "
                    f"WHEN NOT MATCHED THEN INSERT *"
                )
                spark.sql(sql)
            elif allow_dupes:
                # DataFrame writer with mergeSchema=true so name-based
                # column resolution holds even when the schemas drifted
                # in either direction — positional `INSERT INTO target
                # SELECT *` would error on arity mismatch.
                spark.table(staging).write.format("delta").mode("append").option(
                    "mergeSchema", "true"
                ).saveAsTable(target)
                _sync_glue_table_schema(target, spark.table(target).schema)
            else:
                raise RuntimeError(
                    "backfill_promote append: append-mode targets need force_dedup or allow_duplicates"
                )
        else:
            raise RuntimeError(f"backfill_promote: unsupported mode {mode!r}")

        # Promotion succeeded — drop the staging table so the next backfill
        # for the same node doesn't accumulate stale staging artifacts.
        spark.sql(f"DROP TABLE IF EXISTS {staging}")
        return {
            "status": "ok",
            "operation": op,
            "target": target,
            "staging_dropped": staging,
            "columns_added": columns_added,
        }

    if op == "optimize":
        table = event["table"]
        zorder = list(event.get("zorder") or [])
        sql = f"OPTIMIZE {table}"
        if zorder:
            cols = ", ".join(f"`{c}`" for c in zorder)
            sql += f" ZORDER BY ({cols})"
        spark.sql(sql)
        _refresh_table_snapshot(event.get("record"), table)
        return {"status": "ok", "operation": op, "table": table, "zorder": zorder}

    if op == "cluster_alter":
        table = event["table"]
        cluster_by = list(event.get("cluster_by") or [])
        if not cluster_by:
            raise RuntimeError("cluster_alter requires non-empty cluster_by")
        cols = ", ".join(f"`{c}`" for c in cluster_by)
        # ALTER sets the clustering spec on an existing (possibly unclustered)
        # table; OPTIMIZE then physically re-clusters the current data. This
        # is the migration path for tables created before liquid clustering.
        spark.sql(f"ALTER TABLE {table} CLUSTER BY ({cols})")
        spark.sql(f"OPTIMIZE {table}")
        _refresh_table_snapshot(event.get("record"), table)
        return {"status": "ok", "operation": op, "table": table, "cluster_by": cluster_by}

    if op == "vacuum":
        table = event["table"]
        retain = event.get("retain_hours")
        sql = f"VACUUM {table}"
        if retain is not None:
            sql += f" RETAIN {int(retain)} HOURS"
        spark.sql(sql)
        _refresh_table_snapshot(event.get("record"), table)
        return {"status": "ok", "operation": op, "table": table, "retain_hours": retain}

    raise RuntimeError(f"unknown _operation: {op!r}")


def _refresh_table_snapshot(event_record, table_id):
    """After an out-of-band table rewrite (OPTIMIZE / CLUSTER BY / VACUUM),
    re-record the latest commit into the system `tables` table so the
    catalog's snapshot_id does not go stale. Best-effort; a failure logs
    and is swallowed (the data rewrite already succeeded)."""
    if not event_record:
        return
    # _record_table_state / _tables_table_id derive identifiers from these
    # env vars; set them from the event so recording works under both the
    # local runOperation dispatch (which does not set them) and cloud Lambda.
    for env_key, rec_key in (
        ("CLAVESA_CATALOG", "catalog"),
        ("CLAVESA_SYSTEM_CATALOG", "system_catalog"),
        ("CLAVESA_SCHEMA", "schema"),
        ("CLAVESA_PIPELINE", "pipeline"),
        ("CLAVESA_NODE", "node"),
    ):
        v = event_record.get(rec_key)
        if v:
            os.environ[env_key] = str(v)
    try:
        _record_table_state(
            event_record.get("run_id", ""),
            event_record.get("output_key", "default"),
            table_id,
        )
    except Exception as exc:  # noqa: BLE001
        print(
            f"[clavesa] optimize: tables-snapshot refresh failed for {table_id!r}: {exc!r}",
            file=sys.stderr,
        )


def _apply_snapshot_props(props):
    """Stamp clavesa provenance into Delta's commit metadata for this session.

    Sets `spark.databricks.delta.commitInfo.userMetadata` to a JSON-encoded
    copy of ``props`` on the active Spark session. Every Delta commit
    produced after this call — DataFrame writes AND SparkSQL ``MERGE INTO``
    statements alike — carries the same userMetadata payload, surfaced via
    ``DESCRIBE HISTORY <table>``. The session-conf approach is preferred
    over per-writer ``.option("userMetadata", ...)`` because the latter
    doesn't cover MERGE (a SQL statement, not a DataFrameWriter), and we
    want uniform provenance across all write shapes in a single run.

    No-op on empty / None ``props`` — leaves any prior scope's userMetadata
    in place rather than clearing it.
    """
    if not props:
        return
    _spark().conf.set(
        "spark.databricks.delta.commitInfo.userMetadata",
        json.dumps(props),
    )


def _run_transform(event, context, run_id=""):  # noqa: ARG001
    """Inner body of handler() — runs one transform invocation. Extracted so
    handler() can wrap it in timing + node_runs telemetry without nesting
    the transform logic in a try/finally that obscures the data path.

    Event shape (v0.12+):
      {
        "inputs":  {"alias": <descriptor>, ...},
        "outputs": {"key":   <descriptor>, ...}
      }

    Each descriptor is either:
      - string: an S3 URI / local path / Delta table id (existing semantics).
      - dict (input):  {"kind": "partitioned_path", "path": "s3://...",
                        "partitions": [...], "start_from": "..."}
        Runner walks the partition tree, filters by stored watermark, reads
        only new partitions, and advances the watermark on success.
      - dict (output): {"kind": "delta_table", "table_id": "<id>"|"",
                        "mode": "replace"|"append"}
        Mode "append" switches the writer from createOrReplace to append.

    Output routing (per ADR-018):
      - Path (contains "/"): plain Parquet at the destination.
      - Empty / identifier:  Delta table at `<glue_db>.<node>__<key>`,
        where `<glue_db>` follows the ADR-016 `_glue_db()` encoding of
        the (workspace_catalog, pipeline_schema) env pair.

    Skip semantics: when every partitioned input has zero new partitions, the
    handler returns {"status": "skipped"} without invoking the user transform
    or touching outputs. Watermarks unchanged.
    """
    inputs = event.get("inputs", {}) or {}
    outputs = event.get("outputs", {}) or {}
    language = os.environ.get("CLAVESA_LANGUAGE", "sql")
    logic_path = os.environ.get("CLAVESA_LOGIC_S3_PATH")
    if not logic_path:
        raise RuntimeError("CLAVESA_LOGIC_S3_PATH is not set")

    # Backfill mode (Gate 1). When `_backfill.node` matches this Lambda's
    # CLAVESA_NODE, swap the cursor window + redirect outputs to the
    # staging table. Non-target nodes in the same SFN execution see
    # `_backfill.node` mismatch and skip — backfill is per-node by design,
    # not per-pipeline. `_backfill.target_outputs` (a map of output_key →
    # staging table id) overrides the canonical target per output; mode
    # is forced to "replace" so re-runs of the same backfill rewrite the
    # staging cleanly. Watermarks are neither read nor advanced.
    backfill_full = event.get("_backfill")
    my_node = os.environ.get("CLAVESA_NODE", "")
    backfill: dict[str, Any] | None = None
    if isinstance(backfill_full, dict):
        if backfill_full.get("node") == my_node:
            backfill = backfill_full
        else:
            return {"status": "skipped", "reason": f"backfill targets {backfill_full.get('node')!r}, this node is {my_node!r}"}

    # Provenance stamped into every Iceberg snapshot this run writes. A
    # backfill is self-evident from the `_backfill` block; other runs carry a
    # `_trigger` value set by whichever path started them — `manual` (CLI /
    # ad-hoc), `event` (SQS poller), `scheduled` (EventBridge rule). Cloud
    # runs receive the SFN execution input under `_execution_input`; local
    # runs get `_trigger` on the event directly.
    if backfill is not None:
        trigger = "backfill"
    else:
        exec_input = event.get("_execution_input")
        trigger = (
            event.get("_trigger")
            or (exec_input.get("_trigger") if isinstance(exec_input, dict) else None)
            or ""
        )
    snapshot_props = {"clavesa.trigger": trigger, "clavesa.run-id": run_id}
    _apply_snapshot_props(snapshot_props)

    logic = _read_text(logic_path)
    spark = _spark()

    # Resolve inputs. Track watermark advances to commit after outputs succeed.
    input_dfs: dict[str, Any] = {}
    pending_watermarks: list[dict[str, Any]] = []
    saw_partitioned = False
    forced = _is_forced(event, my_node)
    for alias, src in inputs.items():
        if isinstance(src, dict) and src.get("kind") == "partitioned_path":
            saw_partitioned = True
        df, advance = _resolve_input(spark, alias, src, backfill=backfill, forced=forced)
        if df is None:
            # Empty partitioned input — skip the entire run.
            return {"status": "skipped", "reason": f"input {alias!r} has no new partitions"}
        # Bundle-level input cache: a plain table/path input that ≥2 nodes of
        # this pipeline run read (e.g. silver.enriched, read by every gold dim)
        # is persisted on first use and handed to every later node, so the
        # shared upstream is scanned from S3 once per run instead of once per
        # node. pipeline_handler seeds _BUNDLE_SHARED_INPUTS and unpersists when
        # the run ends. Inactive (set empty) for single-node handler() calls.
        if isinstance(src, str) and src in _BUNDLE_SHARED_INPUTS:
            cached = _BUNDLE_INPUT_CACHE.get(src)
            if cached is not None:
                df = cached
            else:
                df = df.cache()
                _BUNDLE_INPUT_CACHE[src] = df
        df.createOrReplaceTempView(alias)
        input_dfs[alias] = df
        if advance is not None:
            pending_watermarks.append(advance)

    # Run the transform.
    if language == "sql":
        result = {"default": spark.sql(logic)}
    elif language == "python":
        mod = _load_script(logic)
        if not hasattr(mod, "transform"):
            raise RuntimeError(
                "User script must define a top-level 'transform(spark, inputs)' function."
            )
        raw = mod.transform(spark, input_dfs)
        if not isinstance(raw, dict):
            raise TypeError(f"transform() must return a dict, got {type(raw)}")
        result = raw
    else:
        raise RuntimeError(f"unsupported CLAVESA_LANGUAGE: {language!r}")

    # Write outputs.
    written: dict[str, str] = {}
    backfill_targets = (backfill or {}).get("target_outputs", {}) if backfill else {}
    for key, df in result.items():
        spec = _resolve_output(key, outputs.get(key, ""), all_outputs=result)
        if backfill and key in backfill_targets:
            # Redirect this output to its staging table; always replace so
            # backfill retries rewrite the staging cleanly.
            spec = {**spec, "kind": "delta_table", "target": backfill_targets[key],
                    "mode": "replace", "merge_keys": [], "merge_update": {}, "cluster_by": []}
        if spec["kind"] == "path":
            df.write.mode("overwrite").parquet(spec["target"])
        else:
            table_id = spec["target"]
            db_part = table_id.rsplit(".", 1)[0]
            _ensure_database(spark, db_part)
            # Liquid-clustering columns: an explicit cluster_by wins; else a
            # merge output clusters by its merge_keys (set at create, see #18).
            cluster_cols = spec["cluster_by"] or (spec["merge_keys"] if spec["mode"] == "merge" else [])
            if spec["mode"] == "merge":
                # First run: no target yet, MERGE has nothing to match
                # against. Create the table and skip MERGE for this run.
                if not spark.catalog.tableExists(table_id):
                    _create_delta_table(df, table_id, cluster_cols)
                else:
                    # Additive schema evolution (GH #61). Delta 4.0's `MERGE
                    # WITH SCHEMA EVOLUTION` does NOT evolve this runner's SQL
                    # shape (proven by the TestRunner_SchemaEvolution_Merge*
                    # docker tests): explicit `target.c = source.c` assignments
                    # fail analysis against the un-evolved target schema
                    # (DELTA_MERGE_UNRESOLVED_EXPRESSION), and `UPDATE SET *` /
                    # `INSERT *` expand over the TARGET's columns, silently
                    # dropping extra source columns. So the runner evolves the
                    # target itself: ALTER TABLE ADD COLUMNS for every source-
                    # frame column missing from the target, typed from the
                    # source frame's Spark schema, nullable — pre-existing rows
                    # read NULL. Deliberately loud: a failed ALTER fails the
                    # run instead of letting the MERGE silently drop a column.
                    existing = set(spark.table(table_id).schema.fieldNames())
                    new_fields = [f for f in df.schema.fields if f.name not in existing]
                    if new_fields:
                        add_cols = ", ".join(
                            f"`{f.name}` {f.dataType.simpleString()}" for f in new_fields
                        )
                        spark.sql(f"ALTER TABLE {table_id} ADD COLUMNS ({add_cols})")
                        print(
                            f"[clavesa] output {key!r}: schema evolved, +{','.join(f.name for f in new_fields)}",
                            file=sys.stderr,
                            flush=True,
                        )
                    staging = f"__merge_src_{key}"
                    df.createOrReplaceTempView(staging)
                    on_clause = " AND ".join(
                        f"target.`{col}` = source.`{col}`" for col in spec["merge_keys"]
                    )
                    # Tier 1 scan-bound: for every merge_key that is also a
                    # skipping/clustering column, inject a static target-only
                    # literal predicate so Delta data-skipping prunes the MERGE
                    # target scan. Provably semantics-free: any matching target
                    # row already satisfies these bounds. (GH #62)
                    from pyspark.sql import functions as F  # noqa: PLC0415

                    bound_cols = _merge_bound_cols(
                        spec["merge_keys"], spec.get("cluster_by") or [], spec.get("bound_by")
                    )
                    bb = spec.get("bound_by") or []
                    # Persist the staging frame across the bound collects, the
                    # Tier-2 tripwire, and the MERGE itself. Un-persisted, each
                    # consumer recomputes the whole transform (several full
                    # recomputations per merge output), and a non-deterministic
                    # transform could recompute key values outside the collected
                    # bounds — the bound would then exclude a real match and
                    # silently duplicate it. Persisting freezes the batch and
                    # closes that window (GH #70).
                    persisted = bool(bound_cols or bb)
                    if persisted:
                        df.persist()
                    try:
                        bound_preds = []
                        for bc in bound_cols:
                            vals = [
                                r[0]
                                for r in df.select(bc)
                                .where(F.col(bc).isNotNull())
                                .distinct()
                                .limit(_MERGE_BOUND_IN_THRESHOLD + 1)
                                .collect()
                            ]
                            if len(vals) > _MERGE_BOUND_IN_THRESHOLD:
                                # Sample was truncated; the limited min/max is NOT
                                # the true table-batch min/max. Recompute the true
                                # bounds for the BETWEEN predicate.
                                mn, mx = df.agg(F.min(bc), F.max(bc)).first()
                                p = _bound_predicate_sql(bc, [mn, mx], force_between=True)
                            else:
                                p = _bound_predicate_sql(bc, vals)
                            if p:
                                bound_preds.append(p)
                        if bound_preds:
                            on_clause = "(" + on_clause + ") AND " + " AND ".join(bound_preds)
                            print(
                                f"[clavesa] output {key!r}: MERGE scan bounded on {bound_cols}",
                                file=sys.stderr,
                                flush=True,
                            )
                        # Tier 2 determinism tripwire (GH #62): when the author
                        # opts into bound_by, verify within THIS staging batch that
                        # the merge keys functionally determine the bound_by columns.
                        # A key tuple mapping to >1 distinct bound_by tuple proves
                        # bound_by is NOT determined by the keys, so the static bound
                        # could drop a real match and silently duplicate. This is a
                        # tripwire over the small staging batch, not a proof: it
                        # cannot catch the insert-only case where each key appears
                        # exactly once in the batch yet collides with a differently-
                        # valued historical row in the target.
                        if bb:
                            offenders = (
                                df.select(*spec["merge_keys"], *bb)
                                .distinct()
                                .groupBy(*spec["merge_keys"])
                                .count()
                                .where(F.col("count") > 1)
                                .limit(1)
                                .count()
                            )
                            if offenders:
                                raise RuntimeError(
                                    f"output {key!r}: bound_by={bb} is not functionally determined by "
                                    f"merge_keys={spec['merge_keys']} in this batch — refusing to bound "
                                    f"the MERGE scan (would risk silent duplicates). Remove bound_by or "
                                    f"fix the merge keys."
                                )
                        if spec["merge_update"]:
                            set_clause = _merge_set_clause(
                                df.schema.fieldNames(), spec["merge_keys"], spec["merge_update"]
                            )
                            when_matched = f"WHEN MATCHED THEN UPDATE SET {set_clause}"
                        else:
                            when_matched = "WHEN MATCHED THEN UPDATE SET *"
                        # Plain MERGE INTO — the former WITH SCHEMA EVOLUTION
                        # clause was removed deliberately: it never applied to
                        # this SQL shape in Delta 4.0 (see the evolution
                        # comment above), so additive column adds are handled
                        # entirely by the pre-MERGE ALTER. After the ALTER the
                        # new columns are ordinary target columns: explicit
                        # `target.c = source.c` assignments resolve, and
                        # `UPDATE SET *` / `INSERT *` expand over the evolved
                        # target schema. The scan-bound predicates are
                        # unaffected by evolution: they bound merge_keys /
                        # bound_by columns, which exist in both schemas by
                        # construction. Column removals and type changes stay
                        # out of scope (additive evolution only).
                        spark.sql(
                            f"MERGE INTO {table_id} target USING {staging} source "
                            f"ON {on_clause} "
                            f"{when_matched} "
                            f"WHEN NOT MATCHED THEN INSERT *"
                        )
                    finally:
                        if persisted:
                            df.unpersist()
            elif spec["mode"] == "append":
                if cluster_cols and not spark.catalog.tableExists(table_id):
                    # First write of a clustered append table: create it with
                    # CLUSTER BY so the layout is set; later appends preserve it.
                    _create_delta_table(df, table_id, cluster_cols)
                else:
                    # Delta's mode("append").saveAsTable auto-creates if the
                    # table doesn't exist yet; no need to branch on tableExists.
                    try:
                        existing = set(spark.table(table_id).schema.fieldNames())
                        new_cols = [c for c in df.schema.fieldNames() if c not in existing]
                        if new_cols:
                            print(f"[clavesa] output {key!r}: schema evolved, +{','.join(new_cols)}", file=sys.stderr, flush=True)
                    except Exception:  # table may not exist yet on first append; skip silently
                        pass
                    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_id)
            else:
                if cluster_cols and not spark.catalog.tableExists(table_id):
                    # First create of a clustered replace table: CTAS with
                    # CLUSTER BY. Later overwrites preserve the clustering.
                    _create_delta_table(df, table_id, cluster_cols)
                else:
                    # Replace semantics: the transform's current output IS the
                    # table, schema included. When the frame's (name, type)
                    # shape drifted from the existing table — added or removed
                    # columns, type changes — overwriteSchema lets the table
                    # follow the DataFrame instead of failing Delta's schema
                    # enforcement at saveAsTable (GH #39). Gated on detected
                    # drift so the steady-state overwrite is byte-identical to
                    # before and never touches the table metadata.
                    drifted = False
                    try:
                        drifted = _schema_drifted(spark.table(table_id).schema, df.schema)
                    except Exception:  # table doesn't exist yet; plain overwrite creates it
                        pass
                    writer = df.write.format("delta").mode("overwrite")
                    if drifted:
                        print(f"[clavesa] output {key!r}: schema changed, overwriting table schema", file=sys.stderr, flush=True)
                        writer = writer.option("overwriteSchema", "true")
                    writer.saveAsTable(table_id)
                    if drifted and cluster_cols:
                        # overwriteSchema replaces the table metadata wholesale;
                        # re-assert liquid clustering so the spec survives an
                        # evolving overwrite (same ALTER the cluster_alter
                        # operation uses; metadata-only, no rewrite).
                        cols = ", ".join(f"`{c}`" for c in cluster_cols[:_MAX_CLUSTER_COLS])
                        spark.sql(f"ALTER TABLE {table_id} CLUSTER BY ({cols})")
            # Write the real schema into Glue's StorageDescriptor.Columns so
            # Athena's browser / information_schema / Lake Formation see the
            # genuine columns instead of Spark's generic `col array<string>`
            # stub. Use the committed table schema (post schema-evolution on
            # the merge path). No-op outside cloud / Glue mode; re-runs are
            # idempotent and re-sync after Spark overwrite resets Glue.
            _sync_glue_table_schema(table_id, spark.table(table_id).schema)
        written[key] = spec["target"]

        # Per-output opt-in column statistics (v0.24+). Computed off the
        # source DataFrame, which is exactly the post-commit table state
        # for replace-mode outputs and the rows this run contributed for
        # append/merge — best-effort; a stats-write failure logs but does
        # not mask the transform outcome.
        if spec.get("stats") and spec["kind"] == "delta_table" and not (backfill and key in backfill_targets):
            try:
                _emit_column_stats(
                    spark=spark,
                    df=df,
                    run_id=run_id,
                    output_key=key,
                    table_identifier=spec["target"],
                )
            except Exception as stats_exc:  # noqa: BLE001
                print(
                    f"[clavesa] column_stats write failed for {spec['target']!r}: {stats_exc!r}",
                    file=sys.stderr,
                )

    # Advance watermarks + ack drained queue messages. Best-effort atomicity:
    # outputs committed first, so a failure here causes the next run to
    # reprocess the same partitions / re-read the same objects (at-least-once on
    # the input side). Mode="replace" outputs absorb the duplicate; "append"
    # outputs would dupe rows. Document, don't solve.
    #
    # An advance record carries EITHER a watermark (uri + new_cursor) OR an ack
    # (notification-drain queue + receipt handles), never both. Watermark writes
    # run first, then queue deletes — both best-effort, neither masks the
    # transform outcome.
    for adv in pending_watermarks:
        if "uri" in adv:
            _write_watermark(adv["uri"], adv["new_cursor"])
    for adv in pending_watermarks:
        ack = adv.get("ack")
        if ack:
            _delete_sqs_messages(ack["queue_url"], ack.get("handles") or [], region=ack.get("region"))

    response: dict[str, Any] = {"status": "ok", "outputs": written}
    if saw_partitioned:
        response["watermarks_advanced"] = [
            {"uri": adv["uri"], "cursor": list(adv["new_cursor"])}
            for adv in pending_watermarks
            if "uri" in adv
        ]
    return response


# ---------------------------------------------------------------------------
# node_runs metadata capture (option A in TODO bucket A: runner self-reports
# at end of every invocation). Failures here never mask the original outcome —
# we'd rather lose a metadata row than fail a successful transform run.
# ---------------------------------------------------------------------------


def _build_node_run_row(
    *,
    run_id: str,
    started_ms: int,
    ended_ms: int,
    status: str,
    error_class: str | None,
    error_msg: str | None,
    cold_start: bool,
    context: Any,
    sf_execution_arn: str | None = None,
    env: dict[str, str] | None = None,
    output_rows: int | None = None,
    peak_rss_mb: int | None = None,
    spark_metrics: dict | None = None,
) -> dict[str, Any]:
    """Pure helper: construct one node_runs row from invocation telemetry.

    Kept side-effect-free so it's unit-testable without Spark. ``env``
    defaults to ``os.environ`` but tests pass a dict to assert behavior
    around missing values.

    ``sf_execution_arn`` comes from the orchestration's SFN payload —
    when present, the row joins to the EventBridge-populated runs table
    on this column. Falls back to empty string when invoked outside of
    Step Functions (local CLI runs, ad-hoc Lambda invocations).
    """
    env_map: dict[str, str] = dict(os.environ if env is None else env)

    pipeline = env_map.get("CLAVESA_PIPELINE", "default")
    node = env_map.get("CLAVESA_NODE", "node")
    compute_target = (
        "lambda" if env_map.get("AWS_LAMBDA_FUNCTION_NAME") else "local"
    )

    memory_mb: int | None = None
    request_id: str | None = None
    if context is not None:
        raw = getattr(context, "memory_limit_in_mb", None)
        if raw is not None:
            try:
                memory_mb = int(raw)
            except (TypeError, ValueError):
                memory_mb = None
        rid = getattr(context, "aws_request_id", None)
        if rid:
            request_id = str(rid)

    started_iso = _dt.datetime.fromtimestamp(
        started_ms / 1000, tz=_dt.timezone.utc
    )
    ended_iso = _dt.datetime.fromtimestamp(
        ended_ms / 1000, tz=_dt.timezone.utc
    )

    # Truncate long error messages so a runaway traceback doesn't blow up
    # the Iceberg manifest. 4 KB matches the typical Lambda log truncation
    # threshold and is plenty for the message itself; full traces are still
    # in CloudWatch.
    if error_msg is not None and len(error_msg) > 4096:
        error_msg = error_msg[:4093] + "..."

    # Triage metadata: which build of the runner produced this row, and
    # which `?ref=vX.Y.Z` was the orchestration module pinned to. Both come
    # from env (set by the Lambda function in cloud, by service.RunPipeline
    # in local). Empty strings when unset — older runners that didn't pass
    # the env still join cleanly via the schema-evolution writer option.
    runner_image_digest = env_map.get("CLAVESA_RUNNER_IMAGE_DIGEST", "")
    module_version = env_map.get("CLAVESA_MODULE_VERSION", "")

    row = {
        "run_id": run_id,
        "pipeline": pipeline,
        "node": node,
        "started_at": started_iso,
        "ended_at": ended_iso,
        "duration_ms": max(ended_ms - started_ms, 0),
        "status": status,
        "compute_target": compute_target,
        "memory_mb": memory_mb,
        "cold_start": cold_start,
        "lambda_request_id": request_id,
        "sf_execution_arn": sf_execution_arn or "",
        "error_class": error_class,
        "error_msg": error_msg,
        "runner_image_digest": runner_image_digest,
        "module_version": module_version,
        # Sum of added-records across every Iceberg-mode output for this
        # invocation. Sourced from each output's snapshot summary at write
        # time — no extra .count() scan, just a manifest read. None when
        # the run had no Iceberg outputs (path-mode destinations, skipped
        # runs) or when summary capture failed.
        "output_rows": output_rows,
    }
    # Resource + Spark task-metric columns. peak_rss_mb is process-lifetime
    # high-water (monotonic across warm-worker reuse); the rest are
    # per-invocation aggregates from the event-log tail. All nullable — None
    # when capture was unavailable (off-Linux, no tasks, read failure).
    row["peak_rss_mb"] = peak_rss_mb
    sm = spark_metrics or {}
    for k in _SPARK_METRIC_KEYS:
        row[k] = sm.get(k)
    return row


def _system_table_location(table_name: str) -> str | None:
    """Absolute S3 LOCATION for a workspace system-catalog table.

    Cloud only: CLAVESA_SYSTEM_WAREHOUSE is set by the transform
    module to `s3://<bucket>/_system/pipelines/`. Returning a concrete
    location at create time pins `node_runs` / `tables` to the workspace-
    shared prefix rather than letting GlueCatalog default them under the
    invoking pipeline's per-pipeline `_warehouse/`. Returns None for
    local/preview (HadoopCatalog default is fine — single warehouse).
    """
    base = os.environ.get("CLAVESA_SYSTEM_WAREHOUSE")
    if not base:
        return None
    return base.rstrip("/") + "/" + table_name + "/"


def _node_runs_table_id() -> str:
    """<system_glue_db>.node_runs — workspace-wide observability table
    (ADR-016 "Workspace system catalog", v0.20.0). Every pipeline appends
    here; the `pipeline` column distinguishes rows."""
    return f"{_system_glue_db()}.node_runs"


def _node_runs_schema():
    """Explicit Iceberg schema. Without this, createDataFrame infers types
    from the first row — ``error_msg=None`` would be inferred as void and
    refuse subsequent string values, and Iceberg refuses to evolve void→
    string in the same commit.
    """
    from pyspark.sql.types import (  # noqa: PLC0415
        BooleanType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType([
        StructField("run_id", StringType(), False),
        StructField("pipeline", StringType(), False),
        StructField("node", StringType(), False),
        StructField("started_at", TimestampType(), False),
        StructField("ended_at", TimestampType(), True),
        StructField("duration_ms", LongType(), True),
        StructField("status", StringType(), False),
        StructField("compute_target", StringType(), True),
        StructField("memory_mb", IntegerType(), True),
        StructField("cold_start", BooleanType(), True),
        StructField("lambda_request_id", StringType(), True),
        # Joins to <pipeline>.runs.sf_execution_arn (EventBridge writer).
        # Empty string when the runner was invoked outside of Step Functions.
        StructField("sf_execution_arn", StringType(), True),
        StructField("error_class", StringType(), True),
        StructField("error_msg", StringType(), True),
        # Triage columns added in the bucket-A item-1 slice. Both nullable
        # so older tables that haven't evolved yet still satisfy the
        # writer's schema-merge contract.
        StructField("runner_image_digest", StringType(), True),
        StructField("module_version", StringType(), True),
        # Output rowcount summed across all Iceberg-mode outputs for this
        # invocation. Nullable: path-mode-only runs and skipped runs leave
        # it null; older tables not carrying the column read as null too.
        StructField("output_rows", LongType(), True),
        # Resource + Spark task-metric columns (Spark-observability slice).
        # All nullable LongType. `peak_rss_mb` is the process-lifetime
        # high-water mark (VmHWM) — monotonic across warm-worker
        # invocations, not per-invocation. The remaining 14 are
        # per-invocation aggregates parsed from the event-log tail written
        # by this run (sums/max/counts over its tasks + stages). Null when
        # capture was unavailable: off-Linux (no /proc), a run that
        # launched no tasks, or an event-log read failure.
        StructField("peak_rss_mb", LongType(), True),
        StructField("peak_execution_memory_mb", LongType(), True),
        StructField("memory_spilled_bytes", LongType(), True),
        StructField("disk_spilled_bytes", LongType(), True),
        StructField("shuffle_read_bytes", LongType(), True),
        StructField("shuffle_write_bytes", LongType(), True),
        StructField("input_bytes", LongType(), True),
        StructField("input_records", LongType(), True),
        StructField("num_stages", LongType(), True),
        StructField("num_tasks", LongType(), True),
        StructField("num_failed_tasks", LongType(), True),
        StructField("jvm_gc_time_ms", LongType(), True),
        StructField("executor_cpu_time_ms", LongType(), True),
        StructField("executor_run_time_ms", LongType(), True),
        StructField("max_task_duration_ms", LongType(), True),
    ])


_SYSTEM_TABLE_PROPERTIES = {
    "delta.logRetentionDuration": "interval 24 hours",
    "delta.deletedFileRetentionDuration": "interval 24 hours",
    "delta.checkpointInterval": "10",
}


def _set_system_table_properties(spark, table_id: str) -> None:
    """Bound the _delta_log growth of the workspace bookkeeping tables.
    Called once, right after a system table is first created. These tables
    are append-mostly operational logs written once per node per run, so a
    short log/tombstone retention (hours, not Delta's 30-day default) lets
    the periodic checkpoint truncate stale commit files instead of letting
    them accumulate into thousands of LIST-expensive entries (GH #53). The
    active compaction/VACUUM is a separate scheduled maintenance pipeline;
    this only keeps the log bounded between those runs. Best-effort: a
    failure logs to stderr and is swallowed, same posture as the recorders."""
    props = ", ".join(f"'{k}' = '{v}'" for k, v in _SYSTEM_TABLE_PROPERTIES.items())
    try:
        spark.sql(f"ALTER TABLE {table_id} SET TBLPROPERTIES ({props})")
    except Exception as exc:  # noqa: BLE001
        print(
            f"[clavesa] could not set system-table properties on {table_id}: {exc!r}",
            file=sys.stderr,
        )


def _record_node_run(row: dict[str, Any]) -> None:
    """Append one row to the pipeline's node_runs Iceberg table.

    Creates the namespace and table on first call; appends thereafter.
    `mergeSchema=true` lets us evolve the schema forward (added columns
    backfill as null on older snapshots) without an explicit ALTER TABLE
    when the runner adds columns between releases.
    """
    spark = _spark()
    table_id = _node_runs_table_id()
    db_part = table_id.rsplit(".", 1)[0]
    _ensure_database(spark, db_part)

    df = spark.createDataFrame([row], schema=_node_runs_schema())
    location = _system_table_location("node_runs")
    created = not spark.catalog.tableExists(table_id)
    writer = df.write.format("delta").mode("append").option("mergeSchema", "true")
    if created and location:
        # Delta's external-location pin: .option("path", …) at first write
        # registers the table at the workspace-shared system prefix instead
        # of letting the metastore default it under the invoking pipeline.
        writer = writer.option("path", location)
    writer.saveAsTable(table_id)
    if created:
        _set_system_table_properties(spark, table_id)
    _sync_glue_table_schema(table_id, df.schema, location=location)


def _runs_table_id() -> str:
    """<system_glue_db>.runs — workspace-wide rollup table. Every
    pipeline's executions land here; `pipeline` column filters.

    Cloud writes go through the runs_writer Lambda
    (`modules/orchestration/aws/runs_writer/`), which is also pointed at
    the workspace system DB as of v0.20.0. Local has no SFN, so
    service.RunPipeline drives the same write through this runner mode.
    Schema and column order must stay in lockstep with the cloud writer
    or LocalProvider.Runs() will project mismatched values.
    """
    return f"{_system_glue_db()}.runs"


def _runs_schema():
    """Explicit Delta schema for the workspace runs table. Same NOT-NULL
    discipline as _node_runs_schema for the same reason: an inferred
    void column blocks subsequent string appends. ADR-018: this is the
    single source of truth — the v1.x mirror in runs_writer/index.py
    is gone, runs_writer_handler below writes via _record_run which
    consults this schema directly.
    """
    from pyspark.sql.types import (  # noqa: PLC0415
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType([
        StructField("run_id", StringType(), False),
        StructField("pipeline", StringType(), False),
        StructField("sf_execution_arn", StringType(), True),
        StructField("status", StringType(), False),
        StructField("trigger", StringType(), True),
        StructField("target_table", StringType(), True),
        StructField("started_at", TimestampType(), False),
        StructField("ended_at", TimestampType(), True),
        StructField("duration_ms", LongType(), True),
        StructField("failed_step", StringType(), True),
        StructField("error_class", StringType(), True),
        StructField("error_msg", StringType(), True),
    ])


def _record_run(row: dict[str, Any]) -> None:
    """Append one row to the pipeline's runs Iceberg table.

    Same create-or-append branching as _record_node_run.
    """
    spark = _spark()
    table_id = _runs_table_id()
    db_part = table_id.rsplit(".", 1)[0]
    _ensure_database(spark, db_part)

    df = spark.createDataFrame([row], schema=_runs_schema())
    created = not spark.catalog.tableExists(table_id)
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_id)
    if created:
        _set_system_table_properties(spark, table_id)
    _sync_glue_table_schema(table_id, df.schema)


def _tables_table_id() -> str:
    """<system_glue_db>.tables — workspace-wide denormalized snapshot
    metadata across every Delta output produced by every pipeline. Lives
    in the system catalog's `pipelines` schema alongside runs / node_runs
    (ADR-016 v0.20.0); `pipeline` column filters per pipeline.
    """
    return f"{_system_glue_db()}.tables"


def _tables_schema():
    """One row per Delta-output write. Append-only history — query
    `MAX(snapshot_ts) GROUP BY table_id` for current-state.

    Schema column names retain the Iceberg-era vocabulary
    (`snapshot_id`, `snapshot_ts`) for v2.0.0 since they're already
    populated in existing system tables and the data-shape stability is
    more valuable than the rename. Under Delta the values are the commit
    version (int) and the commit timestamp respectively.
    """
    from pyspark.sql.types import (  # noqa: PLC0415
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType([
        StructField("pipeline", StringType(), False),
        StructField("node", StringType(), False),
        StructField("output_key", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("table_id", StringType(), False),
        StructField("snapshot_id", LongType(), True),
        StructField("snapshot_ts", TimestampType(), True),
        StructField("row_count", LongType(), True),
        StructField("file_count", IntegerType(), True),
        StructField("total_bytes", LongType(), True),
        # Joins to node_runs.run_id so users can answer "which invocation
        # produced this snapshot?" without scanning logs.
        StructField("last_writer_run_id", StringType(), True),
    ])


def _record_table_state(run_id: str, output_key: str, table_id: str) -> int | None:
    """Append one row to <pipeline>.tables describing the latest commit of
    `table_id`. Called after every Delta-mode output write.

    Reads Delta's `DESCRIBE HISTORY` for version + timestamp +
    operationMetrics + userMetadata. Cheap (transaction-log read, no
    data-file scan).

    Returns the latest commit's added-records count (or None if metrics
    didn't carry it) so the caller can accumulate output_rows across all
    outputs for the node_runs row.

    Best-effort: a write failure logs to stderr and returns. Losing a
    metadata row is strictly better than failing a transform whose data
    already committed.
    """
    spark = _spark()

    # Pull the latest commit row. operationMetrics + userMetadata replace
    # Iceberg's `summary` map.
    from pyspark.sql import functions as F  # noqa: PLC0415

    table_ident = table_id
    hist_rows = (
        spark.sql(f"DESCRIBE HISTORY {table_ident}")
        .orderBy(F.col("version").desc())
        .limit(1)
        .collect()
    )
    if not hist_rows:
        return

    snap = hist_rows[0]
    metrics_raw = snap["operationMetrics"] or {}
    # operationMetrics is a Spark map<string,string>; values are str-typed
    # regardless of operation. Normalise to int-or-None.
    def _metric(key: str) -> int | None:
        raw = metrics_raw.get(key)
        if raw is None:
            return None
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None

    pipeline = os.environ.get("CLAVESA_PIPELINE", "default")
    node = os.environ.get("CLAVESA_NODE", "node")
    table_name = table_ident.rsplit(".", 1)[-1]

    # operationMetrics keys vary by operation:
    #   WRITE / append / overwrite → `numOutputRows`, `numOutputBytes`,
    #     `numFiles`.
    #   MERGE → `numTargetRowsInserted`, `numTargetRowsUpdated`,
    #     `numTargetRowsDeleted`, `numTargetFilesAdded`,
    #     `numTargetFilesRemoved`, `numTargetBytesAdded` /
    #     `numTargetBytesRemoved`. There is no single
    #     "current row count of the table" in metrics — that's a
    #     full-table query, not a commit metric.
    # For the row_count column we therefore report the rows touched by
    # the latest commit, not the table-wide count. (v1.x's Iceberg
    # `total-records` summary key WAS a table-wide count, but the
    # Iceberg/Delta semantics diverge here; we accept the change.)
    operation = snap["operation"] or ""
    if "MERGE" in operation.upper():
        # Touched rows = inserted + updated + deleted post-image side.
        touched = (
            (_metric("numTargetRowsInserted") or 0)
            + (_metric("numTargetRowsUpdated") or 0)
        )
        row_count_val: int | None = touched if touched else None
    else:
        row_count_val = _metric("numOutputRows")

    row = {
        "pipeline": pipeline,
        "node": node,
        "output_key": output_key,
        "table_name": table_name,
        "table_id": table_id,
        "snapshot_id": int(snap["version"]),
        "snapshot_ts": snap["timestamp"],
        "row_count": row_count_val,
        "file_count": _metric("numFiles") or _metric("numTargetFilesAdded"),
        "total_bytes": _metric("numOutputBytes") or _metric("numTargetBytesAdded"),
        "last_writer_run_id": run_id,
    }

    tables_id = _tables_table_id()
    db_part = tables_id.rsplit(".", 1)[0]
    _ensure_database(spark, db_part)

    df = spark.createDataFrame([row], schema=_tables_schema())
    location = _system_table_location("tables")
    created = not spark.catalog.tableExists(tables_id)
    writer = df.write.format("delta").mode("append").option("mergeSchema", "true")
    if created and location:
        writer = writer.option("path", location)
    writer.saveAsTable(tables_id)
    if created:
        _set_system_table_properties(spark, tables_id)
    _sync_glue_table_schema(tables_id, df.schema, location=location)

    # Net rows this commit contributed.
    #
    # Mapping under Delta (operationMetrics):
    #   - MERGE: `numTargetRowsInserted` is the added count; we ignore
    #     `numTargetRowsUpdated` (existing rows mutated in place are not
    #     "new rows" for the purpose of "Rows written"). `numTargetRowsDeleted`
    #     is the deleted count.
    #   - append: `numOutputRows` minus 0.
    #   - overwrite / replace: `numOutputRows` is the new row count; we
    #     don't have the previous-version row count here without an extra
    #     query, so we report it as added. Matches the v1.x Iceberg branch
    #     (added-records minus deleted-records) within the limits of what
    #     Delta exposes per-commit.
    # If a fresh operation surfaces a new key we haven't mapped, the call
    # returns 0 rather than guessing — the system table still gets the row
    # via the append above, only the accumulated output_rows count loses
    # this commit's contribution.
    if "MERGE" in operation.upper():
        added = _metric("numTargetRowsInserted")
        deleted = _metric("numTargetRowsDeleted")
    else:
        added = _metric("numOutputRows")
        deleted = None
    if added is None:
        return None
    net = added - (deleted or 0)
    return net if net > 0 else 0


# ---------------------------------------------------------------------------
# column_stats — opt-in per-column profile (null %, distinct count, top-K,
# min/max, percentiles) computed off the source DataFrame at write time and
# appended to the workspace system catalog. Drives the catalog page's "is
# this column worth graphing?" affordance.
# ---------------------------------------------------------------------------

# High-cardinality cutoff for top-K. Above this distinct-value count, the
# top-10 list is noise: the long tail dwarfs each bucket and the runner
# pays for an extra group-by per column. The UI shows a "high cardinality"
# badge instead and the row's top_10 stays empty.
_COLUMN_STATS_TOPK_CUTOFF = 1000
_COLUMN_STATS_TOPK_LIMIT = 10
# Cap the per-output top-K compute to bound cost on very wide tables. The
# row's top_10 is left empty (not the same as "skipped due to cardinality")
# for any column past this cap — the UI doesn't distinguish today, the
# distinction matters when wide-table billing data shows up.
_COLUMN_STATS_TOPK_MAX_COLUMNS = 20


def _column_stats_table_id() -> str:
    """<system_glue_db>.column_stats — workspace-wide opt-in column
    profile table (v0.24+). Multi-writer; `pipeline` / `node` /
    `output_key` columns scope each row, and `snapshot_id` joins to the
    Delta commit version the stats describe. Lives alongside node_runs /
    runs / tables in the system catalog's `pipelines` schema (ADR-016).
    """
    return f"{_system_glue_db()}.column_stats"


def _column_stats_schema():
    """Explicit Iceberg schema. Uniform across column types (min/max
    stringified, percentiles as nullable doubles, top_10 as a nested
    array<struct>) so a single schema serves every dtype the runner
    might profile.
    """
    from pyspark.sql.types import (  # noqa: PLC0415
        ArrayType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    top_struct = StructType([
        StructField("value", StringType(), True),
        StructField("count", LongType(), True),
    ])
    return StructType([
        StructField("run_id", StringType(), False),
        StructField("pipeline", StringType(), False),
        StructField("node", StringType(), False),
        StructField("output_key", StringType(), False),
        StructField("table_identifier", StringType(), False),
        StructField("snapshot_id", LongType(), True),
        StructField("snapshot_ts", TimestampType(), True),
        StructField("column_name", StringType(), False),
        StructField("column_type", StringType(), False),
        StructField("row_count", LongType(), True),
        StructField("null_count", LongType(), True),
        StructField("null_pct", DoubleType(), True),
        StructField("approx_count_distinct", LongType(), True),
        StructField("min_value", StringType(), True),
        StructField("max_value", StringType(), True),
        StructField("approx_p50", DoubleType(), True),
        StructField("approx_p95", DoubleType(), True),
        StructField("top_10", ArrayType(top_struct), True),
        StructField("computed_at", TimestampType(), False),
    ])


def _is_numeric_type(spark_type) -> bool:
    """approx_percentile + numeric min/max only make sense on numeric
    types. Decimal counts (Iceberg / Spark store amounts as DecimalType
    routinely)."""
    from pyspark.sql.types import (  # noqa: PLC0415
        ByteType,
        DecimalType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        ShortType,
    )

    return isinstance(
        spark_type,
        (ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType),
    )


def _read_latest_snapshot(spark, table_identifier):
    """Latest (version, timestamp) from Delta's DESCRIBE HISTORY for the
    given table, or (None, None) if no commits are visible yet — defensive:
    the write we just performed should always produce one, but a brand-new
    table where the catalog hasn't refreshed gets the safe (None, None).

    Returns the same shape as the v1.x Iceberg signature
    (snapshot_id, committed_at); under Delta the values are commit version
    + commit timestamp respectively. Column-name compatibility on the
    column_stats schema is the reason for keeping the function name and
    return shape stable.
    """
    from pyspark.sql import functions as F  # noqa: PLC0415

    table_ident = table_identifier
    try:
        rows = (
            spark.sql(f"DESCRIBE HISTORY {table_ident}")
            .orderBy(F.col("version").desc())
            .select("version", "timestamp")
            .limit(1)
            .collect()
        )
    except Exception:  # noqa: BLE001
        return None, None
    if not rows:
        return None, None
    return int(rows[0]["version"]), rows[0]["timestamp"]


def _emit_column_stats(*, spark, df, run_id, output_key, table_identifier):
    """Compute per-column stats off `df` and append one row per column to
    the workspace system column_stats table. Caller catches exceptions —
    best-effort, see _run_transform's wrapper.
    """
    from pyspark.sql import functions as F  # noqa: PLC0415
    from pyspark.sql.types import StringType  # noqa: PLC0415

    columns = list(df.schema.fields)
    if not columns:
        return

    # Single aggregation pass: row_count + per-column null_count,
    # approx_count_distinct, min, max, and percentiles for numerics.
    aggs = [F.count(F.lit(1)).alias("__row_count")]
    for f in columns:
        aggs.append(F.sum(F.col(f.name).isNull().cast("long")).alias(f"{f.name}__nulls"))
        aggs.append(F.approx_count_distinct(F.col(f.name)).alias(f"{f.name}__distinct"))
        # min/max over the underlying type so numeric ordering is
        # preserved (cast-then-min would lex-sort: "99" > "499"),
        # then string-cast the *result* for uniform wire shape.
        aggs.append(F.min(F.col(f.name)).cast(StringType()).alias(f"{f.name}__min"))
        aggs.append(F.max(F.col(f.name)).cast(StringType()).alias(f"{f.name}__max"))
        if _is_numeric_type(f.dataType):
            aggs.append(
                F.percentile_approx(F.col(f.name).cast("double"), [0.5, 0.95]).alias(
                    f"{f.name}__pcts"
                )
            )

    agg_row = df.agg(*aggs).collect()[0]
    row_count = int(agg_row["__row_count"]) if agg_row["__row_count"] is not None else 0

    # Per-column top-K, capped to keep the cost bounded on wide tables.
    # Only fire for columns under the cardinality cutoff — above it the
    # tail dominates and the result reads as noise.
    top_k_by_col: dict[str, list[dict]] = {}
    top_k_columns_used = 0
    for f in columns:
        if top_k_columns_used >= _COLUMN_STATS_TOPK_MAX_COLUMNS:
            break
        distinct = agg_row[f"{f.name}__distinct"]
        if distinct is None or distinct == 0 or distinct > _COLUMN_STATS_TOPK_CUTOFF:
            continue
        try:
            rows = (
                df.groupBy(F.col(f.name).cast(StringType()).alias("value"))
                .count()
                .orderBy(F.col("count").desc())
                .limit(_COLUMN_STATS_TOPK_LIMIT)
                .collect()
            )
        except Exception:  # noqa: BLE001
            # Best-effort per column — keep going so the rest of the
            # stats land even if one column's top-K errors out.
            continue
        top_k_by_col[f.name] = [
            {"value": r["value"], "count": int(r["count"])} for r in rows
        ]
        top_k_columns_used += 1

    snapshot_id, snapshot_ts = _read_latest_snapshot(spark, table_identifier)
    pipeline = os.environ.get("CLAVESA_PIPELINE", "default")
    node = os.environ.get("CLAVESA_NODE", "node")
    computed_at = _dt.datetime.now(tz=_dt.timezone.utc)

    rows_out = []
    for f in columns:
        null_count = agg_row[f"{f.name}__nulls"]
        null_count_i = int(null_count) if null_count is not None else None
        null_pct = None
        if null_count_i is not None and row_count > 0:
            null_pct = float(null_count_i) / float(row_count)
        elif row_count == 0:
            null_pct = 0.0
        distinct = agg_row[f"{f.name}__distinct"]
        p50 = p95 = None
        if _is_numeric_type(f.dataType):
            pcts = agg_row[f"{f.name}__pcts"]
            if pcts is not None and len(pcts) >= 2:
                p50 = float(pcts[0]) if pcts[0] is not None else None
                p95 = float(pcts[1]) if pcts[1] is not None else None
        rows_out.append({
            "run_id": run_id,
            "pipeline": pipeline,
            "node": node,
            "output_key": output_key,
            "table_identifier": table_identifier,
            "snapshot_id": snapshot_id,
            "snapshot_ts": snapshot_ts,
            "column_name": f.name,
            "column_type": f.dataType.simpleString(),
            "row_count": row_count,
            "null_count": null_count_i,
            "null_pct": null_pct,
            "approx_count_distinct": int(distinct) if distinct is not None else None,
            "min_value": agg_row[f"{f.name}__min"],
            "max_value": agg_row[f"{f.name}__max"],
            "approx_p50": p50,
            "approx_p95": p95,
            "top_10": top_k_by_col.get(f.name),
            "computed_at": computed_at,
        })

    _record_column_stats(rows_out)


def _record_column_stats(rows):
    """Append per-column stats rows to the system column_stats Iceberg
    table. Same create-or-append branching pattern as _record_node_run
    and _record_table_state, including the `_system_table_location`
    override that pins the table to the workspace-shared `_system/`
    prefix in cloud warehouses.
    """
    if not rows:
        return
    spark = _spark()
    table_id = _column_stats_table_id()
    db_part = table_id.rsplit(".", 1)[0]
    _ensure_database(spark, db_part)

    df = spark.createDataFrame(rows, schema=_column_stats_schema())
    location = _system_table_location("column_stats")
    created = not spark.catalog.tableExists(table_id)
    writer = df.write.format("delta").mode("append").option("mergeSchema", "true")
    if created and location:
        writer = writer.option("path", location)
    writer.saveAsTable(table_id)
    if created:
        _set_system_table_properties(spark, table_id)
    _sync_glue_table_schema(table_id, df.schema, location=location)


def run_record_run() -> None:
    """CLAVESA_RECORD_RUN=1 mode — read one row's fields from stdin JSON,
    append to <pipeline>.runs.

    Stdin shape (timestamps as Unix millis so we don't fight ISO parsing
    across Go/Python nanosecond precision):
        {
          "run_id": "...", "pipeline": "...", "sf_execution_arn": "",
          "status": "SUCCEEDED" | "FAILED",
          "trigger": "manual",
          "started_at_ms": 173..., "ended_at_ms": 173...,
          "duration_ms": 1234,
          "failed_step": "", "error_class": "", "error_msg": ""
        }
    """
    payload = json.loads(sys.stdin.read())

    started_ms = int(payload["started_at_ms"])
    ended_ms_raw = payload.get("ended_at_ms")
    ended_dt = None
    if ended_ms_raw is not None:
        ended_dt = _dt.datetime.fromtimestamp(int(ended_ms_raw) / 1000, tz=_dt.timezone.utc)

    error_msg = payload.get("error_msg") or None
    if error_msg is not None and len(error_msg) > 4096:
        error_msg = error_msg[:4093] + "..."

    row = {
        "run_id": str(payload["run_id"]),
        "pipeline": str(payload["pipeline"]),
        "sf_execution_arn": payload.get("sf_execution_arn") or "",
        "status": str(payload["status"]),
        "trigger": payload.get("trigger") or "",
        "target_table": payload.get("target_table") or None,
        "started_at": _dt.datetime.fromtimestamp(started_ms / 1000, tz=_dt.timezone.utc),
        "ended_at": ended_dt,
        "duration_ms": int(payload["duration_ms"]) if payload.get("duration_ms") is not None else None,
        "failed_step": payload.get("failed_step") or "",
        "error_class": payload.get("error_class") or "",
        "error_msg": error_msg,
    }
    _record_run(row)

    # Backfill node_runs rows for cascade-skipped nodes — service.RunPipeline
    # bypasses the runner for these so they wouldn't otherwise appear in the
    # Runs grid. Sharing this invocation's Spark session keeps the cost ~0s
    # vs invoking the runner once per skipped node (Spark startup × N).
    cascade = payload.get("cascade_skipped_nodes") or []
    if cascade:
        run_id_str = str(payload["run_id"])
        pipeline_name = str(payload["pipeline"])
        sf_arn = payload.get("sf_execution_arn") or ""
        runner_image_digest = os.environ.get("CLAVESA_RUNNER_IMAGE_DIGEST", "")
        module_version = os.environ.get("CLAVESA_MODULE_VERSION", "")
        for entry in cascade:
            entry_started_ms = int(entry.get("started_at_ms") or started_ms)
            entry_ended_ms = int(entry.get("ended_at_ms") or entry_started_ms)
            _record_node_run({
                "run_id": run_id_str,
                "pipeline": pipeline_name,
                "node": str(entry["node"]),
                "started_at": _dt.datetime.fromtimestamp(entry_started_ms / 1000, tz=_dt.timezone.utc),
                "ended_at": _dt.datetime.fromtimestamp(entry_ended_ms / 1000, tz=_dt.timezone.utc),
                "duration_ms": max(entry_ended_ms - entry_started_ms, 0),
                "status": "skipped",
                "compute_target": "local",
                "memory_mb": None,
                "cold_start": None,
                "lambda_request_id": None,
                "sf_execution_arn": sf_arn,
                "error_class": "",
                "error_msg": entry.get("reason") or "all upstreams skipped",
                "runner_image_digest": runner_image_digest,
                "module_version": module_version,
                "output_rows": None,
            })

    print(json.dumps({"status": "ok"}), flush=True)


_RUNS_TERMINAL_STATUSES = frozenset({"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"})

# Allowed values for runs.trigger. Each start path stamps one of these
# into the SFN execution input under the `_trigger` key; runs_writer
# reads it back via _runs_writer_extract_trigger and stores it on the
# runs row. Keep this set in sync with the orchestration emitter
# (tfgen.go) — adding a new trigger requires bumping both sides.
_RUNS_TRIGGER_VALUES = frozenset({
    "manual", "scheduled", "event", "backfill", "backfill-direct",
    # Cross-pipeline auto-trigger (ADR-016 §6): producer's
    # EventBridge rule stamps `_trigger = "upstream"` on the SFN input
    # and carries the producer name separately in `_upstream_pipeline`.
    "upstream",
})


def _runs_writer_extract_trigger(raw_input):
    """Read `_trigger` from the SFN execution input. The orchestration
    emitter smuggles a value into `_trigger` from each known start path
    (scheduled via EventBridge target, event via the SQS poller); manual
    runs via console / CLI / `clavesa pipeline run-cloud` either set it
    explicitly or leave it absent — default the latter to "manual" so
    the column never reads as a missing-data NULL.

    SFN's EventBridge payload presents `input` as a JSON-encoded string.
    Old runs (or runs with malformed input) gracefully fall back to
    "manual".
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
    if value not in _RUNS_TRIGGER_VALUES:
        return "manual"
    return value


def _runs_writer_extract_target_table(raw_input):
    """Pick the staging table id out of `_backfill.target_outputs` if
    present. Backfill runs route each output to a parallel
    `<target>__backfill__<run_id>` table; we record the staging id on
    the runs row so the UI/CLI can find it later. v1: single-output
    backfills are the only shape, so we report the first staging table
    when there are multiple."""
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
        for k in sorted(targets):
            v = targets[k]
            if isinstance(v, str) and v:
                return v
    return None


def _runs_writer_parse_cause(cause):
    """Best-effort extraction of (error_msg, failed_step) from an SFN
    cause. Lambda failure causes are JSON with errorMessage; state-
    machine-level failures may be plain text. failed_step is not in the
    EventBridge payload — leave empty and let callers query
    GetExecutionHistory if they need it.
    """
    if not cause:
        return "", ""
    try:
        parsed = json.loads(cause)
        if isinstance(parsed, dict):
            msg = parsed.get("errorMessage") or parsed.get("Cause") or cause
            return _runs_writer_truncate(str(msg)), ""
    except (ValueError, TypeError):
        pass
    return _runs_writer_truncate(cause), ""


def _runs_writer_truncate(s, limit=4096):
    if len(s) <= limit:
        return s
    return s[: limit - 3] + "..."


def _runs_writer_build_row(detail):
    """Translate one EventBridge `detail` payload into the dict shape
    `_record_run` expects. Mirrors the v1.x boto3+Athena runs_writer
    (internal/orchestration/sidecar/runs_writer/index.py:_build_row),
    minus the Athena value rendering — the Delta path takes typed
    values (datetime, int, None) directly.
    """
    sf_execution_arn = str(detail.get("executionArn") or "")
    run_id = sf_execution_arn.rsplit(":", 1)[-1] if sf_execution_arn else ""

    started_ms = detail.get("startDate")
    stopped_ms = detail.get("stopDate")
    duration_ms = None
    if isinstance(started_ms, int) and isinstance(stopped_ms, int):
        duration_ms = max(stopped_ms - started_ms, 0)

    status = str(detail.get("status") or "")
    failed_step = ""
    error_class = ""
    error_msg = None
    if status != "SUCCEEDED":
        error_class = _runs_writer_truncate(str(detail.get("error") or ""))
        error_msg_raw, failed_step = _runs_writer_parse_cause(str(detail.get("cause") or ""))
        error_msg = error_msg_raw or None

    started_dt = None
    if isinstance(started_ms, int) and started_ms > 0:
        started_dt = _dt.datetime.fromtimestamp(started_ms / 1000, tz=_dt.timezone.utc)
    ended_dt = None
    if isinstance(stopped_ms, int) and stopped_ms > 0:
        ended_dt = _dt.datetime.fromtimestamp(stopped_ms / 1000, tz=_dt.timezone.utc)

    return {
        "run_id": run_id,
        "pipeline": os.environ.get("CLAVESA_PIPELINE", ""),
        "sf_execution_arn": sf_execution_arn,
        "status": status,
        "trigger": _runs_writer_extract_trigger(detail.get("input")),
        "target_table": _runs_writer_extract_target_table(detail.get("input")),
        "started_at": started_dt,
        "ended_at": ended_dt,
        "duration_ms": duration_ms,
        "failed_step": failed_step,
        "error_class": error_class,
        "error_msg": error_msg,
    }


def runs_writer_handler(event, context):
    """Lambda entry point for the runs_writer image (ADR-018).

    Replaces the v1.x boto3 + Athena `INSERT INTO` runs_writer
    (internal/orchestration/sidecar/runs_writer/index.py). Athena's
    Delta support is read-only, so INSERT won't work on the new Delta
    runs table; instead we reuse the runner image — which already
    carries Spark + Delta + the IAM scope to write any table in the
    workspace catalog — and call `_record_run` directly.

    Cold start is heavier than the zip Lambda (~5s vs ~1s) but the path
    is proven; delta-rs in a zip was the alternative but adds non-
    trivial packaging infrastructure (pip-install-into-source-dir or a
    Lambda layer) for a write that fires once per terminal SFN run.

    `started_at_ms` / `ended_at_ms` in the payload are kept for
    backward-compat with the local CLAVESA_RECORD_RUN=1 stdin shape;
    the EventBridge `detail` carries `startDate` / `stopDate` instead
    so this handler does its own translation.
    """
    detail = (event or {}).get("detail") or {}
    status = detail.get("status")
    if status not in _RUNS_TERMINAL_STATUSES:
        # RUNNING events fire too — only terminal states write a row;
        # the "currently running" view comes from SFN ListExecutions.
        return {"skipped": "non-terminal status: " + repr(status)}

    row = _runs_writer_build_row(detail)
    _record_run(row)
    return {"ok": True, "run_id": row["run_id"], "status": status}


def handler(event, context):
    """Lambda / local production entry point.

    Wraps ``_run_transform`` with timing and node_runs metadata capture.
    The metadata write is best-effort — a failed write logs to stderr but
    does not affect the transform outcome (or its exception, if any).

    Operation kinds (Gate 1): when ``event["_operation"]`` is set, the
    Lambda performs a backfill control-plane action (promote, discard)
    against Iceberg via SparkSQL instead of running the user transform.
    The runner image already carries Spark + Iceberg + the IAM scope to
    read/write any table in the workspace catalog, so we route these
    through the same Lambda rather than introducing a parallel ops
    Lambda or doing the write from Athena (Athena's MERGE syntax
    requires column enumeration and lacks `SET *`; SparkSQL accepts both).
    """
    if isinstance(event, dict) and event.get("_operation"):
        return _run_operation(event)

    started_ms = int(time.time() * 1000)
    is_cold_start = _SPARK is None
    # Byte offset into the Spark event log at handler entry. The post-run
    # metric read seeks past this so a warm-worker reuse aggregates only
    # this invocation's task events, not the prior run's tail.
    eventlog_offset = _event_log_offset()
    run_id = uuid.uuid4().hex
    # Threaded by the orchestration emitter (v0.13+) — empty string for
    # runs outside Step Functions (local CLI, ad-hoc invocations).
    sf_execution_arn = ""
    if isinstance(event, dict):
        raw_arn = event.get("_sf_execution_arn", "")
        if raw_arn:
            sf_execution_arn = str(raw_arn)
    status = "ok"
    error_class: str | None = None
    error_msg: str | None = None
    # Sum of added-records across this run's Iceberg outputs. Stays None
    # for path-mode-only runs and skipped runs (the column should be
    # NULL, not 0, so analytics queries can distinguish "0 rows produced"
    # from "no Iceberg outputs").
    output_rows_total: int | None = None

    # Per-node live in-flight progress: a daemon poller pushes Spark
    # stage/task snapshots to <warehouse>/_progress/<run>/<node>.json on
    # whatever backend the warehouse resolves to (S3 for a deployed run,
    # the local filesystem otherwise — see _progress_target). The Go reader
    # lists the same tree for RUNNING nodes. Written for ANY runtime: there
    # is no longer an is_cloud gate or a stdout progress channel — the bundle
    # path (pipeline_handler) drives every node through handler(), so this
    # one poller per node is the single progress source for both per-node
    # Lambdas and local/cloud-local bundle runs.
    progress_poller = None
    node = os.environ.get("CLAVESA_NODE", "")
    progress_target = _progress_target(os.environ, sf_execution_arn, node)
    if progress_target is not None:
        def _emit_progress(payload, _target=progress_target):
            # "status": "running" tags this as a live in-flight tick so the
            # backend can tell it apart from the terminal marker written in
            # the finally block below. Best-effort via _write_progress.
            _write_progress(
                _target,
                {
                    **payload,
                    "status": "running",
                    "updated_ms": int(time.time() * 1000),
                },
            )

        progress_poller = _ProgressPoller(node, _emit_progress)
        progress_poller.start()

    try:
        response = _run_transform(event, context, run_id=run_id)
        if isinstance(response, dict) and response.get("status") == "skipped":
            status = "skipped"
        # Record one row per Iceberg-mode output into <pipeline>.tables so
        # the catalog page can show row count + last refresh per table
        # without each viewer re-querying every transform's snapshots view.
        # Path-mode outputs (plain Parquet at a destination override) are
        # skipped — they don't have an Iceberg snapshot to read from.
        if isinstance(response, dict) and isinstance(response.get("outputs"), dict):
            for output_key, target in response["outputs"].items():
                if not isinstance(target, str):
                    continue
                # Delta table identifiers are dotted (``db.table`` for
                # legacy spark_catalog or ``catalog.schema.table`` for
                # ADR-019 V2); path-mode targets are filesystem/S3 paths
                # and skipped here.
                if "/" in target or "\\" in target:
                    continue
                if target.count(".") < 1:
                    continue
                try:
                    added = _record_table_state(run_id, output_key, target)
                    if added is not None:
                        output_rows_total = (output_rows_total or 0) + added
                except Exception as table_exc:  # noqa: BLE001
                    print(
                        f"[clavesa] tables row write failed for {target!r}: {table_exc!r}",
                        file=sys.stderr,
                    )
        # Surface output_rows on the response so the Go orchestrator can
        # stamp it onto state.json (and the dashboard's node-detail
        # drawer can read it without spinning Spark up to query the
        # Iceberg node_runs table). None when the transform wrote no
        # Iceberg outputs — path-mode-only writes and skipped runs both
        # land here.
        if isinstance(response, dict) and output_rows_total is not None:
            response["output_rows"] = output_rows_total
        return response
    except Exception as exc:
        status = "failed"
        error_class = type(exc).__name__
        # str(exc) carries the human-readable message — for PySpark
        # exceptions (AnalysisException &c.) repr(exc) is just
        # "AnalysisException()" with the message dropped. Fall back to
        # repr only when str is empty.
        error_msg = str(exc) or repr(exc)
        raise
    finally:
        if progress_poller is not None:
            progress_poller.stop()
        ended_ms = int(time.time() * 1000)
        # Terminal marker: overwrite the last running tick with a final
        # snapshot so the progress file is AUTHORITATIVE for completed nodes.
        # The backend no longer has to infer "done" from a stale-but-running
        # file — a terminal "status" (succeeded/failed/skipped) is written
        # explicitly. Carries output_rows (the per-node added-rows count, the
        # same value stamped on the response) so the Go node-runs fast path
        # can read it without re-querying. No stage/task counters here on
        # purpose; the progress bar only shows for in-flight nodes. "metrics"
        # is reserved for future executor/shuffle telemetry and ships empty
        # for now. Best-effort: a write failure must never affect or mask the
        # transform outcome.
        if progress_target is not None:
            terminal = (
                "failed"
                if status == "failed"
                else "skipped"
                if status == "skipped"
                else "succeeded"
            )
            _write_progress(
                progress_target,
                {
                    "status": terminal,
                    "started_ms": started_ms,
                    "ended_ms": ended_ms,
                    "output_rows": output_rows_total,
                    "error": (error_msg or "")[:500]
                    if status == "failed"
                    else "",
                    "updated_ms": ended_ms,
                    "metrics": {},
                },
            )
        try:
            # Resource + Spark task-metric capture. Both are best-effort and
            # already inside this try, so a failure here logs and is
            # swallowed below without ever masking the transform outcome.
            peak_rss = _read_peak_rss_mb()
            spark_metrics = _read_spark_metrics(eventlog_offset)
            row = _build_node_run_row(
                run_id=run_id,
                started_ms=started_ms,
                ended_ms=ended_ms,
                status=status,
                error_class=error_class,
                error_msg=error_msg,
                cold_start=is_cold_start,
                context=context,
                sf_execution_arn=sf_execution_arn,
                output_rows=output_rows_total,
                peak_rss_mb=peak_rss,
                spark_metrics=spark_metrics,
            )
            _record_node_run(row)
        except Exception as record_exc:  # noqa: BLE001
            print(
                f"[clavesa] node_runs write failed: {record_exc!r}",
                file=sys.stderr,
            )


def _topo_sort_transforms(transforms):
    """Defensive topological sort of a pipeline-bundle transforms list.

    The event SHOULD already arrive topo-ordered from the emitter; this guards
    against a mis-ordered payload (GH #6) so a consumer never runs before the
    sibling that produces its input table. Kahn's algorithm over each entry's
    declared ``parents``, with a stable tie-break by node name for determinism.

    Parents naming nodes not present in this bundle are ignored (they are
    resolved as already-materialised tables, not ordering constraints). On a
    dependency cycle the original order is returned unchanged rather than
    raising, so the runner still attempts execution.
    """
    by_node = {}
    order_in = []
    for t in transforms:
        if not isinstance(t, dict):
            # Non-dict entries can't be ordered; bail to the original list so
            # the main loop's own isinstance guard handles them.
            return transforms
        node = str(t.get("node", "") or "")
        by_node[node] = t
        order_in.append(node)

    # in-degree counts only parents that are present in this bundle.
    indeg = {n: 0 for n in order_in}
    children = {n: [] for n in order_in}
    for t in transforms:
        node = str(t.get("node", "") or "")
        for p in t.get("parents") or []:
            if p in by_node and p != node:
                children[p].append(node)
                indeg[node] += 1

    ready = sorted(n for n in order_in if indeg[n] == 0)
    ordered = []
    while ready:
        cur = ready.pop(0)
        ordered.append(cur)
        newly = []
        for c in children[cur]:
            indeg[c] -= 1
            if indeg[c] == 0:
                newly.append(c)
        if newly:
            ready = sorted(ready + newly)

    if len(ordered) != len(order_in):
        # Cycle (or duplicate node names): fall back to the input order.
        return transforms
    return [by_node[n] for n in ordered]


# Bundle-level shared-input cache. pipeline_handler runs every transform in one
# Spark session, so an input read by multiple nodes (the medallion pattern: one
# silver table feeding many gold dims) need only be scanned from S3 once for the
# whole run. _BUNDLE_SHARED_INPUTS holds the descriptors worth caching (seeded
# per run); _BUNDLE_INPUT_CACHE holds the persisted DataFrames (populated lazily
# by _run_transform on first use, released when the run ends). Empty outside a
# bundle run, so single-transform handler()/preview paths are unaffected.
_BUNDLE_SHARED_INPUTS: set[str] = set()
_BUNDLE_INPUT_CACHE: dict[str, Any] = {}


def _bundle_shared_inputs(transforms) -> set[str]:
    """Plain string input descriptors referenced by ≥2 transforms in the
    bundle — the ones worth persisting once and reusing. dict descriptors
    (partitioned_path incremental reads, with per-node windows + watermark
    side effects) are never cached.
    """
    counts: dict[str, int] = {}
    for t in transforms:
        if not isinstance(t, dict):
            continue
        for src in (t.get("inputs") or {}).values():
            if isinstance(src, str) and src:
                counts[src] = counts.get(src, 0) + 1
    return {src for src, n in counts.items() if n >= 2}


def pipeline_handler(event, context):
    """Pipeline-bundle entry point: run every transform in a pipeline
    sequentially in one Spark session, reusing the module-level ``_SPARK``
    singleton across calls so the JVM cold-start is paid once per
    invocation instead of once per transform.

    Event shape (event["_pipeline_run"] must be truthy to dispatch here):

      {
        "_pipeline_run": True,
        "run_id": "<uuid>",
        "transforms": [
          {
            "node":     "<node_id>",
            "language": "sql"|"python",
            "logic_path": "/absolute/path/to/logic.txt",
            "inputs":  {alias: <descriptor>, ...},
            "outputs": {key: <descriptor>, ...},
            "parents": ["upstream_node", ...]
          },
          ...                 # topo-ordered
        ],
        "_sf_execution_arn": "<run_id>",  # ties node_runs rows to runs row
        "_trigger":          "manual"|"scheduled"|"event"|"upstream",
      }

    Cascade-skip rule (mirrors internal/service/run.go:283-303): if every
    intra-pipeline parent of a transform skipped this run, the transform
    is skipped without invoking the runner — upstream tables haven't
    changed, so the output wouldn't either.

    Progress reporting: per-node live + terminal progress is written by
    handler() (invoked per transform below) to
    ``<warehouse>/_progress/<run>/<node>.json`` — S3 for a deployed run,
    the local filesystem otherwise — which the Go reader watches. The bundle
    loop no longer emits per-node ``_event`` progress lines on stdout. The
    aggregated pipeline result is still the LAST line on stdout (the
    non-``_event`` dict with overall status / failed_node / per-node
    statuses) that the Go-side caller parses as the final result.

    Stops on first transform failure — downstream transforms would fail
    anyway with missing input tables.

    The Lambda image CMD points here, but single-node payloads — backfill
    stage (`_backfill`), promote/discard (`_operation`), ad-hoc invokes —
    carry no `_pipeline_run` bundle. Route them to ``handler`` (the consumer
    of `_backfill`/`_operation`), mirroring ``run_local``'s dispatch. Without
    this the bundle loop below sees ``transforms == []`` and returns a no-op
    ``status: ok`` in ~40ms, never running the staging compute. The single
    per-pipeline Lambda doesn't bake per-node CLAVESA_NODE / _LANGUAGE /
    _LOGIC_S3_PATH env (``pipeline_handler`` sets them per transform), so seed
    them from the backfill payload before delegating; handler() reads them
    from ``os.environ``. ``_operation`` payloads need none of this — handler()
    routes them to ``_run_operation`` before any node logic runs.
    """
    if isinstance(event, dict) and not event.get("_pipeline_run"):
        bf = event.get("_backfill")
        if isinstance(bf, dict):
            if bf.get("node"):
                os.environ["CLAVESA_NODE"] = str(bf["node"])
            if event.get("language"):
                os.environ["CLAVESA_LANGUAGE"] = str(event["language"])
            if event.get("logic_path"):
                os.environ["CLAVESA_LOGIC_S3_PATH"] = str(event["logic_path"])
        # GH #43: single-node payloads (backfill stage, promote/discard,
        # ad-hoc) run on the same warm container as bundles — give them the
        # same /tmp-pressure recycle on entry and session reset on failure,
        # or a failed backfill stage leaves a poisoned session for the next
        # invocation.
        if _tmp_pressure_exceeded():
            print(
                "[clavesa] /tmp >50% full at handler entry — recycling Spark session",
                file=sys.stderr,
                flush=True,
            )
            _reset_spark_session()
        try:
            return handler(event, context)
        except Exception:
            _reset_spark_session()
            raise

    # ADR-024 slice 5: deployed (s3 warehouse) bundle runs serialize on the
    # same warehouse run lock the Go side acquires (internal/runlock) — a
    # scheduled Lambda run and any other compute contend on one S3 object at
    # <pipeline>/_locks/run.json. Acquired before any Spark work; released
    # in the finally once the run is observably terminal. Non-s3 warehouses
    # no-op (local docker bundle runs are already locked Go-side via the
    # file backend). Single-node payloads above never reach here, so they
    # never take the lock (backfill staging is private; operations are
    # covered later).
    # Version skew: an already-deployed Lambda only enforces this after its
    # image is rebuilt + redeployed (`workspace upgrade` + deploy).
    lease, held_result = _acquire_run_lease(event)
    if held_result is not None:
        return held_result
    try:
        return _run_pipeline_bundle(event, context)
    finally:
        if lease is not None:
            lease.release()


def _run_lock_holder(event):
    """Identity stamped into the run-lock lease (mirrors runlock.Holder).

    run_id: cloud SFN payloads carry the execution ARN as _sf_execution_arn
    ($$.Execution.Id, tfgen.go emitStateMachine) and no run_id; local bundle
    events carry run_id. Prefer run_id, fall back to the execution ARN.
    """
    import socket  # noqa: PLC0415

    run_id = str(event.get("run_id") or event.get("_sf_execution_arn") or "") or "unknown"
    fn = os.environ.get("AWS_LAMBDA_FUNCTION_NAME")
    host = fn or socket.gethostname()
    # Same keying as _record_node_run's compute_target: a local docker
    # container driving an s3 warehouse (ADR-024 cloud-local compute) is
    # compute=local even though it takes the same S3 lock.
    compute = "lambda" if fn else "local"
    return {"run_id": run_id, "compute": compute, "host": host, "pid": os.getpid()}


def _acquire_run_lease(event):
    """Acquire the warehouse run lock for a pipeline-bundle run.

    Returns (lease, None) on success with the heartbeat already running,
    (None, None) when the warehouse is not s3:// (no lock taken), and —
    when the lock is held by another run — either raises RuntimeError (on
    Lambda, so the SFN execution FAILS with the holder in a readable cause)
    or returns (None, failed_result) for the local CLAVESA_RUN path,
    mirroring pipeline_handler's standard failure shape.
    """
    warehouse = os.environ.get("CLAVESA_WAREHOUSE", "")
    if not warehouse.startswith("s3://"):
        return None, None
    import run_lock  # noqa: PLC0415 — sibling module, COPY'd next to runner.py

    try:
        lease = run_lock.acquire_for_warehouse(warehouse, _run_lock_holder(event))
    except run_lock.HeldError as exc:
        msg = str(exc)
        print(
            json.dumps({
                "_event": "failed",
                "node": "",
                "error_class": "RunLockHeld",
                "error_msg": msg,
            }),
            flush=True,
        )
        if os.environ.get("AWS_LAMBDA_FUNCTION_NAME"):
            # GH #2 mirror: a returned dict would make Step Functions mark
            # the execution SUCCEEDED — raise so it FAILS with the holder
            # identity in the cause.
            raise RuntimeError("clavesa runner: " + msg) from None
        return None, {
            "status": "failed",
            "transforms": [],
            "failed_node": None,
            "error_class": "RunLockHeld",
            "error_msg": msg,
        }
    if lease is not None:
        lease.start_heartbeat()
    return lease, None


def _run_pipeline_bundle(event, context):
    """The bundle loop body of ``pipeline_handler`` — runs every transform
    sequentially in one Spark session. Split out of pipeline_handler so the
    run-lock wrapper there can release the lease in a finally."""
    transforms = event.get("transforms", []) or []
    # Defensive: the emitter SHOULD already topo-order this list, but a
    # mis-ordered payload (GH #6) would otherwise run a consumer before its
    # parent's table exists. Re-sort by declared parents before executing.
    transforms = _topo_sort_transforms(transforms)
    # Seed the shared-input cache for this run: inputs ≥2 nodes read get
    # scanned once and reused. Released in the finally below.
    _BUNDLE_SHARED_INPUTS.clear()
    _BUNDLE_SHARED_INPUTS.update(_bundle_shared_inputs(transforms))
    _BUNDLE_INPUT_CACHE.clear()
    parents_by_node = {
        t.get("node"): list(t.get("parents") or [])
        for t in transforms
        if isinstance(t, dict)
    }
    sf_execution_arn = str(event.get("_sf_execution_arn", "") or "")
    # The CLI's StartExecution input lands nested under _execution_input
    # (ASL threads $$.Execution.Input there); only the ASL-hoisted keys are
    # top-level. Read both so local bundle invocations (flat) and cloud SFN
    # invocations (nested) behave identically.
    exec_input = event.get("_execution_input")
    if not isinstance(exec_input, dict):
        exec_input = {}
    trigger = str(event.get("_trigger") or exec_input.get("_trigger") or "")
    # Forward force flags into every sub_event so handler() / _resolve_input
    # see them on a per-node basis (force_nodes scopes the bypass when set).
    force_flag = bool(event.get("_force") or exec_input.get("_force"))
    force_nodes = list(
        event.get("_force_nodes") or exec_input.get("_force_nodes") or []
    )

    results: list[dict[str, Any]] = []
    skipped_set: set[str] = set()
    overall_status = "ok"
    failed_node: str | None = None

    for t in transforms:
        if not isinstance(t, dict):
            continue
        node = str(t.get("node", "") or "")
        if not node:
            continue

        # Cascade-skip: only fires when the node has ≥1 intra-pipeline
        # parent AND every parent skipped this run. Source-only transforms
        # (no intra-pipeline parents) fall through to the runner's own
        # per-input skip decision.
        ps = parents_by_node.get(node, [])
        if ps and all(p in skipped_set for p in ps):
            note = "all upstreams skipped"
            # handler() is NOT invoked for a cascade-skip, so write the
            # terminal progress marker here (every other node's marker is
            # written by handler() itself via _progress_target/_write_progress).
            _now = int(time.time() * 1000)
            _write_progress(
                _progress_target(os.environ, sf_execution_arn, node),
                {
                    "status": "skipped",
                    "started_ms": _now,
                    "ended_ms": _now,
                    "output_rows": None,
                    "error": "",
                    "updated_ms": _now,
                    "metrics": {},
                },
            )
            skipped_set.add(node)
            results.append({"node": node, "status": "skipped", "note": note})
            continue

        # Per-transform env: handler() reads CLAVESA_NODE / CLAVESA_LANGUAGE
        # / CLAVESA_LOGIC_S3_PATH from os.environ, so set them inline.
        # _SPARK is the module-level singleton (built lazily in _spark())
        # and survives across these assignments — that's the whole point
        # of the bundle.
        os.environ["CLAVESA_NODE"] = node
        os.environ["CLAVESA_LANGUAGE"] = str(t.get("language", "sql") or "sql")
        logic_path = str(t.get("logic_path", "") or "")
        if logic_path:
            os.environ["CLAVESA_LOGIC_S3_PATH"] = logic_path

        sub_event: dict[str, Any] = {
            "inputs": t.get("inputs", {}) or {},
            "outputs": t.get("outputs", {}) or {},
            "_sf_execution_arn": sf_execution_arn,
            "_trigger": trigger,
        }
        if force_flag:
            sub_event["_force"] = True
            if force_nodes:
                sub_event["_force_nodes"] = force_nodes

        # GH #43: if a prior successful run left enough shuffle/spill residue
        # that /tmp is already half-full, recycle the session now so Spark's
        # shutdown hooks clean its own temp dirs before we start fresh. This
        # keeps the fast warm path for the common case while preventing slow
        # accumulation from filling the disk mid-transform.
        if _tmp_pressure_exceeded():
            print(
                "[clavesa] /tmp >50% full before transform — recycling Spark session",
                file=sys.stderr,
                flush=True,
            )
            _reset_spark_session()

        # Per-node progress is now written by handler() itself: it resolves
        # _progress_target from the warehouse and drives its own poller +
        # terminal marker to <warehouse>/_progress/<run>/<node>.json. The
        # bundle loop no longer starts a poller or emits stdout _event lines
        # — those were the old stdout progress channel, replaced by the
        # progress-file tree the Go reader watches for both cloud and
        # cloud-local runs.
        try:
            resp = handler(sub_event, context)
        except Exception as exc:  # noqa: BLE001
            # handler() already wrote a failed node_runs row AND the failed
            # terminal progress marker in its finally block; just record the
            # failure and stop the pipeline run.
            err_class = type(exc).__name__
            err_msg = str(exc) or repr(exc)
            results.append({
                "node": node,
                "status": "failed",
                "error_class": err_class,
                "error_msg": err_msg,
            })
            overall_status = "failed"
            failed_node = node
            # GH #43: recycle the session after any transform failure so a
            # corrupt/poisoned SparkContext doesn't survive into the next warm
            # invocation. SparkContext.stop() triggers Spark's own shutdown
            # hooks, which delete blockmgr and spill dirs under /tmp.
            _reset_spark_session()
            break

        node_status = (
            resp.get("status") if isinstance(resp, dict) else None
        ) or "ok"
        output_rows = resp.get("output_rows") if isinstance(resp, dict) else None
        if node_status == "skipped":
            note = resp.get("reason", "") if isinstance(resp, dict) else ""
            skipped_set.add(node)
            results.append({"node": node, "status": "skipped", "note": note})
        else:
            results.append({
                "node": node,
                "status": "ok",
                "output_rows": output_rows,
            })

        # Test-only hook (#23): when CLAVESA_TEST_KILL_SESSION_AFTER_NODE names
        # the node just finished, deterministically kill the cached py4j session
        # WITHOUT clearing the global, so the dead handle stays cached and the
        # next node's _spark() must self-heal it. Named like
        # CLAVESA_CONNECT_SESSION_TIMEOUT to mark it obviously test-only.
        if os.environ.get("CLAVESA_TEST_KILL_SESSION_AFTER_NODE") == node:
            try:
                if _SPARK is not None:
                    _SPARK.stop()
            except Exception:  # noqa: BLE001 — best-effort kill for the test
                pass

    # Every node has run — release the shared inputs cached for this bundle.
    for cached in _BUNDLE_INPUT_CACHE.values():
        try:
            cached.unpersist()
        except Exception:  # noqa: BLE001 — best-effort cleanup
            pass
    _BUNDLE_INPUT_CACHE.clear()
    _BUNDLE_SHARED_INPUTS.clear()

    result = {
        "status": overall_status,
        "transforms": results,
        "failed_node": failed_node,
    }

    # GH #2: returning a clean dict makes the Lambda invocation "succeed"
    # at the AWS API layer, so Step Functions marks the execution
    # SUCCEEDED regardless of payload. Re-raise after the node_runs rows
    # are written so SFN sees a real task failure and the cross-pipeline
    # EventBridge rule (filtered on detail.status = SUCCEEDED) does NOT
    # fire downstream pipelines. Local mode (`clavesa pipeline run`)
    # parses the dict directly via internal/service/run.go — keep
    # returning it there.
    if overall_status == "failed" and os.environ.get("AWS_LAMBDA_FUNCTION_NAME"):
        last = results[-1] if results else {}
        raise RuntimeError(
            "clavesa runner: {node} failed ({cls}: {msg})".format(
                node=failed_node,
                cls=last.get("error_class", "Error"),
                msg=last.get("error_msg", "no error message"),
            )
        )
    return result


def run_local() -> None:
    """CLAVESA_RUN=1 mode — read event JSON from stdin, invoke handler, print result.

    Used by `clavesa pipeline run` to drive transforms via the same handler
    that backs Lambda. The event shape is identical to the Lambda contract.

    Pipeline-bundle mode (event["_pipeline_run"] truthy) routes to
    ``pipeline_handler``, which loops through all transforms in one Spark
    session. Single-transform events still route through ``handler``
    (preview, ad-hoc invocations, the per-transform path before Phase B
    of the bundle rollout).
    """
    event = json.loads(sys.stdin.read())
    if isinstance(event, dict) and event.get("_pipeline_run"):
        result = pipeline_handler(event, None)
    else:
        result = handler(event, None)
    print(json.dumps(result), flush=True)


# ---------------------------------------------------------------------------
# Query mode (CLAVESA_QUERY=1) — local-cloud parity per ADR-014
# ---------------------------------------------------------------------------


def run_query() -> None:
    """CLAVESA_QUERY=1 mode — run one Spark SQL statement, emit JSON to stdout.

    Used by the Go-side observability LocalProvider to read Iceberg-backed
    surfaces (node_runs, runs, snapshots, transform output tables) for
    compute = "local" pipelines. Cloud's CloudProvider goes through Athena
    for the same reads; this is the local equivalent at the same SQL surface.

    SQL is read from CLAVESA_SQL (env) or stdin when the env is empty —
    long queries with line breaks fit poorly in env vars.

    Output: {"columns": [name...], "column_types": [type...],
    "rows": [[v, v, ...], ...]} on success, or {"error": "..."} on failure
    (exit 1). column_types carries Spark's simpleString of each column's
    DataType so the cloud side's Athena type strings have a parity-matched
    counterpart in the SampleTable / Query response (ADR-014). Values are
    JSON-encoded primitives or stringified for non-JSON-native types
    (timestamps, decimals).
    """
    sql = os.environ.get("CLAVESA_SQL", "").strip()
    if not sql:
        sql = sys.stdin.read().strip()
    if not sql:
        print(json.dumps({"error": "no SQL provided (CLAVESA_SQL or stdin)"}), flush=True)
        sys.exit(1)

    spark = _spark()
    df = spark.sql(sql)

    columns = list(df.columns)
    column_types = [f.dataType.simpleString() for f in df.schema.fields]
    # Use Pandas as the JSON serialization shim so timestamps / decimals /
    # nested types format consistently across Spark versions. Pandas matches
    # how preview-mode emits rows, keeping the on-the-wire format aligned.
    pdf = df.toPandas()
    rows_records = json.loads(pdf.to_json(orient="records", date_format="iso"))
    rows = [[r.get(c) for c in columns] for r in rows_records]

    print(json.dumps({"columns": columns, "column_types": column_types, "rows": rows}), flush=True)


# ---------------------------------------------------------------------------
# Query-server mode (CLAVESA_QUERY_SERVER=1) — long-lived warm Spark Connect
# ---------------------------------------------------------------------------


def _start_connect_plugin() -> None:
    """Start the Spark Connect gRPC server inside this Python's py4j JVM.

    Uses the SparkConnectPlugin mechanism. The same JVM that hosts our
    Delta catalog also exposes a Connect endpoint on the configured port.
    Notebook REPL subprocesses (Slice 1) and the warm worker's own /query
    handler both talk to it via gRPC, getting per-session SparkSession
    isolation without separate driver JVMs.

    Builds via the same py4j SparkSession.builder as _spark() so the
    Delta catalog + S3A config from spark_conf is applied to the JVM
    that serves Connect clients.
    """
    from pyspark.sql import SparkSession  # noqa: PLC0415
    from spark_conf import clavesa_spark_conf, spark_master  # noqa: PLC0415

    port = os.environ.get("CLAVESA_CONNECT_PORT", "15002")
    builder = (
        SparkSession.builder.appName("clavesa-connect-host")
        .master(spark_master())
        .config("spark.plugins", "org.apache.spark.sql.connect.SparkConnectPlugin")
        .config("spark.connect.grpc.binding.port", port)
        # The warm worker (CLAVESA_QUERY_SERVER=1) lives for hours or days and
        # holds a single long-lived Connect session that may sit idle between
        # dashboard reads. Spark Connect's session manager GCs sessions with no
        # incoming RPC after this timeout (default 60m in Spark 4.0), which
        # invalidates our cached client handle → [INVALID_HANDLE.SESSION_CLOSED]
        # on the next /query. Push the timeout out to several days so a warm but
        # idle session is not reaped during normal use. The recover-once retry in
        # _query_to_payload is the backstop if it is reaped anyway. Key/units
        # verified against Spark 4.0 (pyspark[connect]==4.0.2): the value is a
        # time string, default "60m". Overridable via CLAVESA_CONNECT_SESSION_TIMEOUT
        # (used to force a short reap when exercising the recover-once path).
        .config(
            "spark.connect.session.manager.defaultSessionTimeout",
            os.environ.get("CLAVESA_CONNECT_SESSION_TIMEOUT", "7d"),
        )
    )
    for k, v in clavesa_spark_conf().items():
        builder = builder.config(k, v)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    # Stash the py4j session so /parse can reach the JVM-side SqlParser
    # directly. parsePlan needs sessionState().sqlParser(), which Spark
    # Connect's client session does not expose — it lives in the host
    # JVM that owns this Connect plugin.
    global _SPARK
    _SPARK = session


def _connect_session():
    """Lazy Spark Connect client session, pinned to the in-container plugin.

    Each call returns the same module-level session. The session_id starts as
    a stable UUID derived from the literal "_clavesa_catalog" (Spark Connect
    requires UUID format), disjoint from notebook REPL ids (Slice 1). It is
    NOT reused after a close: _reset_connect_session rotates it to a fresh
    UUID, because Spark Connect tombstones a reaped session id and rebuilding
    with the same id just fails again with SESSION_CLOSED.
    """
    global _CONNECT, _CONNECT_SESSION_ID
    if _CONNECT is None:
        import uuid  # noqa: PLC0415
        from pyspark.sql.connect.session import SparkSession  # noqa: PLC0415

        port = os.environ.get("CLAVESA_CONNECT_PORT", "15002")
        if _CONNECT_SESSION_ID is None:
            _CONNECT_SESSION_ID = str(uuid.uuid5(uuid.NAMESPACE_OID, "_clavesa_catalog"))
        _CONNECT = (
            SparkSession.builder
            .remote(f"sc://localhost:{port}/;session_id={_CONNECT_SESSION_ID}")
            .getOrCreate()
        )
    return _CONNECT


def _reset_connect_session() -> None:
    """Drop the cached Connect client so the next _connect_session() rebuilds it.

    Best-effort stop of the stale handle first (it may already be dead), then
    clear the module global AND rotate the session id. The rotation is the
    load-bearing part: Spark Connect tombstones a reaped/closed session id, so
    the next _connect_session() must connect with a FRESH id to get a clean
    session — reusing the old id fails again with SESSION_CLOSED. A fresh UUID
    stays disjoint from notebook REPL ids."""
    global _CONNECT, _CONNECT_SESSION_ID
    import uuid  # noqa: PLC0415

    old = _CONNECT
    _CONNECT = None
    _CONNECT_SESSION_ID = str(uuid.uuid4())
    if old is not None:
        try:
            old.stop()
        except Exception:  # noqa: BLE001 — stale handle; nothing useful to do
            pass


def _is_session_closed(exc: BaseException) -> bool:
    """True when the exception signals a GC'd / invalid Spark Connect session.

    Spark Connect raises SparkConnectException / SparkSQLException whose message
    carries the server-side error class. We match on the message text rather
    than the exception type to stay robust across Spark versions. The real
    message is:
        (org.apache.spark.SparkSQLException) [INVALID_HANDLE.SESSION_CLOSED]
        The handle ... is invalid. Session was closed."""
    msg = str(exc).lower()
    return (
        "invalid_handle" in msg
        or "session_closed" in msg
        or "session was closed" in msg
    )


def _query_to_payload(sql: str) -> dict:
    """Run one SparkSQL statement via Connect, return the warm-worker shape.

    If the first attempt fails because the cached Connect session was GC'd
    server-side (idle-session reaping), reset and rebuild the session and retry
    ONCE. Any non-session error — and a second failure of the same kind —
    propagates to the caller, which returns it in the error envelope."""
    try:
        return _run_query(sql)
    except Exception as exc:  # noqa: BLE001
        if not _is_session_closed(exc):
            raise
        # Cached handle points at a reaped session. Rebuild and retry once.
        _reset_connect_session()
        _connect_session()
        return _run_query(sql)


def _run_query(sql: str) -> dict:
    """Single Connect round-trip → warm-worker payload shape. No retry."""
    df = _connect_session().sql(sql)
    columns = list(df.columns)
    column_types = [f.dataType.simpleString() for f in df.schema.fields]
    pdf = df.toPandas()
    records = json.loads(pdf.to_json(orient="records", date_format="iso"))
    rows = [[r.get(c) for c in columns] for r in records]
    return {"columns": columns, "column_types": column_types, "rows": rows}


def _connect_select1() -> None:
    """`SELECT 1` round-trip through Connect with the same recover-once retry.

    Used by the startup warmup and the /healthz probe so a session reaped while
    the worker sat idle self-heals instead of wedging the worker — /healthz then
    reflects true health (200) after recovery rather than 503-flapping."""
    try:
        _connect_session().sql("SELECT 1").collect()
    except Exception as exc:  # noqa: BLE001
        if not _is_session_closed(exc):
            raise
        _reset_connect_session()
        _connect_session().sql("SELECT 1").collect()


def _parse_sql(sql: str) -> dict:
    """Parse-only check via the JVM-side Catalyst SqlParser.

    Returns {"ok": True} on success or {"ok": False, "error": "<msg>"} on
    parse failure. Uses the py4j SparkSession stashed by
    ``_start_connect_plugin`` so we go straight to the JVM's
    ``sessionState().sqlParser().parsePlan(sql)`` — Spark Connect's
    client session does not expose this seam.
    """
    if _SPARK is None:
        # /parse should only be called after the worker is healthy, which
        # implies _start_connect_plugin has populated _SPARK. Defensive
        # fallback: build the session lazily.
        _spark()
    try:
        _SPARK._jsparkSession.sessionState().sqlParser().parsePlan(sql)
        return {"ok": True}
    except Exception as exc:  # noqa: BLE001
        # py4j.protocol.Py4JJavaError carries the JVM exception's message
        # on .java_exception.getMessage(); other shapes (transient gateway
        # errors) fall back to str(exc). Spark's parse errors are the
        # most useful — they include a pointer-into-SQL hint that
        # surfaces the offending token.
        java_exc = getattr(exc, "java_exception", None)
        if java_exc is not None:
            try:
                msg = java_exc.getMessage()
            except Exception:  # noqa: BLE001
                msg = str(exc)
        else:
            msg = str(exc)
        if not msg:
            msg = str(exc)
        return {"ok": False, "error": msg}


def _transpile_sql(sql: str) -> dict:
    """Transpile Spark SQL → Athena/Trino SQL via sqlglot.

    Returns {"ok": True, "trino": "<sql>"} on success or
    {"ok": False, "error": "<msg>", "line": <int|None>, "col": <int|None>}
    on failure. Like ``_parse_sql`` this NEVER raises — the HTTP layer
    forwards the envelope as 200 and the client inspects ``ok``.

    sqlglot is imported lazily so the stdlib-only unit test can stub it
    via sys.modules (the same trick used for pyspark/boto3). RAISE level
    is required so genuinely unmappable Spark constructs error out
    instead of silently emitting wrong SQL.
    """
    import sqlglot  # noqa: PLC0415

    try:
        out = sqlglot.transpile(
            sql,
            read="spark",
            write="athena",
            unsupported_level=sqlglot.ErrorLevel.RAISE,
        )
        return {"ok": True, "trino": out[0]}
    except sqlglot.errors.ParseError as exc:
        # ParseError carries a .errors list of dicts with line/col/description
        # pointing at the offending token in the input Spark SQL.
        line = None
        col = None
        errs = getattr(exc, "errors", None)
        if errs:
            first = errs[0]
            line = first.get("line")
            col = first.get("col")
        return {"ok": False, "error": str(exc), "line": line, "col": col}
    except sqlglot.errors.UnsupportedError as exc:
        # Valid Spark, but no Athena equivalent — no positional info.
        return {"ok": False, "error": str(exc), "line": None, "col": None}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": str(exc), "line": None, "col": None}


def run_query_server() -> None:
    """CLAVESA_QUERY_SERVER=1 mode — warm Spark Connect server + HTTP query proxy.

    Wired by `clavesa ui` so the Catalog/dashboard/TableDetail surfaces share
    one warm JVM instead of paying the ~18-30s cold start on every read.

    Hosts two endpoints, sequentially: first starts the Spark Connect plugin
    (long-lived gRPC server inside this Python's JVM, bound on port 15002),
    then serves the legacy HTTP /healthz + /query interface by acting as a
    Connect client to localhost:15002.

    Why route /query through Connect rather than the local py4j session: we
    dogfood the same path notebook REPLs will use in Slice 1; if Iceberg-
    through-Connect has gaps, we find them here, not deep in notebook work.

    Routes:
      GET  /healthz  → 200 once Connect is warm AND `SELECT 1` round-trips.
                       503 if Spark/Connect is gone (JVM crash, gRPC dead);
                       Go-side persistent runner evicts and respawns.
      POST /query    → body: {"sql": "..."}. Returns {columns, column_types,
                       rows} or {"error": "..."} on failure (200 either way —
                       client inspects envelope).
    """
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer  # noqa: PLC0415
    import notebook_supervisor  # noqa: PLC0415

    port = int(os.environ.get("CLAVESA_QUERY_SERVER_PORT", "8765"))

    # Start the embedded Connect plugin (long-lived). The plugin's gRPC
    # server lives in JVM threads — it stays up as long as this Python
    # process holds the SparkSession.
    _start_connect_plugin()
    # Force the Connect client to connect now so /healthz returning 200
    # implies the next /query won't pay any handshake cost. Recover-once in
    # case the session was reaped between plugin start and first probe.
    _connect_select1()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/healthz":
                # Round-trip a trivial query through Connect — catches both
                # "container up, JVM dead" and "JVM up, Connect gRPC dead".
                # Without this probe Go never evicts because the HTTP socket
                # stays open.
                try:
                    _connect_select1()
                except Exception as exc:  # noqa: BLE001
                    self._json(503, {"error": f"spark connect dead: {exc}"})
                    return
                self._respond(200, b"ok", content_type="text/plain")
                return
            if self.path == "/repls" or self.path.startswith("/repl/"):
                self._proxy_supervisor("GET")
                return
            self._respond(404, b"", content_type="text/plain")

        def do_DELETE(self) -> None:  # noqa: N802
            if self.path.startswith("/repl/"):
                self._proxy_supervisor("DELETE")
                return
            self._respond(404, b"", content_type="text/plain")

        def _proxy_supervisor(self, method: str) -> None:
            length = int(self.headers.get("Content-Length", "0") or 0)
            body = self.rfile.read(length) if length else b""
            try:
                status, payload = notebook_supervisor.handle_repl_request(method, self.path, body)
            except Exception as exc:  # noqa: BLE001
                self._json(500, {"error": f"supervisor: {exc}"})
                return
            self._json(status, payload)

        def do_POST(self) -> None:  # noqa: N802
            if self.path.startswith("/repl/") or self.path == "/repls":
                self._proxy_supervisor("POST")
                return
            if self.path not in ("/query", "/parse"):
                self._respond(404, b"", content_type="text/plain")
                return
            length = int(self.headers.get("Content-Length", "0") or 0)
            body = self.rfile.read(length).decode("utf-8") if length else ""
            try:
                req = json.loads(body) if body else {}
                sql = (req.get("sql") or "").strip()
            except Exception as exc:  # noqa: BLE001
                self._json(400, {"error": f"bad request body: {exc}"})
                return
            if not sql:
                if self.path == "/parse":
                    self._json(400, {"ok": False, "error": "empty SQL"})
                else:
                    self._json(400, {"error": "missing sql"})
                return
            if self.path == "/parse":
                # _parse_sql never raises — it returns the {ok,error}
                # envelope. The HTTP layer just forwards it as 200; the
                # client (Go side) inspects ``ok`` to distinguish parse
                # failure from transport failure.
                self._json(200, _parse_sql(sql))
                return
            try:
                payload = _query_to_payload(sql)
            except Exception as exc:  # noqa: BLE001
                # Match CLAVESA_QUERY=1: error envelope, 200 status —
                # the client distinguishes success vs failure by inspecting
                # the JSON, not the HTTP code.
                self._json(200, {"error": str(exc)})
                return
            self._json(200, payload)

        def _json(self, code: int, payload: dict) -> None:
            data = json.dumps(payload).encode("utf-8")
            self._respond(code, data, content_type="application/json")

        def _respond(self, code: int, data: bytes, *, content_type: str) -> None:
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            if data:
                self.wfile.write(data)

        def log_message(self, fmt: str, *args) -> None:  # noqa: ARG002
            # Quiet — match the rest of the runner's stdout discipline.
            return

    # ThreadingHTTPServer so a /repl/<id>/cancel request can land while a
    # /repl/<id>/cell is still mid-execution on a separate thread (Slice 1).
    # The /query path remains effectively single-threaded by virtue of the
    # Spark Connect catalog session being one logical client; concurrent
    # /query calls just serialize at the Connect layer.
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()


def run_transpile_server() -> None:
    """CLAVESA_TRANSPILE_SERVER=1 mode — long-lived, NON-Spark sqlglot transpile server.

    Wired by `clavesa ui` to transpile authored Spark serving-SQL to
    Athena/Trino for cloud serving. No Spark, no Connect, no pyspark — just
    a pure-Python HTTP server over sqlglot, so it starts in milliseconds and
    carries none of the JVM cold-start weight of the query server.

    Routes:
      GET  /healthz   → 200 once sqlglot is imported (done at startup, so a
                        200 means the next /transpile won't pay an import).
      POST /transpile → body: {"sql": "..."}. Returns {"ok": True, "trino":
                        "..."} or {"ok": False, "error": "...", "line": ...,
                        "col": ...} (200 either way — client inspects ``ok``,
                        exactly like /parse). Empty sql → 400.
    """
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer  # noqa: PLC0415
    # Import once at startup so a failed import surfaces immediately and
    # /healthz only returns 200 once the transpiler is actually loadable.
    import sqlglot  # noqa: PLC0415, F401

    port = int(os.environ.get("CLAVESA_TRANSPILE_SERVER_PORT", "8770"))
    print(f"clavesa transpile server listening on :{port}", flush=True)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/healthz":
                # sqlglot imported at startup ⇒ ready.
                self._respond(200, b"ok", content_type="text/plain")
                return
            self._respond(404, b"", content_type="text/plain")

        def do_POST(self) -> None:  # noqa: N802
            if self.path != "/transpile":
                self._respond(404, b"", content_type="text/plain")
                return
            length = int(self.headers.get("Content-Length", "0") or 0)
            body = self.rfile.read(length).decode("utf-8") if length else ""
            try:
                req = json.loads(body) if body else {}
                sql = (req.get("sql") or "").strip()
            except Exception as exc:  # noqa: BLE001
                self._json(400, {"ok": False, "error": f"bad request body: {exc}"})
                return
            if not sql:
                self._json(400, {"ok": False, "error": "empty SQL"})
                return
            # _transpile_sql never raises — it returns the {ok,error}
            # envelope. The HTTP layer forwards it as 200; the client
            # inspects ``ok`` to distinguish transpile failure from
            # transport failure.
            self._json(200, _transpile_sql(sql))

        def _json(self, code: int, payload: dict) -> None:
            data = json.dumps(payload).encode("utf-8")
            self._respond(code, data, content_type="application/json")

        def _respond(self, code: int, data: bytes, *, content_type: str) -> None:
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            if data:
                self.wfile.write(data)

        def log_message(self, fmt: str, *args) -> None:  # noqa: ARG002
            # Quiet — match the rest of the runner's stdout discipline.
            return

    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()


def run_connect_server() -> None:
    """CLAVESA_CONNECT_SERVER=1 mode — Spark Connect server only, no HTTP proxy.

    Reserved for the Slice 1 architecture where the in-container Python
    supervisor for notebook REPLs runs as a separate process from the Connect
    server. Slice 0's query-server mode embeds the plugin and the HTTP server
    in the same process, so this entry isn't wired by Go yet — but exists for
    standalone testing (`docker run -e CLAVESA_CONNECT_SERVER=1 ... clavesa-runner`).
    """
    import signal  # noqa: PLC0415

    _start_connect_plugin()
    port = os.environ.get("CLAVESA_CONNECT_PORT", "15002")
    print(f"clavesa connect server listening on :{port}", flush=True)
    signal.pause()  # block until SIGTERM/SIGINT — clean `docker stop`


# ---------------------------------------------------------------------------
# Metastore-server mode (CLAVESA_METASTORE_SERVER=1) — long-lived Derby
# Network Server, the shared local metastore (the local analog of Glue).
# ---------------------------------------------------------------------------


def run_metastore_server() -> None:
    """CLAVESA_METASTORE_SERVER=1 mode — launch the Derby Network Server.

    Owns ``$CLAVESA_WAREHOUSE/_metastore/metastore_db`` and serves it over
    JDBC on port 1527 so multiple local Spark processes (the warm query
    worker, on-demand pipeline-run containers, preview, one-shot query,
    notebooks) connect as CLIENTS via ``jdbc:derby://<addr>/metastore_db``
    instead of contending for embedded Derby's single-writer lock. This is
    the local twin of cloud's shared Glue Data Catalog.

    Long-lived: exec's the JVM Network Server, which blocks accepting
    connections until ``docker stop`` (SIGTERM) tears it down. Derby's
    ``started and ready to accept connections`` readiness line goes to
    stdout (NOT suppressed) so the Go side can poll ``docker logs`` for it
    before pointing clients at the server.

    The derby + derbyshared engine jars and the derbynet network-server jar
    all live in ``$SPARK_HOME/jars`` (download_jars.sh). We resolve the full
    classpath from that directory so the launch picks up whatever Derby the
    image ships without hardcoding versioned filenames.
    """
    warehouse = os.environ.get("CLAVESA_WAREHOUSE", "/tmp/clavesa-warehouse")
    metastore_dir = warehouse.rstrip("/") + "/_metastore"
    os.makedirs(metastore_dir, exist_ok=True)

    port = os.environ.get("CLAVESA_METASTORE_PORT", "1527")

    spark_home = os.environ.get(
        "SPARK_HOME", "/var/lang/lib/python3.12/site-packages/pyspark"
    )
    jars_dir = os.path.join(spark_home, "jars")
    # Whole jars dir on the classpath — the Derby Network Server only needs
    # derby/derbyshared/derbynet, but globbing the dir keeps this resilient
    # to version bumps and is identical to how Spark itself loads them.
    classpath = os.path.join(jars_dir, "*")

    cmd = [
        "java",
        "-cp",
        classpath,
        f"-Dderby.system.home={metastore_dir}",
        "org.apache.derby.drda.NetworkServerControl",
        "start",
        "-h",
        "0.0.0.0",
        "-p",
        port,
        "-noSecurityManager",
    ]
    print(
        f"clavesa metastore server: derby.system.home={metastore_dir} "
        f"listening on 0.0.0.0:{port}",
        flush=True,
    )
    # exec replaces this Python process so SIGTERM from `docker stop` reaches
    # the JVM directly and Derby's stdout readiness line is the container's
    # stdout (no buffering layer in between).
    os.execvp(cmd[0], cmd)


if os.environ.get("CLAVESA_METASTORE_SERVER") == "1":
    try:
        run_metastore_server()
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"error": str(exc)}), file=sys.stderr, flush=True)
        sys.exit(1)
elif os.environ.get("CLAVESA_PREVIEW") == "1":
    try:
        run_preview()
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"error": str(exc)}), file=sys.stderr, flush=True)
        sys.exit(1)
elif os.environ.get("CLAVESA_RUN") == "1":
    try:
        run_local()
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"error": str(exc)}), file=sys.stderr, flush=True)
        sys.exit(1)
elif os.environ.get("CLAVESA_QUERY") == "1":
    try:
        run_query()
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"error": str(exc)}), flush=True)
        sys.exit(1)
elif os.environ.get("CLAVESA_QUERY_SERVER") == "1":
    try:
        run_query_server()
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"error": str(exc)}), file=sys.stderr, flush=True)
        sys.exit(1)
elif os.environ.get("CLAVESA_CONNECT_SERVER") == "1":
    try:
        run_connect_server()
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"error": str(exc)}), file=sys.stderr, flush=True)
        sys.exit(1)
elif os.environ.get("CLAVESA_RECORD_RUN") == "1":
    try:
        run_record_run()
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"error": str(exc)}), file=sys.stderr, flush=True)
        sys.exit(1)
elif os.environ.get("CLAVESA_TRANSPILE_SERVER") == "1":
    try:
        run_transpile_server()
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"error": str(exc)}), file=sys.stderr, flush=True)
        sys.exit(1)
