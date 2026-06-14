"""Unit tests for runner/run_lock.py — the Lambda-side warehouse run lease
(ADR-024 slice 5, the Python mirror of internal/runlock's S3 backend).

Pure / stdlib-only — no Spark, no Docker, no boto3. The S3 conditional-PUT
semantics (If-None-Match: * / If-Match: <etag> / ETag rotation) are faked by
an in-memory client object raising botocore-shaped errors (an ``.response``
dict with Error.Code + ResponseMetadata.HTTPStatusCode — run_lock classifies
duck-typed, exactly like it classifies real botocore ClientErrors).

The pipeline_handler integration tests at the bottom load runner.py with the
same import-stub trick as test_progress.py / test_is_forced.py and stub the
``run_lock`` sibling import via sys.modules.

Run with: python3 tests/runner/test_run_lock.py
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import io
import json
import os
import sys
import types
import unittest
from contextlib import redirect_stderr
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RUNNER = REPO / "runner" / "runner.py"
RUN_LOCK = REPO / "runner" / "run_lock.py"


def _load_run_lock():
    """run_lock.py is stdlib-only at import time (boto3 is imported lazily
    inside acquire_for_warehouse), so no stubs are needed to load it."""
    spec = importlib.util.spec_from_file_location("run_lock_under_test", str(RUN_LOCK))
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _load_runner():
    """Import runner.py with boto3/pyspark stubbed — same trick as
    test_progress.py / test_is_forced.py."""
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
# Fake S3 with conditional-write semantics
# ---------------------------------------------------------------------------


class FakeClientError(Exception):
    """botocore.ClientError shape: run_lock reads .response duck-typed."""

    def __init__(self, code, status):
        self.response = {
            "Error": {"Code": code},
            "ResponseMetadata": {"HTTPStatusCode": status},
        }
        super().__init__(code)


class _Body:
    def __init__(self, data):
        self._data = data

    def read(self):
        return self._data


class FakeS3:
    """In-memory S3 honoring IfMatch / IfNoneMatch with rotating ETags."""

    def __init__(self):
        self.objects = {}  # (bucket, key) -> (bytes, etag)
        self._counter = 0
        self.put_calls = 0

    def _next_etag(self):
        self._counter += 1
        return '"etag-%d"' % self._counter

    def seed(self, bucket, key, doc):
        """Unconditional write (a foreign writer — e.g. the smoke script's
        fake lease, or a usurping acquirer)."""
        etag = self._next_etag()
        self.objects[(bucket, key)] = (json.dumps(doc).encode("utf-8"), etag)
        return etag

    def doc(self, bucket, key):
        data, _ = self.objects[(bucket, key)]
        return json.loads(data.decode("utf-8"))

    def get_object(self, Bucket, Key):  # noqa: N803 — boto3 casing
        obj = self.objects.get((Bucket, Key))
        if obj is None:
            raise FakeClientError("NoSuchKey", 404)
        data, etag = obj
        return {"Body": _Body(data), "ETag": etag}

    def put_object(self, Bucket, Key, Body, ContentType=None, IfMatch=None, IfNoneMatch=None):  # noqa: N803
        self.put_calls += 1
        cur = self.objects.get((Bucket, Key))
        if IfNoneMatch == "*" and cur is not None:
            raise FakeClientError("PreconditionFailed", 412)
        if IfMatch is not None and (cur is None or cur[1] != IfMatch):
            raise FakeClientError("PreconditionFailed", 412)
        etag = self._next_etag()
        body = Body if isinstance(Body, bytes) else str(Body).encode("utf-8")
        self.objects[(Bucket, Key)] = (body, etag)
        return {"ETag": etag}


# ---------------------------------------------------------------------------
# Test scaffolding
# ---------------------------------------------------------------------------

T0 = dt.datetime(2026, 6, 13, 12, 0, 0, tzinfo=dt.timezone.utc)

BUCKET = "smoke-bucket"
KEY = "taxi/_locks/run.json"

HOLDER_A = {"run_id": "run-a", "compute": "lambda", "host": "fn-a", "pid": 1}
HOLDER_B = {"run_id": "run-b", "compute": "local", "host": "laptop-b", "pid": 2}


class Clock:
    def __init__(self, now=T0):
        self.now = now

    def advance(self, seconds):
        self.now = self.now + dt.timedelta(seconds=seconds)

    def __call__(self):
        return self.now


class RunLockTestCase(unittest.TestCase):
    def setUp(self):
        self.rl = _load_run_lock()
        self.s3 = FakeS3()
        self.clock = Clock()

    def acquire(self, holder=HOLDER_A):
        return self.rl.acquire(self.s3, BUCKET, KEY, holder, now_fn=self.clock)

    def seed_lease(self, holder=HOLDER_B, state="held", acquired_offset_s=0, expires_offset_s=120):
        return self.s3.seed(BUCKET, KEY, {
            "holder": dict(holder),
            "acquired_at": self.rl._format_ts(self.clock.now + dt.timedelta(seconds=acquired_offset_s)),
            "expires_at": self.rl._format_ts(self.clock.now + dt.timedelta(seconds=expires_offset_s)),
            "ttl_s": 120,
            "nonce": "foreign-nonce",
            "state": state,
            "module_version": "vtest",
        })


# ---------------------------------------------------------------------------
# Key derivation
# ---------------------------------------------------------------------------


class LockTargetTests(RunLockTestCase):
    def test_deployed_warehouse_uri(self):
        # The exact shape tfgen emits for the pipeline Lambda.
        self.assertEqual(
            self.rl.lock_target("s3://my-bucket/taxi/_warehouse/"),
            ("my-bucket", "taxi/_locks/run.json"),
        )

    def test_no_trailing_slash(self):
        self.assertEqual(
            self.rl.lock_target("s3://my-bucket/taxi/_warehouse"),
            ("my-bucket", "taxi/_locks/run.json"),
        )

    def test_prefix_without_warehouse_segment(self):
        # Same key as Go's runlock.New("s3://my-bucket", "taxi").
        self.assertEqual(
            self.rl.lock_target("s3://my-bucket/taxi/"),
            ("my-bucket", "taxi/_locks/run.json"),
        )

    def test_nested_prefix(self):
        self.assertEqual(
            self.rl.lock_target("s3://b/team/taxi/_warehouse/"),
            ("b", "team/taxi/_locks/run.json"),
        )

    def test_non_s3_is_none(self):
        self.assertIsNone(self.rl.lock_target("/tmp/clavesa-warehouse"))
        self.assertIsNone(self.rl.lock_target(""))
        self.assertIsNone(self.rl.lock_target(None))

    def test_bucket_root_is_none(self):
        # No pipeline prefix to scope the lock under — refuse rather than
        # invent a workspace-global lock key.
        self.assertIsNone(self.rl.lock_target("s3://my-bucket/_warehouse/"))
        self.assertIsNone(self.rl.lock_target("s3://my-bucket/"))
        self.assertIsNone(self.rl.lock_target("s3://"))


# ---------------------------------------------------------------------------
# Acquire
# ---------------------------------------------------------------------------


class AcquireTests(RunLockTestCase):
    def test_acquire_free(self):
        lease = self.acquire()
        self.assertIsNotNone(lease)
        doc = self.s3.doc(BUCKET, KEY)
        self.assertEqual(doc["state"], "held")
        self.assertEqual(doc["holder"], HOLDER_A)
        self.assertEqual(doc["ttl_s"], 120)
        self.assertTrue(doc["nonce"])
        self.assertEqual(
            self.rl._parse_ts(doc["expires_at"]),
            T0 + dt.timedelta(seconds=120),
        )
        self.assertEqual(self.rl._parse_ts(doc["acquired_at"]), T0)

    def test_held_raises_structured_error(self):
        self.seed_lease(holder=HOLDER_B, acquired_offset_s=-8)
        with self.assertRaises(self.rl.HeldError) as cm:
            self.acquire()
        exc = cm.exception
        self.assertEqual(exc.holder["run_id"], "run-b")
        self.assertEqual(exc.holder["compute"], "local")
        msg = str(exc)
        self.assertIn("run lock held by run run-b", msg)
        self.assertIn("compute=local", msg)
        self.assertIn("host=laptop-b", msg)
        self.assertIn("acquired 8s ago", msg)
        # The held doc was not touched.
        self.assertEqual(self.s3.doc(BUCKET, KEY)["holder"], HOLDER_B)

    def test_tombstone_takeover(self):
        self.seed_lease(holder=HOLDER_B, state="released")
        lease = self.acquire()
        self.assertIsNotNone(lease)
        doc = self.s3.doc(BUCKET, KEY)
        self.assertEqual(doc["state"], "held")
        self.assertEqual(doc["holder"], HOLDER_A)

    def test_expired_takeover(self):
        # Expired 31s past expires_at: beyond the 30s grace — take over.
        self.seed_lease(holder=HOLDER_B, expires_offset_s=-31)
        lease = self.acquire()
        self.assertIsNotNone(lease)
        self.assertEqual(self.s3.doc(BUCKET, KEY)["holder"], HOLDER_A)

    def test_expired_within_grace_still_held(self):
        # Expired, but within the 30s clock-skew grace — still held.
        self.seed_lease(holder=HOLDER_B, expires_offset_s=-10)
        with self.assertRaises(self.rl.HeldError):
            self.acquire()

    def test_raced_create_reports_winner(self):
        # The conditional create loses because a winner appeared between the
        # GET (NoSuchKey) and the PUT If-None-Match — fake by making the
        # object appear only at put time.
        s3 = self.s3
        real_put = s3.put_object

        def racing_put(**kwargs):
            if not s3.objects:
                self.seed_lease(holder=HOLDER_B)
            return real_put(**kwargs)

        s3.put_object = racing_put
        with self.assertRaises(self.rl.HeldError) as cm:
            self.acquire()
        self.assertEqual(cm.exception.holder["run_id"], "run-b")


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------


class HeartbeatTests(RunLockTestCase):
    def test_renew_extends_expiry(self):
        lease = self.acquire()
        self.clock.advance(30)
        lease.renew()
        self.assertFalse(lease.lost)
        doc = self.s3.doc(BUCKET, KEY)
        self.assertEqual(
            self.rl._parse_ts(doc["expires_at"]),
            T0 + dt.timedelta(seconds=30 + 120),
        )
        self.assertEqual(doc["state"], "held")
        self.assertEqual(doc["holder"], HOLDER_A)

    def test_renew_412_sets_lost_no_raise(self):
        lease = self.acquire()
        # A usurper rewrites the object — our etag is now stale.
        usurper_etag = self.seed_lease(holder=HOLDER_B)
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            lease.renew()  # must NOT raise
        self.assertTrue(lease.lost)
        self.assertIn("RUN LOCK LOST", stderr.getvalue())
        # The usurper's lease is untouched.
        self.assertEqual(self.s3.objects[(BUCKET, KEY)][1], usurper_etag)
        self.assertEqual(self.s3.doc(BUCKET, KEY)["holder"], HOLDER_B)
        # Subsequent renews are no-ops (no further writes).
        puts = self.s3.put_calls
        lease.renew()
        self.assertEqual(self.s3.put_calls, puts)

    def test_renew_transient_error_retries_no_lost(self):
        lease = self.acquire()
        real_put = self.s3.put_object

        def flaky_put(**kwargs):
            raise FakeClientError("SlowDown", 503)

        self.s3.put_object = flaky_put
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            lease.renew()  # logged, not raised, not lost
        self.assertFalse(lease.lost)
        self.assertIn("will retry", stderr.getvalue())
        # Next tick succeeds again.
        self.s3.put_object = real_put
        self.clock.advance(30)
        lease.renew()
        self.assertFalse(lease.lost)

    def test_heartbeat_thread_renews(self):
        lease = self.acquire()
        before = self.s3.doc(BUCKET, KEY)["expires_at"]
        self.clock.advance(5)
        lease.start_heartbeat(interval_s=0.01)
        deadline = dt.datetime.now() + dt.timedelta(seconds=2)
        while self.s3.doc(BUCKET, KEY)["expires_at"] == before:
            if dt.datetime.now() > deadline:
                self.fail("heartbeat thread never renewed the lease")
        lease.release()


# ---------------------------------------------------------------------------
# Release
# ---------------------------------------------------------------------------


class ReleaseTests(RunLockTestCase):
    def test_release_writes_tombstone(self):
        lease = self.acquire()
        self.clock.advance(42)
        lease.release()
        doc = self.s3.doc(BUCKET, KEY)
        self.assertEqual(doc["state"], "released")
        self.assertEqual(doc["holder"], HOLDER_A)
        self.assertEqual(
            self.rl._parse_ts(doc["expires_at"]),
            T0 + dt.timedelta(seconds=42),
        )
        # Tombstone unblocks the next acquirer immediately.
        lease2 = self.rl.acquire(self.s3, BUCKET, KEY, HOLDER_B, now_fn=self.clock)
        self.assertIsNotNone(lease2)
        self.assertEqual(self.s3.doc(BUCKET, KEY)["holder"], HOLDER_B)

    def test_release_idempotent(self):
        lease = self.acquire()
        lease.release()
        puts = self.s3.put_calls
        lease.release()
        self.assertEqual(self.s3.put_calls, puts)

    def test_release_after_loss_does_not_clobber_usurper(self):
        lease = self.acquire()
        usurper_etag = self.seed_lease(holder=HOLDER_B)
        with redirect_stderr(io.StringIO()):
            lease.renew()  # discovers the loss
        self.assertTrue(lease.lost)
        lease.release()
        # The usurper's lease object is byte-identical: no write happened.
        self.assertEqual(self.s3.objects[(BUCKET, KEY)][1], usurper_etag)
        doc = self.s3.doc(BUCKET, KEY)
        self.assertEqual(doc["state"], "held")
        self.assertEqual(doc["holder"], HOLDER_B)

    def test_release_racing_takeover_does_not_clobber(self):
        # Loss not yet discovered (no heartbeat ran): the release's If-Match
        # is the guard — it 412s and leaves the usurper alone.
        lease = self.acquire()
        usurper_etag = self.seed_lease(holder=HOLDER_B)
        lease.release()  # must not raise
        self.assertEqual(self.s3.objects[(BUCKET, KEY)][1], usurper_etag)
        self.assertEqual(self.s3.doc(BUCKET, KEY)["holder"], HOLDER_B)
        self.assertTrue(lease.lost)


# ---------------------------------------------------------------------------
# acquire_for_warehouse plumbing
# ---------------------------------------------------------------------------


class AcquireForWarehouseTests(RunLockTestCase):
    def test_non_s3_warehouse_is_noop(self):
        self.assertIsNone(
            self.rl.acquire_for_warehouse("/tmp/wh", HOLDER_A, client=self.s3)
        )
        self.assertEqual(self.s3.put_calls, 0)

    def test_s3_warehouse_acquires_at_derived_key(self):
        lease = self.rl.acquire_for_warehouse(
            "s3://%s/taxi/_warehouse/" % BUCKET, HOLDER_A,
            client=self.s3, now_fn=self.clock,
        )
        self.assertIsNotNone(lease)
        self.assertIn((BUCKET, KEY), self.s3.objects)


# ---------------------------------------------------------------------------
# pipeline_handler integration (runner.py:_acquire_run_lease / _run_lock_holder)
# ---------------------------------------------------------------------------


class _EnvPatch:
    def __init__(self, **kv):
        self.kv = kv
        self.saved = {}

    def __enter__(self):
        for k, v in self.kv.items():
            self.saved[k] = os.environ.get(k)
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def __exit__(self, *exc):
        for k, old in self.saved.items():
            if old is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = old


def _stub_run_lock_module(behavior):
    """Install a fake `run_lock` sibling module so runner.py's lazy
    `import run_lock` resolves to it (runner/ isn't on sys.path here)."""
    mod = types.ModuleType("run_lock")

    class HeldError(Exception):
        pass

    mod.HeldError = HeldError
    mod.acquire_for_warehouse = behavior(HeldError)
    sys.modules["run_lock"] = mod
    return mod


class RunnerAcquireLeaseTests(unittest.TestCase):
    def setUp(self):
        self.runner = _load_runner()
        self.addCleanup(sys.modules.pop, "run_lock", None)

    def test_non_s3_warehouse_no_lock(self):
        with _EnvPatch(CLAVESA_WAREHOUSE="/tmp/wh"):
            lease, held = self.runner._acquire_run_lease({"run_id": "r1"})
        self.assertIsNone(lease)
        self.assertIsNone(held)

    def test_held_local_returns_failed_result_shape(self):
        msg = "run lock held by run other (compute=lambda, host=fn, acquired 3s ago)"

        def behavior(held_cls):
            def acquire_for_warehouse(_uri, _holder, **_kw):
                raise held_cls(msg)
            return acquire_for_warehouse

        _stub_run_lock_module(behavior)
        with _EnvPatch(CLAVESA_WAREHOUSE="s3://b/p/_warehouse/", AWS_LAMBDA_FUNCTION_NAME=None):
            stdout = io.StringIO()
            sys_stdout, sys.stdout = sys.stdout, stdout
            try:
                lease, held = self.runner._acquire_run_lease({"run_id": "r1"})
            finally:
                sys.stdout = sys_stdout
        self.assertIsNone(lease)
        self.assertEqual(held["status"], "failed")
        self.assertEqual(held["transforms"], [])
        self.assertIsNone(held["failed_node"])
        self.assertEqual(held["error_class"], "RunLockHeld")
        self.assertEqual(held["error_msg"], msg)
        # The standard progress channel saw the failure event.
        event_line = json.loads(stdout.getvalue().strip().splitlines()[0])
        self.assertEqual(event_line["_event"], "failed")
        self.assertEqual(event_line["error_class"], "RunLockHeld")

    def test_held_on_lambda_raises_with_readable_cause(self):
        msg = "run lock held by run other (compute=local, host=laptop, acquired 5s ago)"

        def behavior(held_cls):
            def acquire_for_warehouse(_uri, _holder, **_kw):
                raise held_cls(msg)
            return acquire_for_warehouse

        _stub_run_lock_module(behavior)
        with _EnvPatch(CLAVESA_WAREHOUSE="s3://b/p/_warehouse/", AWS_LAMBDA_FUNCTION_NAME="clavesa-p-runner"):
            with redirect_stderr(io.StringIO()):
                sys_stdout, sys.stdout = sys.stdout, io.StringIO()
                try:
                    with self.assertRaises(RuntimeError) as cm:
                        self.runner._acquire_run_lease({"run_id": "r1"})
                finally:
                    sys.stdout = sys_stdout
        self.assertIn(msg, str(cm.exception))
        self.assertTrue(str(cm.exception).startswith("clavesa runner: "))

    def test_acquired_lease_heartbeat_started(self):
        calls = {}

        class FakeLease:
            def start_heartbeat(self):
                calls["hb"] = True

        def behavior(_held_cls):
            def acquire_for_warehouse(uri, holder, **_kw):
                calls["uri"] = uri
                calls["holder"] = holder
                return FakeLease()
            return acquire_for_warehouse

        _stub_run_lock_module(behavior)
        with _EnvPatch(CLAVESA_WAREHOUSE="s3://b/p/_warehouse/", AWS_LAMBDA_FUNCTION_NAME="clavesa-p-runner"):
            lease, held = self.runner._acquire_run_lease(
                {"_sf_execution_arn": "arn:aws:states:eu-north-1:1:execution:x:y"}
            )
        self.assertIsNone(held)
        self.assertIsInstance(lease, FakeLease)
        self.assertTrue(calls.get("hb"))
        self.assertEqual(calls["uri"], "s3://b/p/_warehouse/")
        # run_id falls back to the SFN execution ARN; compute is lambda;
        # host is the function name.
        self.assertEqual(calls["holder"]["run_id"], "arn:aws:states:eu-north-1:1:execution:x:y")
        self.assertEqual(calls["holder"]["compute"], "lambda")
        self.assertEqual(calls["holder"]["host"], "clavesa-p-runner")

    def test_holder_prefers_run_id(self):
        with _EnvPatch(AWS_LAMBDA_FUNCTION_NAME=None):
            holder = self.runner._run_lock_holder(
                {"run_id": "r-42", "_sf_execution_arn": "arn:x"}
            )
        self.assertEqual(holder["run_id"], "r-42")
        # No AWS_LAMBDA_FUNCTION_NAME → a local container driving an s3
        # warehouse (ADR-024 cloud-local compute): same keying as
        # _record_node_run's compute_target.
        self.assertEqual(holder["compute"], "local")
        self.assertTrue(holder["host"])  # gethostname fallback
        self.assertEqual(holder["pid"], os.getpid())


if __name__ == "__main__":
    unittest.main()
