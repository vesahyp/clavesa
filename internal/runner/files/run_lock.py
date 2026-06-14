"""Warehouse-resident run lease for the deployed pipeline Lambda (ADR-024).

Python mirror of internal/runlock's S3 backend: one lease object at
``s3://<bucket>/<pipeline>/_locks/run.json``, CAS via S3 conditional PUT
(``If-None-Match: *`` to create, ``If-Match: <etag>`` to take over / renew /
release). Every compute — the deployed Lambda, a laptop, a second laptop —
contends on the same object, which is what protects the single-driver Delta
log (S3SingleDriverLogStore tolerates exactly one Spark driver writing a
table at a time).

Protocol constants and semantics are copied from internal/runlock/runlock.go
and MUST stay in sync with it:

  - lease JSON: ``{"holder": {"run_id", "compute", "host", "pid"},
    "acquired_at", "expires_at" (RFC3339 UTC), "ttl_s": 120, "nonce",
    "state": "held" | "released", "module_version"}``
  - TTL 120s; the heartbeat renews every 30s via conditional PUT If-Match;
  - takeover is allowed when ``state == "released"`` (tombstone) OR the
    lease is expired more than 30s past ``expires_at`` (clock-skew grace);
  - a lost heartbeat (412) logs loudly and sets ``.lost`` — it NEVER aborts
    the in-flight run; the Delta commit is the last defense;
  - release writes a ``state: "released"`` tombstone via If-Match rather
    than DELETE (S3 DELETE carries no If-Match, so the tombstone is the only
    race-free "free" signal); a release after a lost heartbeat writes
    nothing, so it can never clobber the usurper's lease.

stdlib + boto3 only. boto3 (which ships in the Lambda image) is imported
lazily inside ``acquire_for_warehouse`` so the stdlib-only unit tests can
drive the whole protocol through a fake client object.

Version-skew note: an already-deployed Lambda only enforces this lock once
its image carries this module — i.e. after ``workspace upgrade`` + deploy.
Until then only the Go-side local file lock and the best-effort pre-flight
in service.RunPipelineCloud guard the warehouse.
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import re
import sys
import threading
import uuid

# Mirror internal/runlock/runlock.go's constants exactly.
TTL_S = 120
HEARTBEAT_INTERVAL_S = 30.0
TAKEOVER_GRACE_S = 30

STATE_HELD = "held"
STATE_RELEASED = "released"


class HeldError(Exception):
    """The run lock is currently held by another run.

    ``str(exc)`` mirrors internal/runlock's HeldError.Error() so the same
    "run lock held by run <id> (compute=…, host=…, acquired …s ago)" message
    surfaces regardless of which side rejected the run.
    """

    def __init__(self, holder, acquired_at=None, expires_at=None, now_fn=None):
        self.holder = dict(holder or {})
        self.acquired_at = acquired_at
        self.expires_at = expires_at
        age_s = 0
        if acquired_at is not None:
            now = (now_fn or _utc_now)()
            age_s = max(0, int((now - acquired_at).total_seconds()))
        super().__init__(
            "run lock held by run {rid} (compute={compute}, host={host}, "
            "acquired {age}s ago)".format(
                rid=self.holder.get("run_id") or "unknown",
                compute=self.holder.get("compute", ""),
                host=self.holder.get("host", ""),
                age=age_s,
            )
        )


# ---------------------------------------------------------------------------
# Time helpers — RFC3339 UTC, interoperable with Go's time.Time JSON encoding.
# ---------------------------------------------------------------------------


def _utc_now():
    return _dt.datetime.now(_dt.timezone.utc)


def _format_ts(dt):
    """RFC3339 UTC with a Z suffix (what Go's time.Time unmarshals)."""
    return dt.astimezone(_dt.timezone.utc).isoformat().replace("+00:00", "Z")


_FRACTION_RE = re.compile(r"\.(\d+)")


def _parse_ts(raw):
    """Parse an RFC3339 timestamp as Go's time.Time emits it.

    Go writes up to nanosecond fractions; ``fromisoformat`` caps at
    microseconds — trim the fraction to 6 digits before parsing.
    """
    s = str(raw or "").strip().replace("Z", "+00:00")
    s = _FRACTION_RE.sub(lambda m: "." + m.group(1)[:6], s, count=1)
    return _dt.datetime.fromisoformat(s)


# ---------------------------------------------------------------------------
# S3 error classification — duck-typed off botocore's ClientError shape
# (exc.response["Error"]["Code"] / ["ResponseMetadata"]["HTTPStatusCode"])
# so the module never needs to import botocore itself.
# ---------------------------------------------------------------------------


def _error_info(exc):
    resp = getattr(exc, "response", None)
    code, status = "", 0
    if isinstance(resp, dict):
        code = str((resp.get("Error") or {}).get("Code") or "")
        try:
            status = int((resp.get("ResponseMetadata") or {}).get("HTTPStatusCode") or 0)
        except (TypeError, ValueError):
            status = 0
    return code, status


def _is_not_found(exc):
    code, status = _error_info(exc)
    return code in ("NoSuchKey", "NotFound", "404") or status == 404


def _is_conditional_failure(exc):
    """Mirrors runlock/s3.go isConditionalFailure: 412 PreconditionFailed
    (If-Match / If-None-Match miss) and 409 ConditionalRequestConflict
    (concurrent conditional writers)."""
    code, status = _error_info(exc)
    return code in ("PreconditionFailed", "ConditionalRequestConflict") or status in (409, 412)


# ---------------------------------------------------------------------------
# Storage primitives
# ---------------------------------------------------------------------------


def _get(client, bucket, key):
    """Return (lease_doc, etag) or (None, "") when no lease object exists."""
    try:
        out = client.get_object(Bucket=bucket, Key=key)
    except Exception as exc:  # noqa: BLE001 — classified below
        if _is_not_found(exc):
            return None, ""
        raise
    body = out["Body"].read()
    if isinstance(body, bytes):
        body = body.decode("utf-8")
    return json.loads(body), str(out.get("ETag") or "")


def _put(client, bucket, key, doc, *, if_match=None, if_none_match=None):
    """Conditional PUT; returns the new ETag. Conditional failures raise the
    client's own exception — callers classify via _is_conditional_failure."""
    kwargs = {
        "Bucket": bucket,
        "Key": key,
        "Body": json.dumps(doc).encode("utf-8"),
        "ContentType": "application/json",
    }
    if if_match is not None:
        kwargs["IfMatch"] = if_match
    if if_none_match is not None:
        kwargs["IfNoneMatch"] = if_none_match
    out = client.put_object(**kwargs)
    return str((out or {}).get("ETag") or "")


# ---------------------------------------------------------------------------
# Key derivation
# ---------------------------------------------------------------------------


def lock_target(warehouse_uri):
    """Derive the (bucket, key) of the run-lock object from CLAVESA_WAREHOUSE.

    Deployed Lambdas get ``CLAVESA_WAREHOUSE =
    "s3://<bucket>/<pipeline>/_warehouse/"`` (tfgen.go); the lock lives at
    ``<pipeline>/_locks/run.json`` — the same key the Go side computes
    (runlock.New: ``path.Join(prefix, pipeline, "_locks", "run.json")``
    against the bucket root), deliberately under the pipeline prefix so the
    existing per-pipeline IAM grant (``${bucket}/${pipeline}/*/*``) covers it
    without a policy change.

    Returns None for any non-s3 warehouse (local runs take the Go-side file
    lock instead) and for a bucket-root warehouse with no pipeline prefix to
    scope the lock under.
    """
    uri = str(warehouse_uri or "")
    if not uri.startswith("s3://"):
        return None
    bucket, _, prefix = uri[len("s3://"):].partition("/")
    if not bucket:
        return None
    segments = [s for s in prefix.split("/") if s]
    if segments and segments[-1] == "_warehouse":
        segments = segments[:-1]
    if not segments:
        return None
    return bucket, "/".join(segments + ["_locks", "run.json"])


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


def _new_doc(holder, now, ttl_s, module_version):
    holder = holder or {}
    return {
        "holder": {
            "run_id": str(holder.get("run_id", "") or ""),
            "compute": str(holder.get("compute", "") or ""),
            "host": str(holder.get("host", "") or ""),
            "pid": int(holder.get("pid", 0) or 0),
        },
        "acquired_at": _format_ts(now),
        "expires_at": _format_ts(now + _dt.timedelta(seconds=ttl_s)),
        "ttl_s": int(ttl_s),
        "nonce": uuid.uuid4().hex,
        "state": STATE_HELD,
        "module_version": module_version
        or os.environ.get("CLAVESA_MODULE_VERSION", "unknown"),
    }


def _held_from(cur, now_fn):
    acquired_at = expires_at = None
    try:
        acquired_at = _parse_ts(cur.get("acquired_at"))
    except (TypeError, ValueError):
        pass
    try:
        expires_at = _parse_ts(cur.get("expires_at"))
    except (TypeError, ValueError):
        pass
    return HeldError(cur.get("holder") or {}, acquired_at, expires_at, now_fn=now_fn)


def acquire(client, bucket, key, holder, *, ttl_s=TTL_S, now_fn=None, module_version=None):
    """Take the lease or raise HeldError carrying the current holder.

    Mirrors runlock.lock.Acquire: free (no object), tombstoned
    (``state == "released"``), and expired past TAKEOVER_GRACE_S all acquire
    via conditional PUT; anything else raises HeldError. A lost
    create/takeover race re-reads the object and reports whoever won.
    """
    now_fn = now_fn or _utc_now
    doc = _new_doc(holder, now_fn(), ttl_s, module_version)
    cur, etag = _get(client, bucket, key)
    try:
        if cur is None:
            etag = _put(client, bucket, key, doc, if_none_match="*")
        else:
            try:
                expired = now_fn() > _parse_ts(cur.get("expires_at")) + _dt.timedelta(
                    seconds=TAKEOVER_GRACE_S
                )
            except (TypeError, ValueError):
                # Unparseable lease: treat as abandoned — the conditional
                # replace below still loses cleanly if someone owns it.
                expired = True
            if cur.get("state") == STATE_RELEASED or expired:
                etag = _put(client, bucket, key, doc, if_match=etag)
            else:
                raise _held_from(cur, now_fn)
    except HeldError:
        raise
    except Exception as exc:  # noqa: BLE001 — classified below
        if not _is_conditional_failure(exc):
            raise
        # Lost the create/takeover race. Report whoever won.
        try:
            won, _ = _get(client, bucket, key)
        except Exception:  # noqa: BLE001 — best-effort identification
            won = None
        if won is not None:
            raise _held_from(won, now_fn) from None
        raise HeldError({"run_id": "unknown"}, now_fn=now_fn) from None
    return Lease(client, bucket, key, doc, etag, ttl_s=ttl_s, now_fn=now_fn)


def acquire_for_warehouse(warehouse_uri, holder, *, client=None, **kwargs):
    """Acquire the run lease for a deployed (s3://) warehouse.

    Returns None (no lock taken) for non-s3 warehouses — local docker bundle
    runs are already serialized by the Go-side file lock. Raises HeldError
    when another run holds the lease.
    """
    target = lock_target(warehouse_uri)
    if target is None:
        return None
    if client is None:
        import boto3  # noqa: PLC0415 — ships in the Lambda image

        client = boto3.client("s3")
    return acquire(client, target[0], target[1], holder, **kwargs)


class Lease:
    """A held run lease.

    ``start_heartbeat()`` keeps it renewed from a daemon thread (the
    ``_ProgressPoller`` pattern in runner.py); ``release()`` writes the
    ``state: "released"`` tombstone once the run is observably terminal.
    ``lost`` reports that a renewal discovered the lease was taken away —
    by design the in-flight run continues anyway: aborting a mid-flight
    Spark write is worse than racing, and the Delta commit is the last
    defense.
    """

    def __init__(self, client, bucket, key, doc, etag, *, ttl_s=TTL_S, now_fn=None):
        self._client = client
        self._bucket = bucket
        self._key = key
        self._doc = doc
        self._etag = etag
        self._ttl_s = ttl_s
        self._now = now_fn or _utc_now
        self._mu = threading.Lock()
        self._lost = False
        self._released = False
        # NB: must NOT be named ``_stop`` — see _ProgressPoller in runner.py
        # (shadowing threading.Thread._stop corrupts Spark's worker fork).
        self._stop_event = threading.Event()
        self._thread = None

    @property
    def lost(self):
        with self._mu:
            return self._lost

    def holder(self):
        with self._mu:
            return dict(self._doc.get("holder") or {})

    def _where(self):
        return "s3://%s/%s" % (self._bucket, self._key)

    def start_heartbeat(self, interval_s=HEARTBEAT_INTERVAL_S):
        """Spawn the renewal daemon thread. Safe to call once; release()
        stops it. No-op when the lease is already released."""
        with self._mu:
            if self._thread is not None or self._released:
                return
            self._thread = threading.Thread(
                target=self._heartbeat_loop,
                args=(interval_s,),
                daemon=True,
                name="clavesa-run-lock-heartbeat",
            )
        self._thread.start()

    def _heartbeat_loop(self, interval_s):
        while not self._stop_event.wait(interval_s):
            try:
                self.renew()
            except Exception as exc:  # noqa: BLE001 — never crash the thread
                print(
                    "[clavesa] run lock heartbeat at %s raised (will retry): %r"
                    % (self._where(), exc),
                    file=sys.stderr,
                    flush=True,
                )
            if self.lost:
                return

    def renew(self):
        """One renewal: extend expires_at by TTL via conditional PUT If-Match.

        A conditional failure means the lock was taken over: log LOUDLY, set
        ``lost``, and return — NEVER abort the in-flight run from here.
        Transient errors are logged and retried on the next tick.
        """
        with self._mu:
            if self._released or self._lost:
                return
            doc = dict(self._doc)
            doc["expires_at"] = _format_ts(self._now() + _dt.timedelta(seconds=self._ttl_s))
            try:
                etag = _put(self._client, self._bucket, self._key, doc, if_match=self._etag)
            except Exception as exc:  # noqa: BLE001 — classified below
                if _is_conditional_failure(exc):
                    self._lost = True
                    print(
                        "[clavesa] RUN LOCK LOST at %s: lease for run %s was taken "
                        "over mid-run — continuing the in-flight run anyway (the "
                        "Delta commit is the last defense against the race)"
                        % (self._where(), (self._doc.get("holder") or {}).get("run_id", "")),
                        file=sys.stderr,
                        flush=True,
                    )
                else:
                    print(
                        "[clavesa] run lock heartbeat at %s failed (will retry): %r"
                        % (self._where(), exc),
                        file=sys.stderr,
                        flush=True,
                    )
                return
            self._doc = doc
            self._etag = etag

    def release(self):
        """Stop the heartbeat and tombstone the lease (``state: "released"``)
        so the next acquirer takes over immediately instead of waiting out
        the TTL. Idempotent and best-effort — a failure here must never mask
        the run outcome (same stance as the node_runs writer).

        After a lost heartbeat nothing is written: the usurper owns the
        object now and our If-Match etag is stale — skipping the write is
        what guarantees we can't clobber their lease (mirrors Go's
        Lease.Release).
        """
        self._stop_event.set()
        t = self._thread
        if t is not None and t.is_alive():
            t.join(timeout=5.0)
        with self._mu:
            if self._released:
                return
            self._released = True
            if self._lost:
                return
            doc = dict(self._doc)
            doc["state"] = STATE_RELEASED
            doc["expires_at"] = _format_ts(self._now())
            try:
                _put(self._client, self._bucket, self._key, doc, if_match=self._etag)
            except Exception as exc:  # noqa: BLE001 — classified below
                if _is_conditional_failure(exc):
                    # Someone already took the lease over after a TTL lapse —
                    # nothing left to free, and writing would clobber them.
                    self._lost = True
                else:
                    print(
                        "[clavesa] release run lock at %s failed: %r"
                        % (self._where(), exc),
                        file=sys.stderr,
                        flush=True,
                    )
