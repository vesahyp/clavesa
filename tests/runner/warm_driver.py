"""Warm-session exec driver for the docker-gated runner integration suite.

TEST ASSET — lives in tests/runner/, mounted into the runner image at
container start by the Go harness (warm_test.go). It is NOT part of the
production runner and never ships in the image.

Purpose: the data-semantics integration tests (merge bounds, schema
evolution, promote, incremental CDF, OPTIMIZE) don't care how Spark
started — paying a full container + Spark JVM cold boot (~25s) per test
only re-proves the launcher. This driver boots ONE classic py4j session via
the production ``runner._spark()`` and then executes each test's driver
snippet against it, mirroring how a warm Lambda container reuses its
process and calls ``handler()`` repeatedly on one recycled SparkSession
(GH #43).

Wire protocol (modelled on notebook_repl.py): line-delimited JSON both ways.

Request, one JSON object per stdin line:

    {"id": "req-1", "source": "<python snippet>"}
    {"id": "req-2", "op": "exit"}

Response, one line on stdout, prefixed with a sentinel so the Go side can
pick it out of Spark's stdout noise:

    CLAVESA_WARM_JSON:{"id": "req-1", "ok": true, "result": <RESULT>}
    CLAVESA_WARM_JSON:{"id": "req-1", "ok": false, "error": "<traceback>"}

Snippet contract: each snippet runs via ``exec`` in a FRESH namespace that
contains the shared ``spark`` session; it communicates its outcome by
assigning a JSON-serializable dict to ``RESULT`` (replaces the cold-path
``print("RESULT_LINE:" + ...)`` convention). Isolation between snippets is
the tests' responsibility: unique database names per test, per-test
watermark dirs, no reliance on temp views or env left by a prior snippet.
"""

from __future__ import annotations

import json
import os
import sys
import traceback

SENTINEL = "CLAVESA_WARM_JSON:"


def _emit(payload: dict) -> None:
    """One sentinel-prefixed JSON line on fd 1, immune to stdout redirects
    and interleaving with Spark's own stdout chatter (single os.write)."""
    line = SENTINEL + json.dumps(payload, default=str) + "\n"
    os.write(1, line.encode("utf-8"))


def main() -> int:
    sys.path.insert(0, "/var/task")
    # The warehouse is frozen at session build; everything else
    # (CLAVESA_LANGUAGE, CLAVESA_LOGIC_S3_PATH, CLAVESA_WATERMARKS, catalog/
    # schema/node) is read per handler() call and set by each snippet.
    os.environ.setdefault("CLAVESA_WAREHOUSE", "/warm/wh")

    from runner import _spark  # noqa: PLC0415 — needs sys.path first

    spark = _spark()
    _emit({"id": "__ready__", "ok": True})

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except json.JSONDecodeError as exc:
            _emit({"id": "__parse__", "ok": False, "error": f"bad json: {exc}"})
            continue

        rid = str(req.get("id", ""))
        if req.get("op") == "exit":
            _emit({"id": rid, "ok": True, "result": {"exiting": True}})
            return 0

        source = req.get("source", "")
        ns: dict = {"spark": spark, "RESULT": None}
        try:
            exec(compile(source, f"<warm:{rid}>", "exec"), ns)  # noqa: S102
            _emit({"id": rid, "ok": True, "result": ns.get("RESULT")})
        except BaseException:  # noqa: BLE001 — report any snippet failure, keep serving
            _emit({"id": rid, "ok": False, "error": traceback.format_exc()})
    return 0


if __name__ == "__main__":
    sys.exit(main())
