"""Unit tests for the runner's ADR-017 slice 1 http source path.

Run with: python3 tests/runner/test_http_source.py

Exercises only the pure-python download helper (`_download_http_to_tmp`)
— the Spark dispatch path is covered end-to-end by the docker-gated
runner_test.go suite once a workspace registry source is wired through
a transform.
"""

from __future__ import annotations

import http.server
import os
import socketserver
import threading
import importlib.util
import sys
import types
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RUNNER = REPO / "runner" / "runner.py"


def _load_runner():
    """Same stub-and-import trick as test_node_runs_row.py — keeps the
    test stdlib-only by faking the heavy native imports out before
    runner.py runs.
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


def _serve_bytes(payload: bytes):
    """Return (host, port, server, thread). Caller stops the server."""
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_a, **_k):
            pass

    server = socketserver.TCPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server.server_address[0], server.server_address[1], server, thread


def test_download_writes_file_and_caches():
    runner = _load_runner()
    payload = b"PAR1fake-parquet-bytes"
    host, port, server, _ = _serve_bytes(payload)
    try:
        url = f"http://{host}:{port}/data.parquet"
        path1 = runner._download_http_to_tmp(url)
        assert os.path.exists(path1), f"download not at {path1}"
        with open(path1, "rb") as f:
            assert f.read() == payload
        # Second call returns the same path (cached on disk).
        path2 = runner._download_http_to_tmp(url)
        assert path2 == path1
    finally:
        server.shutdown()
        if os.path.exists(path1):
            os.remove(path1)


def test_download_filename_preserves_extension():
    runner = _load_runner()
    host, port, server, _ = _serve_bytes(b"x")
    try:
        url = f"http://{host}:{port}/path/to/events.json"
        path = runner._download_http_to_tmp(url)
        assert path.endswith("events.json"), path
    finally:
        server.shutdown()
        if os.path.exists(path):
            os.remove(path)


if __name__ == "__main__":
    test_download_writes_file_and_caches()
    test_download_filename_preserves_extension()
    print("ok")
