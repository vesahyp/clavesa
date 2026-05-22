"""Unit tests for ADR-017 slice 2 header auth on http sources.

Run with: python3 tests/runner/test_http_auth.py

Exercises _resolve_secret + _resolve_http_headers + the request-header
path through _download_http_to_tmp. Stdlib-only mock server inspects the
Authorization header on each request so the test can assert the runner
actually injected it.
"""

from __future__ import annotations

import http.server
import importlib.util
import os
import socketserver
import sys
import tempfile
import threading
import types
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RUNNER = REPO / "runner" / "runner.py"


def _load_runner():
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


def _serve_authed(expected_auth: str, payload: bytes):
    seen_headers: list[dict] = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            seen_headers.append({k: v for k, v in self.headers.items()})
            if self.headers.get("Authorization") != expected_auth:
                self.send_response(401)
                self.end_headers()
                self.wfile.write(b"forbidden")
                return
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_a, **_k):
            pass

    server = socketserver.TCPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, seen_headers


def test_env_backend_resolves_value():
    runner = _load_runner()
    os.environ["MY_TEST_TOKEN"] = "topsecret"
    try:
        v = runner._resolve_secret("env:MY_TEST_TOKEN")
        assert v == "topsecret", v
    finally:
        del os.environ["MY_TEST_TOKEN"]


def test_file_backend_strips_trailing_newline():
    runner = _load_runner()
    with tempfile.NamedTemporaryFile("w", suffix=".secret", delete=False) as f:
        f.write("topsecret\n")
        path = f.name
    try:
        v = runner._resolve_secret(f"file:{path}")
        assert v == "topsecret", repr(v)
    finally:
        os.remove(path)


def test_env_backend_missing_var_raises():
    runner = _load_runner()
    try:
        runner._resolve_secret("env:DEFINITELY_NOT_SET_ANYWHERE")
    except RuntimeError as e:
        assert "DEFINITELY_NOT_SET_ANYWHERE" in str(e)
        return
    raise AssertionError("expected RuntimeError")


def test_unknown_backend_raises():
    runner = _load_runner()
    try:
        runner._resolve_secret("smb://nope")
    except RuntimeError as e:
        assert "unknown secret backend" in str(e)
        return
    raise AssertionError("expected RuntimeError")


def test_resolve_http_headers_assembles_bearer_token():
    runner = _load_runner()
    os.environ["STRIPE_KEY_FOR_TEST"] = "sk_test_abc"
    try:
        h = runner._resolve_http_headers({
            "kind": "header",
            "header_name": "Authorization",
            "value_prefix": "Bearer ",
            "secret": "env:STRIPE_KEY_FOR_TEST",
        })
        assert h == {"Authorization": "Bearer sk_test_abc"}, h
    finally:
        del os.environ["STRIPE_KEY_FOR_TEST"]


def test_resolve_http_headers_none_returns_none():
    runner = _load_runner()
    assert runner._resolve_http_headers(None) is None
    assert runner._resolve_http_headers({}) is None


def test_download_injects_header_into_request():
    runner = _load_runner()
    payload = b"PAR1authed-bytes"
    server, seen = _serve_authed("Bearer abc123", payload)
    host, port = server.server_address
    try:
        url = f"http://{host}:{port}/auth.parquet"
        path = runner._download_http_to_tmp(url, headers={"Authorization": "Bearer abc123"})
        assert os.path.exists(path), path
        with open(path, "rb") as f:
            assert f.read() == payload
        assert any(h.get("Authorization") == "Bearer abc123" for h in seen), seen
    finally:
        server.shutdown()
        if os.path.exists(path):
            os.remove(path)


if __name__ == "__main__":
    test_env_backend_resolves_value()
    test_file_backend_strips_trailing_newline()
    test_env_backend_missing_var_raises()
    test_unknown_backend_raises()
    test_resolve_http_headers_assembles_bearer_token()
    test_resolve_http_headers_none_returns_none()
    test_download_injects_header_into_request()
    print("ok")
