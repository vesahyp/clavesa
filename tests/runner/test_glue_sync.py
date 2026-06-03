"""Unit tests for `_sync_glue_table_schema` in runner/runner.py.

Run with: python3 tests/runner/test_glue_sync.py
   or:    python3 -m unittest tests/runner/test_glue_sync.py

These cover the cloud system-table repair path (the `location=` arg) that
fixes the orphaned Glue registration: `.option("path", …)` through the
Glue catalog leaves `StorageDescriptor.Location` at an empty
`…-__PLACEHOLDER__` stub and a null `spark.sql.sources.provider`, so
Athena / the catalog UI can't find the Delta log. The repair must run
even when the columns already match (the column-match short-circuit must
NOT skip the location fix). `location=None` (user-output tables) must
still short-circuit on a column match and never touch Location/provider.

boto3 is stubbed module-wide (same trick as test_node_runs_row.py); the
glue client is injected per-test via `boto3.client`.
"""

from __future__ import annotations

import importlib.util
import os
import sys
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


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _Field:
    """Minimal stand-in for a PySpark StructField — exposes `name` and a
    `dataType` whose `simpleString()` returns the Hive/Glue type string."""

    def __init__(self, name, type_str):
        self.name = name
        self.dataType = types.SimpleNamespace(simpleString=lambda t=type_str: t)


class _Schema:
    def __init__(self, pairs):
        self.fields = [_Field(n, t) for n, t in pairs]


class _FakeGlue:
    def __init__(self, table):
        self._table = table
        self.updated = None  # captures the TableInput of the last update_table

    def get_table(self, DatabaseName, Name):  # noqa: N803 — boto3 kwarg names
        return {"Table": self._table}

    def update_table(self, DatabaseName, TableInput):  # noqa: N803
        self.updated = TableInput


def _install_glue(runner, fake):
    """Point the runner's lazily-imported boto3.client at our fake glue."""
    sys.modules["boto3"].client = lambda *_a, **_k: fake  # type: ignore[attr-defined]


def _with_cloud_warehouse(fn):
    """Run fn with CLAVESA_WAREHOUSE set to an s3:// path (cloud / Glue mode);
    restore the prior value afterward."""
    saved = os.environ.get("CLAVESA_WAREHOUSE")
    os.environ["CLAVESA_WAREHOUSE"] = "s3://demo-bucket/_warehouse"
    try:
        fn()
    finally:
        if saved is None:
            os.environ.pop("CLAVESA_WAREHOUSE", None)
        else:
            os.environ["CLAVESA_WAREHOUSE"] = saved


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_system_table_repairs_orphaned_location_even_when_columns_match():
    """The reported cloud bug: a system table whose Glue entry has a
    `…-__PLACEHOLDER__` Location, a null provider, and a Hive-default SerDe,
    with columns ALREADY matching the written schema. The repair must still
    fire — set Location to the passed warehouse path and stamp the Delta
    provider — i.e. the column-match short-circuit does NOT skip it.
    """
    runner = _load_runner()

    cols = [{"Name": "run_id", "Type": "string"}, {"Name": "node", "Type": "string"}]
    table = {
        "Name": "node_runs",
        "StorageDescriptor": {
            "Columns": cols,
            "Location": "s3://demo-bucket/_warehouse/sys.db/node_runs-__PLACEHOLDER__",
            "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"},
        },
        "Parameters": {"EXTERNAL": "TRUE"},
    }
    fake = _FakeGlue(table)
    _install_glue(runner, fake)

    schema = _Schema([("run_id", "string"), ("node", "string")])
    target_location = "s3://demo-bucket/_system/pipelines/node_runs/"

    def go():
        runner._sync_glue_table_schema("sys.node_runs", schema, location=target_location)

    _with_cloud_warehouse(go)

    assert fake.updated is not None, "update_table must be called to repair the registration"
    sd = fake.updated["StorageDescriptor"]
    assert sd["Location"] == target_location, sd["Location"]
    params = fake.updated["Parameters"]
    assert params.get("spark.sql.sources.provider") == "delta", params
    assert params.get("spark.sql.partitionProvider") == "catalog", params
    # SerdeInfo and other SD fields are carried forward untouched.
    assert sd["SerdeInfo"]["SerializationLibrary"].endswith("LazySimpleSerDe")


def test_system_table_short_circuits_when_already_repaired():
    """Once Location + provider + columns all match, the repair path must
    short-circuit (no update_table) — idempotent on healthy registrations."""
    runner = _load_runner()

    cols = [{"Name": "run_id", "Type": "string"}]
    target_location = "s3://demo-bucket/_system/pipelines/node_runs/"
    table = {
        "Name": "node_runs",
        "StorageDescriptor": {"Columns": cols, "Location": target_location},
        "Parameters": {"spark.sql.sources.provider": "delta"},
    }
    fake = _FakeGlue(table)
    _install_glue(runner, fake)

    schema = _Schema([("run_id", "string")])

    def go():
        runner._sync_glue_table_schema("sys.node_runs", schema, location=target_location)

    _with_cloud_warehouse(go)
    assert fake.updated is None, "healthy system registration must not be rewritten"


def test_location_none_short_circuits_on_matching_columns():
    """User-output-table callers pass location=None: a column match must
    short-circuit with no update_table, exactly as before this change."""
    runner = _load_runner()

    cols = [{"Name": "a", "Type": "string"}, {"Name": "b", "Type": "bigint"}]
    table = {
        "Name": "out",
        "StorageDescriptor": {
            "Columns": cols,
            "Location": "s3://demo-bucket/_warehouse/db.db/out-__PLACEHOLDER__",
        },
        "Parameters": {},  # no provider — but location=None must NOT repair it
    }
    fake = _FakeGlue(table)
    _install_glue(runner, fake)

    schema = _Schema([("a", "string"), ("b", "bigint")])

    def go():
        runner._sync_glue_table_schema("db.out", schema)  # location defaults to None

    _with_cloud_warehouse(go)
    assert fake.updated is None, "location=None + matching columns must short-circuit"


def test_location_none_syncs_columns_but_leaves_location_untouched():
    """location=None with a column mismatch still rewrites columns (existing
    behaviour) but must NOT touch Location or add the delta provider."""
    runner = _load_runner()

    existing_location = "s3://demo-bucket/_warehouse/db.db/out"
    table = {
        "Name": "out",
        "StorageDescriptor": {
            "Columns": [{"Name": "col", "Type": "array<string>"}],  # generic stub
            "Location": existing_location,
        },
        "Parameters": {"table_type": "DELTA"},  # stale table_type must be dropped
    }
    fake = _FakeGlue(table)
    _install_glue(runner, fake)

    schema = _Schema([("a", "string"), ("b", "bigint")])

    def go():
        runner._sync_glue_table_schema("db.out", schema)

    _with_cloud_warehouse(go)

    assert fake.updated is not None, "column mismatch must trigger an update"
    sd = fake.updated["StorageDescriptor"]
    assert sd["Location"] == existing_location, "location=None must not change Location"
    params = fake.updated["Parameters"]
    assert "spark.sql.sources.provider" not in params, "location=None must not add provider"
    assert "table_type" not in params, "stale table_type must still be dropped"
    assert [(c["Name"], c["Type"]) for c in sd["Columns"]] == [("a", "string"), ("b", "bigint")]


def test_noop_when_warehouse_is_not_s3():
    """Local Hadoop-catalog runs (CLAVESA_WAREHOUSE not s3://) never touch
    Glue, even with a location passed — local behaviour stays identical."""
    runner = _load_runner()
    fake = _FakeGlue({"Name": "node_runs", "StorageDescriptor": {}, "Parameters": {}})
    _install_glue(runner, fake)

    saved = os.environ.get("CLAVESA_WAREHOUSE")
    os.environ.pop("CLAVESA_WAREHOUSE", None)  # local: unset / non-s3
    try:
        runner._sync_glue_table_schema(
            "sys.node_runs",
            _Schema([("run_id", "string")]),
            location="s3://demo-bucket/_system/pipelines/node_runs/",
        )
    finally:
        if saved is not None:
            os.environ["CLAVESA_WAREHOUSE"] = saved
    assert fake.updated is None, "non-s3 warehouse must be a no-op"


# ---------------------------------------------------------------------------
# Test runner — same pattern as test_node_runs_row.py.
# ---------------------------------------------------------------------------


def _all_tests():
    g = globals()
    return [(name, fn) for name, fn in sorted(g.items()) if name.startswith("test_") and callable(fn)]


def main():
    passed = 0
    failed: list[tuple[str, str]] = []
    for name, fn in _all_tests():
        try:
            fn()
        except Exception as e:
            failed.append((name, f"{type(e).__name__}: {e}"))
            print(f"FAIL  {name}  →  {type(e).__name__}: {e}")
        else:
            passed += 1
            print(f"PASS  {name}")
    total = passed + len(failed)
    print(f"\n{passed}/{total} passed", "FAIL" if failed else "OK")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
