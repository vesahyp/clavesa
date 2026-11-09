"""Unit tests for runner/unity_catalog.py (ADR-027).

Stdlib only: no Spark, no network. The REST client is replaced by a fake
that records calls and answers from a dict, so the decisions (take over,
refuse a managed table, which lineage edges, which jobs) are tested
without Databricks. The real calls were proven against the test workspace.

Run with: python3 tests/runner/test_unity_catalog.py
"""

from __future__ import annotations

import importlib.util
import io
import os
import sys
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest import mock

REPO = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("unity_catalog", str(REPO / "runner" / "unity_catalog.py"))
uc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(uc)


class FakeClient:
    def __init__(self, answers=None, fail=None):
        self.calls = []
        self.answers = answers or {}
        self.fail = fail or {}

    def call(self, method, path, body=None, ok404=False):
        self.calls.append((method, path, body))
        key = (method, path.split("?")[0])
        if key in self.fail:
            raise uc.UCError(self.fail[key])
        if key in self.answers:
            return self.answers[key]
        if ok404:
            return None
        return {}


class FakeSpark:
    """Just enough of SparkSession for prepare()."""

    def __init__(self, exists=False, location=""):
        self.exists = exists
        self.location = location
        self.sql_calls = []

        class _Cat:
            def tableExists(_, _tid):  # noqa: N805
                return self.exists

        self.catalog = _Cat()

    def sql(self, stmt):
        self.sql_calls.append(stmt)
        outer = self

        class _R:
            def collect(_):  # noqa: N805
                return [{"location": outer.location}]

        return _R()


CFG = {"host": "https://dbc-1.cloud.databricks.com", "client_id": "app", "catalog": "cat",
       "schema": "orders", "publish": True, "lineage": True, "jobs": []}
TABLES = "/api/2.1/unity-catalog/tables/"


class ConfigTest(unittest.TestCase):
    def test_not_connected(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(uc.config())

    def test_local_warehouse_is_refused(self):
        env = {"CLAVESA_DATABRICKS_HOST": "https://h", "CLAVESA_WAREHOUSE": "/ws/.clavesa/warehouse"}
        with mock.patch.dict(os.environ, env, clear=True), redirect_stderr(io.StringIO()) as err:
            self.assertIsNone(uc.config())
        self.assertIn("warehouse is local", err.getvalue())

    def test_cloud(self):
        env = {"CLAVESA_DATABRICKS_HOST": "https://h/", "CLAVESA_WAREHOUSE": "s3://b/p/_warehouse/",
               "CLAVESA_UC_CATALOG": "cat", "CLAVESA_UC_SCHEMA": "orders", "CLAVESA_UC_LINEAGE": "false",
               "CLAVESA_DATABRICKS_JOBS": "[12, 34]"}
        with mock.patch.dict(os.environ, env, clear=True):
            cfg = uc.config()
        self.assertEqual(cfg["host"], "https://h")
        self.assertTrue(cfg["publish"])
        self.assertFalse(cfg["lineage"])
        self.assertEqual(cfg["jobs"], [12, 34])


class NamesTest(unittest.TestCase):
    def test_full_name(self):
        self.assertEqual(uc.full_name(CFG, "clavesa_ws__orders.daily"), "cat.orders.daily")

    def test_s3_uri(self):
        self.assertEqual(uc.s3_uri("s3a://b/k"), "s3://b/k")
        self.assertEqual(uc.s3_uri("s3://b/k"), "s3://b/k")

    def test_explorer_url(self):
        self.assertEqual(uc.explorer_url(CFG, "cat.orders.daily"),
                         "https://dbc-1.cloud.databricks.com/explore/data/cat/orders/daily")


class PrepareTest(unittest.TestCase):
    def test_new_table(self):
        ctx = uc.prepare(FakeClient(), CFG, FakeSpark(), "db.daily")
        self.assertEqual(ctx, {"full_name": "cat.orders.daily", "state": "new"})

    def test_managed_table_is_refused(self):
        client = FakeClient({("GET", TABLES + "cat.orders.daily"): {"table_type": "MANAGED", "data_source_format": "DELTA"}})
        with self.assertRaises(uc.UCError) as cm:
            uc.prepare(client, CFG, FakeSpark(), "db.daily")
        self.assertIn("only write external Delta tables", str(cm.exception))

    def test_external_table_is_taken_over(self):
        client = FakeClient({("GET", TABLES + "cat.orders.daily"): {
            "table_type": "EXTERNAL", "data_source_format": "DELTA",
            "storage_location": "s3://team-bucket/sales/daily", "columns": [{"name": "a", "type_text": "int"}]}})
        spark = FakeSpark(exists=False)
        with redirect_stderr(io.StringIO()):
            ctx = uc.prepare(client, CFG, spark, "db.daily")
        self.assertEqual(ctx["state"], "external")
        # The owner marker is looked for in the Delta log first (Unity
        # Catalog had none), then the Glue table is pointed at the folder.
        self.assertEqual(spark.sql_calls, [
            "DESCRIBE DETAIL delta.`s3a://team-bucket/sales/daily`",
            "CREATE TABLE db.daily USING DELTA LOCATION 's3://team-bucket/sales/daily'",
        ])

    def test_external_table_elsewhere_in_glue_is_refused(self):
        client = FakeClient({("GET", TABLES + "cat.orders.daily"): {
            "table_type": "EXTERNAL", "data_source_format": "DELTA", "storage_location": "s3://team-bucket/sales/daily"}})
        spark = FakeSpark(exists=True, location="s3a://clavesa-bucket/orders/_warehouse/x")
        with self.assertRaises(uc.UCError) as cm:
            uc.prepare(client, CFG, spark, "db.daily")
        self.assertIn("pipeline reset", str(cm.exception))

    def test_table_of_another_pipeline_is_refused(self):
        client = FakeClient({("GET", TABLES + "cat.orders.daily"): {
            "table_type": "EXTERNAL", "data_source_format": "DELTA", "storage_location": "s3://b/x",
            "properties": {"clavesa.pipeline": "other"}}})
        with mock.patch.dict(os.environ, {"CLAVESA_PIPELINE": "orders"}), self.assertRaises(uc.UCError) as cm:
            uc.prepare(client, CFG, FakeSpark(), "db.daily")
        self.assertIn("two pipelines may not write one table", str(cm.exception))



class OutputNameTest(unittest.TestCase):
    def test_unity_catalog_table_forms(self):
        self.assertEqual(uc.full_name(CFG, "db.daily", "a.b.c"), "a.b.c")
        self.assertEqual(uc.full_name(CFG, "db.daily", "sales.daily_orders"), "cat.sales.daily_orders")
        self.assertEqual(uc.full_name(CFG, "db.daily", "daily_orders_clavesa"), "cat.orders.daily_orders_clavesa")
        self.assertEqual(uc.full_name(CFG, "db.daily", ""), "cat.orders.daily")


class SwitchSafetyTest(unittest.TestCase):
    """ADR-027: the paused-step check, the replace guard, uc_location, readers."""

    JOB = "/api/2.1/jobs/get"
    RUNS = "/api/2.1/jobs/runs/list"

    def test_paused_job_passes(self):
        client = FakeClient({
            ("GET", self.JOB): {"settings": {"name": "orders", "schedule": {"quartz_cron_expression": "0 0 3 * * ?", "pause_status": "PAUSED"}}},
            ("GET", self.RUNS): {"runs": []}})
        with redirect_stderr(io.StringIO()):
            ctx = uc.prepare(client, CFG, FakeSpark(), "db.daily", {"replaces_job": 7, "uc_table": "a.b.c"})
        self.assertEqual(ctx["full_name"], "a.b.c")

    def test_unpaused_schedule_is_refused_before_any_table_call(self):
        client = FakeClient({("GET", self.JOB): {"settings": {"name": "orders", "schedule": {"quartz_cron_expression": "x"}}}})
        with self.assertRaises(uc.UCError) as cm:
            uc.prepare(client, CFG, FakeSpark(), "db.daily", {"replaces_job": 7})
        self.assertIn("unpaused schedule", str(cm.exception))
        self.assertFalse(any(TABLES in c[1] for c in client.calls))

    def test_active_run_is_refused(self):
        client = FakeClient({
            ("GET", self.JOB): {"settings": {"name": "orders"}},
            ("GET", self.RUNS): {"runs": [{"run_id": 99}]}})
        with self.assertRaises(uc.UCError) as cm:
            uc.check_step_paused(client, 7, "a.b.c")
        self.assertIn("active runs [99]", str(cm.exception))

    def test_missing_job_is_refused(self):
        with self.assertRaises(uc.UCError) as cm:
            uc.check_step_paused(FakeClient(), 7, "a.b.c")
        self.assertIn("does not exist", str(cm.exception))

    def test_replace_on_a_partitioned_foreign_table_is_refused(self):
        client = FakeClient({("GET", TABLES + "cat.orders.daily"): {
            "table_type": "EXTERNAL", "data_source_format": "DELTA", "storage_location": "s3://team/daily",
            "columns": [{"name": "day", "type_text": "string", "partition_index": 0}]}})
        with self.assertRaises(uc.UCError) as cm:
            uc.prepare(client, CFG, FakeSpark(), "db.daily", {"mode": "replace"})
        self.assertIn("replace_partitions", str(cm.exception))
        with redirect_stderr(io.StringIO()):
            ctx = uc.prepare(client, CFG, FakeSpark(), "db.daily", {"mode": "replace_partitions"})
        self.assertEqual(ctx["state"], "external")

    def test_new_table_at_uc_location(self):
        ctx = uc.prepare(FakeClient(), CFG, FakeSpark(), "db.daily", {"uc_location": "s3a://team/clavesa/daily/"})
        self.assertEqual(ctx["create_location"], "s3://team/clavesa/daily")

    def test_new_table_already_elsewhere_is_refused(self):
        spark = FakeSpark(exists=True, location="s3a://clavesa/orders/_warehouse/daily")
        with self.assertRaises(uc.UCError):
            uc.prepare(FakeClient(), CFG, spark, "db.daily", {"uc_location": "s3://team/clavesa/daily"})

    def test_readers_get_select(self):
        client = FakeClient()
        uc.grant_readers(client, {**CFG, "readers": ["analysts"]}, "a.b.c")
        self.assertEqual(client.calls, [("PATCH", "/api/2.1/unity-catalog/permissions/table/a.b.c",
                                         {"changes": [{"principal": "analysts", "add": ["SELECT"]}]})])
        client = FakeClient()
        uc.grant_readers(client, CFG, "a.b.c")
        self.assertEqual(client.calls, [])

class DeltaLogOwnerTest(unittest.TestCase):
    def test_marker_only_in_the_delta_log_still_guards(self):
        """After a Databricks write, Unity Catalog has no clavesa.pipeline;
        the Delta log's marker still refuses a second pipeline."""
        client = FakeClient({("GET", TABLES + "cat.orders.daily"): {
            "table_type": "EXTERNAL", "data_source_format": "DELTA", "storage_location": "s3://b/x"}})

        class _Spark(FakeSpark):
            def sql(self, stmt):
                self.sql_calls.append(stmt)

                class _R:
                    def collect(_):  # noqa: N805
                        return [{"properties": {"clavesa.pipeline": "other"}, "location": ""}]

                return _R()

        with mock.patch.dict(os.environ, {"CLAVESA_PIPELINE": "orders"}), self.assertRaises(uc.UCError) as cm:
            uc.prepare(client, CFG, _Spark(), "db.daily", {"mode": "replace_partitions"})
        self.assertIn("'other'", str(cm.exception))


class MarkDeltaTest(unittest.TestCase):
    """The ownership marker lives in the Delta log, so it survives Unity
    Catalog re-reading properties after a Databricks write."""

    class _Spark:
        def __init__(self, props):
            self.props, self.stmts = props, []

        def sql(self, stmt):
            self.stmts.append(stmt)
            props = self.props

            class _R:
                def collect(_):  # noqa: N805
                    return [{"properties": props}]

            return _R()

    def test_sets_missing_marker(self):
        spark = self._Spark({"delta.minWriterVersion": "7"})
        with mock.patch.dict(os.environ, {"CLAVESA_PIPELINE": "uc_events", "CLAVESA_NODE": "daily_events"}):
            uc.mark_delta(spark, "db.t")
        self.assertEqual(spark.stmts[-1], "ALTER TABLE db.t SET TBLPROPERTIES ('clavesa.pipeline' = 'uc_events', 'clavesa.node' = 'daily_events')")

    def test_no_commit_when_present(self):
        spark = self._Spark({"clavesa.pipeline": "uc_events", "clavesa.node": "daily_events"})
        with mock.patch.dict(os.environ, {"CLAVESA_PIPELINE": "uc_events", "CLAVESA_NODE": "daily_events"}):
            uc.mark_delta(spark, "db.t")
        self.assertEqual(len(spark.stmts), 1)  # only the DESCRIBE DETAIL


class ReadTest(unittest.TestCase):
    def test_vend_read(self):
        client = FakeClient({("POST", "/api/2.1/unity-catalog/temporary-table-credentials"): {
            "aws_temp_credentials": {"access_key_id": "AK", "secret_access_key": "SK", "session_token": "T"}}})
        aws = uc.vend_read(client, {"table_id": "tid", "full_name": "a.b.c"})
        self.assertEqual(aws["access_key_id"], "AK")
        self.assertEqual(client.calls[0][2], {"table_id": "tid", "operation": "READ"})

    def test_missing_table(self):
        with self.assertRaises(uc.UCError):
            uc.get_table(FakeClient(), "a.b.c")

    def test_bucket_of(self):
        self.assertEqual(uc.bucket_of("s3a://team-bucket/x/y"), "team-bucket")

    def test_uc_table_input_links_as_a_table(self):
        endpoint, label = uc._input_endpoint(FakeClient(), CFG, "orders", {"kind": "uc_table", "table": "a.b.c"})
        self.assertEqual(endpoint, {"table": {"name": "a.b.c"}})


class LineageTest(unittest.TestCase):
    def test_edges_go_through_the_step(self):
        client = FakeClient({("GET", TABLES + "cat.orders.raw"): {"name": "raw"}})
        inputs = {
            "src": {"kind": "http", "url": "https://example.com/o.parquet"},
            "up": "clavesa_ws__orders.raw",
        }
        env = {"CLAVESA_PIPELINE": "orders", "CLAVESA_NODE": "daily"}
        with mock.patch.dict(os.environ, env), redirect_stderr(io.StringIO()):
            uc.push_lineage(client, CFG, inputs, "cat.orders.daily", ["a", "b"])
        edges = [c[2] for c in client.calls if c[1] == "/api/2.0/lineage-tracking/external-lineage"]
        step = {"external_metadata": {"name": "clavesa_orders_daily"}}
        self.assertIn(step, [e["source"] for e in edges])
        targets = [(e["source"], e["target"]) for e in edges]
        self.assertIn((step, {"table": {"name": "cat.orders.daily"}}), targets)
        self.assertIn(({"table": {"name": "cat.orders.raw"}}, step), targets)
        self.assertIn(({"external_metadata": {"name": "clavesa_orders_in_src"}}, step), targets)
        # Every edge has an external metadata object on one side (P0).
        for src, tgt in targets:
            self.assertTrue("external_metadata" in src or "external_metadata" in tgt)

    def test_existing_metadata_is_updated(self):
        client = FakeClient(fail={("POST", "/api/2.0/lineage-tracking/external-metadata"): "HTTP 409: ALREADY_EXISTS"})
        with mock.patch.dict(os.environ, {"CLAVESA_PIPELINE": "p", "CLAVESA_NODE": "n"}), redirect_stderr(io.StringIO()):
            uc.push_lineage(client, CFG, {}, "cat.orders.daily", ["a"])
        self.assertTrue(any(c[0] == "PATCH" for c in client.calls))


class JobsTest(unittest.TestCase):
    def test_start_jobs(self):
        client = FakeClient({
            ("POST", "/api/2.1/jobs/run-now"): {"run_id": 77},
            ("GET", "/api/2.1/jobs/runs/get"): {"run_page_url": "https://h/jobs/1/runs/77"},
        })
        with redirect_stderr(io.StringIO()):
            got = uc.start_jobs(client, {**CFG, "jobs": [1]})
        self.assertEqual(got, [{"job_id": 1, "run_id": 77, "run_page_url": "https://h/jobs/1/runs/77"}])

    def test_a_job_that_fails_to_start_is_recorded(self):
        client = FakeClient(fail={("POST", "/api/2.1/jobs/run-now"): "HTTP 403: no permission"})
        with redirect_stderr(io.StringIO()):
            got = uc.start_jobs(client, {**CFG, "jobs": [1]})
        self.assertIn("403", got[0]["error"])


if __name__ == "__main__":
    unittest.main()
