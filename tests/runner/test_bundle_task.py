"""Unit tests for runner/bundle_task.py (ADR-031): the pure parts that need
no Spark. Run with plain python3 (make test-runner-py)."""

import importlib.util
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location(
    "bundle_task", os.path.join(HERE, "..", "..", "runner", "bundle_task.py")
)
bt = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bt)


class SQLTextTest(unittest.TestCase):
    def test_split_statements(self):
        got = bt.split_statements("SELECT ';' AS x; -- a;b\nSELECT 2;\n\n;  /* only ; a comment */")
        self.assertEqual(got, ["SELECT ';' AS x", "-- a;b\nSELECT 2"])

    def test_markers_skip_strings_and_casts(self):
        self.assertEqual(bt.used_markers("SELECT ':nope', x::int FROM t WHERE d = :run_date AND c = :c"),
                         ["run_date", "c"])

    def test_mustache(self):
        self.assertEqual(bt.substitute_mustache("USE CATALOG IDENTIFIER({{catalog}}); {{ other }} {{q}}",
                                                {"catalog": "main", "q": "it's"}),
                         "USE CATALOG IDENTIFIER('main'); {{ other }} 'it\\'s'")


class NamesTest(unittest.TestCase):
    def names(self, **kw):
        return bt.Names(mode="glue", known={"main.sales.orders_raw", "main.sales.orders_clean"},
                        writes={"main.sales.orders_clean"}, **kw)

    def test_three_level_names_go_to_glue(self):
        n = self.names()
        sql = ("MERGE INTO main.sales.orders_clean AS t USING (SELECT o.addr.city FROM main.sales.orders_raw o) s "
               "ON t.id = s.id WHEN MATCHED THEN UPDATE SET *")
        self.assertEqual(
            n.rewrite_sql(sql),
            "MERGE INTO `main__sales`.`orders_clean` AS t USING (SELECT o.addr.city FROM "
            "`main__sales`.`orders_raw` o) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET *",
        )

    def test_use_then_short_names(self):
        n = self.names()
        self.assertTrue(n.use("USE CATALOG main"))
        self.assertTrue(n.use("USE SCHEMA IDENTIFIER(:s)", {"s": "sales"}))
        self.assertEqual(n.rewrite_sql("SELECT orders_raw FROM orders_raw JOIN x"),
                         "SELECT orders_raw FROM `main__sales`.`orders_raw` JOIN x")
        self.assertFalse(n.use("SELECT 1"))
        self.assertTrue(n.use("USE CATALOG IDENTIFIER('main')"))
        self.assertEqual(n.rewrite_sql("SELECT * FROM IDENTIFIER('main.sales.orders_raw')"),
                         "SELECT * FROM `main__sales`.`orders_raw`")
        self.assertEqual(n.rewrite_sql("CREATE TABLE IF NOT EXISTS sales.orders_clean (id BIGINT)"),
                         "CREATE TABLE IF NOT EXISTS `main__sales`.`orders_clean` (id BIGINT)")
        self.assertEqual(n.rewrite_sql("SELECT main.sales.orders_raw(1), count(x) FROM t"),
                         "SELECT main.sales.orders_raw(1), count(x) FROM t")

    def test_catalog_map_and_shadow(self):
        n = bt.Names(mode="glue", catalogs={"main": "prod"}, known={"main.s.t"}, shadow={"main.s.t"})
        self.assertEqual(n.table("main.s.t"), "`prod__s`.`t_clavesa`")
        u = bt.Names(mode="unity", known={"main.s.t"})
        self.assertEqual(u.table("main.s.t"), "`main`.`s`.`t`")

    def test_write_outside_the_list_is_refused(self):
        n = self.names()
        with self.assertRaises(bt.TaskError):
            n.table("main.sales.other", write=True)
        with self.assertRaises(bt.TaskError):
            n.table("orders_clean", write=True)  # no catalog
        self.assertEqual(n.table("temp_view"), "temp_view")

    def test_string_literal_untouched(self):
        n = self.names()
        self.assertEqual(n.rewrite_sql("SELECT 'main.sales.orders_raw' FROM main.sales.orders_raw"),
                         "SELECT 'main.sales.orders_raw' FROM `main__sales`.`orders_raw`")


class RefsTest(unittest.TestCase):
    def test_resolve(self):
        s = bt.RunState({"key": "daily", "name": "Daily"}, {"run_date": "2026-11-13"}, "r1", start=0)
        s.values["ingest"] = {"row_count": 7}
        s.results["ingest"] = {"result_state": "success"}
        self.assertEqual(s.resolve("{{job.parameters.run_date}}/{{tasks.ingest.values.row_count}}"), "2026-11-13/7")
        self.assertEqual(s.resolve("{{job.start_time.iso_date}} {{tasks.ingest.result_state}}"), "1970-01-01 success")
        self.assertEqual(s.resolve("{{input.a}}", item={"a": 1}), "1")
        with self.assertRaises(bt.TaskError):
            s.resolve("{{job.parameters.nope}}")
        with self.assertRaises(bt.TaskError):
            s.resolve("{{workspace.id}}")


class WheelTest(unittest.TestCase):
    def test_argv(self):
        res = lambda v: v.replace("{{x}}", "1")  # noqa: E731
        self.assertEqual(bt.wheel_argv({"named_parameters": {"catalog": "main", "n": "{{x}}"}}, res),
                         ["--catalog=main", "--n=1"])
        self.assertEqual(bt.wheel_argv({"parameters": ["--a", "{{x}}"]}, res), ["--a", "1"])


class DBUtilsTest(unittest.TestCase):
    def test_widgets_and_task_values(self):
        s = bt.RunState({}, {}, "r")
        d = bt.DBUtils(s, {"a": "1"})
        d.widgets.text("b", "two")
        self.assertEqual((d.widgets.get("a"), d.widgets.get("b")), ("1", "two"))
        with self.assertRaises(bt.InputWidgetNotDefined):
            d.widgets.get("c")
        s.current_task = "t1"
        d.jobs.taskValues.set("k", [1, 2])
        self.assertEqual(d.jobs.taskValues.get("t1", "k"), [1, 2])
        with self.assertRaises(bt.NotebookExit):
            d.notebook.exit("done")

    def test_secret_from_env(self):
        os.environ["CLAVESA_DBX_SECRET__sales__api_key"] = "x"
        try:
            self.assertEqual(bt.Secrets().get("sales", "api_key"), "x")
        finally:
            del os.environ["CLAVESA_DBX_SECRET__sales__api_key"]
        os.environ.pop("CLAVESA_DBX_SECRET_PREFIX", None)
        with self.assertRaises(bt.TaskError):
            bt.Secrets().get("sales", "missing")


class UnityTest(unittest.TestCase):
    """catalog: unity with a fake unity_catalog module and a fake Spark."""

    def test_shadow_prepare_publish_lineage(self):
        calls = []

        class FakeDF:
            def createOrReplaceTempView(self, name):  # noqa: N802
                calls.append(("view", name))

        class FakeUC:
            def prepare(self, client, cfg, spark, table_id, spec):
                calls.append(("prepare", table_id, spec["uc_table"], spec.get("replaces_job")))
                return {"full_name": spec["uc_table"], "state": "new"}

            def publish(self, client, cfg, spark, table_id, ctx):
                calls.append(("publish", table_id, ctx["full_name"]))
                return {"full_name": ctx["full_name"], "action": "registered", "url": "u"}

            def push_lineage(self, client, cfg, inputs, out, cols):
                calls.append(("lineage", sorted((k, v["table"]) for k, v in inputs.items()), out, cols))

            def open_table(self, spark, client, name):
                calls.append(("open", name))
                return FakeDF(), {"vended": False, "location": "s3://b/x"}

        class FakeSchema:
            def fieldNames(self):  # noqa: N802
                return ["id"]

        class FakeTable:
            schema = FakeSchema()

        class FakeSpark:
            def sql(self, q):
                calls.append(("sql", q))

            def table(self, name):
                return FakeTable()

        names = bt.Names(mode="unity", known={"main.s.raw", "main.s.clean", "main.ext.ref"},
                         writes={"main.s.clean"}, shadow={"main.s.raw", "main.s.clean"},
                         job_writes={"main.s.raw", "main.s.clean"})
        u = bt.UnityTask(FakeSpark(), names, {"replaces_job": "77"}, "clean", uc_module=FakeUC(), client=object(),
                         cfg={"lineage": True})
        u.before()
        # In-job read: the shadow Glue table; outside read: a view over Unity Catalog.
        self.assertEqual(names.table("main.s.raw"), "`main__s`.`raw_clavesa`")
        self.assertEqual(names.table("main.ext.ref"), "`__uc_main_ext_ref`")
        recs = u.after(["main.s.raw", "main.ext.ref"])
        self.assertIn(("prepare", "main__s.clean_clavesa", "main.s.clean_clavesa", 77), calls)
        self.assertIn(("open", "main.ext.ref"), calls)
        self.assertIn(("publish", "main__s.clean_clavesa", "main.s.clean_clavesa"), calls)
        self.assertIn(("lineage", [("main.ext.ref", "main.ext.ref"), ("main.s.raw", "main.s.raw_clavesa")],
                       "main.s.clean_clavesa", ["id"]), calls)
        self.assertEqual(recs[0]["lineage"], "pushed")

    def test_unity_needs_a_connection(self):
        class NoConfig:
            def config(self):
                return None

        with self.assertRaises(bt.TaskError):
            bt.UnityTask(None, bt.Names(mode="unity"), {}, "t", uc_module=NoConfig())


class ReadFilesTest(unittest.TestCase):
    def test_read_files_becomes_a_view(self):
        calls = []

        class R:
            def format(self, f):
                calls.append(("format", f))
                return self

            def option(self, k, v):
                calls.append(("option", k, v))
                return self

            def load(self, p):
                calls.append(("load", p))
                return self

            def createOrReplaceTempView(self, name):  # noqa: N802
                calls.append(("view", name))

        class S:
            read = R()

        out = bt.expand_read_files(S(), "SELECT a FROM read_files(\n 's3://b/x/', format => \"json\", header => true) WHERE a > 1")
        view = [c for c in calls if c[0] == "view"][0][1]
        self.assertEqual(out, f"SELECT a FROM {view} WHERE a > 1")
        self.assertIn(("format", "json"), calls)
        self.assertIn(("option", "header", "true"), calls)
        self.assertIn(("load", "s3a://b/x/"), calls)
        with self.assertRaises(bt.TaskError):
            bt.expand_read_files(S(), "SELECT * FROM STREAM read_files('x', format => 'json')")
        with self.assertRaises(bt.TaskError):
            bt.expand_read_files(S(), "SELECT * FROM read_files('x')")
        self.assertEqual(bt.expand_read_files(S(), "SELECT 'read_files(x)'"), "SELECT 'read_files(x)'")


class CloudEventTest(unittest.TestCase):
    def test_prepare_cloud_event(self):
        import json as _json
        import tempfile
        import types

        objects = {
            "p/_bundle/bundle-job.json": _json.dumps({
                "job": "daily", "catalog": "glue", "parameters": [{"name": "run_date", "default": "2026-11-13"}],
                "tasks": {"a": {"task_key": "a", "task": {"task_key": "a"}}},
            }).encode(),
            "p/_bundle/files/sql/a.sql": b"SELECT 1",
        }

        class Body:
            def __init__(self, b):
                self.b = b

            def read(self):
                return self.b

        class Pager:
            def paginate(self, Bucket, Prefix):  # noqa: N803
                return [{"Contents": [{"Key": k} for k in objects if k.startswith(Prefix)]}]

        class S3:
            def get_object(self, Bucket, Key):  # noqa: N803
                return {"Body": Body(objects[Key])}

            def get_paginator(self, name):
                return Pager()

            def download_file(self, bucket, key, dst):
                with open(dst, "wb") as f:
                    f.write(objects[key])

        fake = types.ModuleType("boto3")
        fake.client = lambda name: S3()
        saved = sys.modules.get("boto3")
        sys.modules["boto3"] = fake
        try:
            ev = {"transforms": [{"node": "a", "language": "task", "logic_path": "s3://bkt/p/_bundle/bundle-job.json"}],
                  "_sf_execution_arn": "arn:aws:states:x:execution:sm:run-1",
                  "_execution_input": {"params": {"run_date": "2026-11-12"}}}
            out = bt.prepare_cloud_event(ev)
        finally:
            if saved is None:
                sys.modules.pop("boto3", None)
            else:
                sys.modules["boto3"] = saved
        job = out["_bundle_job"]
        self.assertEqual(job["params"], {"run_date": "2026-11-12"})
        self.assertEqual(out["transforms"][0]["task"]["task_key"], "a")
        with open(os.path.join(job["bundle_root"], "sql", "a.sql")) as f:
            self.assertEqual(f.read(), "SELECT 1")
        self.assertIs(bt.prepare_cloud_event({"transforms": [{"language": "sql"}]})["transforms"][0].get("task"), None)

    def test_database_location(self):
        os.environ["CLAVESA_WAREHOUSE"] = "s3://wh-bucket/pipe/_warehouse/"
        try:
            self.assertEqual(bt.database_location("main__sales"), "s3://wh-bucket/_bundle_warehouse/main__sales.db/")
        finally:
            del os.environ["CLAVESA_WAREHOUSE"]
        self.assertEqual(bt.database_location("main__sales"), "")


if __name__ == "__main__":
    result = unittest.main(exit=False, verbosity=1).result
    sys.exit(0 if result.wasSuccessful() else 1)
