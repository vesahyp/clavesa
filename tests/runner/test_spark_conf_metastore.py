"""Unit tests for the local Derby metastore branch in runner/spark_conf.py.

spark_conf.py is import-side-effect-free and does NOT import pyspark, so we
can import it directly (no boto3/pyspark stubbing needed).

Covers the embedded vs network metastore selection driven by
CLAVESA_METASTORE_ADDR (shared local Derby Network Server, the local analog
of cloud's shared Glue).

Run with: python3 tests/runner/test_spark_conf_metastore.py
"""

from __future__ import annotations

import importlib.util
import os
import sys
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SPARK_CONF = REPO / "runner" / "spark_conf.py"


def _load_spark_conf():
    spec = importlib.util.spec_from_file_location("spark_conf", str(SPARK_CONF))
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


class MetastoreBranchTests(unittest.TestCase):
    def setUp(self):
        self.spark_conf = _load_spark_conf()
        # Isolate from any inherited / host env.
        for k in (
            "CLAVESA_METASTORE_ADDR",
            "CLAVESA_WAREHOUSE",
            "CLAVESA_CATALOG",
            "CLAVESA_SYSTEM_CATALOG",
            "CLAVESA_S3_ENDPOINT",
        ):
            os.environ.pop(k, None)

    def tearDown(self):
        os.environ.pop("CLAVESA_METASTORE_ADDR", None)

    def test_embedded_default_when_addr_unset(self):
        conf = self.spark_conf.clavesa_spark_conf(warehouse="/tmp/wh")
        self.assertEqual(conf["spark.sql.catalogImplementation"], "hive")
        self.assertEqual(
            conf["spark.hadoop.javax.jdo.option.ConnectionURL"],
            "jdbc:derby:;databaseName=/tmp/wh/_metastore/metastore_db;create=true",
        )
        # Embedded pins derby.system.home; no network client driver.
        self.assertEqual(
            conf["spark.driver.extraJavaOptions"],
            "-Dderby.system.home=/tmp/wh/_metastore",
        )
        self.assertNotIn(
            "spark.hadoop.javax.jdo.option.ConnectionDriverName", conf
        )

    def test_network_when_addr_set(self):
        os.environ["CLAVESA_METASTORE_ADDR"] = "clavesa-metastore-xyz:1527"
        conf = self.spark_conf.clavesa_spark_conf(warehouse="/tmp/wh")
        self.assertEqual(conf["spark.sql.catalogImplementation"], "hive")
        self.assertEqual(
            conf["spark.hadoop.javax.jdo.option.ConnectionURL"],
            "jdbc:derby://clavesa-metastore-xyz:1527/metastore_db;create=true",
        )
        self.assertEqual(
            conf["spark.hadoop.javax.jdo.option.ConnectionDriverName"],
            "org.apache.derby.jdbc.ClientDriver",
        )
        # Client must NOT set derby.system.home (server-side only).
        self.assertNotIn("spark.driver.extraJavaOptions", conf)

    def test_s3_branch_ignores_metastore_addr(self):
        # Cloud (Glue) branch is untouched: even with CLAVESA_METASTORE_ADDR
        # set, an s3:// warehouse federates to Glue, not networked Derby.
        os.environ["CLAVESA_METASTORE_ADDR"] = "clavesa-metastore-xyz:1527"
        conf = self.spark_conf.clavesa_spark_conf(warehouse="s3://bucket/wh")
        self.assertEqual(conf["spark.sql.catalogImplementation"], "hive")
        self.assertNotIn(
            "spark.hadoop.javax.jdo.option.ConnectionURL", conf
        )
        self.assertIn("spark.hive.imetastoreclient.factory.class", conf)


if __name__ == "__main__":
    unittest.main()
