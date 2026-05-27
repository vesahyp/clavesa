"""Shared Spark/Delta config used by every entry mode of the runner.

Originally inlined in ``runner._spark()``. Extracted so the Spark Connect
server launch and any in-container Connect client can pin the same Delta
extensions + S3A wiring as the legacy py4j path — one source of truth for
``spark.sql.extensions``, ``spark.sql.catalog.spark_catalog`` (DeltaCatalog),
the warehouse, and the S3A filesystem mappings.

ADR-018: Delta replaced Iceberg at v2.0.0. The catalog story moved from
``spark.sql.catalog.clavesa = SparkCatalog`` (Iceberg) to wrapping the
session catalog with ``DeltaCatalog``; tables resolve through Spark's
default ``spark_catalog`` rather than a named one. The Glue Hive metastore
federation (Spark talks to Glue as a Hive metastore client via the
``AWSGlueDataCatalogHiveClientFactory`` from the sdaberdaku Glue client
JAR set, sub-slice 15) is the cloud catalog path; local mode persists
through a Derby JDBC metastore under the workspace warehouse (sub-slice
10).

Change Data Feed is enabled session-wide (sub-slice 4) via the
``delta.properties.defaults.enableChangeDataFeed = true`` knob so every
Delta table the runner creates carries CDF on by default. This is what
makes ``_resolve_input``'s ``readChangeFeed`` path returning typed change
rows (``_change_type`` / ``_commit_version`` / ``_commit_timestamp``)
work without any per-table TBLPROPERTIES the user has to remember.

This module is import-side-effect-free and does NOT import ``pyspark``. It
returns plain ``dict[str, str]`` of Spark config keys, leaving the consumer
to apply them (``SparkSession.builder.config(k, v)`` for the py4j path,
``--conf k=v`` for ``start-connect-server.sh``).
"""

from __future__ import annotations

import os


def clavesa_spark_conf(
    warehouse: str | None = None,
    *,
    s3_endpoint: str | None = None,
) -> dict[str, str]:
    """Return the full Spark config dict for a clavesa runner session.

    Args:
        warehouse: Spark warehouse path where managed Delta tables land.
            ``s3://...`` routes through the S3 log store (single-writer);
            anything else uses the local filesystem (local dev / preview).
            Defaults to ``$CLAVESA_WAREHOUSE`` or ``/tmp/clavesa-warehouse``.
        s3_endpoint: Optional S3A endpoint override (moto / MinIO test
            infra). Defaults to ``$CLAVESA_S3_ENDPOINT``.
    """
    if warehouse is None:
        warehouse = os.environ.get("CLAVESA_WAREHOUSE", "/tmp/clavesa-warehouse")
    if s3_endpoint is None:
        s3_endpoint = os.environ.get("CLAVESA_S3_ENDPOINT")

    is_s3 = warehouse.startswith("s3://")

    conf: dict[str, str] = {
        "spark.driver.bindAddress": "127.0.0.1",
        "spark.driver.host": "127.0.0.1",
        "spark.ui.enabled": "false",
        "spark.sql.session.timeZone": "UTC",
        "spark.ui.showConsoleProgress": "false",
        # Delta SQL extensions — enables MERGE INTO, time-travel syntax,
        # OPTIMIZE/VACUUM, and Change Data Feed reads.
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        # Wrap Spark's session catalog with DeltaCatalog so saveAsTable /
        # CREATE TABLE / etc. recognize Delta tables and dispatch
        # accordingly. The underlying metastore is whatever Spark is
        # configured to use (Hive locally; Glue Hive metastore federation
        # in cloud — wired in sub-slice 2).
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        # CDF on by default for every Delta table the runner creates. The
        # runner's incremental-read path (sub-slice 4) reads via
        # ``readChangeFeed``, which only returns rows for tables where CDF
        # was enabled at write time. Setting the session-wide default at
        # runner boot means we don't have to remember to stamp
        # ``TBLPROPERTIES (delta.enableChangeDataFeed = true)`` at every
        # write site — including the four system tables and any user
        # transform output.
        "spark.databricks.delta.properties.defaults.enableChangeDataFeed": "true",
        # Where managed tables land. Local mode: filesystem path. Cloud:
        # the pipeline's S3 bucket prefix, set by orchestration via
        # CLAVESA_WAREHOUSE.
        "spark.sql.warehouse.dir": warehouse,
    }

    if not is_s3:
        # Local-mode catalog persistence. Each `pipeline run` spawns one
        # docker container per transform; without a persistent metastore
        # the Hive catalog is in-memory Derby and forgets every table on
        # container exit. Downstream transforms then can't resolve
        # `spark.table("<db>.<table>")` against upstream outputs from the
        # previous container, and `saveAsTable` on an already-on-disk
        # Delta table fails with DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION.
        #
        # Plant the Derby DB next to the warehouse so it shares the
        # workspace-level mount and persists across containers. Files:
        #   <warehouse>/_metastore/metastore_db/   (catalog state)
        #   <warehouse>/_metastore/derby.log       (Derby's per-process log)
        #
        # Derby is single-writer; sequential transforms in one
        # `pipeline run` are fine. Two `pipeline run`s against the same
        # workspace at the same time would conflict — Derby surfaces this
        # as a clear lockfile error, and the second invocation can be
        # rerun.
        #
        # Cloud (is_s3) skips this branch: the runner federates against
        # the Glue Data Catalog via Hive metastore protocol (see the
        # is_s3 block below), so there's no Derby in the picture.
        metastore_dir = warehouse.rstrip("/") + "/_metastore"
        conf["spark.sql.catalogImplementation"] = "hive"
        conf["spark.hadoop.javax.jdo.option.ConnectionURL"] = (
            f"jdbc:derby:;databaseName={metastore_dir}/metastore_db;create=true"
        )
        # Derby writes derby.log to the JVM's cwd by default. Pin it
        # under the warehouse so it doesn't sprinkle the user's home dir
        # / cwd with derby.log files.
        conf["spark.driver.extraJavaOptions"] = f"-Dderby.system.home={metastore_dir}"

    if is_s3:
        # Single-writer log store. clavesa is single-writer per table by
        # design (one Step Functions execution per pipeline at a time),
        # so we skip the DynamoDB-backed lock store entirely. This is the
        # same posture as Iceberg's per-table single-writer assumption
        # we ran with on v1.x; the Delta log store knob is just where
        # it gets configured.
        conf["spark.delta.logStore.s3.impl"] = (
            "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"
        )

        # Hive metastore federation to AWS Glue Data Catalog. Without
        # this, Spark's session catalog falls through to InMemoryCatalog
        # in cloud mode: `saveAsTable("<db>.<table>")` writes Parquet +
        # `_delta_log/` to S3 correctly but the table only lives in the
        # Lambda container's memory and vanishes on exit — UI Catalog,
        # Athena, and cross-pipeline reads see nothing.
        #
        # The Iceberg-era runner solved this with `spark.sql.catalog.
        # clavesa = SparkCatalog` + iceberg-glue (deleted in sub-slice 8).
        # Delta has no named-catalog Glue plugin upstream; the canonical
        # path is to wire AWS Glue as Hive's IMetaStoreClient via
        # `AWSGlueDataCatalogHiveClientFactory` and run Spark with
        # `catalogImplementation=hive`. The supporting JARs (Glue Hive
        # client + Spark-4-patched hive-common / hive-exec) land in
        # $SPARK_HOME/jars via download_jars.sh.
        #
        # Spark 4 reads the factory from `spark.hive.imetastoreclient.
        # factory.class`; Hive's own code path also reads `spark.hadoop.
        # hive.metastore.client.factory.class`. Setting both is the
        # safe posture across the call sites that touch each.
        conf["spark.sql.catalogImplementation"] = "hive"
        conf["spark.hive.imetastoreclient.factory.class"] = (
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
        )
        conf["spark.hadoop.hive.metastore.client.factory.class"] = (
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
        )

    # S3A wiring applies unconditionally — ADR-017 slice 3 made local
    # pipelines able to read s3:// sources, so the scheme mapping must
    # be live even when the warehouse itself is local.
    conf["spark.hadoop.fs.s3a.aws.credentials.provider"] = (
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    )
    conf["spark.hadoop.fs.s3.impl"] = "org.apache.hadoop.fs.s3a.S3AFileSystem"
    conf["spark.hadoop.fs.s3n.impl"] = "org.apache.hadoop.fs.s3a.S3AFileSystem"

    if s3_endpoint:
        conf["spark.hadoop.fs.s3a.endpoint"] = s3_endpoint
        conf["spark.hadoop.fs.s3a.path.style.access"] = "true"
        conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] = (
            "true" if s3_endpoint.startswith("https://") else "false"
        )

    return conf


def spark_master() -> str:
    """Spark master URL — overridable via env for tests."""
    return os.environ.get("CLAVESA_SPARK_MASTER", "local[*]")
