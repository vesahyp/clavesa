"""Shared Spark/Iceberg config used by every entry mode of the runner.

Originally inlined in ``runner._spark()``. Extracted so the Spark Connect
server launch and any in-container Connect client can pin the same Iceberg
catalog + S3A wiring as the legacy py4j path — one source of truth for
``spark.sql.catalog.clavesa.*``, ``spark.sql.extensions``, and the S3A
filesystem mappings.

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
        warehouse: Iceberg warehouse path. ``s3://...`` routes through
            GlueCatalog; anything else uses the file-based HadoopCatalog
            (local dev / preview). Defaults to ``$CLAVESA_WAREHOUSE`` or
            ``/tmp/clavesa-warehouse``.
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
        # Iceberg session extensions — enables CALL syntax, MERGE INTO, etc.
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
        "spark.sql.catalog.clavesa": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.clavesa.warehouse": warehouse,
    }

    if is_s3:
        # Cloud: Glue Data Catalog as the metastore so Athena queries
        # tables natively (ADR-013). S3FileIO is Iceberg-AWS's S3 client.
        conf["spark.sql.catalog.clavesa.catalog-impl"] = (
            "org.apache.iceberg.aws.glue.GlueCatalog"
        )
        conf["spark.sql.catalog.clavesa.io-impl"] = (
            "org.apache.iceberg.aws.s3.S3FileIO"
        )
    else:
        # Local dev / preview: Hadoop catalog — file-based, no metastore.
        conf["spark.sql.catalog.clavesa.type"] = "hadoop"

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
