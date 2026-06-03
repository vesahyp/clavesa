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

# Local directory Spark writes its event log to (one file per Spark
# application). Owned here because clavesa_spark_conf() is the single seam
# every session-build path goes through; the runner imports this constant
# for its post-run event-log tail-parse.
EVENTLOG_DIR = "/tmp/clavesa-eventlog"


def _runtime_vcpus() -> float:
    """Best-effort vCPU count for the current runtime.

    Lambda allocates CPU proportional to memory — one full vCPU per 1769 MB
    (so 3008 MB ≈ 1.7 vCPU, 10240 MB ≈ 5.8 vCPU) — and exposes the memory in
    AWS_LAMBDA_FUNCTION_MEMORY_SIZE. That env is the reliable signal on Lambda
    (os.cpu_count() reports the underlying host, not the allocated slice). Off
    Lambda (local dev, a future Fargate task) fall back to the CPU count.
    """
    mem = os.environ.get("AWS_LAMBDA_FUNCTION_MEMORY_SIZE")
    if mem:
        try:
            return max(1.0, int(mem) / 1769.0)
        except ValueError:
            pass
    return float(os.cpu_count() or 1)


def shuffle_partitions() -> int:
    """Right-size spark.sql.shuffle.partitions to the runtime.

    Spark's default of 200 is sized for a cluster. On the small-data tiers
    clavesa targets (Lambda especially) it just spawns ~200 near-empty tasks
    per shuffle, so a 14-node pipeline spends its budget on task scheduling
    rather than work — the dominant cost behind the gold pipeline's ~35s/node
    and the 900s Lambda timeout when stats are on. Scale to ~4 partitions per
    vCPU, clamped so a 1-vCPU Lambda keeps a little parallelism and a large
    future Fargate task doesn't overshoot:

        memory_mb   ~vCPU   partitions
          1769        1          4
          3008        1.7        8
          3540        2          8
          5308        3         12
          7076        4         16
          8846        5         20
         10240        5.8       24
    """
    return max(4, min(64, round(_runtime_vcpus()) * 4))


def clavesa_spark_conf(
    warehouse: str | None = None,
    *,
    s3_endpoint: str | None = None,
    catalog: str | None = None,
    system_catalog: str | None = None,
) -> dict[str, str]:
    """Return the full Spark config dict for a clavesa runner session.

    Args:
        warehouse: Spark warehouse path where managed Delta tables land.
            ``s3://...`` routes through the S3 log store (single-writer);
            anything else uses the local filesystem (local dev / preview).
            Defaults to ``$CLAVESA_WAREHOUSE`` or ``/tmp/clavesa-warehouse``.
        s3_endpoint: Optional S3A endpoint override (moto / MinIO test
            infra). Defaults to ``$CLAVESA_S3_ENDPOINT``.
        catalog: Workspace catalog identifier (ADR-019). When set, the
            runner registers ``spark.sql.catalog.<catalog>`` as a V2
            DeltaCatalog with its own warehouse pinned at
            ``<warehouse>/<catalog>``, so three-level addresses
            (``<catalog>.<schema>.<table>``) resolve natively. Defaults
            to ``$CLAVESA_CATALOG``; empty / unset skips the V2 wiring
            and the runner stays on the legacy ``spark_catalog`` flat
            two-segment shape (used by preview / connect-server hosts
            where no pipeline context is in play).
        system_catalog: Workspace system catalog identifier. Same V2
            registration treatment as ``catalog`` so observability
            tables (``runs`` / ``node_runs`` / ``tables`` /
            ``column_stats`` under the ``pipelines`` schema) land at
            ``<warehouse>/<system_catalog>/`` and are addressable as
            ``<system_catalog>.pipelines.<table>``. Defaults to
            ``$CLAVESA_SYSTEM_CATALOG``.
    """
    if warehouse is None:
        warehouse = os.environ.get("CLAVESA_WAREHOUSE", "/tmp/clavesa-warehouse")
    if s3_endpoint is None:
        s3_endpoint = os.environ.get("CLAVESA_S3_ENDPOINT")
    if catalog is None:
        catalog = os.environ.get("CLAVESA_CATALOG", "")
    if system_catalog is None:
        system_catalog = os.environ.get("CLAVESA_SYSTEM_CATALOG", "")

    is_s3 = warehouse.startswith("s3://")

    conf: dict[str, str] = {
        "spark.driver.bindAddress": "127.0.0.1",
        "spark.driver.host": "127.0.0.1",
        "spark.ui.enabled": "false",
        "spark.sql.session.timeZone": "UTC",
        "spark.ui.showConsoleProgress": "false",
        # (Spark event log is enabled conditionally near the end of this
        # function — see the EVENTLOG_DIR block — so a missing/unwritable
        # /tmp can never crash SparkContext init.)
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
        # Coalesce small output files at write time (and cluster-on-write for
        # liquid-clustered tables) so file counts stay bounded without anyone
        # running OPTIMIZE. autoCompact is intentionally left off (write
        # amplification on heavily-MERGEd targets); explicit compaction is a
        # separate command.
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        # Where managed tables land. Local mode: filesystem path. Cloud:
        # the pipeline's S3 bucket prefix, set by orchestration via
        # CLAVESA_WAREHOUSE.
        "spark.sql.warehouse.dir": warehouse,
        # Right-size shuffle parallelism to the runtime (see shuffle_partitions
        # docstring). The 200 default is a cluster value; on Lambda it's pure
        # task-scheduling overhead. AQE coalescing stays on so a stage whose
        # output is smaller than this still collapses to fewer tasks.
        "spark.sql.shuffle.partitions": str(shuffle_partitions()),
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
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

        metastore_addr = os.environ.get("CLAVESA_METASTORE_ADDR")
        if metastore_addr:
            # NETWORK metastore (shared local Derby Network Server). When
            # CLAVESA_METASTORE_ADDR is set, connect to the per-workspace
            # Derby Network Server as a CLIENT over JDBC instead of opening
            # the embedded single-writer DB directly. This lets the warm
            # query worker and on-demand pipeline-run containers run
            # side-by-side without colliding on the embedded Derby lock —
            # the local analog of cloud's shared Glue. The server owns
            # derby.system.home and the DB files; clients just speak JDBC,
            # so we do NOT set derby.system.home here. ``;create=true`` is
            # harmless on a client connect — the server auto-creates the
            # DB (and the Hive schema on first connect) the same as the
            # embedded path.
            conf["spark.hadoop.javax.jdo.option.ConnectionURL"] = (
                f"jdbc:derby://{metastore_addr}/metastore_db;create=true"
            )
            conf["spark.hadoop.javax.jdo.option.ConnectionDriverName"] = (
                "org.apache.derby.jdbc.ClientDriver"
            )
        else:
            # EMBEDDED metastore (today's default — back-compat for CI /
            # pure one-shot invocations with no metastore server running).
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

    # ADR-019 Slice 4 attempted to register V2 multi-catalogs under
    # ``spark.sql.catalog.<catalog>=DeltaCatalog`` so transforms could
    # address tables as three-level ``<catalog>.<schema>.<table>``
    # natively. Delta 4.0's ``DeltaCatalog`` extends
    # ``DelegatingCatalogExtension`` and only gets its session-catalog
    # delegate populated when registered under the ``spark_catalog``
    # name; any non-session V2 registration trips a
    # ``NullPointerException`` from ``DelegatingCatalogExtension.name()``
    # the moment Spark's analyzer runs its ``isSessionCatalog`` check on
    # any SQL touching the catalog. Until Delta ships a delegate-free V2
    # implementation, local-mode writes ride through ``spark_catalog``
    # (DeltaCatalog wrap above) at the two-segment
    # ``<catalog>__<schema>.<table>`` shape. The on-disk warehouse
    # layout still moves to ``<warehouse>/<catalog>/<schema>/<table>/``
    # via the ``LOCATION`` clause ``_ensure_database`` stamps at
    # ``CREATE DATABASE`` time — same target tree Slice 5's cloud Glue
    # V2 cutover will mirror.
    _ = catalog
    _ = system_catalog

    # Spark event log: SparkListenerTaskEnd / StageCompleted events land as
    # newline-delimited JSON that the runner tails after each transform to
    # aggregate per-invocation task metrics (peak execution memory, spill,
    # shuffle, GC/CPU time) onto node_runs. Uncompressed + no block-update
    # events so the tail is plain, byte-seekable JSON.
    #
    # Spark's EventLoggingListener fails SparkContext init outright if
    # `spark.eventLog.dir` does not exist (FileNotFoundException). EVERY
    # session built from this conf — the Lambda/local handler, preview, AND
    # the warm query / Spark Connect servers — would crash on a missing dir,
    # so create it here and only enable eventLog if that succeeds. A
    # read-only /tmp degrades to "no metrics" instead of "no Spark".
    try:
        os.makedirs(EVENTLOG_DIR, exist_ok=True)
        eventlog_ok = os.path.isdir(EVENTLOG_DIR)
    except OSError:
        eventlog_ok = False
    if eventlog_ok:
        conf["spark.eventLog.enabled"] = "true"
        conf["spark.eventLog.dir"] = "file://" + EVENTLOG_DIR
        conf["spark.eventLog.logBlockUpdates.enabled"] = "false"
        conf["spark.eventLog.compress"] = "false"

    return conf


def spark_master() -> str:
    """Spark master URL — overridable via env for tests."""
    return os.environ.get("CLAVESA_SPARK_MASTER", "local[*]")
