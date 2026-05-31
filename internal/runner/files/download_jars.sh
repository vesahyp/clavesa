#!/usr/bin/env bash
# Minimal JAR set for Spark 4.0 + Delta Lake + Glue Data Catalog + S3.
# Adapted from aws-samples/spark-on-aws-lambda but trimmed to what clavesa
# actually needs: source-data S3 reads (plain Parquet) and Delta table writes
# via Glue catalog (ADR-018; v2.0.0 cutover from Iceberg).
set -e

SPARK_HOME=${SPARK_HOME:?SPARK_HOME must be set}
JARS_DIR="${SPARK_HOME}/jars"
mkdir -p "${JARS_DIR}"

# Delta Lake — for writing Delta tables to S3 with Glue as the Hive metastore
# (ADR-018). delta-spark is the user-facing Spark integration; delta-storage
# is the protocol implementation it depends on. We don't pull
# delta-storage-s3-dynamodb because clavesa is single-writer per table by
# design (one Step Functions execution at a time); the SingleDriverLogStore
# is what spark_conf.py configures.
DELTA_VERSION=4.0.0
wget -q "https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/${DELTA_VERSION}/delta-spark_2.13-${DELTA_VERSION}.jar" -P "${JARS_DIR}/"
wget -q "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" -P "${JARS_DIR}/"

# AWS Glue Data Catalog client for Apache Hive Metastore — registers Delta
# tables in Glue when the runner runs in cloud mode (sub-slice 15). Spark 4
# upstream still hardcodes the Hive `IMetaStoreClient` factory, so we need
# the HIVE-12679 patch on top of Hive 2.3.10 to make the factory pluggable.
# Three JARs together:
#   1. aws-glue-datacatalog-spark-client.jar — the factory implementation
#      (`com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory`)
#      wired in spark_conf.py's `is_s3` branch.
#   2. hive-common-2.3.10.jar (patched) — overwrites the bundled hive-common
#      so the factory can be selected via SparkConf.
#   3. hive-exec-2.3.10-core.jar (patched) — same patch, hive-exec side.
#
# Source: github.com/sdaberdaku/aws-glue-data-catalog-spark-client release
# v4.0.2 — built for Spark 4.0.2 + Hadoop 3.4.1 + Hive 2.3.10 (matches our
# runner exactly). Upstream awslabs/aws-glue-data-catalog-client-for-apache-
# hive-metastore stops at Hive 3 / Spark 3; this fork carries the Spark 4
# build. The patched hive jars MUST replace the bundled ones (same filename)
# in $SPARK_HOME/jars, hence `wget -O`.
GLUE_HIVE_CLIENT_VERSION=v4.0.2
GLUE_HIVE_CLIENT_URL="https://github.com/sdaberdaku/aws-glue-data-catalog-spark-client/releases/download/${GLUE_HIVE_CLIENT_VERSION}"
HIVE2_VERSION=2.3.10
wget -q -O "${JARS_DIR}/aws-glue-datacatalog-spark-client.jar" "${GLUE_HIVE_CLIENT_URL}/aws-glue-datacatalog-spark-client.jar"
wget -q -O "${JARS_DIR}/hive-common-${HIVE2_VERSION}.jar" "${GLUE_HIVE_CLIENT_URL}/hive-common-${HIVE2_VERSION}.jar"
wget -q -O "${JARS_DIR}/hive-exec-${HIVE2_VERSION}-core.jar" "${GLUE_HIVE_CLIENT_URL}/hive-exec-${HIVE2_VERSION}-core.jar"

# Hadoop-AWS — for plain spark.read.parquet("s3://...") on source data that
# isn't Delta-formatted, plus Delta's own S3 reads (Delta writes through the
# Hadoop FileSystem API). Must match Spark 4.0.2's bundled hadoop-client
# (3.4.1) — the 3.4.x `core-default.xml` uses duration strings like "60s"
# for `fs.s3a.connection.timeout`, and hadoop-aws < 3.4 parses those via
# `getInt()` which throws `NumberFormatException: For input string: "60s"`.
# Hadoop 3.4 switched the AWS dep from `com.amazonaws:aws-java-sdk-bundle`
# (SDK v1) to `software.amazon.awssdk:bundle` (SDK v2); we pull the v2
# version pinned by hadoop-project 3.4.1. We keep the v1 bundle alongside
# because Delta-Spark's S3 log-store still imports SDK v1 classes at the
# version Spark 4 ships, so removing it breaks Delta writes.
HADOOP_AWS_VERSION=3.4.1
AWS_SDK_V2_VERSION=2.24.6
AWS_SDK_V1_VERSION=1.12.262
wget -q "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" -P "${JARS_DIR}/"
wget -q "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_V2_VERSION}/bundle-${AWS_SDK_V2_VERSION}.jar" -P "${JARS_DIR}/"
wget -q "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_V1_VERSION}/aws-java-sdk-bundle-${AWS_SDK_V1_VERSION}.jar" -P "${JARS_DIR}/"

# Derby Network Server + client driver — for the shared local metastore
# topology. pyspark/jars already ships the embedded Derby engine split
# (derby + derbyshared + derbytools, all 10.16.1.1), which the legacy
# embedded path (jdbc:derby:;databaseName=...) uses. The networked path
# needs two MORE jars NOT bundled by pyspark:
#   1. derbynet-10.16.1.1.jar — org.apache.derby.drda.NetworkServerControl,
#      the long-lived Derby Network Server the CLAVESA_METASTORE_SERVER mode
#      launches so one Derby DB serves many Spark clients over JDBC.
#   2. derbyclient-10.16.1.1.jar — org.apache.derby.jdbc.ClientDriver, the
#      network client driver Spark loads when CLAVESA_METASTORE_ADDR is set
#      (spark_conf.py's network branch) to connect as a client.
# Version MUST match the bundled engine jars (10.16.1.1) so client + server
# + engine speak the same wire/format.
DERBY_VERSION=10.16.1.1
wget -q "https://repo1.maven.org/maven2/org/apache/derby/derbynet/${DERBY_VERSION}/derbynet-${DERBY_VERSION}.jar" -P "${JARS_DIR}/"
wget -q "https://repo1.maven.org/maven2/org/apache/derby/derbyclient/${DERBY_VERSION}/derbyclient-${DERBY_VERSION}.jar" -P "${JARS_DIR}/"

# Spark Connect server — long-lived gRPC driver that the warm worker hosts so
# notebook REPL subprocesses (Slice 1) and the catalog query client share one
# JVM with per-session isolation. Only loaded when the CLAVESA_CONNECT_SERVER
# entry mode runs; Lambda transform-runner invocations never instantiate the
# Connect plugin.
#
# We let Ivy (via spark-submit --packages) resolve the full dep tree instead
# of hand-picking JARs. Spark Connect ships with non-trivial shaded gRPC/
# protobuf companions whose exact set is version-sensitive. Trusting Spark's
# own resolver keeps us future-proof across patch releases. Network access
# is required at IMAGE BUILD TIME only (same as the wgets above); runtime
# uses the cached JARs and stays fully offline.
#
# Ordering note: this runs BEFORE Dockerfile's COPY runner/spark-class step,
# so spark-submit still has upstream `spark-class` underneath it, which knows
# how to do --packages resolution. Our stripped replacement (needed for the
# Lambda runtime) gets layered on top after.
SPARK_VERSION=4.0.2
echo "Resolving Spark Connect transitive deps via Ivy..."
mkdir -p /tmp/clavesa-ivy
# Spark-submit with --packages resolves the Ivy dep tree before launching the
# driver. We pass a no-op script so the driver starts, runs in ~2s, and exits;
# every transitive Connect JAR lands in /tmp/clavesa-ivy/jars as a side effect.
# (--version alone won't work — it short-circuits before --packages is parsed.)
echo 'print("ivy resolution complete")' > /tmp/clavesa-noop.py
"${SPARK_HOME}/bin/spark-submit" \
    --packages "org.apache.spark:spark-connect_2.13:${SPARK_VERSION}" \
    --conf "spark.jars.ivy=/tmp/clavesa-ivy" \
    --master "local[1]" \
    /tmp/clavesa-noop.py
rm /tmp/clavesa-noop.py

# Copy resolved JARs into the Spark classpath, skipping anything pyspark
# already bundles (avoids duplicate-class headaches).
copied=0
for jar in /tmp/clavesa-ivy/jars/*.jar; do
    [ -f "$jar" ] || continue
    name="$(basename "$jar")"
    if [ ! -f "${JARS_DIR}/${name}" ]; then
        cp "$jar" "${JARS_DIR}/"
        copied=$((copied + 1))
    fi
done
echo "Added ${copied} Spark Connect JAR(s) from Ivy resolution."
rm -rf /tmp/clavesa-ivy

echo "Installed JARs:"
ls -1 "${JARS_DIR}/" | grep -E "delta|hadoop-aws|aws-java-sdk-bundle|connect|grpc|protobuf|glue-datacatalog|hive-common|hive-exec|derby" | sort
