#!/usr/bin/env bash
# Minimal JAR set for Spark 3.5 + Iceberg + Glue Data Catalog + S3.
# Adapted from aws-samples/spark-on-aws-lambda but trimmed to what clavesa
# actually needs: source-data S3 reads (plain Parquet) and Iceberg table writes
# via Glue catalog.
set -e

SPARK_HOME=${SPARK_HOME:?SPARK_HOME must be set}
JARS_DIR="${SPARK_HOME}/jars"
mkdir -p "${JARS_DIR}"

# Iceberg + AWS — for writing Iceberg tables to S3 with Glue Data Catalog as
# the metastore (ADR-013). The aws-bundle shades AWS SDK v2 + S3FileIO + the
# GlueCatalog implementation; no separate AWS SDK download needed for Iceberg.
ICEBERG_VERSION=1.6.1
wget -q "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" -P "${JARS_DIR}/"
wget -q "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" -P "${JARS_DIR}/"

# Hadoop-AWS — for plain spark.read.parquet("s3://...") on source data that
# isn't Iceberg-formatted. Iceberg's S3FileIO covers the Iceberg path; this
# covers the source-read path. aws-java-sdk-bundle is AWS SDK v1, paired with
# hadoop-aws.
HADOOP_AWS_VERSION=3.3.4
AWS_SDK_V1_VERSION=1.12.262
wget -q "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" -P "${JARS_DIR}/"
wget -q "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_V1_VERSION}/aws-java-sdk-bundle-${AWS_SDK_V1_VERSION}.jar" -P "${JARS_DIR}/"

# Spark Connect server — long-lived gRPC driver that the warm worker hosts so
# notebook REPL subprocesses (Slice 1) and the catalog query client share one
# JVM with per-session isolation. Stable in Spark 3.5. Only loaded when the
# CLAVESA_CONNECT_SERVER entry mode runs; Lambda transform-runner invocations
# never instantiate the Connect plugin.
#
# We let Ivy (via spark-submit --packages) resolve the full dep tree instead
# of hand-picking JARs — Spark Connect ships with non-trivial shaded gRPC/
# protobuf companions whose exact set is version-sensitive. Trusting Spark's
# own resolver keeps us future-proof across patch releases. Network access
# is required at IMAGE BUILD TIME only (same as the wgets above); runtime
# uses the cached JARs and stays fully offline.
#
# Ordering note: this runs BEFORE Dockerfile's COPY runner/spark-class step,
# so spark-submit still has upstream `spark-class` underneath it, which knows
# how to do --packages resolution. Our stripped replacement (needed for the
# Lambda runtime) gets layered on top after.
SPARK_VERSION=3.5.3
echo "Resolving Spark Connect transitive deps via Ivy..."
mkdir -p /tmp/clavesa-ivy
# Spark-submit with --packages resolves the Ivy dep tree before launching the
# driver. We pass a no-op script so the driver starts, runs in ~2s, and exits;
# every transitive Connect JAR lands in /tmp/clavesa-ivy/jars as a side effect.
# (--version alone won't work — it short-circuits before --packages is parsed.)
echo 'print("ivy resolution complete")' > /tmp/clavesa-noop.py
"${SPARK_HOME}/bin/spark-submit" \
    --packages "org.apache.spark:spark-connect_2.12:${SPARK_VERSION}" \
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
ls -1 "${JARS_DIR}/" | grep -E "iceberg|hadoop-aws|aws-java-sdk-bundle|connect|grpc|protobuf" | sort
