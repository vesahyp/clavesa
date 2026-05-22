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

echo "Installed JARs:"
ls -1 "${JARS_DIR}/" | grep -E "iceberg|hadoop-aws|aws-java-sdk-bundle"
