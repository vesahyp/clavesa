FROM public.ecr.aws/lambda/python:3.12

# JRE for PySpark. Java 11 (not 17) — Spark 3.5.x's StorageUtils accesses
# sun.nio.ch.DirectBuffer, which Java 17's module system blocks without
# explicit --add-opens flags. Matches the version SoAL uses.
#
# tzdata is needed by Pandas' to_json date serialization — without it any
# query returning a TimestampType column fails with "No time zone found
# with key :/etc/localtime" the moment the result hits CLAVESA_QUERY's
# pdf.to_json(date_format="iso") shim. Lambda's base image strips it for
# size; cost of adding back is ~3 MB.
RUN dnf install -y java-11-amazon-corretto-headless wget tzdata && dnf clean all
ENV JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto
ENV PATH=$JAVA_HOME/bin:$PATH
ENV TZ=UTC

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV SPARK_HOME=/var/lang/lib/python3.12/site-packages/pyspark

# Iceberg + AWS + hadoop-aws JARs for Iceberg writes via Glue Data Catalog
# and source-data S3 reads.
COPY download_jars.sh /tmp/
RUN chmod +x /tmp/download_jars.sh && /tmp/download_jars.sh && rm /tmp/download_jars.sh

# PySpark's upstream `spark-class` uses bash process substitution and `ps`,
# both of which fail in Lambda's restricted runtime. Replace it with the
# stripped-down version from aws-samples/spark-on-aws-lambda — it skips
# launcher.Main and execs java directly with a hard-coded classpath.
COPY spark-class ${SPARK_HOME}/bin/spark-class
RUN chmod +x ${SPARK_HOME}/bin/spark-class

COPY runner.py ${LAMBDA_TASK_ROOT}/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Stamp the clavesa module version into the image so the runner can
# self-report it on every node_runs / runs row without the orchestrator
# having to thread it through env vars on each invocation. Build callers
# (`make build-runner`, `workspace.Init`'s docker build) pass the current
# `?ref=vX.Y.Z` they pin to. Defaults to "unknown" so an unbuilt-from-source
# image (rare) still produces a parseable string instead of empty rows.
ARG CLAVESA_MODULE_VERSION=unknown
ENV CLAVESA_MODULE_VERSION=$CLAVESA_MODULE_VERSION

ENTRYPOINT ["/entrypoint.sh"]
CMD ["runner.handler"]
