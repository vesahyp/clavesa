FROM public.ecr.aws/lambda/python:3.12

# JRE for PySpark. Spark 4.0.x requires Java 17.
#
# tzdata is needed by Pandas' to_json date serialization. Without it any
# query returning a TimestampType column fails with "No time zone found
# with key :/etc/localtime" the moment the result hits CLAVESA_QUERY's
# pdf.to_json(date_format="iso") shim. Lambda's base image strips it for
# size; cost of adding back is ~3 MB.
RUN dnf install -y java-17-amazon-corretto-headless wget tzdata && dnf clean all
ENV JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto
ENV PATH=$JAVA_HOME/bin:$PATH
ENV TZ=UTC

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV SPARK_HOME=/var/lang/lib/python3.12/site-packages/pyspark

# Delta Lake + hadoop-aws JARs for Delta writes via Glue Hive metastore
# (ADR-018) and source-data S3 reads.
COPY download_jars.sh /tmp/
RUN chmod +x /tmp/download_jars.sh && /tmp/download_jars.sh && rm /tmp/download_jars.sh

# PySpark's upstream `spark-class` uses bash process substitution and `ps`,
# both of which fail in Lambda's restricted runtime. Replace it with the
# stripped-down version from aws-samples/spark-on-aws-lambda — it skips
# launcher.Main and execs java directly with a hard-coded classpath.
COPY spark-class ${SPARK_HOME}/bin/spark-class
RUN chmod +x ${SPARK_HOME}/bin/spark-class

COPY runner.py ${LAMBDA_TASK_ROOT}/
COPY spark_conf.py ${LAMBDA_TASK_ROOT}/
COPY notebook_supervisor.py ${LAMBDA_TASK_ROOT}/
COPY notebook_repl.py ${LAMBDA_TASK_ROOT}/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Spark Connect server port — bound only when the warm-worker container runs
# in CLAVESA_CONNECT_SERVER mode (see entrypoint.sh). Lambda invocations
# ignore EXPOSE; this is documentation + a hint to `docker run -P`.
EXPOSE 15002

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
