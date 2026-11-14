# Stage 1: the awslabs Spark Structured Streaming Kinesis connector
# (ADR-028 stream mode). It is not on Maven Central, so it is built here from a
# pinned commit: the v2.0.0 tag, the first release built for Spark 4 (4.0.1).
# Later commits on main move to Spark 4.1 APIs, which the 4.0.2 runner cannot
# load. -Dspark.version pins the compile to the runner's Spark. The output is
# the connector's shaded jar (its own deps relocated).
#
# runner/patches/ holds local fixes; see each file's header.
FROM maven:3.9-eclipse-temurin-17 AS kinesis-connector
ARG KINESIS_CONNECTOR_SHA=dd327d1c6e5896b1fb27bc41fbe4bb0bc5dc02a0
WORKDIR /src
RUN git init -q . \
 && git remote add origin https://github.com/awslabs/spark-sql-kinesis-connector.git \
 && git fetch -q --depth 1 origin ${KINESIS_CONNECTOR_SHA} \
 && git checkout -q FETCH_HEAD
# Local fixes on top of the pinned commit (one file per fix, each explains
# itself). git apply fails the build if a patch no longer applies.
COPY patches/ /patches/
RUN git apply --verbose /patches/kinesis-connector-*.patch
RUN mvn -q -B -DskipTests -Dgpg.skip -Dmaven.javadoc.skip=true -Dspark.version=4.0.2 package \
 && cp "$(ls target/spark-streaming-sql-kinesis-connector_2.13-*.jar | grep -v -E 'sources|javadoc|original' | head -1)" /kinesis-connector.jar

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

# Kinesis source for stream mode (stage 1 above).
COPY --from=kinesis-connector /kinesis-connector.jar ${SPARK_HOME}/jars/spark-streaming-sql-kinesis-connector_2.13.jar

# PySpark's upstream `spark-class` uses bash process substitution and `ps`,
# both of which fail in Lambda's restricted runtime. Replace it with the
# stripped-down version from aws-samples/spark-on-aws-lambda — it skips
# launcher.Main and execs java directly with a hard-coded classpath.
COPY spark-class ${SPARK_HOME}/bin/spark-class
RUN chmod +x ${SPARK_HOME}/bin/spark-class

COPY runner.py ${LAMBDA_TASK_ROOT}/
COPY run_lock.py ${LAMBDA_TASK_ROOT}/
COPY stream_query.py ${LAMBDA_TASK_ROOT}/
COPY spark_conf.py ${LAMBDA_TASK_ROOT}/
COPY unity_catalog.py ${LAMBDA_TASK_ROOT}/
COPY bundle_task.py ${LAMBDA_TASK_ROOT}/
COPY notebook_supervisor.py ${LAMBDA_TASK_ROOT}/
COPY notebook_repl.py ${LAMBDA_TASK_ROOT}/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# User-supplied extra Python deps for transform UDFs (clavesa runner
# requirements / .clavesa/runner-requirements.txt). Empty by default → no-op.
# Placed after the heavy Java/pip/JAR layers so editing it rebuilds only this
# cheap tail and leaves the base cached; above CLAVESA_MODULE_VERSION so a
# version bump doesn't re-run it.
COPY extra-requirements.txt .
RUN pip install --no-cache-dir -r extra-requirements.txt

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
