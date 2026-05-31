#!/bin/sh
# Eight modes:
#   CLAVESA_METASTORE_SERVER=1 — long-lived Derby Network Server. Owns
#                                 $CLAVESA_WAREHOUSE/_metastore/metastore_db and serves it over JDBC
#                                 on port 1527 so the warm query worker and on-demand pipeline-run
#                                 containers share one metastore instead of fighting over the embedded
#                                 Derby single-writer lock (the local analog of cloud's shared Glue).
#   CLAVESA_PREVIEW=1        — UI preview path; reads inputs from env, writes JSON to stdout.
#   CLAVESA_RUN=1            — local pipeline-run path; reads event JSON from stdin, calls handler, writes JSON to stdout.
#   CLAVESA_QUERY=1          — Spark-SQL query path; reads SQL from stdin (or env), writes {columns,rows} JSON to stdout.
#                                 Used by observability.LocalProvider for compute=local pipelines (ADR-014).
#   CLAVESA_QUERY_SERVER=1   — long-lived warm Spark Connect server + HTTP query proxy. Hosts the
#                                 SparkConnectPlugin on CLAVESA_CONNECT_PORT (default 15002) so notebook
#                                 REPL subprocesses (Slice 1) can connect with per-session SparkSession
#                                 isolation, AND serves the legacy HTTP /healthz + /query on
#                                 CLAVESA_QUERY_SERVER_PORT (default 8765) via a Connect client. Wired by
#                                 `clavesa ui` for the Catalog/dashboards/TableDetail surfaces.
#   CLAVESA_CONNECT_SERVER=1 — Spark Connect server only (no HTTP proxy). Reserved for the standalone
#                                 supervisor topology; not wired by Go in Slice 0 but available for
#                                 testing via `docker run -e CLAVESA_CONNECT_SERVER=1 ...`.
#   CLAVESA_RECORD_RUN=1     — local pipeline-rollup path; reads one runs-table row from stdin, appends to <pipeline>.runs.
#                                 Driven by service.RunPipeline at end-of-run; the local twin of the cloud runs_writer Lambda.
#   default                     — Lambda runtime; hand off to the standard bootstrap.
if [ "$CLAVESA_METASTORE_SERVER" = "1" ] || [ "$CLAVESA_PREVIEW" = "1" ] || [ "$CLAVESA_RUN" = "1" ] || [ "$CLAVESA_QUERY" = "1" ] || [ "$CLAVESA_QUERY_SERVER" = "1" ] || [ "$CLAVESA_CONNECT_SERVER" = "1" ] || [ "$CLAVESA_RECORD_RUN" = "1" ]; then
    exec python /var/task/runner.py
else
    exec /lambda-entrypoint.sh "$@"
fi
