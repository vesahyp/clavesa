#!/bin/sh
# Six modes:
#   CLAVESA_PREVIEW=1        — UI preview path; reads inputs from env, writes JSON to stdout.
#   CLAVESA_RUN=1            — local pipeline-run path; reads event JSON from stdin, calls handler, writes JSON to stdout.
#   CLAVESA_QUERY=1          — Spark-SQL query path; reads SQL from stdin (or env), writes {columns,rows} JSON to stdout.
#                                 Used by observability.LocalProvider for compute=local pipelines (ADR-014).
#   CLAVESA_QUERY_SERVER=1   — long-lived warm Spark over HTTP. Listens on CLAVESA_QUERY_SERVER_PORT
#                                 (default 8765); served paths: GET /healthz, POST /query. Wired by
#                                 `clavesa ui` so the Catalog/dashboards/TableDetail surfaces share
#                                 one warm JVM instead of paying cold-start on every read.
#   CLAVESA_RECORD_RUN=1     — local pipeline-rollup path; reads one runs-table row from stdin, appends to <pipeline>.runs.
#                                 Driven by service.RunPipeline at end-of-run; the local twin of the cloud runs_writer Lambda.
#   default                     — Lambda runtime; hand off to the standard bootstrap.
if [ "$CLAVESA_PREVIEW" = "1" ] || [ "$CLAVESA_RUN" = "1" ] || [ "$CLAVESA_QUERY" = "1" ] || [ "$CLAVESA_QUERY_SERVER" = "1" ] || [ "$CLAVESA_RECORD_RUN" = "1" ]; then
    exec python /var/task/runner.py
else
    exec /lambda-entrypoint.sh "$@"
fi
