#!/usr/bin/env bash
# cloud-smoke.sh — per-release cloud verification gate.
#
#   cloud-smoke.sh run     drive the built bin/clavesa against the persistent
#                          deployed smoke workspace and assert through
#                          Athena / Glue / CloudWatch / the UI HTTP API.
#                          Writes .cloud-smoke-green.json on full green
#                          (consumed by `make release-check`).
#   cloud-smoke.sh setup   one-time creation of that workspace (re-runnable
#                          after a destroy; refuses to clobber an existing dir).
#
# Most assertions go via `aws athena` (written when `clavesa query` was
# local-only). The query CLI is warehouse-aware now (ADR-024) — the
# dedicated `clavesa query` leg below dogfoods it; migrate the remaining
# Athena assertions onto the CLI over time.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# ---------------------------------------------------------------------------
# Config (env-overridable)
# ---------------------------------------------------------------------------
SMOKE_WS="${SMOKE_WS:-$HOME/clavesa-workspaces/smoke}"
SMOKE_PIPELINE="${SMOKE_PIPELINE:-taxi}"
SMOKE_NODES="${SMOKE_NODES:-trips,revenue_by_payment}"
SMOKE_STATS_NODE="${SMOKE_STATS_NODE:-trips}"
SMOKE_PROFILE="${SMOKE_PROFILE:-personal}"
# Day 1 of the seed layout — never consumed by normal runs because setup
# registers the source with --start-from at day 2, so the backfill leg has
# a clean partition to stage (dodges GH #36: staging a window the canonical
# table already covers).
SMOKE_BACKFILL_FROM="${SMOKE_BACKFILL_FROM:-2026/06/01}"
SMOKE_BACKFILL_TO="${SMOKE_BACKFILL_TO:-2026/06/01}"
BIN="${BIN:-$REPO_ROOT/bin/clavesa}"
case "$BIN" in
  /*) : ;;
  *)  BIN="$REPO_ROOT/$BIN" ;;
esac

FAILURES=0
UI_PID=""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
STEP_NO=0
banner() {
  STEP_NO=$((STEP_NO + 1))
  echo ""
  echo "=================================================================="
  echo "  [$STEP_NO] $*"
  echo "=================================================================="
}

pass() { echo "  ✓ $*"; }

# Soft assertion failure: record + continue so one run reports everything.
fail() {
  echo "  ✗ $*" >&2
  FAILURES=$((FAILURES + 1))
}

# Hard failure: later steps would be meaningless — stop now.
die() {
  echo "✗ FATAL: $*" >&2
  exit 1
}

cleanup() {
  if [[ -n "$UI_PID" ]] && kill -0 "$UI_PID" 2>/dev/null; then
    kill "$UI_PID" 2>/dev/null || true
    wait "$UI_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

require_tools() {
  # docker: workspace upgrade/deploy rebuild + push the runner image.
  for t in jq aws terraform python3 curl docker; do
    command -v "$t" >/dev/null || die "required tool not found: $t"
  done
  [[ -x "$BIN" ]] || die "clavesa binary not found at $BIN — run 'make build' first"
}

# Resolve AWS profile + region from workspace state. Fails loud with a
# pointer to setup when the workspace isn't there.
resolve_aws_env() {
  [[ -d "$SMOKE_WS" ]] || die "smoke workspace not found at $SMOKE_WS — run '$0 setup' first"
  [[ -f "$SMOKE_WS/clavesa.json" ]] || die "$SMOKE_WS is not a clavesa workspace (no clavesa.json) — run '$0 setup' first"
  local profile_file="$SMOKE_WS/.clavesa/aws-profile.json"
  [[ -f "$profile_file" ]] || die "no AWS profile pinned at $profile_file — run '$0 setup' (or 'clavesa workspace use --profile <p> --env cloud' in $SMOKE_WS)"
  AWS_PROFILE="$(jq -r .profile "$profile_file")"
  [[ -n "$AWS_PROFILE" && "$AWS_PROFILE" != "null" ]] || die "empty profile in $profile_file"
  export AWS_PROFILE
  AWS_REGION="$(aws configure get region || true)"
  [[ -n "$AWS_REGION" ]] || die "no region configured for profile $AWS_PROFILE (aws configure get region)"
  export AWS_REGION
  aws sts get-caller-identity >/dev/null 2>&1 \
    || die "AWS credentials for profile '$AWS_PROFILE' are not usable (aws sts get-caller-identity failed) — refresh credentials, or run '$0 setup'"
}

# Naming derived from the workspace manifest (ADR-016): catalog
# clavesa_<name>, system DB <catalog>_system__pipelines, pipeline DB
# <catalog>__<schema>, Athena workgroup clavesa-<name>
# (modules/workspace/aws/main.tf).
derive_names() {
  WS_NAME="$(jq -r .name "$SMOKE_WS/clavesa.json")"
  CATALOG="$(jq -r '.catalog // empty' "$SMOKE_WS/clavesa.json")"
  [[ -n "$CATALOG" ]] || CATALOG="clavesa_$(echo "$WS_NAME" | tr '-' '_')"
  SYSTEM_CATALOG="$(jq -r '.system_catalog // empty' "$SMOKE_WS/clavesa.json")"
  [[ -n "$SYSTEM_CATALOG" ]] || SYSTEM_CATALOG="${CATALOG}_system"
  SYSTEM_DB="${SYSTEM_CATALOG}__pipelines"
  PIPELINE_SCHEMA="$(echo "$SMOKE_PIPELINE" | tr '-' '_')"
  PIPELINE_DB="${SMOKE_PIPELINE_DB:-${CATALOG}__${PIPELINE_SCHEMA}}"
  WORKGROUP="${SMOKE_WORKGROUP:-clavesa-$WS_NAME}"
  # Runner Lambda is named clavesa-<pipeline>-runner (tfgen.go:567); Lambda
  # logs to /aws/lambda/<function-name> by default (only the SFN log group
  # is terraform-managed: /clavesa/<pipeline>/sfn, tfgen.go:345).
  SMOKE_LOG_GROUP="${SMOKE_LOG_GROUP:-/aws/lambda/clavesa-${SMOKE_PIPELINE}-runner}"
}

# athena_query <sql> — run via the workspace workgroup, poll to terminal
# state, emit data rows (header stripped) as TSV on stdout. Dies on query
# error with Athena's state-change reason.
athena_query() {
  local sql="$1"
  local qid state reason
  qid="$(aws athena start-query-execution \
          --work-group "$WORKGROUP" \
          --query-string "$sql" \
          --query QueryExecutionId --output text)" \
    || die "athena start-query-execution failed for: $sql"
  while :; do
    state="$(aws athena get-query-execution --query-execution-id "$qid" \
              --query QueryExecution.Status.State --output text)"
    case "$state" in
      SUCCEEDED) break ;;
      FAILED|CANCELLED)
        reason="$(aws athena get-query-execution --query-execution-id "$qid" \
                   --query QueryExecution.Status.StateChangeReason --output text)"
        die "athena query $state: $reason
  sql: $sql"
        ;;
      *) sleep 2 ;;
    esac
  done
  # First row of a SELECT result is the header — drop it.
  aws athena get-query-results --query-execution-id "$qid" --output json \
    | jq -r '.ResultSet.Rows[1:][] | [.Data[].VarCharValue // ""] | @tsv'
}

glue_table_exists() {
  aws glue get-table --database-name "$1" --name "$2" >/dev/null 2>&1
}

# iso8601 → epoch millis (macOS date has no -d; python3 is already required).
to_epoch_ms() {
  python3 -c 'import datetime, sys
print(int(datetime.datetime.fromisoformat(sys.argv[1].replace("Z", "+00:00")).timestamp() * 1000))' "$1"
}

free_port() {
  python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

module_version() {
  grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+' "$REPO_ROOT/internal/version/version.go" | head -1
}

# ---------------------------------------------------------------------------
# run — the per-release gate
# ---------------------------------------------------------------------------
cmd_run() {
  require_tools
  resolve_aws_env
  derive_names
  echo "workspace: $SMOKE_WS (name=$WS_NAME profile=$AWS_PROFILE region=$AWS_REGION)"
  echo "pipeline:  $SMOKE_PIPELINE (db=$PIPELINE_DB system_db=$SYSTEM_DB workgroup=$WORKGROUP)"

  local pdir="$SMOKE_WS/$SMOKE_PIPELINE"
  [[ -d "$pdir" ]] || die "pipeline dir $pdir not found — run '$0 setup' first"

  # ----- 1. upgrade ---------------------------------------------------------
  banner "workspace upgrade (binary $(module_version))"
  # `workspace upgrade` already walks every pipeline (module-source rewrite +
  # orchestration re-sync, internal/cli/workspace.go) — a separate
  # `pipeline upgrade` would be a no-op, so it is not run.
  "$BIN" workspace upgrade --workspace "$SMOKE_WS" \
    || die "workspace upgrade failed"
  pass "workspace + pipelines upgraded"

  # ----- 2. deploy everything -----------------------------------------------
  # `clavesa deploy` = workspace infra + runner image push + every pipeline,
  # re-syncing each pipeline's orchestration.tf from THIS binary's emitter
  # first. That re-sync is the point: workspace upgrade skips it when the
  # version hasn't bumped, which made the gate blind to unreleased emitter
  # changes (the GH #43 ephemeral-storage block shipped "green" without ever
  # being applied).
  banner "clavesa deploy (workspace + runner image + all pipelines)"
  "$BIN" deploy --yes --workspace "$SMOKE_WS" \
    || die "clavesa deploy failed"
  pass "workspace + pipelines deployed"

  # ----- 3. runner Lambda config --------------------------------------------
  # Assert the deployed function config matches what the current emitter
  # writes — proves the orchestration re-sync actually reached AWS instead of
  # terraform applying a stale orchestration.tf.
  banner "runner Lambda config (ephemeral storage)"
  local eph
  eph="$(aws lambda get-function-configuration \
    --function-name "clavesa-$SMOKE_PIPELINE-runner" \
    --query 'EphemeralStorage.Size' --output text)" \
    || die "get-function-configuration failed for clavesa-$SMOKE_PIPELINE-runner"
  [[ "$eph" == "10240" ]] \
    || die "runner Lambda ephemeral storage is ${eph}MB, expected 10240MB (GH #43)"
  pass "runner Lambda ephemeral storage = ${eph}MB"

  # ----- 4. pipeline run ----------------------------------------------------
  banner "pipeline run --wait"
  local run_json status exec_arn started_at started_ms
  # --force: the smoke seed is static, so incremental-skip would otherwise
  # no-op every run after the first and the assertions below would test
  # nothing. Outputs are replace-mode; a full re-read is safe.
  run_json="$("$BIN" pipeline run "$pdir" --wait --json --force --workspace "$SMOKE_WS")" \
    || die "pipeline run failed${run_json:+: $run_json}"
  echo "$run_json"
  status="$(echo "$run_json" | jq -r .status)"
  exec_arn="$(echo "$run_json" | jq -r .execution_arn)"
  started_at="$(echo "$run_json" | jq -r .started_at)"
  if [[ "$status" == "SUCCEEDED" ]]; then
    pass "execution SUCCEEDED ($exec_arn)"
  else
    die "expected execution status SUCCEEDED, observed '$status' ($exec_arn)"
  fi
  started_ms="$(to_epoch_ms "$started_at")"

  # ----- 5. Athena assertions ----------------------------------------------
  banner "Athena: node_runs / column_stats / output tables"
  # node_runs keys cloud runs by sf_execution_arn = the SFN execution ARN
  # (tfgen threads $$.Execution.Id as _sf_execution_arn; runner.py stamps it).
  local node_rows run_id
  node_rows="$(athena_query "SELECT node, status, run_id FROM \"$SYSTEM_DB\".\"node_runs\" WHERE sf_execution_arn = '$exec_arn' ORDER BY started_at DESC")"
  echo "$node_rows" | sed 's/^/    /'
  # run_id is per node invocation — take the stats node's own row, not the
  # newest row (that's the last node in the DAG).
  run_id="$(echo "$node_rows" | awk -F'\t' -v n="$SMOKE_STATS_NODE" '$1 == n { print $3; exit }')"
  local node
  for node in $(echo "$SMOKE_NODES" | tr ',' ' '); do
    if echo "$node_rows" | awk -F'\t' -v n="$node" '$1 == n && $2 == "ok" { found = 1 } END { exit !found }'; then
      pass "node_runs: $node status=ok"
    else
      fail "node_runs: expected a row node=$node status=ok for execution $exec_arn; observed: $(echo "$node_rows" | tr '\n' ';')"
    fi
  done

  # A missing table is an assertion failure (the write path never created
  # it), not an infra abort — keep going so one run reports everything.
  local stats_count
  if glue_table_exists "$SYSTEM_DB" "column_stats"; then
    stats_count="$(athena_query "SELECT count(*) FROM \"$SYSTEM_DB\".\"column_stats\" WHERE pipeline = '$SMOKE_PIPELINE' AND node = '$SMOKE_STATS_NODE' AND run_id = '$run_id'")"
    if [[ "${stats_count:-0}" -gt 0 ]]; then
      pass "column_stats: $stats_count rows for $SMOKE_STATS_NODE (run $run_id)"
    else
      fail "column_stats: expected >0 rows for node=$SMOKE_STATS_NODE run_id=$run_id, observed ${stats_count:-0}"
    fi
  else
    fail "column_stats: table $SYSTEM_DB.column_stats does not exist — the stats opt-in never wrote"
  fi

  for node in $(echo "$SMOKE_NODES" | tr ',' ' '); do
    local tbl count
    # Default-output tables are named bare <node> (the __default suffix
    # was retired; only non-default output keys get a suffix).
    tbl="$node"
    if ! glue_table_exists "$PIPELINE_DB" "$tbl"; then
      fail "$PIPELINE_DB.$tbl: table does not exist in Glue — the run never wrote it"
      continue
    fi
    count="$(athena_query "SELECT count(*) FROM \"$PIPELINE_DB\".\"$tbl\"")"
    if [[ "${count:-0}" -gt 0 ]]; then
      pass "$PIPELINE_DB.$tbl: $count rows"
    else
      fail "$PIPELINE_DB.$tbl: expected count(*) > 0, observed ${count:-0}"
    fi
  done

  # ----- 5b. clavesa query (CLI, cloud warehouse) ----------------------------
  # Dogfoods the query CLI per the header note: the workspace warehouse is
  # cloud, so `clavesa query` must dispatch to Athena with the SparkSQL→
  # Trino transpile applied (ADR-023/ADR-024) — the regression this guards
  # is the old parity bug where the CLI silently queried the (empty) local
  # Hadoop warehouse instead. One cheap count over node_runs is enough.
  banner "clavesa query: node_runs count via the CLI (cloud warehouse)"
  local q_json q_count
  q_json="$("$BIN" query "SELECT count(*) AS c FROM $SYSTEM_DB.node_runs" --json --workspace "$SMOKE_WS")" \
    || die "clavesa query failed"
  echo "$q_json" | jq -c . | sed 's/^/    /'
  q_count="$(echo "$q_json" | jq -r '.rows[0][0] // "0"')"
  if [[ "${q_count:-0}" -gt 0 ]]; then
    pass "clavesa query: node_runs has $q_count rows via Athena"
  else
    fail "clavesa query: expected count(*) > 0 from $SYSTEM_DB.node_runs, observed '$q_count' (CLI not routing to the cloud warehouse?)"
  fi

  # ----- 5c-local. pipeline run --compute local (ADR-024 slice 7) ------------
  # Run the WHOLE pipeline in the workspace-local docker runner against the
  # cloud warehouse, instead of an SFN execution. Proves: the CLI succeeds,
  # the locally-computed run lands a fresh runs row + node_runs rows with
  # compute_target='local' in the cloud system warehouse, and the output
  # tables still hold rows (the local run actually produced cloud data).
  # --force: the seed is static and this run follows the SFN run above, so
  # without it every node incremental-skips and the assertions test nothing.
  banner "pipeline run --compute local: run → assert local compute → outputs"
  local cl_json cl_run_id cl_status cl_node_rows
  cl_json="$("$BIN" pipeline run "$pdir" --compute local --wait --json --force --workspace "$SMOKE_WS")" \
    || die "pipeline run --compute local failed${cl_json:+: $cl_json}"
  echo "$cl_json" | jq .
  cl_status="$(echo "$cl_json" | jq -r '.status // ""')"
  cl_run_id="$(echo "$cl_json" | jq -r '.run_id // ""')"
  if [[ "$cl_status" == "SUCCEEDED" ]]; then
    pass "run --compute local: CLI reports SUCCEEDED ($cl_run_id)"
  else
    fail "run --compute local: expected status SUCCEEDED, observed '$cl_status'"
  fi
  if [[ "$(echo "$cl_json" | jq -r '.compute // ""')" == "local" ]]; then
    pass "run --compute local: run JSON reports compute=local"
  else
    fail "run --compute local: expected compute=local in run JSON"
  fi

  # node_runs: a fresh row per node for this local run, all compute_target=local.
  # The local docker runner stamps compute_target='local' off Lambda-env absence;
  # the runs row's sf_execution_arn = run_id ties them together.
  cl_node_rows="$(athena_query "SELECT node, status, compute_target FROM \"$SYSTEM_DB\".\"node_runs\" WHERE sf_execution_arn = '$cl_run_id' ORDER BY started_at DESC")"
  echo "$cl_node_rows" | sed 's/^/    /'
  local cl_node
  for cl_node in $(echo "$SMOKE_NODES" | tr ',' ' '); do
    if echo "$cl_node_rows" | awk -F'\t' -v n="$cl_node" '$1 == n && $2 == "ok" && $3 == "local" { found = 1 } END { exit !found }'; then
      pass "node_runs: $cl_node status=ok compute_target=local"
    else
      fail "node_runs: expected node=$cl_node status=ok compute_target=local for run $cl_run_id; observed: $(echo "$cl_node_rows" | tr '\n' ';')"
    fi
  done

  # runs row for this local run.
  local cl_runs_count
  if glue_table_exists "$SYSTEM_DB" "runs"; then
    cl_runs_count="$(athena_query "SELECT count(*) FROM \"$SYSTEM_DB\".\"runs\" WHERE sf_execution_arn = '$cl_run_id'")"
    if [[ "${cl_runs_count:-0}" -gt 0 ]]; then
      pass "runs: $cl_runs_count row(s) for local run $cl_run_id"
    else
      fail "runs: expected a row for local run $cl_run_id, observed ${cl_runs_count:-0} (Go-side runs-row write to the cloud system warehouse failed?)"
    fi
  else
    fail "runs: table $SYSTEM_DB.runs does not exist — the runs-row write never ran"
  fi

  # Output tables still hold rows (the local run produced cloud data).
  for cl_node in $(echo "$SMOKE_NODES" | tr ',' ' '); do
    local cl_count
    if ! glue_table_exists "$PIPELINE_DB" "$cl_node"; then
      fail "$PIPELINE_DB.$cl_node: table does not exist after the local run"
      continue
    fi
    cl_count="$(athena_query "SELECT count(*) FROM \"$PIPELINE_DB\".\"$cl_node\"")"
    if [[ "${cl_count:-0}" -gt 0 ]]; then
      pass "$PIPELINE_DB.$cl_node: $cl_count rows after local run"
    else
      fail "$PIPELINE_DB.$cl_node: expected count(*) > 0 after local run, observed ${cl_count:-0}"
    fi
  done

  # ----- 6. Glue integrity --------------------------------------------------
  banner "Glue: table locations sane (no PLACEHOLDER, all s3://)"
  local db locs bad
  for db in "$PIPELINE_DB" "$SYSTEM_DB"; do
    locs="$(aws glue get-tables --database-name "$db" \
             --query 'TableList[].[Name,StorageDescriptor.Location]' --output json)" \
      || die "glue get-tables failed for $db"
    bad="$(echo "$locs" | jq -r '.[] | select((.[1] // "") | (contains("PLACEHOLDER") or (startswith("s3://") | not))) | "\(.[0]) → \(.[1] // "<null>")"')"
    if [[ -z "$bad" ]]; then
      pass "$db: $(echo "$locs" | jq length) tables, all locations s3:// and placeholder-free"
    else
      fail "$db: bad StorageDescriptor.Location — $bad"
    fi
  done

  # ----- 7. CloudWatch ------------------------------------------------------
  banner "CloudWatch: runner log group clean ($SMOKE_LOG_GROUP)"
  if ! aws logs describe-log-groups --log-group-name-prefix "$SMOKE_LOG_GROUP" \
        --output json | jq -e --arg g "$SMOKE_LOG_GROUP" '.logGroups[] | select(.logGroupName == $g)' >/dev/null; then
    fail "log group $SMOKE_LOG_GROUP does not exist — it should after a run (override with SMOKE_LOG_GROUP)"
  else
    local events
    events="$(aws logs filter-log-events \
                --log-group-name "$SMOKE_LOG_GROUP" \
                --start-time "$started_ms" \
                --filter-pattern '"no readable Delta log"' \
                --query 'events[].message' --output json)"
    if [[ "$(echo "$events" | jq length)" -eq 0 ]]; then
      pass "no 'no readable Delta log' events since run start"
    else
      fail "expected zero 'no readable Delta log' events since $started_at, observed: $(echo "$events" | jq -c .)"
    fi
  fi

  # ----- 8. UI API ----------------------------------------------------------
  banner "UI HTTP API (clavesa ui --no-browser)"
  local ui_port base
  ui_port="$(free_port)"
  base="http://localhost:$ui_port"
  CLAVESA_ADDR=":$ui_port" "$BIN" ui --no-browser --workspace "$SMOKE_WS" \
    >/tmp/clavesa-smoke-ui.log 2>&1 &
  UI_PID=$!
  local i ready=0
  for i in $(seq 1 30); do
    if curl -sf -o /dev/null "$base/"; then ready=1; break; fi
    kill -0 "$UI_PID" 2>/dev/null || die "UI server exited early — see /tmp/clavesa-smoke-ui.log"
    sleep 1
  done
  [[ "$ready" == 1 ]] || die "UI server did not answer on $base within 30s — see /tmp/clavesa-smoke-ui.log"

  local runs_json newest_status
  runs_json="$(curl -sf "$base/api/data/runs?pipeline=$SMOKE_PIPELINE&limit=5")" \
    || die "GET /api/data/runs failed"
  if [[ "$(echo "$runs_json" | jq '.rows | length')" -gt 0 ]]; then
    newest_status="$(echo "$runs_json" | jq -r '.rows[0].status' | tr '[:upper:]' '[:lower:]')"
    case "$newest_status" in
      ok|succeeded) pass "/api/data/runs: rows non-empty, newest status=$newest_status" ;;
      *) fail "/api/data/runs: expected newest run status ok/succeeded, observed '$newest_status'" ;;
    esac
  else
    fail "/api/data/runs: expected non-empty rows, observed empty"
  fi

  local node_runs_json
  node_runs_json="$(curl -sf "$base/api/data/node-runs?pipeline=$SMOKE_PIPELINE&limit=20")" \
    || die "GET /api/data/node-runs failed"
  for node in $(echo "$SMOKE_NODES" | tr ',' ' '); do
    if echo "$node_runs_json" | jq -e --arg n "$node" '.rows[] | select(.node == $n)' >/dev/null; then
      pass "/api/data/node-runs: includes node $node"
    else
      fail "/api/data/node-runs: expected node $node among rows, observed nodes: $(echo "$node_runs_json" | jq -c '[.rows[].node] | unique')"
    fi
  done

  # dir accepts an absolute pipeline path (pathutil.ResolveDir passes
  # absolute through; the UI normally sends a workspace-relative dir).
  local status_json
  status_json="$(curl -sf -G "$base/api/pipeline/status" --data-urlencode "dir=$pdir")" \
    || die "GET /api/pipeline/status failed"
  if [[ "$(echo "$status_json" | jq -r .deployed)" == "true" ]]; then
    pass "/api/pipeline/status: deployed=true"
  else
    fail "/api/pipeline/status: expected deployed=true, observed $(echo "$status_json" | jq -c '{deployed, state_machine_arn}')"
  fi
  if [[ -n "$(echo "$status_json" | jq -r '.state_machine_arn // empty')" ]]; then
    pass "/api/pipeline/status: state_machine_arn present"
  else
    fail "/api/pipeline/status: expected non-empty state_machine_arn, observed $(echo "$status_json" | jq -c .)"
  fi

  # ----- 8a. cloud-local live progress (ADR-024) ----------------------------
  # The other cloud-local leg (5c-local) proves a local run lands cloud data;
  # this one proves the UI PROGRESS CHANNEL ACTUALLY MOVES for it. The fixed
  # bug: a `pipeline run --compute local` against a cloud warehouse didn't
  # report live per-node progress to the UI — nodes appeared stuck. We can't
  # observe movement after the fact: the `--compute local` CLI path is
  # synchronous (StartRunCloudLocal blocks until the docker bundle terminates,
  # so the run id it prints is already terminal — --wait is implicit). So we
  # run it in the BACKGROUND and poll the states endpoint concurrently, while
  # the docker bundle is still in flight, to catch a node transitioning
  # RUNNING → terminal over time. The run lands in the same cloud system
  # warehouse as 5c-local; --force keeps it from incremental-skipping.
  #
  # The states endpoint only routes `run=local-*` to the filesystem progress
  # provider on a cloud warehouse (handler.providerForRun); an empty `run`
  # would route to the cloud provider and miss the local run. So we discover
  # the new run id from the filesystem progress tree
  # (<pdir>/.clavesa/runs/<runID>/) — the dir that appears after we launch —
  # then poll the HTTP endpoint with that id, exactly as the UI does.
  if [[ "${SMOKE_SKIP_CLOUDLOCAL:-0}" == "1" ]]; then
    banner "cloud-local live-progress leg SKIPPED"
    cat >&2 <<'EOF'
  ##########################################################################
  ##  WARNING: SMOKE_SKIP_CLOUDLOCAL=1 — the cloud-local live-progress    ##
  ##  leg was NOT verified. This leg guards the regression where a        ##
  ##  `pipeline run --compute local` on a cloud warehouse never reported  ##
  ##  live per-node progress to the UI (nodes stuck). A release shipped   ##
  ##  past this skip has an unverified streaming-progress path. Document  ##
  ##  why in the release commit.                                          ##
  ##########################################################################
EOF
  else
    banner "cloud-local progress: run --compute local (async) → states channel moves"
    local clp_bucket cl_log cl_err cl_pid
    cl_log="/tmp/clavesa-smoke-cloudlocal.json"
    cl_err="/tmp/clavesa-smoke-cloudlocal.err"
    : > "$cl_log"
    : > "$cl_err"

    # The live progress channel for a cloud-local run lives in the cloud
    # warehouse bucket: the runner PUTs _progress/<run>/<node>.json and Go
    # writes _progress/<run>/_run.json. Discovering the run id from S3 — rather
    # than any local file (cloud-local writes none) — is ALSO the bucket
    # alignment check: the UI reader lists this same pipeline_bucket, so a
    # local-* prefix appearing here proves writer and reader agree on the bucket.
    clp_bucket="$(terraform -chdir="$SMOKE_WS" output -raw pipeline_bucket)" \
      || die "could not read pipeline_bucket terraform output from $SMOKE_WS"

    # Snapshot the _progress run prefixes that already exist so we spot the new one.
    local existing_runs
    existing_runs="$(aws s3 ls "s3://$clp_bucket/_progress/" 2>/dev/null \
      | awk '{print $2}' | sed 's#/##' || true)"

    # Launch the (synchronous) CLI run in the background so we can observe its
    # progress while it executes. The CLI prints its `--json` result to stdout
    # ($cl_log); the runner-image docker build + any diagnostics go to stderr
    # ($cl_err) — keep them in SEPARATE files so $cl_log stays parseable JSON
    # (merging them with 2>&1 is what made the earlier `jq .status` see noise).
    "$BIN" pipeline run "$pdir" --compute local --force --json --workspace "$SMOKE_WS" \
      >"$cl_log" 2>"$cl_err" &
    cl_pid=$!

    # Discover the new local-* run id from the S3 _progress tree — the prefix
    # that appears after launch (Go writes the RUNNING _run.json at dispatch, so
    # it shows up early). Bounded wait so a run that never writes a progress
    # marker — or writes to a different bucket — fails loud.
    local clp_run_id="" clp_i
    for clp_i in $(seq 1 60); do
      clp_run_id="$(aws s3 ls "s3://$clp_bucket/_progress/" 2>/dev/null \
        | awk '{print $2}' | sed 's#/##' \
        | grep '^local-' \
        | grep -vxF "$existing_runs" \
        | head -1 || true)"
      [[ -n "$clp_run_id" ]] && break
      kill -0 "$cl_pid" 2>/dev/null \
        || die "cloud-local run exited before writing an S3 _progress prefix — stderr: $(tail -n 20 "$cl_err" | tr '\n' ' ')"
      sleep 1
    done
    if [[ -z "$clp_run_id" ]]; then
      fail "cloud-local progress: no new local-* prefix under s3://$clp_bucket/_progress/ within 60s (channel never opened, or writer/reader bucket mismatch)"
    else
      pass "cloud-local progress: observing run $clp_run_id (S3 _progress prefix present in reader bucket)"

      # Poll the states endpoint while the run is in flight. Record, per node,
      # whether we ever saw RUNNING and whether it reached a terminal state;
      # and whether the overall execution reached terminal. The point is
      # MOVEMENT over time, not just a terminal snapshot.
      local clp_states clp_overall="" clp_seen_running="" clp_seen_terminal=""
      local clp_running_nodes="" clp_terminal_nodes="" clp_last="{}"
      for clp_i in $(seq 1 180); do
        clp_states="$(curl -sf -G "$base/api/pipeline/execution/states" \
          --data-urlencode "dir=$pdir" --data-urlencode "run=$clp_run_id" 2>/dev/null || true)"
        if [[ -n "$clp_states" ]]; then
          clp_last="$clp_states"
          clp_overall="$(echo "$clp_states" | jq -r '.status // ""')"
          # Union in any node currently RUNNING.
          clp_running_nodes="$(printf '%s\n%s\n' "$clp_running_nodes" \
            "$(echo "$clp_states" | jq -r '.states | to_entries[] | select(.value.status == "RUNNING") | .key')" \
            | sort -u | sed '/^$/d')"
          # Union in any node that reached a terminal state.
          clp_terminal_nodes="$(printf '%s\n%s\n' "$clp_terminal_nodes" \
            "$(echo "$clp_states" | jq -r '.states | to_entries[] | select(.value.status == "SUCCEEDED" or .value.status == "FAILED" or .value.status == "SKIPPED") | .key')" \
            | sort -u | sed '/^$/d')"
          [[ -n "$clp_running_nodes" ]] && clp_seen_running=1
          case "$clp_overall" in
            SUCCEEDED|FAILED|TIMED_OUT|ABORTED) clp_seen_terminal=1 ;;
          esac
        fi
        # Stop once the overall execution is terminal AND we've seen a node go
        # RUNNING (movement proven). Otherwise keep polling until the CLI exits.
        if [[ -n "$clp_seen_terminal" && -n "$clp_seen_running" ]]; then break; fi
        kill -0 "$cl_pid" 2>/dev/null || { [[ -n "$clp_seen_terminal" ]] && break; }
        sleep 1
      done

      # Reap the background CLI run; its exit code + JSON are authoritative.
      local cl_rc=0
      wait "$cl_pid" 2>/dev/null || cl_rc=$?
      echo "  CLI run JSON: $(tr '\n' ' ' <"$cl_log")"
      echo "  last states:  $(echo "$clp_last" | jq -c '{status, states: (.states | map_values(.status))}' 2>/dev/null || echo "$clp_last")"

      local cl_json_run_id cl_json_status cl_json_compute
      cl_json_run_id="$(jq -r '.run_id // ""' "$cl_log" 2>/dev/null || true)"
      cl_json_status="$(jq -r '.status // ""' "$cl_log" 2>/dev/null || true)"
      cl_json_compute="$(jq -r '.compute // ""' "$cl_log" 2>/dev/null || true)"

      if [[ "$cl_rc" -eq 0 && "$cl_json_status" == "SUCCEEDED" ]]; then
        pass "cloud-local progress: CLI run SUCCEEDED ($cl_json_run_id, compute=$cl_json_compute)"
      else
        fail "cloud-local progress: CLI run did not succeed (rc=$cl_rc status='${cl_json_status:-<none>}'): json=$(tr '\n' ' ' <"$cl_log") stderr=$(tail -n 20 "$cl_err" | tr '\n' ' ')"
      fi
      if [[ -n "$cl_json_run_id" && "$cl_json_run_id" != "$clp_run_id" ]]; then
        fail "cloud-local progress: CLI run_id '$cl_json_run_id' != observed progress run_id '$clp_run_id'"
      fi

      # The core assertion: at least one node transitioned RUNNING → terminal
      # over the poll window. A node seen RUNNING but never terminal is the
      # exact regression — the channel opened but never advanced.
      if [[ -z "$clp_seen_running" ]]; then
        fail "cloud-local progress: no node was ever observed in RUNNING for $clp_run_id — the live-progress channel never moved (the regression this leg guards)"
      else
        pass "cloud-local progress: node(s) observed RUNNING: $(echo "$clp_running_nodes" | tr '\n' ' ')"
        # Every node we saw RUNNING must also be observed in a terminal state.
        local clp_stuck=""
        local clp_node
        while IFS= read -r clp_node; do
          [[ -z "$clp_node" ]] && continue
          echo "$clp_terminal_nodes" | grep -qxF "$clp_node" \
            || clp_stuck="$clp_stuck $clp_node"
        done <<< "$clp_running_nodes"
        if [[ -n "$clp_stuck" ]]; then
          fail "cloud-local progress: node(s) entered RUNNING but never reached a terminal state:$clp_stuck (stuck — the regression this leg guards)"
        else
          pass "cloud-local progress: all RUNNING node(s) reached a terminal state: $(echo "$clp_terminal_nodes" | tr '\n' ' ')"
        fi
      fi

      # Overall execution reached a terminal state via the channel.
      if [[ -n "$clp_seen_terminal" ]]; then
        pass "cloud-local progress: overall execution reached terminal status '$clp_overall'"
      else
        fail "cloud-local progress: overall execution never reached a terminal status via the states channel (last='${clp_overall:-<none>}') for $clp_run_id"
      fi

      # The terminal local run shows up in the runs rollup, mirroring leg 8's
      # /api/data/runs assertion.
      local clp_runs_json
      clp_runs_json="$(curl -sf "$base/api/data/runs?pipeline=$SMOKE_PIPELINE&limit=20")" \
        || die "GET /api/data/runs failed (cloud-local rollup check)"
      if echo "$clp_runs_json" | jq -e --arg r "$clp_run_id" '.rows[] | select(.run_id == $r or .sf_execution_arn == $r)' >/dev/null; then
        pass "cloud-local progress: run $clp_run_id present in /api/data/runs rollup"
      else
        fail "cloud-local progress: run $clp_run_id absent from /api/data/runs rollup; observed run ids: $(echo "$clp_runs_json" | jq -c '[.rows[] | (.run_id // .sf_execution_arn)]')"
      fi
    fi
    rm -f "$cl_log"
  fi

  cleanup
  UI_PID=""

  # ----- 9. Backfill --------------------------------------------------------
  if [[ "${SMOKE_SKIP_BACKFILL:-0}" == "1" ]]; then
    banner "backfill leg SKIPPED"
    cat >&2 <<'EOF'
  ##########################################################################
  ##  WARNING: SMOKE_SKIP_BACKFILL=1 — the backfill leg was NOT verified. ##
  ##  This leg is the regression test for c8f55f2 (cloud backfill that    ##
  ##  reported success without running compute) and exercises GH #36 /    ##
  ##  GH #37 territory. A release shipped past this skip has an           ##
  ##  unverified backfill path. Document why in the release commit.       ##
  ##########################################################################
EOF
  else
    banner "backfill: stage → diff → discard ($SMOKE_STATS_NODE $SMOKE_BACKFILL_FROM..$SMOKE_BACKFILL_TO)"
    local bf_json bf_run_id bf_table bf_db bf_tbl bf_count
    bf_json="$("$BIN" pipeline backfill stage "$pdir" \
                --node "$SMOKE_STATS_NODE" \
                --from "$SMOKE_BACKFILL_FROM" --to "$SMOKE_BACKFILL_TO" \
                --json --workspace "$SMOKE_WS")" \
      || die "backfill stage failed${bf_json:+: $bf_json}"
    echo "$bf_json" | jq .
    bf_run_id="$(echo "$bf_json" | jq -r .run_id)"
    bf_table="$(echo "$bf_json" | jq -r .target_table)"
    [[ "$bf_table" == *.* ]] || die "backfill target_table '$bf_table' is not <db>.<table> — cannot assert via Athena"
    bf_db="${bf_table%%.*}"
    bf_tbl="${bf_table#*.}"

    # c8f55f2 regression: staging that reports ok without running compute
    # leaves an empty (or missing) staging table — count must be > 0.
    if ! glue_table_exists "$bf_db" "$bf_tbl"; then
      fail "staging table $bf_table does not exist in Glue (c8f55f2 regression: stage reported ok without compute)"
    else
      bf_count="$(athena_query "SELECT count(*) FROM \"$bf_db\".\"$bf_tbl\"")"
      if [[ "${bf_count:-0}" -gt 0 ]]; then
        pass "staging table $bf_table has $bf_count rows"
      else
        fail "staging table $bf_table: expected count(*) > 0, observed ${bf_count:-0} (c8f55f2 regression: stage reported ok without compute)"
      fi
    fi

    "$BIN" pipeline backfill diff "$pdir" "$bf_run_id" --json --workspace "$SMOKE_WS" | jq . \
      || die "backfill diff failed"
    pass "backfill diff ran"

    "$BIN" pipeline backfill discard "$pdir" "$bf_run_id" --workspace "$SMOKE_WS" \
      || die "backfill discard failed"
    if aws glue get-table --database-name "$bf_db" --name "$bf_tbl" >/dev/null 2>&1; then
      fail "staging table $bf_table still present in Glue after discard"
    else
      pass "staging table $bf_table gone from Glue after discard"
    fi

    # ----- 9a-local. backfill stage + discard --compute local (ADR-024 slices 6 & 8) -
    # Second stage pass over the SAME never-consumed window, but with the
    # heavy Spark work routed to a local docker runner against the cloud
    # warehouse. Proves: the staging table lands in cloud Glue/S3 with rows,
    # the run JSON reports compute=local, AND node_runs records a row with
    # compute_target='local' (the runner stamps it off Lambda-env absence).
    # The discard then ALSO runs --compute local (slice 8): the staging-table
    # cleanup (_operation payload → _run_operation, DROP) executes in the local
    # docker runner — proving the operation path, not just stage — and must
    # still remove the table from Glue. Promote is intentionally NOT exercised
    # with --compute local here: it MERGEs into the canonical table and would
    # corrupt the smoke workspace's repeatable seed; discard is the safe
    # operation to dogfood the local-compute path against.
    banner "backfill --compute local: stage → assert local compute → discard --compute local ($SMOKE_STATS_NODE)"
    local bfl_json bfl_run_id bfl_table bfl_db bfl_tbl bfl_count bfl_compute bfl_ct
    bfl_json="$("$BIN" pipeline backfill stage "$pdir" \
                  --node "$SMOKE_STATS_NODE" \
                  --from "$SMOKE_BACKFILL_FROM" --to "$SMOKE_BACKFILL_TO" \
                  --compute local \
                  --json --workspace "$SMOKE_WS")" \
      || die "backfill stage --compute local failed${bfl_json:+: $bfl_json}"
    echo "$bfl_json" | jq .
    bfl_run_id="$(echo "$bfl_json" | jq -r .run_id)"
    bfl_table="$(echo "$bfl_json" | jq -r .target_table)"
    bfl_compute="$(echo "$bfl_json" | jq -r '.compute // ""')"
    [[ "$bfl_table" == *.* ]] || die "backfill target_table '$bfl_table' is not <db>.<table>"
    bfl_db="${bfl_table%%.*}"
    bfl_tbl="${bfl_table#*.}"

    if [[ "$bfl_compute" == "local" ]]; then
      pass "stage --compute local: run JSON reports compute=local"
    else
      fail "stage --compute local: expected compute=local in run JSON, observed '${bfl_compute}'"
    fi

    if ! glue_table_exists "$bfl_db" "$bfl_tbl"; then
      fail "local-compute staging table $bfl_table does not exist in Glue"
    else
      bfl_count="$(athena_query "SELECT count(*) FROM \"$bfl_db\".\"$bfl_tbl\"")"
      if [[ "${bfl_count:-0}" -gt 0 ]]; then
        pass "local-compute staging table $bfl_table has $bfl_count rows"
      else
        fail "local-compute staging table $bfl_table: expected count(*) > 0, observed ${bfl_count:-0}"
      fi
    fi

    # The local docker runner stamps compute_target='local' on the node_runs
    # row (Lambda-env absence). Take the most-recent local row for this node.
    bfl_ct="$(athena_query "SELECT compute_target FROM \"$SYSTEM_DB\".\"node_runs\" WHERE node = '$SMOKE_STATS_NODE' AND compute_target = 'local' ORDER BY started_at DESC LIMIT 1")"
    if [[ "$bfl_ct" == "local" ]]; then
      pass "node_runs: $SMOKE_STATS_NODE staged with compute_target='local'"
    else
      fail "node_runs: expected a compute_target='local' row for $SMOKE_STATS_NODE, observed '${bfl_ct:-<none>}'"
    fi

    "$BIN" pipeline backfill discard "$pdir" "$bfl_run_id" --compute local --workspace "$SMOKE_WS" \
      || die "backfill discard --compute local failed"
    if aws glue get-table --database-name "$bfl_db" --name "$bfl_tbl" >/dev/null 2>&1; then
      fail "local-compute staging table $bfl_table still present in Glue after discard --compute local"
    else
      pass "discard --compute local removed staging table $bfl_table from Glue"
    fi
  fi

  # ----- 9b. run-lock contention (ADR-024 slice 5) ----------------------------
  # The deployed Lambda must refuse to run while another compute holds the
  # warehouse run lease (s3://<bucket>/<pipeline>/_locks/run.json), and run
  # cleanly once it's gone. A fake held lease is PUT directly at the lock
  # key; either rejection surface is a pass:
  #   (a) the Go pre-flight in RunPipelineCloud fails fast — CLI exits
  #       non-zero with the holder run id in the message, or
  #   (b) the dispatch goes through and the Lambda fails the SFN execution
  #       with the holder run id in the cause.
  # Version skew: the Lambda only enforces once its image carries
  # run_lock.py — guaranteed here because steps 1-2 upgrade + deploy with
  # the binary under test before any run.
  banner "run lock: contention (fake held lease → run rejected; removal → run OK)"
  local lock_bucket lock_key lease_now lease_exp lock_out lock_rc
  lock_bucket="$(terraform -chdir="$SMOKE_WS" output -raw pipeline_bucket)" \
    || die "could not read pipeline_bucket terraform output from $SMOKE_WS"
  lock_key="$SMOKE_PIPELINE/_locks/run.json"
  lease_now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  # expires_at ~90s out: long enough that the contention run below observes
  # it held, short enough that expiry + the 30s takeover grace self-heals
  # the workspace in ~2 minutes even if the delete-object below never runs.
  lease_exp="$(python3 -c 'import datetime; print((datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=90)).strftime("%Y-%m-%dT%H:%M:%SZ"))')"
  jq -n --arg now "$lease_now" --arg exp "$lease_exp" '{
      holder: {run_id: "smoke-contention-test", compute: "local", host: "cloud-smoke-script", pid: 1},
      acquired_at: $now,
      expires_at: $exp,
      ttl_s: 120,
      nonce: "smoke-contention-nonce",
      state: "held",
      module_version: "smoke"
    }' > /tmp/clavesa-smoke-lease.json
  aws s3api put-object --bucket "$lock_bucket" --key "$lock_key" \
    --content-type application/json --body /tmp/clavesa-smoke-lease.json >/dev/null \
    || die "could not PUT the fake lease to s3://$lock_bucket/$lock_key"

  set +e
  lock_out="$("$BIN" pipeline run "$pdir" --wait --json --force --workspace "$SMOKE_WS" 2>&1)"
  lock_rc=$?
  set -e
  echo "$lock_out" | sed 's/^/    /'
  if [[ "$lock_rc" -ne 0 && "$lock_out" == *smoke-contention-test* ]]; then
    pass "held lease rejected fast by the Go pre-flight (holder run id in the message)"
  elif [[ "$lock_rc" -ne 0 ]]; then
    fail "run failed while the lock was held but the output lacks the holder run id"
  else
    # CLI exit 0: the pre-flight didn't fire (it's best-effort). The
    # execution itself must then have FAILED with the holder in the cause.
    local lock_status lock_arn lock_cause
    lock_status="$(echo "$lock_out" | jq -r '.status // empty' 2>/dev/null || true)"
    lock_arn="$(echo "$lock_out" | jq -r '.execution_arn // empty' 2>/dev/null || true)"
    if [[ "$lock_status" == "FAILED" && -n "$lock_arn" ]]; then
      lock_cause="$(aws stepfunctions describe-execution --execution-arn "$lock_arn" \
                     --query cause --output text 2>/dev/null || true)"
      if [[ "$lock_cause" == *smoke-contention-test* ]]; then
        pass "held lease rejected by the Lambda (execution FAILED, holder run id in the cause)"
      else
        fail "execution FAILED under a held lease but the cause lacks the holder run id: ${lock_cause:-<empty>}"
      fi
    else
      fail "expected the run to be rejected while the lock was held; observed rc=$lock_rc status='${lock_status:-<none>}'"
    fi
  fi

  aws s3api delete-object --bucket "$lock_bucket" --key "$lock_key" >/dev/null \
    || die "could not delete the fake lease s3://$lock_bucket/$lock_key"
  rm -f /tmp/clavesa-smoke-lease.json

  local unlock_json unlock_status
  unlock_json="$("$BIN" pipeline run "$pdir" --wait --json --force --workspace "$SMOKE_WS")" \
    || die "pipeline run after lease removal failed${unlock_json:+: $unlock_json}"
  echo "$unlock_json" | sed 's/^/    /'
  unlock_status="$(echo "$unlock_json" | jq -r .status)"
  if [[ "$unlock_status" == "SUCCEEDED" ]]; then
    pass "run after lease removal SUCCEEDED (lock release/removal unblocks)"
  else
    fail "expected SUCCEEDED after lease removal, observed '$unlock_status'"
  fi

  # ----- 10. summary / stamp ------------------------------------------------
  banner "summary"
  if [[ "$FAILURES" -gt 0 ]]; then
    echo "✗ cloud smoke FAILED: $FAILURES assertion(s) — see ✗ lines above" >&2
    exit 1
  fi
  local stamp="$REPO_ROOT/.cloud-smoke-green.json"
  jq -n \
    --arg version "$(module_version)" \
    --arg commit "$(git -C "$REPO_ROOT" rev-parse HEAD)" \
    --arg timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg workspace "$SMOKE_WS" \
    '{version: $version, commit: $commit, timestamp: $timestamp, workspace: $workspace}' \
    > "$stamp"
  echo "✓ cloud smoke GREEN — stamp written to $stamp"
  cat "$stamp"
}

# ---------------------------------------------------------------------------
# setup — one-time workspace creation (recreate-after-destroy friendly)
# ---------------------------------------------------------------------------
cmd_setup() {
  require_tools
  if [[ -e "$SMOKE_WS" ]]; then
    die "$SMOKE_WS already exists — destroy + remove it first:
  (cd $SMOKE_WS && $BIN pipeline destroy $SMOKE_PIPELINE --yes && $BIN workspace destroy --yes)
  rm -rf $SMOKE_WS"
  fi

  local ws_name
  ws_name="$(basename "$SMOKE_WS")"

  banner "workspace init $ws_name at $SMOKE_WS"
  mkdir -p "$(dirname "$SMOKE_WS")"
  "$BIN" workspace init "$ws_name" --workspace "$SMOKE_WS"

  banner "workspace use --env cloud --profile $SMOKE_PROFILE"
  "$BIN" workspace use --env cloud --profile "$SMOKE_PROFILE" --workspace "$SMOKE_WS"
  resolve_aws_env
  derive_names

  banner "workspace deploy (bucket + ECR + system Glue DB + runner image)"
  "$BIN" workspace deploy --yes --workspace "$SMOKE_WS" \
    || die "workspace deploy failed"

  local bucket
  bucket="$(terraform -chdir="$SMOKE_WS" output -raw pipeline_bucket)" \
    || die "could not read pipeline_bucket terraform output from $SMOKE_WS"
  echo "pipeline bucket: $bucket"

  # ----- seed data: ~200 rows of taxi-like parquet over 3 day-partitions ----
  # Parquet, not CSV: the runner's incremental partitioned read hardcodes
  # spark.read.parquet today (GH #40) — a CSV seed would make the gate red
  # against a known filed limitation instead of against regressions.
  banner "seed s3://$bucket/seed/ (y=2026/m=06/d=01..03, parquet)"
  python3 -c 'import pyarrow' 2>/dev/null \
    || die "python3 with pyarrow is required to generate the parquet seed (pip install pyarrow)"
  local seed_dir
  seed_dir="$(mktemp -d)"
  python3 - "$seed_dir" <<'PYEOF'
import random, sys
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq

out = sys.argv[1]
random.seed(42)
payment_types = ["credit_card", "cash", "dispute", "no_charge"]
days = ["01", "02", "03"]
rows_per_day = 67  # ~200 rows total across 3 partitions
for d in days:
    base = datetime(2026, 6, int(d))
    ts, pay, fare, dist = [], [], [], []
    for _ in range(rows_per_day):
        ts.append(base + timedelta(minutes=random.randint(0, 1439)))
        pay.append(random.choice(payment_types))
        fare.append(round(random.uniform(3.5, 80.0), 2))
        dist.append(round(random.uniform(0.3, 25.0), 2))
    table = pa.table({
        "pickup_ts": pa.array(ts, pa.timestamp("us")),
        "payment_type": pa.array(pay, pa.string()),
        "fare_amount": pa.array(fare, pa.float64()),
        "trip_distance": pa.array(dist, pa.float64()),
    })
    pq.write_table(table, f"{out}/part-{d}.parquet")
PYEOF
  local d
  for d in 01 02 03; do
    aws s3 cp "$seed_dir/part-$d.parquet" "s3://$bucket/seed/y=2026/m=06/d=$d/part.parquet"
  done
  rm -rf "$seed_dir"

  # ----- source: partitioned s3, watermark starts at day 2 ------------------
  # --start-from 2026/06/02 leaves the day-1 partition unconsumed by normal
  # runs, so the run-subcommand's backfill leg can stage it cleanly (GH #36).
  banner "source register trips_raw (s3, parquet, partitions y/m/d, start-from day 2)"
  "$BIN" source register trips_raw \
    --from "s3://$bucket/seed/" \
    --format parquet \
    --partitions y,m,d \
    --start-from "2026/06/02" \
    --workspace "$SMOKE_WS"

  # ----- pipeline + nodes ----------------------------------------------------
  banner "pipeline create $SMOKE_PIPELINE + transforms"
  "$BIN" pipeline create "$SMOKE_PIPELINE" --workspace "$SMOKE_WS"
  local pdir="$SMOKE_WS/$SMOKE_PIPELINE"

  "$BIN" node add "$pdir" --type transform --name trips --workspace "$SMOKE_WS"
  "$BIN" source attach "$pdir" trips_raw --to trips --as raw_trips --workspace "$SMOKE_WS"
  # --output-stats opts the default output into per-column stats — the
  # column_stats assertion in `run` depends on it.
  "$BIN" node edit "$pdir" trips \
    --set 'sql=SELECT pickup_ts, payment_type, fare_amount, trip_distance FROM raw_trips' \
    --output-stats \
    --workspace "$SMOKE_WS"

  "$BIN" node add "$pdir" --type transform --name revenue_by_payment --workspace "$SMOKE_WS"
  "$BIN" node connect "$pdir" --from trips --to revenue_by_payment --input trips --workspace "$SMOKE_WS"
  "$BIN" node edit "$pdir" revenue_by_payment \
    --set 'sql=SELECT payment_type, SUM(fare_amount) AS revenue, COUNT(*) AS trip_count FROM trips GROUP BY payment_type' \
    --workspace "$SMOKE_WS"

  # ----- daily schedule -------------------------------------------------------
  # The scaffolded variables.tf declares trigger_schedule (default null);
  # terraform.tfvars persists the value across `orchestration sync` /
  # `pipeline upgrade` re-emits, which overwrite orchestration.tf only.
  banner "set daily trigger_schedule via terraform.tfvars"
  if [[ -f "$pdir/terraform.tfvars" ]]; then
    die "$pdir/terraform.tfvars already exists — refusing to overwrite"
  fi
  printf 'trigger_schedule = "rate(1 day)"\n' > "$pdir/terraform.tfvars"
  # Re-sync so orchestration.tf reflects the current graph before deploy.
  "$BIN" pipeline orchestration sync "$pdir" --workspace "$SMOKE_WS"

  # ----- deploy + prove ------------------------------------------------------
  banner "pipeline deploy + first run"
  "$BIN" pipeline deploy "$pdir" --yes --workspace "$SMOKE_WS" \
    || die "pipeline deploy failed"
  "$BIN" pipeline run "$pdir" --wait --workspace "$SMOKE_WS" \
    || die "first pipeline run failed"

  banner "setup complete"
  cat <<EOF
Smoke workspace ready at $SMOKE_WS.

The run subcommand needs (already its defaults if you kept the seed layout):
  export SMOKE_WS=$SMOKE_WS
  export SMOKE_PIPELINE=$SMOKE_PIPELINE
  export SMOKE_BACKFILL_FROM=2026/06/01
  export SMOKE_BACKFILL_TO=2026/06/01

Per release:  make smoke-cloud
EOF
}

# ---------------------------------------------------------------------------
case "${1:-run}" in
  run)   cmd_run ;;
  setup) cmd_setup ;;
  *)
    echo "usage: $0 [run|setup]" >&2
    exit 2
    ;;
esac
