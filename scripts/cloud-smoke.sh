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
# Asserts via `aws athena` instead of `clavesa query` because `clavesa query`
# is local-only today (known bug).
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
