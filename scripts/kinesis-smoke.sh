#!/usr/bin/env bash
# Kinesis smoke gate (ADR-028): a real Kinesis stream, a deployed clavesa
# pipeline that reads it on Fargate and then on Lambda, and the checks in
# Databricks itself. It walks the "Read a Kinesis stream into Unity Catalog"
# recipe (docs/cookbook/kinesis-to-unity-catalog.md) on a connected,
# deployed workspace.
#
# What it proves:
#   1. Events put on the stream land in a Delta table registered in Unity
#      Catalog: the row count, the distinct ids, a sum and a sample of rows
#      match what was sent, read from a Databricks SQL warehouse.
#   2. A run stopped while events are arriving loses and doubles nothing:
#      the next run continues from the checkpoint.
#   3. A run with nothing new adds nothing.
#   4. The same pipeline on compute = "lambda" continues the same checkpoint.
#
# Cost: one provisioned shard for the length of the test (about 20
# minutes), small Fargate tasks (2 vCPU) and Lambda runs, and a few minutes
# of a 2X-Small SQL warehouse. Well under 1 USD. The stream is deleted on
# every exit (the trap below), and so is the pipeline unless KEEP=1.
#
#   KIN_SMOKE_WS         workspace (default ~/clavesa-workspaces/ucpub)
#   KIN_SMOKE_PIPELINE   pipeline name (default uc_kinesis)
#   KIN_SMOKE_STREAM     stream name (default clavesa-kinesis-smoke)
#   KIN_SMOKE_WAREHOUSE  Databricks SQL warehouse id (default: the one named
#                        clavesa-switch-test, else the first one)
#   KIN_SMOKE_SQL_PROFILE Databricks CLI profile of a reader of the table
#                        (default dbx-ws). The checks read as that reader,
#                        the way a team reads the table; clavesa's service
#                        principal needs no SQL access.
#   KIN_SMOKE_COMPUTE    "both" (default), "fargate" or "lambda"
#   KEEP=1               keep the pipeline and its table afterwards
#   AWS_PROFILE / AWS_REGION as for any deploy of that workspace
#
# Needs: the databricks CLI and the aws CLI on PATH, python3.
# Run via `make smoke-kinesis` (depends on build).
set -euo pipefail

repo=$(cd "$(dirname "$0")/.." && pwd)
clavesa="${CLAVESA_BIN:-$repo/bin/clavesa}"
ws=${KIN_SMOKE_WS:-$HOME/clavesa-workspaces/ucpub}
pipeline=${KIN_SMOKE_PIPELINE:-uc_kinesis}
stream=${KIN_SMOKE_STREAM:-clavesa-kinesis-smoke}
compute=${KIN_SMOKE_COMPUTE:-both}
schema=kinesis_smoke
node=events
region=${AWS_REGION:-$(aws configure get region || true)}
[[ -n "$region" ]] || { echo "set AWS_REGION" >&2; exit 1; }
export AWS_REGION=$region
cd "$ws"
ver=$("$clavesa" version 2>&1 | grep -o 'v[0-9.]*' | head -1)
[[ -d ".clavesa/modules/$ver" ]] || { echo "$ws is not on $ver: run '$clavesa workspace upgrade' and 'workspace deploy' first" >&2; exit 1; }

pass() { echo "✓ $*"; }
fail() { echo "✗ $*" >&2; exit 1; }
json() { python3 -c "import json,sys; d=json.load(sys.stdin); print(eval(sys.argv[1]))" "$1"; }

# --- clean-up: the stream always goes; the pipeline unless KEEP=1 ----------
# The pipeline's AWS resources, its Unity Catalog table, and its data and
# stream checkpoint in S3 (pipeline destroy keeps those two, together).
reset_pipeline() {
  if [[ -d "$pipeline" ]]; then
    "$clavesa" pipeline destroy "$pipeline" --yes --workspace . >/dev/null 2>&1 \
      && echo "destroyed pipeline $pipeline" || echo "pipeline destroy failed: run it by hand" >&2
    rm -rf "$pipeline"
  fi
  aws s3 rm --recursive --quiet "s3://$bucket/$pipeline/" && echo "removed s3://$bucket/$pipeline/"
}

cleanup() {
  status=$?
  set +e
  echo "--- clean-up"
  if aws kinesis describe-stream-summary --stream-name "$stream" >/dev/null 2>&1; then
    aws kinesis delete-stream --stream-name "$stream" --enforce-consumer-deletion
    echo "deleted stream $stream"
  fi
  [[ "${KEEP:-}" == "1" ]] || reset_pipeline
  # Nothing may keep running: no ECS task in the pipeline's cluster, no
  # running Step Functions execution, no stream.
  for c in $(aws ecs list-clusters --query 'clusterArns[]' --output text | tr '\t' '\n' | grep "$pipeline" || true); do
    n=$(aws ecs list-tasks --cluster "$c" --query 'length(taskArns)' --output text)
    [[ "$n" == "0" ]] && echo "no task running in $c" || echo "WARNING: $n task(s) still running in $c" >&2
  done
  aws kinesis list-streams --query 'StreamNames' --output text | grep -qw "$stream" \
    && echo "WARNING: stream $stream still listed (deleting)" >&2 || echo "stream $stream gone"
  exit $status
}
trap cleanup EXIT

# --- Databricks, as clavesa's service principal ------------------------------
conn=$("$clavesa" workspace databricks --json --workspace .)
host=$(echo "$conn" | json 'd["connection"]["host"]')
catalog=$(echo "$conn" | json 'd["connection"]["catalog"]')
cred=$(echo "$conn" | json 'd["connection"]["credential"]')
client_id=$(echo "$conn" | json 'd["client_id"]')
ref=$("$clavesa" credential show "$cred" --json --workspace . | json 'd["secret"]')
case "$ref" in
  arn:aws:secretsmanager:*)
    secret=$(aws secretsmanager get-secret-value --region "$(echo "$ref" | cut -d: -f4)" --secret-id "$ref" --query SecretString --output text) ;;
  env:*) secret=$(printenv "${ref#env:}") ;;
  *) fail "credential $cred: the smoke reads arn: or env: secrets only" ;;
esac
dbx() {
  DATABRICKS_HOST="$host" DATABRICKS_CLIENT_ID="$client_id" DATABRICKS_CLIENT_SECRET="$secret" \
    DATABRICKS_CONFIG_PROFILE= databricks "$@" 2>/dev/null
}
sql_profile=${KIN_SMOKE_SQL_PROFILE:-dbx-ws}
reader() { databricks --profile "$sql_profile" "$@" 2>/dev/null; }
warehouse=${KIN_SMOKE_WAREHOUSE:-$(reader warehouses list -o json | python3 -c "
import json,sys; ws=json.load(sys.stdin)
w=[x for x in ws if x['name']=='clavesa-switch-test'] or ws
print(w[0]['id'] if w else '')")}
[[ -n "$warehouse" ]] || fail "no SQL warehouse; set KIN_SMOKE_WAREHOUSE"
table="$catalog.$schema.$node"
bucket=$(terraform -chdir=. output -raw pipeline_bucket 2>/dev/null || echo "$(basename "$ws")-clavesa")

# sql "<statement>" prints the first row, columns joined by |.
sql() {
  local body out
  body=$(python3 -c "import json,sys; print(json.dumps({'warehouse_id': sys.argv[1], 'statement': sys.argv[2], 'wait_timeout': '50s', 'on_wait_timeout': 'CONTINUE'}))" "$warehouse" "$1")
  out=$(reader api post /api/2.0/sql/statements --json "$body")
  local id state
  id=$(echo "$out" | json 'd["statement_id"]')
  state=$(echo "$out" | json 'd["status"]["state"]')
  while [[ "$state" == PENDING || "$state" == RUNNING ]]; do
    sleep 3
    out=$(reader api get "/api/2.0/sql/statements/$id")
    state=$(echo "$out" | json 'd["status"]["state"]')
  done
  [[ "$state" == SUCCEEDED ]] || fail "SQL $state: $1: $(echo "$out" | json 'd["status"].get("error", {}).get("message", "")')"
  echo "$out" | json '"|".join(str(c) for c in (d["result"].get("data_array") or [[]])[0])'
}

# --- events -----------------------------------------------------------------
# put_events FROM TO: ids FROM..TO, amount = id / 4, user = u<id mod 7>.
put_events() {
  local from=$1 to=$2 dir
  dir=$(mktemp -d)
  python3 - "$from" "$to" "$dir" <<'PY'
import json, sys
lo, hi, d = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
ids = list(range(lo, hi + 1))
for n, i in enumerate(range(0, len(ids), 500)):
    recs = [{"Data": json.dumps({"id": k, "user": f"u{k % 7}", "amount": f"{k / 4:.2f}"}),
             "PartitionKey": f"u{k % 7}"} for k in ids[i:i + 500]]
    with open(f"{d}/{n:05d}.json", "w") as f:
        json.dump({"Records": recs}, f)
PY
  for f in "$dir"/*.json; do
    failed=$(aws kinesis put-records --stream-name "$stream" --cli-binary-format raw-in-base64-out \
      --cli-input-json "file://$f" --query FailedRecordCount --output text)
    [[ "$failed" == "0" ]] || fail "put-records: $failed records failed"
  done
  rm -rf "$dir"
}

# expect TOTAL: the table holds ids 1..TOTAL exactly once.
expect() {
  local total=$1 got want
  got=$(sql "SELECT count(*), count(DISTINCT id), CAST(sum(amount) AS DECIMAL(18,2)), min(id), max(id) FROM $table")
  want="$total|$total|$(python3 -c "print(f'{sum(range(1, $total + 1)) / 4:.2f}')")|1|$total"
  [[ "$got" == "$want" ]] || fail "$table: count|distinct|sum|min|max = $got, want $want"
  pass "$table in Databricks: $total rows, $total distinct ids, sum(amount) and range match"
  local mid=$(( (total + 1) / 2 ))
  got=$(sql "SELECT concat_ws(',', transform(sort_array(collect_list(struct(id, concat_ws(':', id, user_id, amount) AS s))), x -> x.s)) FROM $table WHERE id IN (1, $mid, $total)")
  want=$(python3 -c "print(','.join(f'{k}:u{k % 7}:{k / 4:.2f}' for k in (1, $mid, $total)))")
  [[ "$got" == "$want" ]] || fail "sample rows: $got, want $want"
  pass "sample rows match what was sent: $got"
}

run_ok() {
  local out
  # A stopped run keeps the pipeline's run lease until it expires (TTL 120 s
  # plus 30 s grace, runner/run_lock.py); wait for it like an operator would.
  for _ in $(seq 1 24); do
    out=$("$clavesa" pipeline run "$pipeline" --wait --workspace . 2>&1) && break
    # Retried: the lease of a stopped run, and IAM propagation of a role
    # that a deploy re-created a few seconds ago (Step Functions then gets
    # UnrecognizedClientException from ECS).
    echo "$out" | grep -q "a run is already in progress\|security token included in the request is invalid" \
      || { echo "$out"; fail "pipeline run"; }
    sleep 10
  done
  echo "$out" | grep -q "a run is already in progress" && fail "run lease still held after 4 minutes"
  echo "$out" | grep -q "Status: SUCCEEDED" || { echo "$out"; fail "pipeline run did not succeed"; }
  echo "$out" | grep -E "^Unity Catalog:" || true
}

# --- 1. the stream: one provisioned shard ------------------------------------
if ! aws kinesis describe-stream-summary --stream-name "$stream" >/dev/null 2>&1; then
  # A new stream: a table and checkpoint left from an earlier stream of the
  # same name (KEEP=1) would not match it.
  reset_pipeline
  aws kinesis create-stream --stream-name "$stream" --shard-count 1 --stream-mode-details StreamMode=PROVISIONED
fi
aws kinesis wait stream-exists --stream-name "$stream"
pass "stream $stream: 1 provisioned shard, ACTIVE"

# --- 2. the source and the pipeline (recipe steps 1 to 3) --------------------
"$clavesa" source show kin_smoke --workspace . >/dev/null 2>&1 \
  || "$clavesa" source register kin_smoke --kind kinesis --stream "$stream" --region "$region" --workspace . >/dev/null
if [[ ! -d "$pipeline" ]]; then
  "$clavesa" pipeline create "$pipeline" --schema "$schema" --workspace . >/dev/null
  "$clavesa" node add "$pipeline" --type transform --name "$node" --workspace . >/dev/null
  "$clavesa" node edit "$pipeline" "$node" --workspace . \
    --set "sql=SELECT CAST(get_json_object(raw, '\$.id') AS BIGINT) AS id, get_json_object(raw, '\$.user') AS user_id, CAST(get_json_object(raw, '\$.amount') AS DECIMAL(12,2)) AS amount, CAST(arrival_ts AS DATE) AS event_date, arrival_ts, _source_partition, _source_offset FROM events" \
    --output-mode append --output-partition-by event_date >/dev/null
  "$clavesa" source attach "$pipeline" kin_smoke --to "$node" --as events --workspace . >/dev/null
  printf 'fargate_cpu       = 2\nfargate_memory_gb = 8\n' > "$pipeline/terraform.tfvars"
fi
first=$([[ "$compute" == lambda ]] && echo lambda || echo fargate)
"$clavesa" node edit "$pipeline" "$node" --set "compute=$first" --workspace . >/dev/null
"$clavesa" pipeline deploy "$pipeline" --yes --workspace . >/dev/null
pass "pipeline $pipeline deployed, compute = $first"

# --- 3. first run: 1000 events ------------------------------------------------
put_events 1 1000
run_ok
expect 1000

# --- 4. a run stopped while events arrive, then the next run -----------------
put_events 1001 1500
( for k in $(seq 0 19); do put_events $((1501 + k * 100)) $((1600 + k * 100)); sleep 6; done ) &
producer=$!
"$clavesa" pipeline run "$pipeline" --workspace . >/dev/null 2>&1 || true
sm=$(aws stepfunctions list-state-machines --query "stateMachines[?contains(name, '$pipeline')].stateMachineArn | [0]" --output text)
exe=$(aws stepfunctions list-executions --state-machine-arn "$sm" --status-filter RUNNING --query 'executions[0].executionArn' --output text)
[[ "$exe" != None ]] || fail "no running execution to stop"
sleep "${KIN_SMOKE_STOP_AFTER:-110}"   # inside the drain: the task has started and is writing batches
aws stepfunctions stop-execution --execution-arn "$exe" --cause "kinesis smoke: stopped mid-run" >/dev/null
wait "$producer"
# The stopped run must have written part of the backlog, else this step
# proves nothing about a restart: more than the first 1000, fewer than all.
partial=$(sql "SELECT count(*) FROM $table")
(( partial > 1000 && partial < 3500 )) \
  || fail "the stopped run left $partial rows; want between 1000 and 3500 (tune KIN_SMOKE_STOP_AFTER)"
pass "stopped run $exe mid-drain while events were arriving: it had committed $partial of 3500 rows"
for _ in $(seq 1 60); do   # the ECS task stops after the execution
  n=$(aws ecs list-tasks --cluster "$(aws ecs list-clusters --query "clusterArns[?contains(@, '$pipeline')] | [0]" --output text)" --query 'length(taskArns)' --output text 2>/dev/null || echo 0)
  [[ "$n" == "0" || "$n" == "None" ]] && break
  sleep 5
done
run_ok
expect 3500

# --- 5. nothing new: nothing added --------------------------------------------
run_ok
expect 3500
pass "a run with no new events added nothing"

# --- 6. the same checkpoint on Lambda ----------------------------------------
if [[ "$compute" == both ]]; then
  "$clavesa" node edit "$pipeline" "$node" --set compute=lambda --workspace . >/dev/null
  "$clavesa" pipeline deploy "$pipeline" --yes --workspace . >/dev/null
  pass "pipeline $pipeline redeployed, compute = lambda"
  put_events 3501 4000
  run_ok
  expect 4000
fi

# --- 7. lineage ---------------------------------------------------------------
n=$(dbx api get "/api/2.0/lineage-tracking/external-lineage?object_info.table.name=$table&lineage_direction=UPSTREAM" | json 'len(d.get("external_lineage_relationships", []))')
[[ "$n" -gt 0 ]] || fail "no lineage upstream of $table"
pass "$n lineage edge(s) upstream of $table"

echo "Kinesis smoke GREEN"
