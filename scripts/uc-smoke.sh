#!/usr/bin/env bash
# Unity Catalog smoke gate (ADR-027 slice 8): drives the built bin/clavesa
# against a connected, deployed workspace and asserts in Databricks itself.
# The steps are the "Feed a Databricks job" recipe's run and Verify steps
# (docs/cookbook/feed-a-databricks-job.md); the one-time setup steps need a
# Databricks admin and are not repeated here.
#
#   UC_SMOKE_WS        workspace (default ~/clavesa-workspaces/ucpub)
#   UC_SMOKE_PIPELINE  a pipeline with a published output and a
#                      databricks-jobs entry (default uc_orders)
#   AWS_PROFILE / AWS_REGION as for any deploy of that workspace
#
# Needs: the databricks CLI and the aws CLI on PATH, python3.
# Run via `make smoke-databricks` (depends on build).
set -euo pipefail

repo=$(cd "$(dirname "$0")/.." && pwd)
clavesa="${CLAVESA_BIN:-$repo/bin/clavesa}"   # CLAVESA_BIN: test an installed binary instead
ws=${UC_SMOKE_WS:-$HOME/clavesa-workspaces/ucpub}
pipeline=${UC_SMOKE_PIPELINE:-uc_orders}
cd "$ws"

pass() { echo "✓ $*"; }
fail() { echo "✗ $*" >&2; exit 1; }
json() { python3 -c "import json,sys; d=json.load(sys.stdin); print(eval(sys.argv[1]))" "$1"; }

# 1. The admin's part still holds.
"$clavesa" workspace databricks --check --workspace . >/dev/null || fail "workspace databricks --check"
pass "connect checks"

# The service principal, for asserting in Databricks as clavesa sees it.
conn=$("$clavesa" workspace databricks --json --workspace .)
host=$(echo "$conn" | json 'd["connection"]["host"]')
cred=$(echo "$conn" | json 'd["connection"]["credential"]')
client_id=$(echo "$conn" | json 'd["client_id"]')
ref=$("$clavesa" credential show "$cred" --json --workspace . | json 'd["secret"]')
case "$ref" in
  arn:aws:secretsmanager:*)
    region=$(echo "$ref" | cut -d: -f4)
    secret=$(aws secretsmanager get-secret-value --region "$region" --secret-id "$ref" --query SecretString --output text) ;;
  env:*) secret=$(printenv "${ref#env:}") ;;
  *) fail "credential $cred: smoke reads arn: or env: secrets only" ;;
esac
dbx() {
  DATABRICKS_HOST="$host" DATABRICKS_CLIENT_ID="$client_id" DATABRICKS_CLIENT_SECRET="$secret" \
    DATABRICKS_CONFIG_PROFILE= databricks "$@" 2>/dev/null
}

# 2. Run (recipe step 6).
out=$("$clavesa" pipeline run "$pipeline" --wait --workspace . 2>&1) || { echo "$out"; fail "pipeline run"; }
arn=$(echo "$out" | sed -n 's/^Started execution: //p')
[[ -n "$arn" ]] || fail "no execution ARN in: $out"
pass "pipeline run $pipeline SUCCEEDED ($arn)"

# 3. The run's Databricks record.
run=$("$clavesa" pipeline databricks-run "$pipeline" "$arn" --json --workspace .)
[[ $(echo "$run" | json 'd["found"]') == "True" ]] || fail "no Databricks record for the run"
table=$(echo "$run" | json 'd["tables"][0]["full_name"]')
job_run=$(echo "$run" | json '(d["jobs"] or [{}])[0].get("run_id", "")')
pass "run wrote $table$( [[ -n "$job_run" ]] && echo ", started Databricks run $job_run")"

# 4. The table in Unity Catalog, clavesa's, external Delta.
t=$(dbx tables get "$table" -o json) || fail "$table not in Unity Catalog"
[[ $(echo "$t" | json 'd["table_type"] + "/" + d["data_source_format"]') == "EXTERNAL/DELTA" ]] || fail "$table is not EXTERNAL/DELTA"
pass "$table is an external Delta table in Unity Catalog"

# 5. Lineage upstream of the table.
n=$(dbx api get "/api/2.0/lineage-tracking/external-lineage?object_info.table.name=$table&lineage_direction=UPSTREAM" | json 'len(d.get("external_lineage_relationships", []))')
[[ "$n" -gt 0 ]] || fail "no lineage upstream of $table"
pass "$n lineage edge(s) upstream of $table"

# 6. The downstream Databricks job run succeeds (it reads the table).
if [[ -n "$job_run" ]]; then
  for _ in $(seq 1 90); do
    state=$(dbx jobs get-run "$job_run" -o json | json 'd["state"]["life_cycle_state"] + "/" + d["state"].get("result_state", "")')
    case "$state" in TERMINATED/*|INTERNAL_ERROR/*|SKIPPED/*) break ;; esac
    sleep 10
  done
  [[ "$state" == "TERMINATED/SUCCESS" ]] || fail "Databricks run $job_run ended $state"
  pass "Databricks run $job_run TERMINATED/SUCCESS"
fi

echo "Unity Catalog smoke GREEN"
