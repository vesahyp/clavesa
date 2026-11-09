#!/usr/bin/env bash
# Shadow-mode gate (docs/cookbook/replace-a-databricks-step.md): drives the
# built bin/clavesa against the test workspace's shadow pipeline and asserts
# the recipe's read-only and clavesa-side steps. No DBUs: the preflight only
# reads, the comparison runs in the local runner container. The switch and
# roll-back need a Databricks admin and SQL, so they are not repeated here;
# docs/ideas/unity-catalog-publish.md records the rehearsal.
#
#   UC_SHADOW_WS        workspace (default ~/clavesa-workspaces/ucpub)
#   UC_SHADOW_PIPELINE  the shadow pipeline (default uc_events)
#   UC_SHADOW_EXISTING  the Databricks step's table (default team_ucpub.sales.daily_events)
#   UC_SHADOW_TABLE     clavesa's shadow table (default <existing>_clavesa)
#   UC_SHADOW_PARTITION / UC_SHADOW_SUMS  (default event_date / amount,quantity)
#   UC_SHADOW_JOB       the step's Databricks job (default 923072819710352)
#   UC_SHADOW_PROFILE   databricks CLI profile for the preflight (default dbx-ws)
#   AWS_PROFILE / AWS_REGION as for any deploy of that workspace
#
# Run via `make smoke-databricks-shadow` (depends on build).
set -euo pipefail

repo=$(cd "$(dirname "$0")/.." && pwd)
clavesa="${CLAVESA_BIN:-$repo/bin/clavesa}"   # CLAVESA_BIN: test an installed binary instead
ws=${UC_SHADOW_WS:-$HOME/clavesa-workspaces/ucpub}
pipeline=${UC_SHADOW_PIPELINE:-uc_events}
existing=${UC_SHADOW_EXISTING:-team_ucpub.sales.daily_events}
shadow=${UC_SHADOW_TABLE:-${existing}_clavesa}
partition=${UC_SHADOW_PARTITION:-event_date}
sums=${UC_SHADOW_SUMS:-amount,quantity}
job=${UC_SHADOW_JOB:-923072819710352}
profile=${UC_SHADOW_PROFILE:-dbx-ws}
cd "$ws"

pass() { echo "✓ $*"; }
fail() { echo "✗ $*" >&2; exit 1; }
json() { python3 -c "import json,sys; d=json.load(sys.stdin); print(eval(sys.argv[1]))" "$1"; }

conn=$("$clavesa" workspace databricks --json --workspace .)
host=$(echo "$conn" | json 'd["connection"]["host"]')
client_id=$(echo "$conn" | json 'd["client_id"]')

# 1. Preflight, read-only, as a person for the service principal.
"$clavesa" workspace databricks preflight --host "$host" --profile "$profile" --principal "$client_id" \
  --table "$existing" --shadow "$shadow" --job "$job" >/dev/null || fail "preflight is not ready"
pass "preflight ready for $existing"

# 2. A shadow run (with no new files it skips, which is a success).
"$clavesa" pipeline run "$pipeline" --wait --workspace . >/dev/null || fail "pipeline run $pipeline"
pass "pipeline run $pipeline SUCCEEDED"

# 3. The daily comparison passes.
res=$("$clavesa" pipeline databricks-compare "$pipeline" --existing "$existing" --shadow "$shadow" \
  --partition "$partition" --sum "$sums" --json --workspace . 2>/dev/null) || true
[[ $(echo "$res" | json 'd["pass"]') == "True" ]] || { echo "$res"; fail "comparison failed"; }
pass "comparison PASS: $(echo "$res" | json 'len(d["partitions"])') partitions of $existing match $shadow"

echo "Shadow-mode smoke GREEN"
