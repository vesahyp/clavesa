#!/usr/bin/env bash
# bundle-e2e — ADR-031's end-to-end test: one Databricks bundle, deployed as
# a Databricks job and as a clavesa job, and the tables compared.
#
# The bundle is Databricks' own default_sql example (what `databricks bundle
# init default-sql` creates), fetched at a pinned commit by
# testdata/e2e/default_sql/prepare.py with the edits its README lists:
#
#   1. clavesa bundle plan on the example as published: the Databricks-only
#      SQL it refuses (shown, not a failure).
#   2. The input: Databricks' public retail-org/sales_orders file, copied
#      once to s3://$E2E_SOURCE_BUCKET/e2e-bundle/ (both sides read it).
#   3. databricks bundle deploy -t dbx + databricks bundle run: the job on
#      a SQL warehouse writes $catalog.$schema.orders_raw / orders_daily.
#   4. clavesa bundle deploy -t aws + clavesa bundle run --wait: the same job
#      on clavesa (Lambda), in shadow mode: <table>_clavesa next to them,
#      registered in Unity Catalog.
#   5. clavesa bundle compare -t aws: PASS or FAIL (no DBUs). The service
#      principal gets SELECT on the two Databricks tables first (as the
#      ADR-027 setup gives it on any table it compares).
#   6. A mixed run (target aws-mixed): orders_raw on clavesa, orders_daily
#      on Databricks, started by clavesa with run-now on that target's job
#      with only [orders_daily] (deployed paused, with run permission for
#      the service principal). Its Databricks run must show orders_raw
#      excluded and orders_daily SUCCESS.
#   7. E2E_TEARDOWN=1: destroy what the test deployed, drop the four tables
#      and the input copy (what this script created). The workspace stays.
#
# Needs a clavesa workspace on the cloud warehouse connected to the
# Databricks workspace (clavesa workspace connect databricks), a Databricks
# CLI profile for the same workspace, and a SQL warehouse (the smallest
# serverless one, auto-stop 1 minute, is plenty: two statements per task).
#
# Env:
#   E2E_WS            the clavesa workspace (required)
#   E2E_PROFILE       Databricks CLI profile (default: DATABRICKS_CONFIG_PROFILE)
#   E2E_WAREHOUSE     SQL warehouse ID for the Databricks job (required)
#   E2E_SOURCE_BUCKET bucket under a Unity Catalog external location both can read (required)
#   E2E_AWS_PROFILE   AWS profile (default: the workspace's)
#   E2E_DEPLOY_WORKSPACE=1  run clavesa workspace deploy first (pushes the runner image)
#   E2E_TEARDOWN=1    remove what the test created at the end
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${CLAVESA_BIN:-$ROOT/bin/clavesa}"
: "${E2E_WS:?set E2E_WS to the clavesa workspace}"
: "${E2E_WAREHOUSE:?set E2E_WAREHOUSE to a Databricks SQL warehouse ID}"
: "${E2E_SOURCE_BUCKET:?set E2E_SOURCE_BUCKET}"
export DATABRICKS_CONFIG_PROFILE="${E2E_PROFILE:-${DATABRICKS_CONFIG_PROFILE:-}}"
AWS_PROFILE="${E2E_AWS_PROFILE:-$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1])).get("profile",""))' "$E2E_WS/.clavesa/aws-profile.json" 2>/dev/null || true)}"
export AWS_PROFILE
HOST="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["databricks"]["host"])' "$E2E_WS/clavesa.json")"
SP="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["client_id"])' "$E2E_WS/.clavesa/credentials/$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["databricks"]["credential"])' "$E2E_WS/clavesa.json").json")"
VARS=(--var "warehouse_id=$E2E_WAREHOUSE" --var "clavesa_principal=$SP")
WORK="$(mktemp -d "${TMPDIR:-/tmp}/clavesa-bundle-e2e.XXXX")"
JOB=default_sql_sql_job
SRC="s3://$E2E_SOURCE_BUCKET/e2e-bundle/retail-org/sales_orders/"
PUBLIC="s3://databricks-datasets-oregon/retail-org/sales_orders/part-00000-tid-1771549084454148016-e2275afd-a5bb-40ed-b044-1774c0fdab2b-105592-1-c000.json"
step() { printf '\n== %s\n' "$*"; }
now() { date -u +%Y-%m-%dT%H:%M:%SZ; }

step "1. the example as published: what clavesa bundle plan refuses"
python3 "$ROOT/testdata/e2e/default_sql/prepare.py" "$WORK/as-published" --host "$HOST" --source-bucket "$E2E_SOURCE_BUCKET" --as-published >/dev/null
"$BIN" bundle plan --bundle "$WORK/as-published" -t aws "${VARS[@]}" | sed -n '1,12p' || true

python3 "$ROOT/testdata/e2e/default_sql/prepare.py" "$WORK/bundle" --host "$HOST" --source-bucket "$E2E_SOURCE_BUCKET"
B="$WORK/bundle"
"$BIN" bundle plan --bundle "$B" -t aws "${VARS[@]}"

step "2. the input in $SRC"
if aws s3 ls "$SRC" >/dev/null 2>&1; then
  echo "present"
else
  aws s3 cp "$PUBLIC" "$SRC" --source-region us-west-2
fi

step "3. Databricks: deploy -t dbx and run"
(cd "$B" && databricks bundle deploy -t dbx "${VARS[@]}")
DBX_START="$(now)"
(cd "$B" && databricks bundle run -t dbx "${VARS[@]}" "$JOB" >"$WORK/dbx-run.json")
DBX_END="$(now)"
echo "Databricks run $DBX_START .. $DBX_END"

step "4. clavesa: deploy -t aws and run"
if [[ "${E2E_DEPLOY_WORKSPACE:-}" == "1" ]]; then
  "$BIN" workspace deploy --workspace "$E2E_WS" --yes
fi
"$BIN" bundle deploy --bundle "$B" -t aws "${VARS[@]}" --workspace "$E2E_WS" --yes
AWS_START="$(now)"
"$BIN" bundle run --bundle "$B" -t aws "${VARS[@]}" --job "$JOB" --workspace "$E2E_WS" --wait
AWS_END="$(now)"
echo "clavesa run $AWS_START .. $AWS_END"

step "5. compare (no DBUs)"
for t in orders_raw orders_daily; do
  databricks grants update table "team_ucpub.sales.$t" \
    --json "{\"changes\":[{\"principal\":\"$SP\",\"add\":[\"SELECT\"]}]}" >/dev/null
done
rc=0
"$BIN" bundle compare --bundle "$B" -t aws "${VARS[@]}" --job "$JOB" --workspace "$E2E_WS" || rc=$?

step "6. mixed: orders_raw on clavesa, orders_daily on Databricks"
(cd "$B" && databricks bundle deploy -t aws-mixed "${VARS[@]}")
"$BIN" bundle deploy --bundle "$B" -t aws-mixed "${VARS[@]}" --workspace "$E2E_WS" --yes
"$BIN" bundle run --bundle "$B" -t aws-mixed "${VARS[@]}" --job "$JOB" --workspace "$E2E_WS" --wait
MIXED_JOB="$(cd "$B" && databricks bundle summary -t aws-mixed "${VARS[@]}" -o json | python3 -c 'import json,sys;print(json.load(sys.stdin)["resources"]["jobs"]["default_sql_sql_job"]["id"])')"
LAST="$(databricks jobs list-runs --job-id "$MIXED_JOB" --limit 1 -o json | python3 -c 'import json,sys;d=json.load(sys.stdin);d=d if isinstance(d,list) else d.get("runs",[]);print(d[0]["run_id"])')"
STATES="$(databricks jobs get-run "$LAST" -o json | python3 -c 'import json,sys;print(" ".join(t["task_key"]+"="+t["state"].get("result_state","") for t in json.load(sys.stdin)["tasks"]))')"
echo "Databricks run $LAST: $STATES"
if [[ "$STATES" != *"orders_daily=SUCCESS"* || "$STATES" == *"orders_raw=SUCCESS"* ]]; then
  echo "✗ run-now with only: want orders_daily=SUCCESS and orders_raw not run"
  rc=1
fi

if [[ "${E2E_TEARDOWN:-}" == "1" ]]; then
  step "7. teardown: what this test created"
  "$BIN" bundle destroy --bundle "$B" -t aws-mixed "${VARS[@]}" --workspace "$E2E_WS" --yes || true
  for t in dbx aws-mixed; do
    (cd "$B" && databricks bundle destroy -t "$t" "${VARS[@]}" --auto-approve) || true
  done
  for t in orders_raw orders_daily orders_raw_clavesa orders_daily_clavesa; do
    databricks tables delete "team_ucpub.sales.$t" 2>/dev/null && echo "dropped team_ucpub.sales.$t" || true
  done
  aws s3 rm "$SRC" --recursive || true
fi
exit $rc
