#!/usr/bin/env bash
# verify-cookbook.sh — deterministic cookbook-recipe verification gate.
#
# The testing doctrine (CLAUDE.md) says internal testing = walking the
# README AND the cookbook recipes literally with the built binary. Only
# the README half is automated (scripts/verify-readme.sh). This gate
# closes the other half: it walks each cookbook recipe's literal CLI
# block against a fresh tempdir workspace and asserts the outcomes, so a
# recipe that rots at the next migration goes RED here instead of lying
# in the docs (GH #86).
#
# Recipes are modular — one `recipe_<name>` function each, sharing the
# moto + workspace setup below; RECIPES (default set below) selects which
# run. s3-trigger's event path (EventBridge→SQS→SFN) is cloud-only and
# lives in smoke-cloud, not here.
#
# This gate earned its keep on its first run: the backfill recipe was RED
# and caught two real bugs the coverage hole had hidden — GH #68 (local
# backfill list/diff read the retired `<db>.db/` Iceberg layout with
# 3-part table ids while the current Delta layout is nested and 2-part)
# and GH #87 (the runner's boto3 partition discovery ignored
# CLAVESA_S3_ENDPOINT). Both are fixed; the recipe is green.
#
# Tiering (GH #86): NOT per-commit. This belongs in release-gates + a
# nightly cron, so rot surfaces within a day of the migration that causes
# it. Do not wire it into `make test` or a git hook. Invoke via
# `make verify-cookbook` (never directly): the make target depends on
# `build`, which guarantees the binary and the runner files embedded in
# it are the current tree, not a stale build.
#
# Prerequisites (on top of verify-readme's jq/curl/python3/docker):
#   - docker running (the pipeline run + backfill stage boot Spark in the
#     runner container; memory-constrained VM ⇒ one Spark workload at a
#     time — this gate runs them sequentially).
#   - python3 able to import boto3, moto, pyarrow:
#         pip install 'moto[server]' boto3 pyarrow
#     moto[server] gives a local S3 endpoint (the partitioned-source
#     recipes need real S3 semantics; there is no local-S3 harness
#     otherwise). boto3 seeds the bucket; pyarrow writes the parquet.
#
# How the local-S3 wiring works (the non-obvious part):
#   - moto binds 0.0.0.0 so BOTH the host and the runner container reach
#     it. The host (boto3 seeding) talks to http://127.0.0.1:<port>; the
#     container talks to http://host.docker.internal:<port>. CLAVESA_S3_
#     ENDPOINT (forwarded into the container by runner.AWSEnvDockerArgs)
#     therefore carries the host.docker.internal form.
#   - dummy AWS creds (moto accepts any) are exported so the CLI + runner
#     both authenticate. The runner's Spark S3A config already honors
#     CLAVESA_S3_ENDPOINT with path-style access + ssl-off for http
#     endpoints (runner/spark_conf.py) — no product change needed there.
#
# Local-S3 endpoint for boto3 (resolved — GH #87): Spark's S3A data read
#   already honored CLAVESA_S3_ENDPOINT, but the runner's boto3 clients —
#   which do PARTITION DISCOVERY for a partitioned s3 source (runner.py
#   `_partition_leaves`, a list_objects_v2 walk) — ignored it and hit real
#   AWS. runner.py now exports AWS_ENDPOINT_URL_S3 from CLAVESA_S3_ENDPOINT
#   at module load, so botocore's native global-endpoint env points every
#   boto3 client at moto. The harness exports the host.docker.internal
#   endpoint below; no product change is needed here anymore.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$REPO_ROOT/bin/clavesa"

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
# Recipes to run (space-separated). Default: all GREEN recipes. A recipe is
# added here only once it walks green against the built binary + docker; a
# recipe that goes RED on a real product bug stays OUT of the default (its
# function survives with a comment naming the bug) so the gate stays green
# and the lead can triage the fix.
# query-your-data and dashboards were held out on one root-caused bug —
# LocalProvider.Query (and the cloud twin) softened a missing-table error to
# an empty result on every path, so `clavesa query` on a nonexistent table
# exited 0 and `dashboards render` didn't flag a broken widget. Fixed via
# QueryQuery.StrictMissing (set on the interactive ad-hoc seam service.Query
# and on RenderDashboard's widget queries); both recipes are back in the
# default and green.
RECIPES="${CLAVESA_COOKBOOK_RECIPES:-multi-stage merge-cdf query-your-data notebooks python-transform http-changing-source dashboards runner-deps s3-bulk-ingest cloudfront-web-analytics backfill}"

# All recipes share ONE workspace named `cookbook` — this is the cookbook's
# own design (one workspace, one taxi dataset, recipes building on each
# other), and it makes each recipe's LITERAL catalog references
# (clavesa_cookbook__taxis, clavesa_cookbook__demo, …) resolve verbatim.
# $WORK is fresh per invocation, so a single-recipe run
# (CLAVESA_COOKBOOK_RECIPES=notebooks) still gets a clean workspace; each
# recipe therefore provisions its own prerequisites via the ensure_* Setup
# helpers below (idempotent — a no-op when an earlier recipe already built
# them in the same run).
WS_NAME="cookbook"
# The taxi HTTP source every non-moto recipe reads (yellow_tripdata_2024-01
# = 2,964,624 rows; deterministic totals). 2024-02 is folded in by merge-cdf.
TAXI_JAN_URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
TAXI_FEB_URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet"
# Deterministic counts for 2024-01.
TAXI_JAN_ROWS=2964624
# moto host binding + endpoints (see header). Port is pickable for
# parallel runs.
MOTO_PORT="${CLAVESA_COOKBOOK_MOTO_PORT:-}"
MOTO_HOST_ENDPOINT=""      # host boto3 seeding + readiness
MOTO_CONTAINER_ENDPOINT="" # forwarded to the runner container

FAILURES=0
SUCCESS=0
WORK=""
MOTO_PID=""
CURRENT_RECIPE="(setup)"
WS=""                # abs path of the shared cookbook workspace (set in main)
WS_INITED=0          # workspace init done once per invocation
UI_PID=""            # headless `clavesa ui` for the dashboards UI assertions
PW_SESSION="cvc-$$"  # playwright-cli session (kept short; daemon handshake caps ~16 chars)

# ---------------------------------------------------------------------------
# Helpers (mirroring verify-readme.sh: pass/fail per assertion, die is fatal)
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
fail() {
  echo "  ✗ $*" >&2
  FAILURES=$((FAILURES + 1))
}
die() {
  echo "✗ FATAL [$CURRENT_RECIPE]: $*" >&2
  exit 1
}

cleanup() {
  # headless UI server (dashboards recipe) + its browser session.
  if [[ -n "$UI_PID" ]] && kill -0 "$UI_PID" 2>/dev/null; then
    kill "$UI_PID" 2>/dev/null || true
    wait "$UI_PID" 2>/dev/null || true
  fi
  if command -v playwright-cli >/dev/null 2>&1; then
    playwright-cli "-s=$PW_SESSION" close >/dev/null 2>&1 || true
  fi
  # moto server.
  if [[ -n "$MOTO_PID" ]] && kill -0 "$MOTO_PID" 2>/dev/null; then
    kill "$MOTO_PID" 2>/dev/null || true
    wait "$MOTO_PID" 2>/dev/null || true
  fi
  # The per-workspace Derby metastore container + docker network outlive
  # the CLI by design; for a throwaway workspace they are orphans. Names
  # are sha256(abs workspace path)[:12] — mirror
  # internal/observability/metastore.go and verify-readme.sh's cleanup.
  if [[ -n "$WORK" ]] && command -v docker >/dev/null; then
    local ws_hash
    ws_hash="$(python3 -c 'import hashlib, sys; print(hashlib.sha256(sys.argv[1].encode()).hexdigest()[:12])' "$WORK/$WS_NAME" 2>/dev/null || true)"
    if [[ -n "$ws_hash" ]]; then
      docker rm -f "clavesa-metastore-$ws_hash" >/dev/null 2>&1 || true
      docker network rm "clavesa-net-$ws_hash" >/dev/null 2>&1 || true
    fi
  fi
  if [[ "$SUCCESS" == 1 ]]; then
    [[ -n "$WORK" ]] && rm -rf "$WORK"
  elif [[ -n "$WORK" ]]; then
    {
      echo ""
      echo "✗ verify-cookbook did not finish green — keeping the workdir for debugging:"
      echo "    workdir:   $WORK"
      echo "    moto log:  $WORK/moto.log"
      echo "    last recipe: $CURRENT_RECIPE"
    } >&2
  fi
}
trap cleanup EXIT

require_tools() {
  for t in jq python3 docker; do
    command -v "$t" >/dev/null || die "required tool not found: $t"
  done
  python3 - <<'PY' || die "python3 must be able to import boto3, moto, pyarrow — pip install 'moto[server]' boto3 pyarrow"
import boto3, moto, pyarrow  # noqa: F401
PY
  [[ -x "$BIN" ]] || die "clavesa binary not found at $BIN — run 'make build' first"
  docker info >/dev/null 2>&1 || die "docker is not running"
}

free_port() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

# clv <args...> — the built binary, always scoped to the throwaway
# workspace and carrying the container-visible S3 endpoint so every
# runner the CLI spawns reaches host moto.
clv() {
  CLAVESA_S3_ENDPOINT="$MOTO_CONTAINER_ENDPOINT" \
    "$BIN" "$@" --workspace "$WS"
}

# ---------------------------------------------------------------------------
# Shared setup: moto + a throwaway local-warehouse workspace
# ---------------------------------------------------------------------------
setup_moto() {
  # Kill a moto server left running by an earlier recipe in this same run
  # (the full default runs both s3-bulk-ingest and backfill) so it doesn't
  # leak past the script and so the port frees up.
  if [[ -n "$MOTO_PID" ]] && kill -0 "$MOTO_PID" 2>/dev/null; then
    kill "$MOTO_PID" 2>/dev/null || true
    wait "$MOTO_PID" 2>/dev/null || true
    MOTO_PID=""
  fi
  # Fresh port per recipe, unless pinned via env for a parallel whole-script
  # run (CLAVESA_COOKBOOK_MOTO_PORT).
  if [[ -n "${CLAVESA_COOKBOOK_MOTO_PORT:-}" ]]; then
    MOTO_PORT="$CLAVESA_COOKBOOK_MOTO_PORT"
  else
    MOTO_PORT="$(free_port)"
  fi
  MOTO_HOST_ENDPOINT="http://127.0.0.1:$MOTO_PORT"
  MOTO_CONTAINER_ENDPOINT="http://host.docker.internal:$MOTO_PORT"

  banner "start moto S3 on 0.0.0.0:$MOTO_PORT"
  python3 -m moto.server -H 0.0.0.0 -p "$MOTO_PORT" >"$WORK/moto.log" 2>&1 &
  MOTO_PID=$!

  # dummy creds — moto accepts any; exported for host boto3 + forwarded
  # to the runner container by runner.AWSEnvDockerArgs.
  export AWS_ACCESS_KEY_ID="testing"
  export AWS_SECRET_ACCESS_KEY="testing"
  export AWS_SESSION_TOKEN="testing"
  export AWS_DEFAULT_REGION="us-east-1"
  export AWS_REGION="us-east-1"
  # The endpoint the runner's boto3 partition-discovery clients need. INERT
  # today (AWSEnvDockerArgs does not forward it — see KNOWN BLOCKER in the
  # header); host boto3 seeding passes endpoint_url explicitly so this has
  # no effect there. Live the moment the forward lands.
  export AWS_ENDPOINT_URL_S3="$MOTO_CONTAINER_ENDPOINT"

  # Readiness = a successful create_bucket via boto3 against the host
  # endpoint (doubles as bucket creation).
  local ready=0 i
  for i in $(seq 1 30); do
    if python3 - "$MOTO_HOST_ENDPOINT" "$BUCKET" <<'PY' 2>>"$WORK/moto.log"; then
import sys, boto3, botocore
ep, bucket = sys.argv[1], sys.argv[2]
s3 = boto3.client("s3", endpoint_url=ep)
try:
    s3.create_bucket(Bucket=bucket)
except botocore.exceptions.ClientError as e:
    if e.response["Error"]["Code"] not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
        raise
PY
      ready=1
      break
    fi
    kill -0 "$MOTO_PID" 2>/dev/null || die "moto exited early — see $WORK/moto.log"
    sleep 1
  done
  [[ "$ready" == 1 ]] || die "moto did not answer on $MOTO_HOST_ENDPOINT within 30s — see $WORK/moto.log"
  pass "moto up; bucket s3://$BUCKET created"
}

# seed_partitioned_parquet — 3 day-partitions of taxi-like parquet under
# s3://<bucket>/seed/y=2026/m=06/d=0{1,2,3}/part.parquet, with a unique
# event_id per row so the transform has a clean merge key. Mirrors the
# cloud-smoke seed shape (parquet, not CSV: the runner's incremental
# partitioned read hardcodes spark.read.parquet, GH #40).
seed_partitioned_parquet() {
  banner "seed s3://$BUCKET/seed/ (y=2026/m=06/d=01..03, parquet)"
  python3 - "$MOTO_HOST_ENDPOINT" "$BUCKET" <<'PY' || die "parquet seed failed"
import io, random, sys
from datetime import datetime, timedelta

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

ep, bucket = sys.argv[1], sys.argv[2]
s3 = boto3.client("s3", endpoint_url=ep)
random.seed(42)
payment_types = ["credit_card", "cash", "dispute", "no_charge"]
days = ["01", "02", "03"]
rows_per_day = 67  # ~200 rows across 3 partitions
event_id = 0
for d in days:
    base = datetime(2026, 6, int(d))
    eid, ts, pay, fare, dist = [], [], [], [], []
    for _ in range(rows_per_day):
        eid.append(event_id)
        event_id += 1
        ts.append(base + timedelta(minutes=random.randint(0, 1439)))
        pay.append(random.choice(payment_types))
        fare.append(round(random.uniform(3.5, 80.0), 2))
        dist.append(round(random.uniform(0.3, 25.0), 2))
    table = pa.table({
        "event_id": pa.array(eid, pa.int64()),
        "pickup_ts": pa.array(ts, pa.timestamp("us")),
        "payment_type": pa.array(pay, pa.string()),
        "fare_amount": pa.array(fare, pa.float64()),
        "trip_distance": pa.array(dist, pa.float64()),
    })
    buf = io.BytesIO()
    pq.write_table(table, buf)
    key = f"seed/y=2026/m=06/d={d}/part.parquet"
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    print(f"  put s3://{bucket}/{key} ({rows_per_day} rows)")
PY
  # Assertion: exactly 3 partition objects landed.
  local n
  n="$(python3 - "$MOTO_HOST_ENDPOINT" "$BUCKET" <<'PY'
import sys, boto3
ep, bucket = sys.argv[1], sys.argv[2]
s3 = boto3.client("s3", endpoint_url=ep)
objs = s3.list_objects_v2(Bucket=bucket, Prefix="seed/").get("Contents", [])
print(len([o for o in objs if o["Key"].endswith("part.parquet")]))
PY
)"
  if [[ "$n" == "3" ]]; then
    pass "seed present: 3 day-partition parquet objects"
  else
    fail "seed: expected 3 partition objects, found $n"
  fi
}

# ---------------------------------------------------------------------------
# Shared Setup helpers (the "Setup (self-contained)" blocks the recipes
# reference). All idempotent: a no-op when an earlier recipe already built
# the artifact in the same $WORK, a full build in a single-recipe run.
# ---------------------------------------------------------------------------

# ensure_workspace — `clavesa workspace init cookbook`, once per invocation.
ensure_workspace() {
  [[ "$WS_INITED" == 1 ]] && return 0
  banner "workspace init $WS_NAME (local warehouse; builds runner image)"
  CLAVESA_S3_ENDPOINT="${MOTO_CONTAINER_ENDPOINT:-}" \
    "$BIN" workspace init "$WS_NAME" --workspace "$WS" || die "workspace init failed"
  WS_INITED=1
  pass "workspace init (local mode)"
}

# ensure_source <name> <url> [format] — register an http source if absent.
ensure_source() {
  local name="$1" url="$2" fmt="${3:-}"
  clv source show "$name" >/dev/null 2>&1 && return 0
  if [[ -n "$fmt" ]]; then
    clv source register "$name" --from "$url" --format "$fmt" || die "source register $name failed"
  else
    clv source register "$name" --from "$url" || die "source register $name failed"
  fi
}

# run_pipeline <pipeline> — `clavesa pipeline run`, capturing output to
# $WORK/run-<pipeline>.log. Dies on a non-zero exit (any node errored);
# `pipeline run` exits non-zero if any transform reports error, so exit 0
# is the "all nodes ok" signal the recipes assert.
run_pipeline() {
  local p="$1"
  banner "pipeline run $p"
  if clv pipeline run "$p" >"$WORK/run-$p.log" 2>&1; then
    pass "pipeline run $p ok (all nodes)"
    return 0
  fi
  echo "---- run-$p.log (tail) ----" >&2
  tail -40 "$WORK/run-$p.log" >&2 || true
  die "pipeline run $p FAILED — see $WORK/run-$p.log"
}

# query_scalar <sql> — first cell of a --json query, shape-agnostic
# (values may be JSON numbers or strings depending on the query path).
query_scalar() {
  clv query "$1" --json 2>/dev/null | jq -r '.rows[0][0] // empty'
}

# assert_count <sql> <expected> <desc> — numeric scalar assertion. Uses
# tonumber so it works whether the JSON encodes the cell as 2964624 or
# "2964624" (the two shapes the cookbook recipes' Verify blocks disagree
# on — see the JSON-shape note in the report).
assert_count() {
  local sql="$1" want="$2" desc="$3" got
  got="$(clv query "$sql" --json 2>"$WORK/q.err" | jq -r '.rows[0][0] | tonumber' 2>/dev/null || true)"
  if [[ "$got" == "$want" ]]; then
    pass "$desc = $got"
  else
    fail "$desc: expected $want, got '${got:-<none>}' (sql: $sql; err: $(head -c 200 "$WORK/q.err" 2>/dev/null))"
  fi
}

# build_taxis — the multi-stage-pipeline.md pipeline (bronze→silver→gold).
# This IS recipe_multi_stage's body; dependent recipes reuse it as Setup.
# Idempotent on the pipeline dir.
build_taxis() {
  ensure_workspace
  ensure_source src_trips "$TAXI_JAN_URL"
  [[ -d "$WS/taxis" ]] && { pass "taxis pipeline already built"; return 0; }

  banner "build taxis: bronze -> silver -> gold (multi-stage-pipeline.md)"
  clv pipeline create taxis || die "pipeline create taxis failed"

  clv node add taxis --type transform --name trips_bronze || die "node add trips_bronze failed"
  clv node edit taxis trips_bronze --set "sql=
    SELECT
      CAST(tpep_pickup_datetime  AS TIMESTAMP) AS pickup_ts,
      CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_ts,
      CAST(passenger_count       AS INT)       AS passenger_count,
      CAST(trip_distance         AS DOUBLE)    AS trip_distance,
      CAST(payment_type          AS INT)       AS payment_type,
      CAST(fare_amount           AS DOUBLE)    AS fare_amount,
      CAST(tip_amount            AS DOUBLE)    AS tip_amount,
      CAST(total_amount          AS DOUBLE)    AS total_amount
    FROM trips" || die "node edit trips_bronze failed"

  clv node add taxis --type transform --name revenue_by_payment || die "node add revenue_by_payment failed"
  clv node edit taxis revenue_by_payment --set "sql=
    SELECT
      payment_type,
      COUNT(*)                                                 AS trips,
      ROUND(SUM(total_amount), 2)                              AS revenue,
      ROUND(AVG(tip_amount / NULLIF(fare_amount, 0)) * 100, 1) AS avg_tip_pct
    FROM trips_bronze
    WHERE pickup_ts IS NOT NULL
    GROUP BY payment_type
    ORDER BY revenue DESC" || die "node edit revenue_by_payment failed"

  clv node add taxis --type transform --name revenue_kpis || die "node add revenue_kpis failed"
  clv node edit taxis revenue_kpis --set "sql=
    SELECT
      SUM(trips)                          AS total_trips,
      ROUND(SUM(revenue), 2)              AS total_revenue,
      ROUND(SUM(revenue) / SUM(trips), 2) AS revenue_per_trip
    FROM revenue_by_payment" || die "node edit revenue_kpis failed"

  clv node connect taxis --from trips_bronze       --to revenue_by_payment --input trips_bronze       || die "connect bronze->silver failed"
  clv node connect taxis --from revenue_by_payment --to revenue_kpis       --input revenue_by_payment || die "connect silver->gold failed"
  clv source attach taxis src_trips --to trips_bronze --as trips || die "source attach taxis failed"

  run_pipeline taxis
}

# build_demo — the query-your-data.md / README demo pipeline (trips +
# revenue_by_payment over src_trips, stats on trips). Idempotent.
build_demo() {
  ensure_workspace
  ensure_source src_trips "$TAXI_JAN_URL"
  [[ -d "$WS/demo" ]] && { pass "demo pipeline already built"; return 0; }

  banner "build demo pipeline (query-your-data.md Setup)"
  clv pipeline create demo || die "pipeline create demo failed"
  clv node add demo --type transform --name trips || die "node add trips failed"
  clv source attach demo src_trips --to trips --as src_trips || die "source attach demo failed"
  clv node edit demo trips --set "sql=SELECT * FROM src_trips" --output-stats || die "node edit trips failed"
  clv node add demo --type transform --name revenue_by_payment || die "node add demo revenue_by_payment failed"
  clv node connect demo --from trips --to revenue_by_payment || die "connect demo edge failed"
  clv node edit demo revenue_by_payment --set "sql=SELECT payment_type, COUNT(*) AS trips, ROUND(SUM(total_amount), 2) AS revenue FROM trips GROUP BY payment_type ORDER BY revenue DESC" || die "node edit demo revenue_by_payment failed"
  run_pipeline demo
}

# ---------------------------------------------------------------------------
# Recipe: multi-stage-pipeline.md
# ---------------------------------------------------------------------------
recipe_multi_stage() {
  CURRENT_RECIPE="multi-stage"
  build_taxis

  banner "recipe multi-stage: assert 3 nodes + gold KPIs"
  # build_taxis already ran the pipeline to exit 0 with all three nodes ok
  # (Verify step 1). The table + count assertions below are the rest.

  # The three tables exist.
  local tables
  tables="$(clv query "SHOW TABLES IN clavesa_cookbook__taxis" --json 2>/dev/null | jq -r '.rows[][]?' || true)"
  for t in trips_bronze revenue_by_payment revenue_kpis; do
    if grep -qw "$t" <<<"$tables"; then
      pass "table $t present in clavesa_cookbook__taxis"
    else
      fail "table $t missing from clavesa_cookbook__taxis (got: $(tr '\n' ' ' <<<"$tables"))"
    fi
  done

  # Bronze carried every row through.
  assert_count "SELECT COUNT(*) FROM clavesa_cookbook__taxis.trips_bronze" \
    "$TAXI_JAN_ROWS" "bronze row count"

  # Gold single-row KPIs: total_trips + total_revenue.
  assert_count "SELECT total_trips FROM clavesa_cookbook__taxis.revenue_kpis" \
    "$TAXI_JAN_ROWS" "gold total_trips"

  local gold_rev
  gold_rev="$(query_scalar "SELECT total_revenue FROM clavesa_cookbook__taxis.revenue_kpis")"
  if [[ "$gold_rev" == "79456384.28" ]]; then
    pass "gold total_revenue = $gold_rev"
  else
    fail "gold total_revenue: expected 79456384.28, got '${gold_rev:-<none>}'"
  fi
}

# ---------------------------------------------------------------------------
# Recipe: merge-cdf.md
# ---------------------------------------------------------------------------
# Keyed upserts (merge on trip_date) + incremental downstream via CDF. Walks
# the recipe's CLI block: build daily_revenue + daily_flagged, run January
# (idempotent at 32 days across re-runs), fold in February (accumulates to
# 60), and assert the February run's daily_flagged output_rows is the
# new-day delta, not the full table.
recipe_merge_cdf() {
  CURRENT_RECIPE="merge-cdf"
  ensure_workspace

  banner "build daily pipeline (merge-cdf.md): src_monthly@Jan, daily_revenue + daily_flagged"
  # src_monthly starts at January; repointed to February below. Register
  # fresh (this recipe owns it) — not via ensure_source, since it's edited.
  clv source show src_monthly >/dev/null 2>&1 \
    || clv source register src_monthly --from "$TAXI_JAN_URL" || die "source register src_monthly failed"

  if [[ ! -d "$WS/daily" ]]; then
    clv pipeline create daily || die "pipeline create daily failed"

    clv node add daily --type transform --name daily_revenue || die "node add daily_revenue failed"
    clv node edit daily daily_revenue --set "sql=
      SELECT
        DATE(CAST(tpep_pickup_datetime AS TIMESTAMP)) AS trip_date,
        COUNT(*)                                       AS trips,
        ROUND(SUM(total_amount), 2)                    AS revenue
      FROM monthly
      WHERE CAST(tpep_pickup_datetime AS TIMESTAMP) >= '2024-01-01'
        AND CAST(tpep_pickup_datetime AS TIMESTAMP) <  '2024-03-01'
      GROUP BY 1" || die "node edit daily_revenue sql failed"
    clv node edit daily daily_revenue --output-merge-keys trip_date || die "daily_revenue merge-keys failed"

    clv node add daily --type transform --name daily_flagged || die "node add daily_flagged failed"
    clv node edit daily daily_flagged --set "sql=
      SELECT trip_date, trips, revenue, revenue > 2000000 AS high_revenue_day
      FROM daily_revenue" || die "node edit daily_flagged sql failed"
    clv node edit daily daily_flagged \
      --output-merge-keys trip_date \
      --incremental-input daily_revenue || die "daily_flagged merge/incremental failed"

    clv node connect daily --from daily_revenue --to daily_flagged --input daily_revenue || die "connect daily edge failed"
    clv source attach daily src_monthly --to daily_revenue --as monthly || die "source attach daily failed"
  else
    pass "daily pipeline already built"
  fi

  # ----- January -----
  run_pipeline daily
  assert_count "SELECT COUNT(*) FROM clavesa_cookbook__daily.daily_revenue" 32 "January daily_revenue days"

  # ----- idempotency: re-run same source, count stays flat -----
  run_pipeline daily
  assert_count "SELECT COUNT(*) FROM clavesa_cookbook__daily.daily_revenue" 32 "daily_revenue days after re-run (idempotent)"

  # ----- fold in February with a one-line source edit -----
  banner "recipe: source edit src_monthly -> February, re-run"
  clv source edit src_monthly --from "$TAXI_FEB_URL" || die "source edit src_monthly failed"
  run_pipeline daily
  assert_count "SELECT COUNT(*) FROM clavesa_cookbook__daily.daily_revenue" 60 "daily_revenue days after February fold-in (accumulated)"

  # ----- incremental downstream: the February run's daily_flagged wrote
  #       only the new-day delta, not the whole table -----
  banner "recipe: assert daily_flagged incremental output_rows (February run)"
  local flagged_rows
  flagged_rows="$(query_scalar "SELECT output_rows FROM clavesa_cookbook_system__pipelines.node_runs WHERE pipeline='daily' AND node='daily_flagged' ORDER BY started_at DESC LIMIT 1")"
  if [[ "$flagged_rows" == "28" ]]; then
    pass "daily_flagged February run output_rows = 28 (incremental, not full 60)"
  else
    # Not the full-table 60 is the real signal; the exact new-day count is
    # what the recipe documents (28). Surface a mismatch for triage.
    fail "daily_flagged February output_rows: expected 28 (merge-cdf.md), got '${flagged_rows:-<none>}' — if this is 29 it is recipe drift (Feb-1 also updates), if 60 the incremental read regressed"
  fi

  # The flagged table still ends up complete at 60 days.
  assert_count "SELECT COUNT(*) FROM clavesa_cookbook__daily.daily_flagged" 60 "daily_flagged total days (complete)"
}

# ---------------------------------------------------------------------------
# Recipe: query-your-data.md
# ---------------------------------------------------------------------------
# Ad-hoc SparkSQL from the terminal. Walks the Verify block: SHOW TABLES,
# a deterministic COUNT, sql lint exit codes (0 good / 1 bad), and a
# missing-table query exiting non-zero.
recipe_query_your_data() {
  CURRENT_RECIPE="query-your-data"
  build_demo

  banner "recipe query-your-data: SHOW TABLES / COUNT / sql lint / error exit"
  local tables
  tables="$(clv query "SHOW TABLES IN clavesa_cookbook__demo" --json 2>/dev/null | jq -r '.rows[][]?' || true)"
  for t in trips revenue_by_payment; do
    if grep -qw "$t" <<<"$tables"; then
      pass "SHOW TABLES lists $t"
    else
      fail "SHOW TABLES missing $t (got: $(tr '\n' ' ' <<<"$tables"))"
    fi
  done

  assert_count "SELECT COUNT(*) AS c FROM clavesa_cookbook__demo.trips" \
    "$TAXI_JAN_ROWS" "clavesa query trips count"

  # sql lint: parse-clean SQL exits 0, unparseable exits non-zero.
  printf 'SELECT 1\n' >"$WORK/ok.sql"
  if clv sql lint "$WORK/ok.sql" >/dev/null 2>&1; then
    pass "sql lint: good SQL exits 0"
  else
    fail "sql lint: good SQL exited non-zero (expected 0)"
  fi
  printf 'SELECT a b c FROM\n' >"$WORK/bad.sql"
  if clv sql lint "$WORK/bad.sql" >/dev/null 2>&1; then
    fail "sql lint: bad SQL exited 0 (expected non-zero)"
  else
    pass "sql lint: bad SQL exits non-zero"
  fi

  # A missing table must error and exit non-zero (the recipe's "safe as a
  # script check" contract). service.Query sets QueryQuery.StrictMissing on
  # the interactive ad-hoc seam, so a query against a nonexistent table now
  # surfaces the error instead of answering empty + exit 0.
  if clv query "SELECT * FROM clavesa_cookbook__demo.nope LIMIT 1" >/dev/null 2>&1; then
    fail "missing-table query exited 0 (expected non-zero) — PRODUCT BUG: local query returns empty + exit 0 on a nonexistent table"
  else
    pass "missing-table query exits non-zero"
  fi
}

# ---------------------------------------------------------------------------
# Recipe: notebooks.md
# ---------------------------------------------------------------------------
# Multi-cell SQL + PySpark scratchpad on the warm Spark session, then
# graduate a cell into a transform. Reads the demo + taxis tables the
# earlier recipes built.
recipe_notebooks() {
  CURRENT_RECIPE="notebooks"
  build_demo
  build_taxis

  banner "recipe notebooks: create + author a 2-cell notebook"
  clv notebook create exploration || die "notebook create failed"
  mkdir -p "$WS/notebooks"
  # Author the two cells on disk (source = list of lines, per the recipe).
  cat >"$WS/notebooks/exploration.ipynb" <<'IPYNB'
{
 "nbformat": 4, "nbformat_minor": 5,
 "metadata": {"kernelspec": {"name": "clavesa-pyspark", "display_name": "Clavesa (PySpark)"},
              "clavesa": {"format_version": 1}},
 "cells": [
  {"cell_type": "code", "id": "c1sql", "metadata": {}, "execution_count": null, "outputs": [],
   "source": ["%%sql\n",
              "SELECT payment_type, COUNT(*) AS n\n",
              "FROM clavesa_cookbook__demo.trips\n",
              "GROUP BY payment_type ORDER BY n DESC"]},
  {"cell_type": "code", "id": "c2py", "metadata": {}, "execution_count": null, "outputs": [],
   "source": ["df = spark.table(\"clavesa_cookbook__taxis.revenue_by_payment\")\n",
              "total = df.selectExpr(\"round(sum(revenue), 2) AS r\").collect()[0][\"r\"]\n",
              "print(f\"total revenue: {total}\")"]}
 ]
}
IPYNB

  # notebook list shows it.
  if clv notebook list 2>/dev/null | grep -qw exploration; then
    pass "notebook list shows exploration"
  else
    fail "notebook list does not show exploration"
  fi

  banner "recipe notebooks: run all cells (--json), assert both ok"
  local nb_json
  nb_json="$(clv notebook run exploration --json 2>"$WORK/nb.err" || true)"
  if jq -e 'length == 2 and all(.[]; .result.status == "ok")' <<<"$nb_json" >/dev/null 2>&1; then
    pass "notebook run: both cells report ok"
  else
    fail "notebook run: not all cells ok — $(head -c 400 <<<"$nb_json"); err: $(head -c 200 "$WORK/nb.err")"
  fi
  # c2py's stdout carries the gold KPI (proves it read the taxis table).
  local c2_out
  c2_out="$(jq -r '.[] | select(.cell_id=="c2py") | .result.stdout // ""' <<<"$nb_json" 2>/dev/null || true)"
  if grep -q "total revenue: 79456384.28" <<<"$c2_out"; then
    pass "notebook c2py stdout = total revenue: 79456384.28"
  else
    fail "notebook c2py stdout mismatch: got '$(head -c 120 <<<"$c2_out")'"
  fi

  banner "recipe notebooks: graduate c1sql into demo/payment_counts"
  clv notebook graduate exploration --cell c1sql --to demo --as payment_counts \
    || die "notebook graduate failed"
  if [[ -f "$WS/demo/transforms/payment_counts.sql" ]]; then
    pass "graduate wrote demo/transforms/payment_counts.sql"
  else
    fail "graduate did not write demo/transforms/payment_counts.sql"
  fi
  if clv node list demo 2>/dev/null | grep -qw payment_counts; then
    pass "node list demo shows payment_counts (graduated node registered)"
  else
    fail "node list demo does not show payment_counts"
  fi
}

# ---------------------------------------------------------------------------
# Recipe: python-transform.md
# ---------------------------------------------------------------------------
# A language=python transform (DataFrame style) ships and runs ok, producing
# the derived columns.
recipe_python_transform() {
  CURRENT_RECIPE="python-transform"
  ensure_workspace
  # The recipe registers a source literally named `trips` (distinct from
  # the demo recipe's src_trips).
  ensure_source trips "$TAXI_JAN_URL"

  if [[ ! -d "$WS/trip_features" ]]; then
    banner "build trip_features (python-transform.md): enrich transform"
    clv pipeline create trip_features || die "pipeline create trip_features failed"
    clv node add trip_features --type transform --name enrich || die "node add enrich failed"
    mkdir -p "$WS/trip_features/transforms"
    cat >"$WS/trip_features/transforms/enrich.py" <<'PY'
from pyspark.sql import DataFrame, functions as F, Window


def transform(spark, inputs: dict[str, DataFrame]) -> dict[str, DataFrame]:
    trips = inputs["trips"]
    global_stats = Window.partitionBy()
    enriched = (
        trips
        .withColumn("fare_z",
            (F.col("fare_amount") - F.mean("fare_amount").over(global_stats))
            / F.stddev("fare_amount").over(global_stats),
        )
        .withColumn("tip_pct",
            F.when(F.col("fare_amount") > 0,
                   F.col("tip_amount") / F.col("fare_amount")).otherwise(None),
        )
        .withColumn("tip_bucket",
            F.when(F.col("tip_pct").isNull(),             F.lit("unknown"))
             .when(F.col("tip_pct") < 0.05,               F.lit("low"))
             .when(F.col("tip_pct") < 0.15,               F.lit("medium"))
             .when(F.col("tip_pct") < 0.25,               F.lit("generous"))
             .otherwise(                                   F.lit("very_generous")),
        )
    )
    return {"default": enriched}
PY
    clv node edit trip_features enrich \
      --set "language=python" \
      --set "python=file(transforms/enrich.py)" || die "node edit enrich failed"
    clv source attach trip_features trips --to enrich --as trips || die "source attach trip_features failed"
  else
    pass "trip_features pipeline already built"
  fi

  run_pipeline trip_features

  # Row count preserved through the python transform.
  assert_count "SELECT COUNT(*) FROM clavesa_cookbook__trip_features.enrich" \
    "$TAXI_JAN_ROWS" "enrich row count (python transform ran)"
  # The derived column exists and is queryable.
  if clv query "SELECT tip_bucket FROM clavesa_cookbook__trip_features.enrich LIMIT 1" >/dev/null 2>&1; then
    pass "enrich exposes the derived tip_bucket column"
  else
    fail "enrich query for tip_bucket failed (derived column missing)"
  fi
}

# ---------------------------------------------------------------------------
# Recipe: http-changing-source.md
# ---------------------------------------------------------------------------
# A moving HTTP feed (Hacker News Algolia API, public/no-auth) folded into a
# merge-keyed dimension (dedup invariant) + an append-keyed snapshot fact
# (keeps history). NETWORK-DEPENDENT (live HN API). Deterministic invariants
# asserted (not exact counts, since the feed moves): stories stays
# deduplicated (count == distinct objectID); the append fact accumulates
# duplicate objectIDs across fetches (count > distinct).
recipe_http_changing_source() {
  CURRENT_RECIPE="http-changing-source"
  ensure_workspace

  banner "build newsfeed (http-changing-source.md): hn source + stories(merge) + snapshot(append)"
  clv source show hn >/dev/null 2>&1 \
    || clv source register hn \
         --from 'https://hn.algolia.com/api/v1/search_by_date?tags=story&hitsPerPage=100' \
         --format json || die "source register hn failed"

  if [[ ! -d "$WS/newsfeed" ]]; then
    clv pipeline create newsfeed || die "pipeline create newsfeed failed"

    clv node add newsfeed --type transform --name stories || die "node add stories failed"
    clv node edit newsfeed stories --set "sql=
      SELECT
        hit.objectID                         AS objectID,
        hit.title                            AS title,
        hit.url                              AS url,
        parse_url(hit.url, 'HOST')           AS domain,
        hit.author                           AS author,
        CAST(hit.points       AS INT)        AS points,
        CAST(hit.num_comments AS INT)        AS num_comments,
        CAST(hit.created_at_i AS TIMESTAMP)  AS created_at
      FROM (SELECT explode(hits) AS hit FROM hn)" || die "node edit stories sql failed"
    clv node edit newsfeed stories --output-merge-keys objectID || die "stories merge-keys failed"
    clv source attach newsfeed hn --to stories --as hn || die "source attach stories failed"

    clv node add newsfeed --type transform --name fact_story_snapshot || die "node add fact failed"
    clv node edit newsfeed fact_story_snapshot --set "sql=
      SELECT
        hit.objectID                          AS objectID,
        CAST(hit.points       AS INT)         AS points,
        CAST(hit.num_comments AS INT)         AS num_comments,
        ROW_NUMBER() OVER (ORDER BY hit.created_at_i DESC) AS feed_rank,
        current_timestamp()                   AS fetched_at
      FROM (SELECT explode(hits) AS hit FROM hn)" || die "node edit fact sql failed"
    clv node edit newsfeed fact_story_snapshot --output-mode append || die "fact append-mode failed"
    clv source attach newsfeed hn --to fact_story_snapshot --as hn || die "source attach fact failed"
  else
    pass "newsfeed pipeline already built"
  fi

  # Two runs so the append fact accumulates duplicate objectIDs across
  # fetches while the merge dimension stays deduplicated.
  run_pipeline newsfeed
  run_pipeline newsfeed

  banner "recipe: assert merge-dedupe (stories) vs append-history (fact)"
  local s_c s_d f_c f_d
  s_c="$(query_scalar "SELECT COUNT(*) FROM clavesa_cookbook__newsfeed.stories")"
  s_d="$(query_scalar "SELECT COUNT(DISTINCT objectID) FROM clavesa_cookbook__newsfeed.stories")"
  if [[ -n "$s_c" && "$s_c" -gt 0 && "$s_c" == "$s_d" ]]; then
    pass "stories deduplicated by merge: count=$s_c == distinct objectID=$s_d"
  else
    fail "stories merge-dedupe invariant broken: count=$s_c distinct=$s_d"
  fi

  f_c="$(query_scalar "SELECT COUNT(*) FROM clavesa_cookbook__newsfeed.fact_story_snapshot")"
  f_d="$(query_scalar "SELECT COUNT(DISTINCT objectID) FROM clavesa_cookbook__newsfeed.fact_story_snapshot")"
  if [[ -n "$f_c" && -n "$f_d" && "$f_c" -gt "$f_d" ]]; then
    pass "fact_story_snapshot keeps history via append: count=$f_c > distinct objectID=$f_d"
  else
    fail "fact append-history invariant broken (expected count > distinct): count=$f_c distinct=$f_d — if the HN feed was frozen between fetches this can tie; network-dependent recipe"
  fi
}

# seed_bulk_parquet <bucket> <prefix> <total_rows> — write <total_rows> rows
# of order-like parquet split across 3 flat (non-partitioned) objects under
# s3://<bucket>/<prefix>. For s3-bulk-ingest (bulk-read a whole prefix).
seed_bulk_parquet() {
  local bucket="$1" prefix="$2" total="$3"
  banner "seed s3://$bucket/$prefix (3 flat parquet objects, $total rows)"
  python3 - "$MOTO_HOST_ENDPOINT" "$bucket" "$prefix" "$total" <<'PY' || die "bulk parquet seed failed"
import io, sys
import boto3
import pyarrow as pa
import pyarrow.parquet as pq

ep, bucket, prefix, total = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
s3 = boto3.client("s3", endpoint_url=ep)
files = 3
per = total // files
oid = 0
for f in range(files):
    n = per if f < files - 1 else total - per * (files - 1)
    ids, amt, region = [], [], []
    for _ in range(n):
        ids.append(oid); oid += 1
        amt.append(round((oid % 500) + 0.5, 2))
        region.append(["us", "eu", "apac"][oid % 3])
    table = pa.table({
        "order_id": pa.array(ids, pa.int64()),
        "amount": pa.array(amt, pa.float64()),
        "region": pa.array(region, pa.string()),
    })
    buf = io.BytesIO(); pq.write_table(table, buf)
    key = f"{prefix}part-{f}.parquet"
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    print(f"  put s3://{bucket}/{key} ({n} rows)")
PY
  pass "bulk seed present ($total rows across 3 objects)"
}

# ---------------------------------------------------------------------------
# Recipe: s3-bulk-ingest.md
# ---------------------------------------------------------------------------
# Read an entire (non-partitioned) S3 prefix into a Delta table in one shot.
# Needs moto (real S3 semantics for the S3A bulk read).
recipe_s3_bulk_ingest() {
  CURRENT_RECIPE="s3-bulk-ingest"
  BUCKET="cookbook-bulk"
  local BULK_ROWS=1200

  setup_moto
  seed_bulk_parquet "$BUCKET" "exports/orders/" "$BULK_ROWS"
  ensure_workspace

  if [[ ! -d "$WS/sales" ]]; then
    banner "build sales (s3-bulk-ingest.md): orders source + orders_raw"
    clv source register orders \
      --from "s3://$BUCKET/exports/orders/" \
      --format parquet || die "source register orders failed"
    clv pipeline create sales || die "pipeline create sales failed"
    clv node add sales --type transform --name orders_raw || die "node add orders_raw failed"
    clv node edit sales orders_raw --set "sql=SELECT * FROM orders" || die "node edit orders_raw failed"
    clv source attach sales orders --to orders_raw --as orders || die "source attach sales failed"
  else
    pass "sales pipeline already built"
  fi

  run_pipeline sales

  # The whole prefix landed as a Delta table.
  assert_count "SELECT COUNT(*) FROM clavesa_cookbook__sales.orders_raw" \
    "$BULK_ROWS" "orders_raw row count (whole prefix bulk-read)"
}

# seed_cloudfront_logs <bucket> — write a deterministic synthetic CloudFront
# standard-log fixture into s3://<bucket>/cloudfront/ as gzipped TSV: two log
# objects, each led by the #Version/#Fields header lines, carrying 10 /t.gif
# beacon hits (4 sessions, 3 visitors, 2 referred) plus one non-beacon request
# to prove the /t.gif stem filter. The cs-uri-query field is DOUBLE URL-encoded
# exactly like a real log (tracker layer + CloudFront layer) so the recipe's
# parse_qs+unquote decode is exercised for real. For cloudfront-web-analytics.
seed_cloudfront_logs() {
  local bucket="$1"
  banner "seed s3://$bucket/cloudfront/ (2 gzipped TSV log objects, 10 beacon hits)"
  python3 - "$MOTO_HOST_ENDPOINT" "$bucket" <<'PY' || die "cloudfront log seed failed"
import gzip, io, sys
from urllib.parse import urlencode, quote
import boto3

ep, bucket = sys.argv[1], sys.argv[2]
s3 = boto3.client("s3", endpoint_url=ep)

# (sid, uid, event, path, ref) — ref "" = direct (no ref param emitted).
rows = [
    ("s1", "u1", "session_start", "/",        "https://news.example.com"),
    ("s1", "u1", "pageview",      "/docs",    ""),
    ("s1", "u1", "pageview",      "/pricing", ""),
    ("s2", "u2", "session_start", "/docs",    ""),
    ("s2", "u2", "pageview",      "/",        ""),
    ("s3", "u3", "session_start", "/",        "https://news.example.com"),
    ("s3", "u3", "pageview",      "/docs",    ""),
    ("s4", "u1", "session_start", "/",        ""),
    ("s4", "u1", "pageview",      "/pricing", ""),
    ("s2", "u2", "pageview",      "/pricing", ""),
]
# Expected (asserted by the recipe): 10 events, 4 sessions, 3 visitors,
# 2 referred sessions, path '/' = 4.

def cf_query(sid, uid, event, path, ref):
    params = {"e": event, "uid": uid, "sid": sid, "p": path, "t": "1717243200000"}
    if ref:
        params["ref"] = ref
    inner = urlencode(params)        # tracker layer (URLSearchParams-style)
    return quote(inner, safe="=&")   # CloudFront layer: re-encode, keep = and &

HEADER = ("#Version: 1.0\n"
          "#Fields: date time x-edge-location sc-bytes c-ip cs-method "
          "cs(Host) cs-uri-stem sc-status cs(Referer) cs(User-Agent) cs-uri-query\n")

def line(stem, query):
    # 12 tab-separated fields = _c0.._c11 (real logs have more; we read these).
    return "\t".join(["2026-06-01", "12:00:00", "HEL50-C1", "35",
                      "203.0.113.10", "GET", "example.com", stem, "200",
                      "-", "Mozilla/5.0", query])

def gzip_object(row_slice, with_noise):
    body = HEADER
    if with_noise:
        body += line("/index.html", "-") + "\n"   # non-beacon: must be filtered
    for r in row_slice:
        body += line("/t.gif", cf_query(*r)) + "\n"
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
        gz.write(body.encode("utf-8"))
    return buf.getvalue()

s3.put_object(Bucket=bucket, Key="cloudfront/E.2026-06-01-12.aaaa.gz",
              Body=gzip_object(rows[:6], True))
s3.put_object(Bucket=bucket, Key="cloudfront/E.2026-06-01-13.bbbb.gz",
              Body=gzip_object(rows[6:], False))
print("  put 2 gzip log objects (10 beacon hits + 1 non-beacon line)")
PY
  local n
  n="$(python3 - "$MOTO_HOST_ENDPOINT" "$bucket" <<'PY'
import sys, boto3
ep, bucket = sys.argv[1], sys.argv[2]
s3 = boto3.client("s3", endpoint_url=ep)
objs = s3.list_objects_v2(Bucket=bucket, Prefix="cloudfront/").get("Contents", [])
print(len([o for o in objs if o["Key"].endswith(".gz")]))
PY
)"
  if [[ "$n" == "2" ]]; then
    pass "seed present: 2 gzipped CloudFront log objects"
  else
    fail "seed: expected 2 log objects, found $n"
  fi
}

# ---------------------------------------------------------------------------
# Recipe: cloudfront-web-analytics.md
# ---------------------------------------------------------------------------
# CloudFront access logs (gzipped TSV) -> parsed /t.gif beacon events -> daily
# rollup. Needs moto (the tsv source reads gzipped log objects over S3A). The
# reader-facing recipe is bring-your-own-bucket, so its deterministic counts
# live only here, asserted against the synthetic log fixture seeded above.
recipe_cloudfront_analytics() {
  CURRENT_RECIPE="cloudfront-web-analytics"
  BUCKET="cookbook-cflogs"

  setup_moto
  seed_cloudfront_logs "$BUCKET"
  ensure_workspace

  if [[ ! -d "$WS/analytics" ]]; then
    banner "build analytics (cloudfront-web-analytics.md): cflogs source + events + daily"
    clv source register cflogs \
      --from "s3://$BUCKET/cloudfront/" \
      --format tsv \
      --read-option header=false \
      --read-option comment=# || die "source register cflogs failed"

    clv pipeline create analytics || die "pipeline create analytics failed"
    clv node add analytics --type transform --name events || die "node add events failed"

    mkdir -p "$WS/analytics/transforms"
    cat >"$WS/analytics/transforms/parse_beacon.py" <<'PY'
"""
parse_beacon — turn CloudFront /t.gif beacon hits into typed event rows.

The runner hands this transform the raw CloudFront access logs as headerless,
tab-separated columns (_c0.._cN — a standard v1.0 log has 30+ fields; we read
only the few we need). We keep the /t.gif requests and pull the analytics
fields out of the query string.

That query string is DOUBLE URL-encoded: the tracker URL-encodes each value,
then CloudFront URL-encodes the whole query field again when it writes the log.
parse_qs peels CloudFront's layer; unquote peels the tracker's.

Positional columns we use (0-indexed) in a CloudFront standard log:
  _c7  cs-uri-stem    (keep == /t.gif)
  _c11 cs-uri-query   (the beacon params — includes the client timestamp t)
"""

from urllib.parse import parse_qs, unquote

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import MapType, StringType


def _parse_qs(query):
    """cs-uri-query -> {beacon field: fully-decoded value}."""
    out = {}
    if not query or query == "-":
        return out
    for key, vals in parse_qs(query, keep_blank_values=True).items():
        if vals:
            out[key] = unquote(vals[0])   # unquote peels the tracker's layer
    return out


def transform(spark, inputs: dict[str, DataFrame]) -> dict[str, DataFrame]:
    parse_udf = F.udf(_parse_qs, MapType(StringType(), StringType()))

    beacons = (
        inputs["logs"]
        .where(F.col("_c7") == "/t.gif")
        .where(F.col("_c11").isNotNull() & (F.col("_c11") != "") & (F.col("_c11") != "-"))
        .withColumn("q", parse_udf(F.col("_c11")))
    )

    events = beacons.select(
        F.col("q")["e"].alias("event"),
        F.col("q")["sid"].alias("session_id"),
        F.col("q")["uid"].alias("visitor_id"),
        F.col("q")["p"].alias("path"),
        F.col("q")["ref"].alias("referrer"),
        # The beacon carries its own millisecond timestamp (t); TRY_CAST so a
        # malformed value becomes NULL instead of failing the read.
        F.expr("timestamp_millis(TRY_CAST(q['t'] AS BIGINT))").alias("event_ts"),
        F.expr("to_date(timestamp_millis(TRY_CAST(q['t'] AS BIGINT)))").alias("day"),
    ).where(F.col("event").isNotNull() & (F.col("event") != ""))

    return {"default": events}
PY

    clv node edit analytics events \
      --set "language=python" \
      --set "python=file(transforms/parse_beacon.py)" || die "node edit events failed"
    clv source attach analytics cflogs --to events --as logs || die "source attach cflogs failed"

    clv node add analytics --type transform --name daily || die "node add daily failed"
    clv node edit analytics daily --set "sql=
      SELECT
        day,
        COUNT(DISTINCT session_id) AS sessions,
        COUNT(DISTINCT visitor_id) AS visitors,
        COUNT(*)                   AS events,
        COUNT(DISTINCT CASE WHEN referrer IS NOT NULL AND referrer <> ''
                            THEN session_id END) AS referred_sessions
      FROM events
      GROUP BY day
      ORDER BY day" || die "node edit daily failed"
    clv node connect analytics --from events --to daily --input events || die "connect events->daily failed"
  else
    pass "analytics pipeline already built"
  fi

  run_pipeline analytics

  # events: one row per /t.gif beacon hit — headers + the non-beacon line dropped.
  assert_count "SELECT COUNT(*) FROM clavesa_cookbook__analytics.events" \
    10 "events row count (10 beacon hits; /t.gif filter applied)"
  # double-decode worked: 4 hits landed on path '/'.
  assert_count "SELECT COUNT(*) FROM clavesa_cookbook__analytics.events WHERE path = '/'" \
    4 "events on path '/' (double URL-decode worked)"
  # daily rollup scalars.
  assert_count "SELECT sessions FROM clavesa_cookbook__analytics.daily" \
    4 "daily distinct sessions"
  assert_count "SELECT visitors FROM clavesa_cookbook__analytics.daily" \
    3 "daily distinct visitors"
  assert_count "SELECT events FROM clavesa_cookbook__analytics.daily" \
    10 "daily total events"
  assert_count "SELECT referred_sessions FROM clavesa_cookbook__analytics.daily" \
    2 "daily referred sessions"
}

# build_daily — the merge-cdf base table daily_revenue (January, keyed on
# trip_date). Dashboards charts it. Idempotent; skips when merge-cdf already
# built the daily pipeline in this run.
build_daily() {
  ensure_workspace
  [[ -d "$WS/daily" ]] && { pass "daily pipeline already built"; return 0; }
  clv source show src_monthly >/dev/null 2>&1 \
    || clv source register src_monthly --from "$TAXI_JAN_URL" || die "source register src_monthly failed"
  banner "build daily pipeline (daily_revenue, January)"
  clv pipeline create daily || die "pipeline create daily failed"
  clv node add daily --type transform --name daily_revenue || die "node add daily_revenue failed"
  clv node edit daily daily_revenue --set "sql=
    SELECT
      DATE(CAST(tpep_pickup_datetime AS TIMESTAMP)) AS trip_date,
      COUNT(*)                                       AS trips,
      ROUND(SUM(total_amount), 2)                    AS revenue
    FROM monthly
    WHERE CAST(tpep_pickup_datetime AS TIMESTAMP) >= '2024-01-01'
      AND CAST(tpep_pickup_datetime AS TIMESTAMP) <  '2024-03-01'
    GROUP BY 1" || die "node edit daily_revenue failed"
  clv node edit daily daily_revenue --output-merge-keys trip_date || die "daily_revenue merge-keys failed"
  clv source attach daily src_monthly --to daily_revenue --as monthly || die "source attach daily failed"
  run_pipeline daily
}

# ---------------------------------------------------------------------------
# UI helpers (dashboards recipe) — mirror verify-readme.sh's playwright-cli
# driving, scoped down to what the dashboards check needs.
# ---------------------------------------------------------------------------
# pw <cmd...> — playwright-cli in this script's session; non-zero on the
# CLI's error/daemon shapes (it exits 0 even on errors).
pw() {
  local out
  out="$(playwright-cli "-s=$PW_SESSION" "$@" 2>&1)" || true
  if grep -qE '^### Error|please run open first|^Error:|Daemon pid=' <<<"$out"; then
    printf '%s\n' "$out" >&2
    return 1
  fi
  printf '%s\n' "$out"
}

# ---------------------------------------------------------------------------
# Recipe: dashboards.md
# ---------------------------------------------------------------------------
# Saved SQL widgets over the taxis + daily tables. CLI half: apply / list /
# render exit 0, and render exits non-zero on a broken widget. UI half
# (guarded on playwright-cli): /dashboards/<slug> renders all widgets with
# zero console errors. Set CLAVESA_COOKBOOK_NO_UI=1 to skip the UI half.
recipe_dashboards() {
  CURRENT_RECIPE="dashboards"
  build_taxis
  build_daily

  banner "recipe dashboards: write spec, apply, list, render"
  local spec="$WORK/taxi-revenue.json"
  cat >"$spec" <<'JSON'
{
  "title": "Taxi Revenue",
  "datasets": [
    {"name": "totals",     "dir": "taxis", "sql": "SELECT total_trips, total_revenue, revenue_per_trip FROM clavesa_cookbook__taxis.revenue_kpis"},
    {"name": "by_payment", "dir": "taxis", "sql": "SELECT CAST(payment_type AS STRING) AS payment_type, revenue FROM clavesa_cookbook__taxis.revenue_by_payment ORDER BY revenue DESC"},
    {"name": "daily",      "dir": "daily", "sql": "SELECT trip_date, revenue FROM clavesa_cookbook__daily.daily_revenue ORDER BY trip_date"}
  ],
  "widgets": [
    {"id": "w_rev",   "type": "big_number", "title": "Total revenue",           "dataset": "totals",     "value_field": "total_revenue", "layout": {"x": 0, "y": 0, "w": 3, "h": 2}},
    {"id": "w_trips", "type": "big_number", "title": "Total trips",             "dataset": "totals",     "value_field": "total_trips",   "layout": {"x": 3, "y": 0, "w": 3, "h": 2}},
    {"id": "w_pay",   "type": "bar",        "title": "Revenue by payment type", "dataset": "by_payment", "x_field": "payment_type", "y_field": "revenue", "layout": {"x": 0, "y": 2, "w": 6, "h": 4}},
    {"id": "w_daily", "type": "line",       "title": "Daily revenue",           "dataset": "daily",      "x_field": "trip_date",    "y_field": "revenue", "layout": {"x": 6, "y": 0, "w": 6, "h": 6}}
  ]
}
JSON

  local apply_out
  apply_out="$(clv dashboards apply "$spec" 2>&1 || true)"
  if grep -q "3 dataset" <<<"$apply_out" && grep -q "4 widget" <<<"$apply_out"; then
    pass "dashboards apply reports 3 datasets / 4 widgets"
  else
    fail "dashboards apply output unexpected: $apply_out"
  fi

  if clv dashboards list 2>/dev/null | grep -qw taxi-revenue; then
    pass "dashboards list shows taxi-revenue"
  else
    fail "dashboards list does not show taxi-revenue"
  fi

  banner "recipe dashboards: render exits 0 (all widgets succeed)"
  if clv dashboards render taxi-revenue >"$WORK/render.log" 2>&1; then
    pass "dashboards render taxi-revenue exits 0"
  else
    echo "---- render.log (tail) ----" >&2; tail -20 "$WORK/render.log" >&2 || true
    fail "dashboards render taxi-revenue exited non-zero (expected 0)"
  fi

  # A dashboard with a broken widget → render exits non-zero. We break the
  # widget with a MISSING COLUMN on a real table: it parses (applies cleanly)
  # but Spark's analyzer rejects it at execution, so RenderDashboard sets
  # w.Error and the command exits non-zero. (A pure SYNTAX error is rejected
  # at APPLY time and never reaches render.) RenderDashboard's widget queries
  # also run with StrictMissing set, so a missing-TABLE widget surfaces the
  # same way rather than rendering an empty chart.
  banner "recipe dashboards: render exits non-zero on a broken widget"
  local badspec="$WORK/broken.json"
  cat >"$badspec" <<'JSON'
{
  "title": "Broken",
  "datasets": [{"name": "bad", "dir": "taxis", "sql": "SELECT no_such_col FROM clavesa_cookbook__taxis.revenue_kpis"}],
  "widgets": [{"id": "wb", "type": "table", "title": "Bad", "dataset": "bad", "layout": {"x": 0, "y": 0, "w": 6, "h": 4}}]
}
JSON
  clv dashboards apply "$badspec" >/dev/null 2>&1 || die "apply broken dashboard failed"
  if clv dashboards render broken >/dev/null 2>&1; then
    fail "dashboards render broken exited 0 (expected non-zero on a failing widget)"
  else
    pass "dashboards render broken exits non-zero (widget error surfaced)"
  fi
  clv dashboards delete broken >/dev/null 2>&1 || true

  # ----- UI half (guarded) -----
  if [[ "${CLAVESA_COOKBOOK_NO_UI:-0}" == 1 ]] || ! command -v playwright-cli >/dev/null 2>&1; then
    echo "  (UI half skipped: playwright-cli unavailable or CLAVESA_COOKBOOK_NO_UI=1 — CLI half asserted above)"
    return 0
  fi

  banner "recipe dashboards: UI /dashboards/taxi-revenue renders all widgets (0 console errors)"
  local uiport base
  uiport="$(free_port)"
  base="http://localhost:$uiport"
  CLAVESA_ADDR=":$uiport" CLAVESA_S3_ENDPOINT="${MOTO_CONTAINER_ENDPOINT:-}" \
    "$BIN" ui --no-browser --workspace "$WS" >"$WORK/ui.log" 2>&1 &
  UI_PID=$!
  local ready=0 i
  for i in $(seq 1 30); do
    if curl -sf -o /dev/null "$base/"; then ready=1; break; fi
    kill -0 "$UI_PID" 2>/dev/null || { fail "UI server exited early — see $WORK/ui.log"; return 0; }
    sleep 1
  done
  if [[ "$ready" != 1 ]]; then
    fail "UI server did not answer on $base within 30s — see $WORK/ui.log"
    return 0
  fi

  pw open "$base/" >/dev/null || { fail "playwright-cli open failed"; return 0; }
  pw goto "$base/dashboards/taxi-revenue" >/dev/null || { fail "playwright-cli goto failed"; return 0; }
  # Give the widgets (warm Spark render) time; poll the snapshot for the four
  # widget titles.
  local snap="" deadline
  deadline=$(( $(date +%s) + 300 ))
  while :; do
    snap="$(pw snapshot 2>/dev/null || true)"
    if grep -q "Total revenue" <<<"$snap" && grep -q "Total trips" <<<"$snap" \
       && grep -q "Revenue by payment type" <<<"$snap" && grep -q "Daily revenue" <<<"$snap"; then
      break
    fi
    if (( $(date +%s) > deadline )); then break; fi
    sleep 5
  done
  for w in "Total revenue" "Total trips" "Revenue by payment type" "Daily revenue"; do
    if grep -q "$w" <<<"$snap"; then
      pass "dashboard widget renders: $w"
    else
      fail "dashboard widget missing from /dashboards/taxi-revenue: $w"
    fi
  done
  # Console errors.
  local cerr errs
  cerr="$(pw console error 2>/dev/null || true)"
  errs="$(sed -n 's/.*(Errors: \([0-9][0-9]*\).*/\1/p' <<<"$cerr" | tail -1)"
  if [[ "$errs" == "0" ]]; then
    pass "dashboards UI: 0 console errors"
  else
    fail "dashboards UI: ${errs:-unparsed} console error(s) — $cerr"
  fi

  pw close >/dev/null 2>&1 || true
  kill "$UI_PID" 2>/dev/null || true
  wait "$UI_PID" 2>/dev/null || true
  UI_PID=""
}

# ---------------------------------------------------------------------------
# Recipe: runner-deps.md
# ---------------------------------------------------------------------------
# Add a third-party pip package (humanize) to the runner image and use it in
# a UDF. HEAVY: the first run after `runner requirements add` rebuilds the
# runner image (new pip layer). Reuses the demo pipeline's trips table.
recipe_runner_deps() {
  CURRENT_RECIPE="runner-deps"
  build_demo

  banner "recipe runner-deps: add humanize to the runner requirements"
  clv runner requirements add humanize || die "runner requirements add failed"
  if clv runner requirements list 2>/dev/null | grep -qw humanize; then
    pass "runner requirements list shows humanize"
  else
    fail "runner requirements list does not show humanize"
  fi
  if [[ -f "$WS/.clavesa/runner-requirements.txt" ]] && grep -qw humanize "$WS/.clavesa/runner-requirements.txt"; then
    pass ".clavesa/runner-requirements.txt tracks humanize"
  else
    fail ".clavesa/runner-requirements.txt missing or does not track humanize"
  fi

  banner "recipe runner-deps: python transform importing humanize"
  mkdir -p "$WS/demo/transforms"
  cat >"$WS/demo/transforms/trip_summary.py" <<'PY'
import humanize
from pyspark.sql import DataFrame, functions as F, types as T


def transform(spark, inputs) -> dict[str, DataFrame]:
    trips = inputs["trips"]

    @F.udf(returnType=T.StringType())
    def humanize_miles(d):
        if d is None:
            return None
        return humanize.intcomma(round(float(d), 1)) + " mi"

    out = (
        trips.select("trip_distance", "total_amount")
        .where(F.col("trip_distance").isNotNull())
        .withColumn("distance_human", humanize_miles(F.col("trip_distance")))
        .limit(1000)
    )
    return {"default": out}
PY
  if [[ ! -f "$WS/demo/main.tf" ]] || ! grep -q "trip_summary" "$WS/demo/main.tf" 2>/dev/null; then
    clv node add demo --type transform --name trip_summary || die "node add trip_summary failed"
    clv node connect demo --from trips --to trip_summary || die "connect trip_summary failed"
    clv node edit demo trip_summary \
      --set "language=python" \
      --set "python=file(transforms/trip_summary.py)" || die "node edit trip_summary failed"
  fi

  # This run rebuilds the runner image with the humanize layer, then runs.
  run_pipeline demo

  # The output carries the humanized column (proves the import resolved).
  if clv query "SELECT distance_human FROM clavesa_cookbook__demo.trip_summary WHERE distance_human LIKE '% mi' LIMIT 1" --json 2>/dev/null | jq -e '.rows | length > 0' >/dev/null 2>&1; then
    pass "trip_summary output has the humanized distance_human column (humanize import worked)"
  else
    fail "trip_summary distance_human column missing/empty — humanize import may have failed"
  fi
}

# ---------------------------------------------------------------------------
# Recipe: backfill.md
# ---------------------------------------------------------------------------
# Walks the CLI block of docs/cookbook/backfill.md literally. The recipe
# assumes "a pipeline that's running incrementally" already exists, so —
# like verify-readme reproduces the README's own workspace — this builds a
# representative one first (partitioned s3 source, merge-mode transform,
# one incremental run consuming the later partitions), then runs the
# recipe's stage → list → diff block against the unconsumed partition.
recipe_backfill() {
  CURRENT_RECIPE="backfill"
  local PIPELINE="stream"
  local NODE="trips"
  local SRC="trips_raw"
  WS="$WORK/$WS_NAME"
  local PDIR="$WS/$PIPELINE"

  BUCKET="cookbook-backfill"

  # ----- shared setup -----
  setup_moto
  seed_partitioned_parquet

  # Guarded init (idempotent): `workspace init` ERRORS on an existing
  # workspace, so in the full run (where an earlier recipe already inited
  # the shared cookbook workspace) a raw init would fail — ensure_workspace
  # no-ops instead.
  ensure_workspace

  # ----- source: partitioned s3, start-from day 2 (day 1 stays unconsumed
  #       so the backfill leg has a clean window — GH #36) -----
  banner "source register $SRC (s3, parquet, partitions y/m/d, start-from day 2)"
  clv source register "$SRC" \
    --from "s3://$BUCKET/seed/" \
    --format parquet \
    --partitions y,m,d \
    --start-from "2026/06/02" || die "source register failed"
  if clv source show "$SRC" 2>/dev/null | grep -q "s3"; then
    pass "source $SRC registered (kind=s3, partitioned)"
  else
    fail "source $SRC not registered as expected"
  fi

  # ----- pipeline + a single merge-mode transform -----
  banner "pipeline create $PIPELINE + merge-mode transform $NODE"
  clv pipeline create "$PIPELINE" || die "pipeline create failed"
  clv node add "$PDIR" --type transform --name "$NODE" || die "node add failed"
  clv source attach "$PDIR" "$SRC" --to "$NODE" --as raw_trips || die "source attach failed"
  # merge_keys=event_id sets mode=merge implicitly — the recipe's
  # recommended-and-safe shape.
  clv node edit "$PDIR" "$NODE" \
    --set 'sql=SELECT event_id, pickup_ts, payment_type, fare_amount, trip_distance FROM raw_trips' \
    --output-merge-keys event_id || die "node edit failed"
  pass "pipeline $PIPELINE authored (merge on event_id)"

  # ----- one incremental run: consumes day 2 + day 3, leaves day 1 -----
  banner "pipeline run $PIPELINE (consumes partitions >= start-from)"
  if clv pipeline run "$PIPELINE" >"$WORK/run.log" 2>&1; then
    pass "pipeline run ok"
  else
    # Diagnose the KNOWN BLOCKER (see header): the runner's boto3 partition
    # listing ignores the S3 endpoint and hits real AWS. The signature lands
    # in the per-node progress channel (the CLI stderr only carries the
    # truncated Spark banner), so scan the warehouse _progress JSONs too.
    if grep -qirE 'InvalidAccessKeyId|does not exist in our records|ListObjectsV2' \
         "$WORK/run.log" "$WS/.clavesa/warehouse/_progress" 2>/dev/null; then
      die "pipeline run FAILED reading the partitioned s3 source — the runner's boto3 partition-discovery client ignores CLAVESA_S3_ENDPOINT and hit REAL AWS (InvalidAccessKeyId). This is the KNOWN BLOCKER documented in the header: AWSEnvDockerArgs must forward AWS_ENDPOINT_URL_S3 (verified to work in the runner image) before this recipe can reach the GH #68 list/diff rot. Not fixed here (no product changes). Full log: $WORK/run.log"
    fi
    die "pipeline run FAILED — backfill can't be exercised without a producing pipeline; see $WORK/run.log and $WORK/moto.log"
  fi

  # =========================================================================
  # The recipe's CLI block (docs/cookbook/backfill.md), literally.
  # =========================================================================

  # --- 1. stage the unconsumed day-1 window. Must go GREEN. ---
  banner "recipe: backfill stage $PIPELINE --node $NODE --from 2026/06/01 --to 2026/06/01"
  local stage_json run_id status
  stage_json="$(clv pipeline backfill stage "$PIPELINE" \
    --node "$NODE" \
    --from 2026/06/01 \
    --to 2026/06/01 \
    --json 2>"$WORK/stage.err" || true)"
  run_id="$(jq -r '.run_id // empty' <<<"$stage_json" 2>/dev/null || true)"
  status="$(jq -r '.status // empty' <<<"$stage_json" 2>/dev/null || true)"
  if [[ -z "$run_id" ]]; then
    die "backfill stage produced no run_id — stage itself failed (real product bug vs harness misconfig?): $(cat "$WORK/stage.err" 2>/dev/null; echo "$stage_json")"
  fi
  if [[ "$status" == "ok" ]]; then
    pass "backfill stage ok — run_id=$run_id, staging=$(jq -r '.target_table // "?"' <<<"$stage_json")"
  else
    die "backfill stage returned status=$status (expected ok): $stage_json ; $(cat "$WORK/stage.err" 2>/dev/null)"
  fi

  # --- 2. list. RED on the current tree (GH #68): the staging table is
  #        written to the nested Delta layout, but listLocalStagingTables
  #        scans the retired <db>.db/ dir → the run does not appear. ---
  banner "recipe: backfill list $PIPELINE  (expect the staged run to appear)"
  local list_json
  list_json="$(clv pipeline backfill list "$PIPELINE" --json 2>"$WORK/list.err" || true)"
  if jq -e --arg r "$run_id" 'any(.[]?; .run_id == $r)' <<<"$list_json" >/dev/null 2>&1; then
    pass "backfill list shows the staged run $run_id"
  else
    fail "backfill list does NOT show the staged run $run_id (GH #68: local list scans the retired <db>.db/ layout) — got: $(head -c 300 <<<"$list_json"); err: $(cat "$WORK/list.err" 2>/dev/null)"
  fi

  # --- 3. diff. RED on the current tree (GH #68): 2-part local staging id
  #        vs localTableDir's 3-part demand + Iceberg-metadata reader on a
  #        Delta table → "malformed staging table id" / run not found. ---
  banner "recipe: backfill diff $PIPELINE $run_id  (expect a clean diff)"
  local diff_json diff_rc
  diff_json="$(clv pipeline backfill diff "$PIPELINE" "$run_id" --json 2>"$WORK/diff.err")" && diff_rc=0 || diff_rc=$?
  if [[ "$diff_rc" == 0 ]] && [[ "$(jq -r '.staging_rows // -1' <<<"$diff_json" 2>/dev/null || echo -1)" -gt 0 ]]; then
    pass "backfill diff clean — staging_rows=$(jq -r '.staging_rows' <<<"$diff_json")"
  else
    fail "backfill diff FAILED (GH #68) — rc=$diff_rc; err: $(cat "$WORK/diff.err" 2>/dev/null); out: $(head -c 300 <<<"$diff_json")"
  fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
require_tools
WORK="$(mktemp -d /tmp/clavesa-verify-cookbook.XXXX)"
# Snapshot the binary into the workdir (a parallel build must not yank it
# out mid-run) and run THAT copy.
cp "$BIN" "$WORK/clavesa"
BIN="$WORK/clavesa"
# Sandbox the active-workspace pointer.
export XDG_CONFIG_HOME="$WORK/xdg-config"
mkdir -p "$XDG_CONFIG_HOME"

# The one shared cookbook workspace (see WS_NAME rationale above).
WS="$WORK/$WS_NAME"

# Per-recipe heartbeat to the release-gates progress log (GH #84 follow-up):
# `tail -f .gates/progress.log` shows "recipe N/M: <name>" during the long
# run. No-op when run standalone (CLAVESA_PROGRESS_LOG unset).
_total_recipes=$(wc -w <<<"$RECIPES" | tr -d ' ')
_recipe_i=0
for r in $RECIPES; do
  _recipe_i=$((_recipe_i + 1))
  if [[ -n "${CLAVESA_PROGRESS_LOG:-}" ]]; then
    echo "$(date -u +%H:%M:%S)  verify-cookbook: recipe $_recipe_i/$_total_recipes: $r" >>"$CLAVESA_PROGRESS_LOG"
  fi
  case "$r" in
    multi-stage)        recipe_multi_stage ;;
    merge-cdf)          recipe_merge_cdf ;;
    query-your-data)    recipe_query_your_data ;;
    notebooks)          recipe_notebooks ;;
    python-transform)   recipe_python_transform ;;
    http-changing-source) recipe_http_changing_source ;;
    s3-bulk-ingest)     recipe_s3_bulk_ingest ;;
    cloudfront-web-analytics) recipe_cloudfront_analytics ;;
    dashboards)         recipe_dashboards ;;
    runner-deps)        recipe_runner_deps ;;
    backfill)           recipe_backfill ;;
    *) die "unknown recipe: $r" ;;
  esac
done

banner "summary"
if [[ "$FAILURES" -gt 0 ]]; then
  echo "✗ cookbook verification FAILED: $FAILURES assertion(s) — see ✗ lines above" >&2
  exit 1
fi
SUCCESS=1
echo "✓ cookbook verification GREEN (recipes: $RECIPES)"
