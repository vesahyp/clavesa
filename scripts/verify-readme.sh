#!/usr/bin/env bash
# verify-readme.sh — deterministic README quick-start verification gate.
#
# Walks the README "Local-only" quick start literally — same workspace /
# pipeline / node / source names, same UI flow (authoring happens in the
# browser via playwright-cli, exactly where the README says to click) —
# then asserts the mandatory pages and claims from CLAUDE.md ("Mandatory
# pages/claims per quick-start run"), each followed by a console-error
# check. PASS (✓) / FAIL (✗) per assertion; non-zero exit on any failure.
#
# Invoke via `make verify-readme` (never directly): the make target
# depends on `build`, which is what guarantees the binary under test —
# and the UI embedded in it — is the current tree, not a stale build.
#
# Deviations from the literal README, each forced by unattended
# automation and kept minimal:
#   - `ui --no-browser`           no browser window can open headlessly;
#                                 playwright-cli drives its own browser.
#   - workspace dir from mktemp   the README's fixed /tmp/clavesa-demo
#                                 would collide across concurrent runs.
#   - XDG_CONFIG_HOME redirect    `workspace init` records the active
#                                 workspace in ~/.config/clavesa/ — the
#                                 redirect keeps the gate from clobbering
#                                 the user's real pointer while still
#                                 exercising the state-file path
#                                 (`ui` without --workspace reads it).
#   - readiness poll on the UI    a backgrounded server needs a ready
#                                 gate before the browser drives it.
#
# UI port: the README's default :8080. The script fails loudly if the
# port is occupied — it does NOT silently pick another one (the README
# claim under test includes the default URL). For parallel use set
# CLAVESA_VERIFY_ADDR (e.g. CLAVESA_VERIFY_ADDR=:8765), which is passed
# to the server via CLAVESA_ADDR.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$REPO_ROOT/bin/clavesa"

# ---------------------------------------------------------------------------
# Config — names are the README's, verbatim. The catalog/schema constants
# derive from them per ADR-016 (workspace demo-ws → catalog clavesa_demo_ws;
# pipeline demo → schema demo). The README's own dashboard SQL names
# `clavesa_demo_ws__demo.revenue_by_payment`, which is this same pair.
# ---------------------------------------------------------------------------
ADDR="${CLAVESA_VERIFY_ADDR:-:8080}"
PORT="${ADDR##*:}"
BASE="http://localhost:$PORT"
WS_NAME="demo-ws"
PIPELINE="demo"
CATALOG="clavesa_demo_ws"
SCHEMA="demo"
SRC_NAME="src_trips"
SRC_URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
NODE1="trips"
NODE2="revenue_by_payment"
SQL1="SELECT * FROM src_trips"
SQL2="SELECT
  payment_type,
  COUNT(*) AS trips,
  ROUND(SUM(total_amount), 2) AS revenue,
  ROUND(AVG(tip_amount / NULLIF(fare_amount, 0)) * 100, 1) AS avg_tip_pct
FROM trips
GROUP BY payment_type
ORDER BY revenue DESC"

RUNS_WANTED=3
# Run 1 gets 35 minutes: GH #47 — the first run in a fresh workspace can
# stall ~26 min on the untimed HTTP source download before recovering and
# reporting ok.
RUN1_TIMEOUT_S=2100
RUNN_TIMEOUT_S=600

# Keep the session name short — playwright-cli's daemon handshake breaks
# on names longer than ~16 chars (observed with 0.1.13).
PW_SESSION="cvr-$$"

FAILURES=0
UI_PID=""
WORK=""
SUCCESS=0
CURRENT_PAGE="(setup)"
SNAP_DUMP_N=0

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
  echo "✗ FATAL [$CURRENT_PAGE]: $*" >&2
  exit 1
}

cleanup() {
  if [[ -n "$UI_PID" ]] && kill -0 "$UI_PID" 2>/dev/null; then
    kill "$UI_PID" 2>/dev/null || true
    wait "$UI_PID" 2>/dev/null || true
  fi
  # Close only this script's browser session — never close-all, the user
  # may have their own playwright-cli sessions open.
  playwright-cli "-s=$PW_SESSION" close >/dev/null 2>&1 || true
  # The per-workspace Derby metastore container (and its docker network)
  # outlive the UI server by design; for a throwaway workspace they are
  # orphans. Names are sha256(abs workspace path)[:12] — mirror
  # internal/observability/metastore.go.
  if [[ -n "$WORK" ]] && command -v docker >/dev/null; then
    local ws_hash
    ws_hash="$(python3 -c 'import hashlib, sys; print(hashlib.sha256(sys.argv[1].encode()).hexdigest()[:12])' "$WORK/clavesa-demo" 2>/dev/null || true)"
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
      echo "✗ verify-readme did not finish green — keeping the workdir for debugging:"
      echo "    workdir:   $WORK"
      echo "    ui log:    $WORK/ui.log"
      echo "    last page: $CURRENT_PAGE"
      [[ "$SNAP_DUMP_N" -gt 0 ]] && echo "    snapshots: $WORK/snapshot-fail-*.yml"
    } >&2
  fi
}
trap cleanup EXIT

require_tools() {
  # docker: `workspace init` builds the runner image; runs execute in it.
  for t in jq curl python3 docker playwright-cli; do
    command -v "$t" >/dev/null || die "required tool not found: $t"
  done
  [[ -x "$BIN" ]] || die "clavesa binary not found at $BIN — run 'make build' first"
}

port_free() {
  python3 - "$1" <<'PY'
import socket, sys
s = socket.socket()
try:
    s.bind(("127.0.0.1", int(sys.argv[1])))
except OSError:
    sys.exit(1)
finally:
    s.close()
PY
}

# pw <cmd...> — playwright-cli in this script's named session. The CLI
# exits 0 even on errors, so failure is detected from its output: the
# "### Error" marker, plus the daemon/session failure shapes that print
# without it. Output is echoed for callers that parse it.
pw() {
  local out
  out="$(playwright-cli "-s=$PW_SESSION" "$@" 2>&1)" || true
  if grep -qE '^### Error|please run open first|^Error:|Daemon pid=' <<<"$out"; then
    printf '%s\n' "$out" >&2
    return 1
  fi
  printf '%s\n' "$out"
}

# ui_do <description> <pw args...> — a click/fill/check that must succeed
# for the walkthrough to continue.
ui_do() {
  local desc="$1"
  shift
  pw "$@" >/dev/null || die "$desc — playwright-cli $* failed"
}

snapshot() { pw snapshot; }

dump_snapshot() {
  SNAP_DUMP_N=$((SNAP_DUMP_N + 1))
  printf '%s\n' "$1" >"$WORK/snapshot-fail-$SNAP_DUMP_N.yml"
  echo "$WORK/snapshot-fail-$SNAP_DUMP_N.yml"
}

# wait_text <ERE> <timeout_s> <description> [poll_interval_s]
# Polls the page accessibility snapshot until the regex matches. Returns
# non-zero on timeout (after dumping the last snapshot for debugging).
wait_text() {
  local re="$1" timeout="$2" desc="$3" interval="${4:-2}"
  local deadline snap
  deadline=$(( $(date +%s) + timeout ))
  while :; do
    snap="$(snapshot || true)"
    if grep -Eq "$re" <<<"$snap"; then
      return 0
    fi
    if (( $(date +%s) > deadline )); then
      echo "  (timeout ${timeout}s waiting for /$re/ — $desc; snapshot: $(dump_snapshot "$snap"))" >&2
      return 1
    fi
    sleep "$interval"
  done
}

# expect_text — wait_text that aborts the walkthrough on timeout (used
# while authoring, where continuing would compound the failure).
expect_text() {
  wait_text "$@" || die "expected page text never appeared: $3"
}

# check_text <ERE> <timeout_s> <description> [poll_interval_s] — soft
# assertion variant for the post-run verification pages.
check_text() {
  if wait_text "$@"; then
    pass "$3"
  else
    fail "$3 — /$1/ not found on $CURRENT_PAGE"
  fi
}

# check_testid <testid> <timeout_s> <description> [poll_interval_s] —
# presence assertion against the data-testid contract. Some contract
# elements (e.g. the ADR-024 engine badge) have no stable display text,
# so snapshot grepping is the wrong tool — probe the DOM directly.
check_testid() {
  local id="$1" timeout="$2" desc="$3" interval="${4:-2}"
  local deadline out
  deadline=$(( $(date +%s) + timeout ))
  while :; do
    out="$(pw eval "() => !!document.querySelector('[data-testid=\"$id\"]')" || true)"
    if grep -qw "true" <<<"$out"; then
      pass "$desc"
      return 0
    fi
    if (( $(date +%s) > deadline )); then
      fail "$desc — [data-testid=$id] not found on $CURRENT_PAGE"
      return 0
    fi
    sleep "$interval"
  done
}

# check_eval <js-fn> <timeout_s> <description> [poll_interval_s] — state
# assertion against the data-testid / data-* contract. Polls a
# playwright-cli eval of an `() => boolean` function until it prints
# true. Use for assertions about state (status color, point counts)
# rather than mere presence — the data-* attributes carry the value.
check_eval() {
  local fn="$1" timeout="$2" desc="$3" interval="${4:-2}"
  local deadline out
  deadline=$(( $(date +%s) + timeout ))
  while :; do
    out="$(pw eval "$fn" || true)"
    if grep -qw "true" <<<"$out"; then
      pass "$desc"
      return 0
    fi
    if (( $(date +%s) > deadline )); then
      fail "$desc — eval never returned true on $CURRENT_PAGE (last output: $out)"
      return 0
    fi
    sleep "$interval"
  done
}

# check_absent <ERE> <description> — asserts against the current snapshot
# (call only after the page has demonstrably rendered).
check_absent() {
  local re="$1" desc="$2" snap
  snap="$(snapshot || true)"
  if grep -Eq "$re" <<<"$snap"; then
    fail "$desc — /$re/ unexpectedly present on $CURRENT_PAGE (snapshot: $(dump_snapshot "$snap"))"
  else
    pass "$desc"
  fi
}

# check_console <page description> — "playwright-cli console error" must
# report 0 errors. A separate --clear call afterwards scopes the next
# page's check to its own messages (with --clear the CLI suppresses the
# summary line this parses).
check_console() {
  local desc="$1" out errs
  out="$(pw console error || true)"
  pw console error --clear >/dev/null 2>&1 || true
  errs="$(sed -n 's/.*(Errors: \([0-9][0-9]*\).*/\1/p' <<<"$out" | tail -1)"
  if [[ -z "$errs" ]]; then
    fail "console check on $desc — could not parse playwright-cli console output: $out"
  elif [[ "$errs" == "0" ]]; then
    pass "console: 0 errors on $desc"
  else
    fail "console: $errs error(s) on $desc — $out"
  fi
}

goto_page() {
  CURRENT_PAGE="$1"
  ui_do "navigate to $1" goto "$BASE$1"
}

runs_json() { curl -sf "$BASE/api/data/runs?pipeline=$PIPELINE&limit=20" || echo '{"rows":[]}'; }
node_runs_json() { curl -sf "$BASE/api/data/node-runs?pipeline=$PIPELINE&limit=100" || echo '{"rows":[]}'; }

# wait_runs_terminal <n> <timeout_s> — block until the runs API shows >= n
# SUCCEEDED rows. Waiting on the API (not the UI) is what guards the 409:
# clicking Run while a run is in flight logs a console error that would
# false-fail the gate.
#
# FAILED is NOT trusted on first sight: per GH #46, a healthy run is shown
# FAILED whenever a long pre-bundle phase (first-run runner-image build,
# slow source download) exceeds the 60s state-file orphan threshold, and
# flips back to RUNNING once the bundle starts writing. Only a FAILED that
# persists across FAIL_STABLE_S of polling is treated as real.
FAIL_STABLE_S=120
wait_runs_terminal() {
  local want="$1" timeout="$2"
  local deadline json nsucc nfail now fail_since=0 i=0
  deadline=$(( $(date +%s) + timeout ))
  while :; do
    json="$(runs_json)"
    nsucc="$(jq '[.rows[]? | select(.status == "SUCCEEDED")] | length' <<<"$json")"
    if [[ "$nsucc" -ge "$want" ]]; then return 0; fi
    now=$(date +%s)
    nfail="$(jq '[.rows[]? | select(.status == "FAILED")] | length' <<<"$json")"
    if [[ "$nfail" -gt 0 ]]; then
      if (( fail_since == 0 )); then
        fail_since=$now
        echo "  (runs API shows FAILED — waiting ${FAIL_STABLE_S}s for a GH #46 flip-back before trusting it)"
      elif (( now - fail_since > FAIL_STABLE_S )); then
        die "pipeline run FAILED (stable > ${FAIL_STABLE_S}s, not a GH #46 transient): $(jq -c '[.rows[]? | select(.status == "FAILED")]' <<<"$json")"
      fi
    else
      fail_since=0
    fi
    if (( now > deadline )); then
      die "timed out (${timeout}s) waiting for run #$want to SUCCEED; observed statuses: $(jq -c '[.rows[]?.status]' <<<"$json")"
    fi
    sleep 10
    # Keep the browser session warm across the long Spark run (run 1 can
    # take ~26+ min, GH #47) so the next UI step doesn't hit a dead session.
    i=$((i + 1))
    if (( i % 12 == 0 )); then
      pw console error >/dev/null 2>&1 || true
    fi
  done
}

# wait_run_recorded <run_id> <timeout_s> — block until the run's node_runs
# rows are queryable from the Delta table (the arn-filtered query goes
# through Spark, not the state.json fast path). This IS mandatory
# assertion 1 — one status=ok node_runs row per transform for this exact
# run. (It also used to be the GH #48 guard against clicking Run while the
# previous run's lock outlived its rollup; the lock now releases at
# terminal, so that job is gone.)
wait_run_recorded() {
  local run_id="$1" timeout="$2"
  local deadline json nok
  deadline=$(( $(date +%s) + timeout ))
  while :; do
    # dir= is required: the arn-filtered (Delta/Spark) path needs the
    # pipeline directory to locate the local warehouse.
    json="$(curl -s "$BASE/api/data/node-runs?pipeline=$PIPELINE&arn=$run_id&limit=10&dir=$PIPELINE" || echo '{"rows":[]}')"
    nok="$(jq '[.rows[]? | select(.status == "ok")] | length' <<<"$json" 2>/dev/null || echo 0)"
    if [[ "$nok" -ge 2 ]]; then break; fi
    if (( $(date +%s) > deadline )); then
      die "timed out (${timeout}s) waiting for run $run_id's node_runs rows in Delta; last response: $(head -c 400 <<<"$json")"
    fi
    sleep 10
  done
  for node in "$NODE1" "$NODE2"; do
    if jq -e --arg n "$node" '.rows[] | select(.node == $n and .status == "ok")' <<<"$json" >/dev/null; then
      pass "node_runs: $node status=ok for run $run_id"
    else
      fail "node_runs: no status=ok row for $node in run $run_id — $(jq -c '[.rows[]? | {node, status}]' <<<"$json")"
    fi
  done
}

# dispatch_run <n> — click Run pipeline until run #n actually exists
# server-side. The GH #48 lock-outlives-the-run window is fixed (the run
# lock releases the moment the previous run is terminal), so the first
# click should dispatch; the retry loop stays as the same defensive move a
# human makes on a 409 toast (a genuinely still-running pipeline, a slow
# dispatch). Each rejected click logs a browser console error; the console
# is cleared after a successful dispatch so later per-page checks aren't
# polluted. A retry being NEEDED between terminal and dispatch again would
# be a GH #48 regression — the attempt-count line below makes it visible.
dispatch_run() {
  local want="$1" attempt json total
  for attempt in $(seq 1 8); do
    ui_do "click Run pipeline (run #$want, attempt $attempt)" click '[data-testid=run-pipeline]'
    local poll_deadline=$(( $(date +%s) + 30 ))
    while (( $(date +%s) <= poll_deadline )); do
      json="$(runs_json)"
      total="$(jq '.rows | length' <<<"$json")"
      if [[ "$total" -ge "$want" ]]; then
        pw console error --clear >/dev/null 2>&1 || true
        (( attempt > 1 )) && echo "  (run #$want dispatched on click attempt $attempt — GH #48 lock held through $(( (attempt - 1) * 30 ))s+ of retries)"
        return 0
      fi
      sleep 5
    done
  done
  die "Run pipeline click for run #$want did not dispatch after 8 attempts over ~4 min — GH #48 (the previous run's in-flight lock not releasing) or a new dispatch failure; see $WORK/ui.log"
}

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
require_tools

port_free "$PORT" || die "port $PORT is already in use — stop whatever is on it, or set CLAVESA_VERIFY_ADDR=:<port> for a parallel run"

WORK="$(mktemp -d /tmp/clavesa-verify-readme.XXXX)"
# Snapshot the binary into the workdir and run THAT copy: anything rewriting
# bin/clavesa mid-walkthrough (e.g. the parallel test-cli gate under
# `make release-gates` running build-bin) must not yank the binary out from
# under a multi-minute run already in flight.
cp "$BIN" "$WORK/clavesa"
BIN="$WORK/clavesa"
# Sandbox the active-workspace pointer (~/.config/clavesa/current-workspace)
# — see the deviation list in the header.
export XDG_CONFIG_HOME="$WORK/xdg-config"
mkdir -p "$XDG_CONFIG_HOME"
# playwright-cli writes its session artifacts (.playwright-cli/) under cwd;
# parking cwd in the workdir keeps them out of the repo and the caller's dir.
cd "$WORK"

WS="$WORK/clavesa-demo"
mkdir -p "$WS"

banner "README step 1-2: workspace init + ui ($WS)"
CURRENT_PAGE="(workspace init)"
"$BIN" workspace init "$WS_NAME" --workspace "$WS" || die "workspace init failed"
pass "workspace init $WS_NAME (runner image built)"

CURRENT_PAGE="(ui server)"
if [[ -n "${CLAVESA_VERIFY_ADDR:-}" ]]; then
  CLAVESA_ADDR="$ADDR" "$BIN" ui --no-browser >"$WORK/ui.log" 2>&1 &
else
  # README-literal: plain `clavesa ui`, default :8080, workspace resolved
  # from the state file `workspace init` just wrote.
  "$BIN" ui --no-browser >"$WORK/ui.log" 2>&1 &
fi
UI_PID=$!
ready=0
for _ in $(seq 1 30); do
  if curl -sf -o /dev/null "$BASE/"; then ready=1; break; fi
  kill -0 "$UI_PID" 2>/dev/null || die "UI server exited early — see $WORK/ui.log"
  sleep 1
done
[[ "$ready" == 1 ]] || die "UI server did not answer on $BASE within 30s — see $WORK/ui.log"
pass "ui serving on $BASE"

ui_do "open browser" open "$BASE/"

# ---------------------------------------------------------------------------
# README browser step 1: register the source
# ---------------------------------------------------------------------------
banner "README browser step 1: register source $SRC_NAME"
CURRENT_PAGE="/"
expect_text "Welcome to your workspace" 30 "Catalog welcome card renders on the empty workspace"
pass "Catalog welcome card rendered"
ui_do "click 'Manage sources' on the welcome card" click 'a:has-text("Manage sources")'
CURRENT_PAGE="/sources"
expect_text "No sources registered yet|Register source" 15 "Sources page renders"
# Two 'Register source' buttons exist on the empty page (header + empty
# state) — both toggle the same form; take the first.
ui_do "open the register-source form" click 'button:has-text("Register source") >> nth=0'
ui_do "fill source name" fill '[data-testid=source-name-input]' "$SRC_NAME"
ui_do "fill source URL" fill '[data-testid=source-url-input]' "$SRC_URL"
expect_text "kind=http" 10 "register-form inference hint shows kind=http"
pass "inference hint shows kind=http (format inferred from .parquet)"
ui_do "click Register" click '[data-testid=register-source-submit]'
expect_text "d37ci6vzurychx.cloudfront.net" 15 "registered source row lists the URL"
pass "$SRC_NAME registered"
check_console "/sources after registering $SRC_NAME"

# ---------------------------------------------------------------------------
# README browser step 2: create the pipeline
# ---------------------------------------------------------------------------
banner "README browser step 2: create pipeline $PIPELINE"
# README: "On the welcome card click Create a pipeline (or Pipelines → New
# pipeline)" — we're on /sources now, so take the parenthetical route.
goto_page "/pipelines"
expect_text "New pipeline" 15 "Pipelines page renders"
# Empty list shows a second 'New pipeline' button in the empty-state card.
ui_do "open the new-pipeline dialog" click 'button:has-text("New pipeline") >> nth=0'
ui_do "fill pipeline name" fill '[data-testid=new-pipeline-name]' "$PIPELINE"
ui_do "click Create" click 'button:text-is("Create")'
# Create navigates to the pipeline dashboard, whose Graph tab is the editor.
CURRENT_PAGE="/pipelines/dashboard?dir=$PIPELINE"
expect_text "Add node" 20 "editor (pipeline dashboard Graph tab) renders"
pass "pipeline $PIPELINE created; editor open"

# ---------------------------------------------------------------------------
# README browser step 3: the landing transform (trips, stats opt-in)
# ---------------------------------------------------------------------------
banner "README browser step 3: build transform $NODE1 (stats + source input + SQL)"
ui_do "open the Add node palette" click '[data-testid=add-node-toggle]'
ui_do "type the node name" fill '[data-testid=palette-node-name]' "$NODE1"
ui_do "click + SQL Transform" click '[data-testid=add-transform]'
expect_text "\b$NODE1\b" 15 "$NODE1 node appears on the canvas"
# Select the node — its config drawer (ConfigPanel) opens on the right,
# on its "code" tab (Inputs + SQL); the Output section lives on "settings".
ui_do "select the $NODE1 node" click "[data-testid=dag-node] div.font-semibold:text-is(\"$NODE1\")"
expect_text "button \"$NODE1\"" 15 "ConfigPanel opens for $NODE1"

# README: settings tab → tick "Compute column stats" under Output →
# Save Output Config.
ui_do "open the drawer settings tab" click '[data-testid=node-settings]'
expect_text "Compute column stats" 10 "Output section renders"
ui_do "tick Compute column stats" check '[data-testid=stats-checkbox]'
ui_do "save the output config" click '[data-testid=save-output-config]'
expect_text "\bSaved\b" 15 "output config (stats=true) saved"
pass "stats opt-in saved for $NODE1"
ui_do "back to the drawer code tab" click 'button:text-is("code")'
expect_text "\bInputs\b" 10 "code tab (Inputs + SQL) renders"

# Inputs → Add: the Source picker is pre-filled with src_trips (the only
# registered source) and the alias mirrors it — exactly the README flow.
ui_do "open the add-input form" click '[data-testid=add-input]'
expect_text "Attach" 10 "add-input form opens"
ui_do "attach $SRC_NAME" click '[data-testid=add-input-submit]'
expect_text "sources\.$SRC_NAME" 15 "input row sources.$SRC_NAME appears"
pass "$SRC_NAME attached to $NODE1"

ui_do "paste the SQL" fill '[data-testid=sql-editor] .cm-content' "$SQL1"
ui_do "click Save" click '[data-testid=save-sql]'
# Save writes <node>.sql into the pipeline dir — assert on disk (the UI
# has no post-save indicator on the SQL editor).
for _ in $(seq 1 10); do
  grep -q "FROM src_trips" "$WS/$PIPELINE/$NODE1.sql" 2>/dev/null && break
  sleep 1
done
grep -q "FROM src_trips" "$WS/$PIPELINE/$NODE1.sql" 2>/dev/null \
  || die "SQL save for $NODE1 never landed in $WS/$PIPELINE/$NODE1.sql"
pass "SQL saved for $NODE1"

# ---------------------------------------------------------------------------
# README browser step 4: the aggregation transform (revenue_by_payment)
# ---------------------------------------------------------------------------
banner "README browser step 4: build transform $NODE2 (node input + SQL)"
ui_do "open the Add node palette" click '[data-testid=add-node-toggle]'
ui_do "type the node name" fill '[data-testid=palette-node-name]' "$NODE2"
ui_do "click + SQL Transform" click '[data-testid=add-transform]'
expect_text "\b$NODE2\b" 15 "$NODE2 node appears on the canvas"
ui_do "select the $NODE2 node" click "[data-testid=dag-node] div.font-semibold:text-is(\"$NODE2\")"
expect_text "button \"$NODE2\"" 15 "ConfigPanel opens for $NODE2"

# Inputs → Add → wire `trips` (the Pipeline-node tab; trips is the only
# upstream transform, pre-selected, alias mirrors it).
ui_do "open the add-input form" click '[data-testid=add-input]'
expect_text "Attach" 10 "add-input form opens"
ui_do "switch to Pipeline node mode" click '[data-testid=mode-node]'
ui_do "attach $NODE1" click '[data-testid=add-input-submit]'
expect_text "\bnode\b" 15 "node-input row for $NODE1 appears"
pass "$NODE1 wired into $NODE2"

ui_do "paste the SQL" fill '[data-testid=sql-editor] .cm-content' "$SQL2"
ui_do "click Save" click '[data-testid=save-sql]'
for _ in $(seq 1 10); do
  grep -q "GROUP BY payment_type" "$WS/$PIPELINE/$NODE2.sql" 2>/dev/null && break
  sleep 1
done
grep -q "GROUP BY payment_type" "$WS/$PIPELINE/$NODE2.sql" 2>/dev/null \
  || die "SQL save for $NODE2 never landed in $WS/$PIPELINE/$NODE2.sql"
pass "SQL saved for $NODE2"
# README step 5: "Close the node panel" (it overlays the Run button).
ui_do "close the config drawer" click '[data-testid=close-node-panel]'
check_console "editor after authoring both transforms"

# ---------------------------------------------------------------------------
# README browser step 5: run it (3× — the README asks for ≥3 runs so the
# dashboard duration-trend chart has more than one point)
# ---------------------------------------------------------------------------
n=1
while [[ "$n" -le "$RUNS_WANTED" ]]; do
  if [[ "$n" -eq 1 ]]; then
    timeout="$RUN1_TIMEOUT_S"   # GH #47 first-run download-stall allowance
  else
    timeout="$RUNN_TIMEOUT_S"
  fi
  banner "README browser step 5: run pipeline ($n of $RUNS_WANTED, timeout ${timeout}s)"
  CURRENT_PAGE="/pipelines/dashboard?dir=$PIPELINE (run $n)"
  # GH #48 is fixed: the run lock releases the moment the run is terminal,
  # so a Run click right after the previous run completes dispatches
  # immediately — no settle needed between runs.
  dispatch_run "$n"
  wait_runs_terminal "$n" "$timeout"
  pass "run $n SUCCEEDED"
  run_id="$(jq -r '.rows[0].run_id // empty' <<<"$(runs_json)")"
  [[ -n "$run_id" ]] || die "run $n reported terminal but the runs API returned no run_id"
  wait_run_recorded "$run_id" 300
  n=$((n + 1))
done

# ---------------------------------------------------------------------------
# Assertions 1-2: run + node-run rollups via the HTTP API
# ---------------------------------------------------------------------------
banner "assert: runs + node_runs rollups"
CURRENT_PAGE="(api)"
runs="$(runs_json)"
succeeded="$(jq '[.rows[]? | select(.status == "SUCCEEDED")] | length' <<<"$runs")"
if [[ "$succeeded" -ge "$RUNS_WANTED" ]]; then
  pass "runs: $succeeded SUCCEEDED (wanted ≥ $RUNS_WANTED)"
else
  fail "runs: expected ≥ $RUNS_WANTED SUCCEEDED, observed $succeeded — $(jq -c '[.rows[]?.status]' <<<"$runs")"
fi

node_runs="$(node_runs_json)"
for node in "$NODE1" "$NODE2"; do
  ok="$(jq --arg n "$node" '[.rows[]? | select(.node == $n and .status == "ok")] | length' <<<"$node_runs")"
  if [[ "$ok" -ge "$RUNS_WANTED" ]]; then
    pass "node_runs: $node has $ok status=ok rows (one per run)"
  else
    fail "node_runs: expected ≥ $RUNS_WANTED ok rows for $node, observed $ok — $(jq -c --arg n "$node" '[.rows[]? | select(.node == $n) | .status]' <<<"$node_runs")"
  fi
done
RUN_ID="$(jq -r '.rows[0].run_id // empty' <<<"$runs")"

# ---------------------------------------------------------------------------
# Assertion 3: Catalog lists ops + output tables
# ---------------------------------------------------------------------------
banner "assert: / (Catalog) table list"
goto_page "/"
check_text "\b$NODE1\b" 60 "Catalog lists $NODE1" 3
check_text "\b$NODE2\b" 15 "Catalog lists $NODE2"
check_text "\bnode_runs\b" 15 "Catalog lists node_runs"
check_text "\bruns\b" 15 "Catalog lists runs"
check_text "\bcolumn_stats\b" 15 "Catalog lists column_stats"
# Default outputs are named bare <node> — the retired __default suffix
# must not resurface.
check_absent "${NODE1}__default|${NODE2}__default" "no __default-suffixed table names"
check_console "/ (Catalog)"

# ---------------------------------------------------------------------------
# Assertion 4: trips table page — schema/sample/commits/lineage + the
# Column profile card (proof the stats=true opt-in worked end to end)
# ---------------------------------------------------------------------------
banner "assert: /tables/$CATALOG/$SCHEMA/$NODE1"
goto_page "/tables/$CATALOG/$SCHEMA/$NODE1"
check_text "Column profile" 180 "Column profile card renders (stats opt-in worked)" 5
check_text "\bNull\b" 15 "column profile shows null %"
check_text "\bDistinct\b" 15 "column profile shows distinct counts"
check_text "p50 / p95" 15 "column profile shows p50/p95 for numerics"
# Per the mandatory list, every profile row shows top-K bars or a "high
# cardinality" badge (the min→max range is the runner's fallback when
# top-K was skipped for a low-cardinality column, e.g. the wide-table
# cap) — presence of the row alone would pass an empty card.
check_eval '() => { const rows = [...document.querySelectorAll("[data-testid=column-profile-row]")]; return rows.length > 0 && rows.every((r) => r.querySelector("[data-testid=profile-topk], [data-testid=profile-high-cardinality], [data-testid=profile-range]")); }' 30 "every profile row shows top-K bars, a high-cardinality badge, or a min→max range"
check_eval '() => !!document.querySelector("[data-testid=profile-topk]")' 15 "column profile renders top-K bars for at least one column"
check_eval '() => !!document.querySelector("[data-testid=profile-min-max], [data-testid=profile-range]")' 15 "column profile shows min/max"
check_text "\bpayment_type\b" 15 "schema lists payment_type"
# Sample rows land via the auto-run query pane ("N rows" footer; the
# column-profile top-K lines also say "rows" but always as "x / y rows").
check_text "[0-9][0-9,]* rows" 300 "sample rows render (query pane returned data)" 5
# ADR-024: the sample-rows card header carries the engine badge. Presence
# by testid, not text — the copy (engine/warehouse qualifiers) is not part
# of the contract.
check_testid "engine-badge-sample-rows" 30 "sample-rows card carries the engine badge"
check_text "Commit timeline" 30 "commit timeline renders"
check_absent "Commit history unavailable" "commit history is populated"
check_text "\bLineage\b" 30 "lineage panel renders"
check_text "\b$SRC_NAME\b" 15 "lineage shows upstream source $SRC_NAME"
check_text "\b$NODE2\b" 15 "lineage shows downstream consumer $NODE2"
check_console "/tables/$CATALOG/$SCHEMA/$NODE1"

# ---------------------------------------------------------------------------
# Assertion 5: revenue_by_payment table page — same panels, and the
# Column profile card must be ABSENT (stats not opted in — that's the point)
# ---------------------------------------------------------------------------
banner "assert: /tables/$CATALOG/$SCHEMA/$NODE2"
goto_page "/tables/$CATALOG/$SCHEMA/$NODE2"
check_text "\bColumns\b" 60 "columns card renders" 3
check_text "\bavg_tip_pct\b" 15 "schema lists avg_tip_pct"
check_text "[0-9][0-9,]* rows" 300 "sample rows render (query pane returned data)" 5
check_text "Commit timeline" 30 "commit timeline renders"
check_text "\bLineage\b" 30 "lineage panel renders"
check_text "$SCHEMA\.$NODE1" 15 "lineage shows upstream table $SCHEMA.$NODE1"
check_absent "Column profile" "Column profile card is absent (stats not opted in)"
check_console "/tables/$CATALOG/$SCHEMA/$NODE2"

# ---------------------------------------------------------------------------
# Assertion 6: the seeded pipeline-runs dashboard — all five widgets
# ---------------------------------------------------------------------------
banner "assert: /dashboards/pipeline-runs-$PIPELINE"
goto_page "/dashboards/pipeline-runs-$PIPELINE"
check_text "Failures \(24h\)" 60 "widget: Failures (24h)" 3
check_text "Runs \(total\)" 15 "widget: Runs (total)"
check_text "Run duration" 15 "widget: Run duration"
check_text "Failures by node" 15 "widget: Failures by node"
check_text "Recent runs" 15 "widget: Recent runs"
# Widget datasets run on the Spark runner locally — give the recent-runs
# table time to fill in.
check_text "SUCCEEDED" 600 "recent-runs table shows SUCCEEDED rows" 10
# "Run duration trend (with multiple points)" — the README mandates ≥3
# runs precisely so this chart has more than one data point. The line
# widget stamps its point count on the chart container
# (data-point-count) so the assertion doesn't count recharts SVG
# internals.
check_eval '() => { const el = document.querySelector("[data-testid=dashboard-widget][data-widget-title=\"Run duration\"] [data-point-count]"); return !!el && Number(el.dataset.pointCount) > 1; }' 300 "Run duration trend has >1 data point" 5
check_console "/dashboards/pipeline-runs-$PIPELINE"

# ---------------------------------------------------------------------------
# Assertion 7: /pipelines lists the demo pipeline with both transforms
# ---------------------------------------------------------------------------
banner "assert: /pipelines"
goto_page "/pipelines"
check_text "\b$PIPELINE\b" 30 "pipeline $PIPELINE listed"
check_text "2 nodes" 15 "pipeline shows 2 nodes (both transforms)"
check_console "/pipelines"

# ---------------------------------------------------------------------------
# Assertions 8-9: pipeline dashboard Runs tab + the run-detail sheet
# ---------------------------------------------------------------------------
banner "assert: /pipelines/dashboard?dir=$PIPELINE (Runs tab + run-detail sheet)"
goto_page "/pipelines/dashboard?dir=$PIPELINE"
check_text "\b$PIPELINE\b" 30 "pipeline dashboard renders"
# The deployment-status row — the sticky health banner (pipeline name,
# health verdict, last-run / success-rate stats).
check_testid "deployment-status" 30 "deployment-status row renders"
ui_do "open the Runs tab" click '[role=tab]:has-text("Runs")'
# Run columns are buttons labeled "Run <id> · <status> · <duration>".
check_text '"Run [^"]+ · ' 60 "Runs tab shows run history columns" 3
check_text "\b$NODE1\b" 15 "Runs grid lists $NODE1"
check_text "\b$NODE2\b" 15 "Runs grid lists $NODE2"
snap="$(snapshot || true)"
ncols="$(grep -Ec '"Run [^"]+ · ' <<<"$snap" || true)"
if [[ "${ncols:-0}" -ge "$RUNS_WANTED" ]]; then
  pass "Runs grid shows $ncols run columns (≥ $RUNS_WANTED)"
else
  fail "Runs grid: expected ≥ $RUNS_WANTED run columns, observed ${ncols:-0} (snapshot: $(dump_snapshot "$snap"))"
fi
# The per-node duration sparkline claim: the Runs grid's duration bars —
# one per run column, height ∝ duration_ms — are the current incarnation
# (the old standalone per-node DurationSparkline was folded into this
# grid when the dashboard was rebuilt around it). One bar per run.
check_eval "() => document.querySelectorAll('[data-testid=run-duration-bar]').length >= $RUNS_WANTED" 30 "Runs grid renders a duration bar per run column (≥ $RUNS_WANTED)"

# The sheet opens by clicking a run column — the bare URL does not auto-open it.
if [[ -n "$RUN_ID" ]]; then
  ui_do "open the run-detail sheet for $RUN_ID" click "[data-testid=run-column][data-run-id=\"$RUN_ID\"]"
  CURRENT_PAGE="/pipelines/dashboard?dir=$PIPELINE&run=$RUN_ID (sheet)"
  check_text "Per-node breakdown" 60 "run-detail sheet opens with the per-node breakdown" 3
  # Scope the remaining sheet assertions to the dialog so the grid behind
  # it can't satisfy them.
  sheet="$(pw snapshot '[role=dialog]' || true)"
  # The triage strip renders "Module <code>vX.Y.Z</code>" and
  # "Runner <code><12-hex digest prefix></code>" — match the code nodes
  # exactly so a 32-hex run_id elsewhere in the sheet can't satisfy them.
  if grep -q "Module" <<<"$sheet" && grep -Eq 'code( \[[^]]*\])?: v[0-9]+\.[0-9]+\.[0-9]+ *$' <<<"$sheet"; then
    pass "triage strip shows the module version"
  else
    fail "triage strip: module version missing from the sheet (snapshot: $(dump_snapshot "$sheet"))"
  fi
  if grep -q "Runner" <<<"$sheet" && grep -Eq 'code( \[[^]]*\])?: [0-9a-f]{12} *$' <<<"$sheet"; then
    pass "triage strip shows the runner-image digest"
  else
    fail "triage strip: runner-image digest missing from the sheet (snapshot: $(dump_snapshot "$sheet"))"
  fi
  for node in "$NODE1" "$NODE2"; do
    if grep -Eq "\b$node\b" <<<"$sheet"; then
      pass "per-node breakdown lists $node"
    else
      fail "per-node breakdown: $node missing from the sheet (snapshot: $(dump_snapshot "$sheet"))"
    fi
  done
  # "Per-node DAG colored by status (both transforms colored)": each DAG
  # node stamps its run status on data-status — presence of the node name
  # alone would pass an uncolored/gray DAG. This run SUCCEEDED, so both
  # transforms must carry succeeded. Scoped to the sheet so the dashboard
  # canvas behind it can't satisfy the check.
  check_eval "() => [\"$NODE1\", \"$NODE2\"].every((n) => document.querySelector('[data-testid=run-detail-sheet] [data-testid=dag-node][data-node=\"' + n + '\"]')?.dataset.status === \"succeeded\")" 120 "run-detail DAG colors both transforms by status (succeeded)" 3
else
  fail "run-detail sheet: no run_id available from /api/data/runs"
fi
check_console "/pipelines/dashboard?dir=$PIPELINE (Runs tab + sheet)"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
banner "summary"
if [[ "$FAILURES" -gt 0 ]]; then
  echo "✗ README verification FAILED: $FAILURES assertion(s) — see ✗ lines above" >&2
  exit 1
fi
SUCCESS=1
echo "✓ README quick-start verification GREEN ($RUNS_WANTED runs, all mandatory pages asserted, 0 console errors)"
