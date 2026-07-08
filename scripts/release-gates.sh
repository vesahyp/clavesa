#!/usr/bin/env bash
# release-gates.sh — run the four release gates and write their green
# stamps, choosing serial vs concurrent by the Docker VM's memory.
#
# Invoked by `make release-gates` (which builds once, then calls this with
# SKIP_BUILD=1 so the gates don't each rebuild). Gates: test, smoke-cloud,
# verify-readme (the "trio"), then verify-cookbook (always last — it boots
# real Spark per recipe and must not share the VM).
#
# WHY THE MEMORY CHECK (GH #84): three Spark-heavy gates at once OOM a
# small Docker VM — the JVM + Derby metastore get killed and the run dies
# with ConnectionReset / ConnectionRefused, masquerading as a regression.
# This cost the v2.14.0 AND v2.16.0 releases (both passed on solo re-runs).
# So the trio runs CONCURRENTLY only when the VM has headroom
# (>= GATES_CONCURRENT_MIN_GIB, default 12) or CLAVESA_GATES_CONCURRENT=1
# forces it; otherwise the trio runs SERIALLY. Reliable-but-slower beats
# fast-but-flaky for a gate that blocks a release. verify-cookbook is
# always serial regardless.
#
# PROGRESS (roughly-where-are-we): every milestone is appended to
# .gates/progress.log with a timestamp — `tail -f .gates/progress.log`
# shows gate-level start/done plus verify-cookbook's per-recipe progress
# (the gate scripts append to $CLAVESA_PROGRESS_LOG when it is set). Each
# gate's full output stays in .gates/<gate>.log.
#
# GATES_DRYRUN=1 prints the plan (mode + gate order) without running
# anything — for validating this orchestrator without a ~40 min gate run.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

GATES_DIR=".gates"
PROGRESS="$GATES_DIR/progress.log"
MIN_GIB="${GATES_CONCURRENT_MIN_GIB:-12}"
VERIFY_ADDR="${CLAVESA_VERIFY_ADDR:-:8089}"

mkdir -p "$GATES_DIR"
rm -f "$GATES_DIR"/test.log "$GATES_DIR"/smoke-cloud.log \
      "$GATES_DIR"/verify-readme.log "$GATES_DIR"/verify-cookbook.log "$PROGRESS"

# hb <msg> — one timestamped heartbeat line to the console and progress.log.
hb() {
  local line
  line="$(date -u +%H:%M:%S)  $*"
  echo "→ $line"
  echo "$line" >>"$PROGRESS"
}

# docker_mem_gib — Docker VM total memory in whole GiB, or 0 if unknown.
docker_mem_gib() {
  local bytes
  bytes="$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo 0)"
  case "$bytes" in
    ''|*[!0-9]*) echo 0;;
    *) echo $(( bytes / 1024 / 1024 / 1024 ));;
  esac
}

# run_gate <name> — run one gate target into its log; echo 0/nonzero status.
# Exposes the progress file to the gate script so it can append finer detail.
run_gate() {
  local g="$1" extra="${2:-}" st=0
  hb "$g: starting"
  # shellcheck disable=SC2086
  CLAVESA_PROGRESS_LOG="$PROGRESS" SKIP_BUILD=1 $extra \
    make "$g" >"$GATES_DIR/$g.log" 2>&1 || st=$?
  if [ "$st" = 0 ]; then hb "$g: ✓ green"; else hb "$g: ✗ failed (exit $st)"; fi
  return "$st"
}

TRIO="test smoke-cloud verify-readme"
mem="$(docker_mem_gib)"
if [ "${CLAVESA_GATES_CONCURRENT:-0}" = 1 ]; then
  MODE="concurrent (forced by CLAVESA_GATES_CONCURRENT=1)"
elif [ "$mem" -ge "$MIN_GIB" ]; then
  MODE="concurrent (Docker VM ${mem} GiB >= ${MIN_GIB} GiB)"
else
  MODE="serial (Docker VM ${mem} GiB < ${MIN_GIB} GiB — GH #84 OOM guard)"
fi

if [ "${GATES_DRYRUN:-0}" = 1 ]; then
  echo "release-gates plan:"
  echo "  mode : $MODE"
  echo "  trio : $TRIO"
  echo "  then : verify-cookbook (always serial)"
  echo "  logs : $GATES_DIR/<gate>.log ; heartbeat: $PROGRESS"
  exit 0
fi

hb "release gates: $MODE"
echo "  verify-readme UI port: $VERIFY_ADDR (cloud-smoke picks a random free port; :8080 untouched)"
rc=0

case "$MODE" in
  concurrent*)
    CLAVESA_PROGRESS_LOG="$PROGRESS" SKIP_BUILD=1 make test          >"$GATES_DIR/test.log"          2>&1 & p_test=$!
    CLAVESA_PROGRESS_LOG="$PROGRESS" SKIP_BUILD=1 make smoke-cloud    >"$GATES_DIR/smoke-cloud.log"   2>&1 & p_smoke=$!
    CLAVESA_PROGRESS_LOG="$PROGRESS" SKIP_BUILD=1 CLAVESA_VERIFY_ADDR="$VERIFY_ADDR" make verify-readme >"$GATES_DIR/verify-readme.log" 2>&1 & p_verify=$!
    hb "trio: launched concurrently (test, smoke-cloud, verify-readme)"
    st=0; wait $p_test  || st=$?; [ "$st" = 0 ] && hb "test: ✓ green"          || { hb "test: ✗ failed (exit $st)"; rc=1; }
    st=0; wait $p_smoke || st=$?; [ "$st" = 0 ] && hb "smoke-cloud: ✓ green"   || { hb "smoke-cloud: ✗ failed (exit $st)"; rc=1; }
    st=0; wait $p_verify|| st=$?; [ "$st" = 0 ] && hb "verify-readme: ✓ green" || { hb "verify-readme: ✗ failed (exit $st)"; rc=1; }
    ;;
  serial*)
    st=0; run_gate test || st=$?; [ "$st" = 0 ] || rc=1
    st=0; run_gate smoke-cloud || st=$?; [ "$st" = 0 ] || rc=1
    st=0; CLAVESA_VERIFY_ADDR="$VERIFY_ADDR" run_gate verify-readme || st=$?; [ "$st" = 0 ] || rc=1
    ;;
esac

# verify-cookbook is always serial and last — Spark-heavy per recipe.
st=0; run_gate verify-cookbook || st=$?; [ "$st" = 0 ] || rc=1

if [ "$rc" = 0 ]; then
  hb "all release gates green — stamps: .test-green.json .verify-readme-green.json .cloud-smoke-green.json .verify-cookbook-green.json"
else
  hb "release gates FAILED — see the ✗ lines above and .gates/<gate>.log"
fi
exit "$rc"
