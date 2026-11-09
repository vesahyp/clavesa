#!/usr/bin/env bash
# make test-stream: prove the stream mode (ADR-028 slice 1) on this machine.
#
# Starts LocalStack Kinesis and a single-node Kafka on a private docker
# network, makes a fresh tempdir workspace, and drives `bin/clavesa stream run`
# against them. Asserts exact row counts in the Delta table for:
#
#   1. kinesis basic        10,000 events, one drain
#   2. kinesis kill x20     20 kill -9 cycles while events arrive, then drain
#   3. kinesis shard split  split a shard mid-stream, per-key order kept;
#                           then a new query on the split stream reads all
#   4. producer duplicate   the same events put twice, --record-key drops them
#   5. checkpoint reset     delete the checkpoint, --recover MERGEs, no dupes
#   6. kafka basic          10,000 events on 3 partitions, one drain
#   7. kafka kill x5        5 kill -9 cycles, then drain
#
# Nothing touches AWS: the endpoints are local containers and the
# credentials are dummies. Images are pinned by digest. LocalStack is pinned
# to 4.9 (2025-10-06), the last line that starts without an account; newer
# images stop with "License activation failed".
#
# Env: KILL_CYCLES (default 20), KAFKA_KILL_CYCLES (default 5),
#      KEEP=1 to leave the workspace and containers for debugging.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/bin/clavesa"
KILL_CYCLES="${KILL_CYCLES:-20}"
KAFKA_KILL_CYCLES="${KAFKA_KILL_CYCLES:-5}"

LOCALSTACK_IMAGE="localstack/localstack:4.9@sha256:59373b4a27dba3ec15c4db0b7455f0cb68c8a42300e2cbbe362b440f9a131697"
KAFKA_IMAGE="apache/kafka:3.9.1@sha256:4ceccc577f03f51f6af8dbfda55194d0d892f4fa7913ffbded567ce3895622ed"

NET="clavesa-stream-test"
LS="clavesa-stream-test-localstack"
KAFKA="clavesa-stream-test-kafka"
WS_NAME="streamtest"
WS="$(mktemp -d "${TMPDIR:-/tmp}/clavesa-stream-test.XXXXXX")/ws"
WAREHOUSE="$WS/.clavesa/warehouse"
IMAGE="clavesa/${WS_NAME}/transform-runner:latest"
STREAM_CONTAINER_PREFIX="clavesa-stream-${WS_NAME}-"

export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1
export CLAVESA_NO_BROWSER=1

RESULTS=()
FAILED=0
RUN_FAILURES="$(dirname "$WS")/run-failures.txt"

log() { printf '%s  %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

cleanup() {
  if [ "${KEEP:-0}" = "1" ]; then
    log "KEEP=1: workspace $WS and containers left in place"
    return
  fi
  for c in $(docker ps -aq --filter "name=${STREAM_CONTAINER_PREFIX}"); do docker rm -f "$c" >/dev/null 2>&1 || true; done
  docker rm -f "$LS" "$KAFKA" >/dev/null 2>&1 || true
  # The workspace's shared metastore joined $NET; take it off before the
  # network goes, then let the CLI's reaper handle the container itself.
  for c in $(docker network inspect -f '{{range .Containers}}{{.Name}} {{end}}' "$NET" 2>/dev/null); do
    docker network disconnect -f "$NET" "$c" >/dev/null 2>&1 || true
  done
  docker network rm "$NET" >/dev/null 2>&1 || true
  for c in $(docker ps -aq --filter "name=${WS_NAME}"); do docker rm -f "$c" >/dev/null 2>&1 || true; done
  rm -rf "$(dirname "$WS")"
}
trap cleanup EXIT

harness() {
  # Never fail the script here; a broken step shows up as a FAIL in check.
  docker run --rm --network "$NET" \
    -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_REGION \
    -e KINESIS_ENDPOINT=http://localstack:4566 \
    -v "$ROOT/tests/stream:/harness:ro" -v "$WAREHOUSE:$WAREHOUSE" \
    --entrypoint python "$IMAGE" /harness/harness.py "$@" 2>/dev/null | tail -1
}

# stream_run <name> <flags...>: one CLI run; prints the CLI's JSON summary.
# A failed run is recorded as a FAIL with the tail of its log, and the
# script goes on, so one failure does not hide the other results.
stream_run() {
  local name="$1"; shift
  # A container killed in the previous cycle can still be on its way out;
  # its name must be free before the next run takes it.
  docker rm -f "${STREAM_CONTAINER_PREFIX}${name}" >/dev/null 2>&1 || true
  local rc=0
  "$BIN" stream run "$name" --workspace "$WS" --network "$NET" --json "$@" \
    2>>"$WS/stream-$name.log" || rc=$?
  if [ "$rc" != "0" ] && [ "${STREAM_RUN_EXPECT_KILL:-0}" != "1" ]; then
    # stream_run runs inside $(...), a subshell, so record the failure in
    # a file; the report reads it back.
    echo "FAIL  stream run $name exited $rc" >>"$RUN_FAILURES"
    {
      echo "---- stream run $name exited $rc; last lines of $WS/stream-$name.log:"
      grep -v -E '^\s+at |^\s*$' "$WS/stream-$name.log" | tail -25 | cut -c1-400
      echo "----"
    } >&2
  fi
  return 0
}

kinesis_flags() { echo "--kind kinesis --stream $1 --endpoint http://kinesis.us-east-1.localstack:4566 --region us-east-1"; }
kafka_flags() { echo "--kind kafka --bootstrap-servers kafka:9092 --topic $1"; }

table_path() {
  local log; log="$(find "$WAREHOUSE" -type d -name _delta_log -path "*/$1/_delta_log" -print -quit)"
  [ -n "$log" ] && dirname "$log"
  return 0
}

json_get() {
  python3 - "$1" "$2" <<'PY'
import json, sys
try:
    print(json.loads(sys.argv[1]).get(sys.argv[2]))
except Exception:
    print(None)
PY
}

check() { # check <test> <what> <got> <want>
  if [ "$3" = "$4" ]; then
    RESULTS+=("PASS  $1: $2 = $3")
  else
    RESULTS+=("FAIL  $1: $2 = $3, want $4")
    FAILED=1
  fi
}

count_table() { # count_table <test> <table> <want rows> [--order]
  local path; path="$(table_path "$2")"
  if [ -z "$path" ]; then check "$1" "table exists" "no" "yes"; return; fi
  local out; out="$(harness count --path "$path" ${4:-} || true)"
  log "$1: $out"
  check "$1" "rows" "$(json_get "$out" rows)" "$3"
  check "$1" "duplicates" "$(json_get "$out" duplicates)" "0"
  if [ "${4:-}" = "--order" ]; then
    check "$1" "per-key order violations" "$(json_get "$out" order_violations)" "0"
  fi
}

# kill_cycles <test> <name> <cycles> <flags...>: start the stream without
# --drain while a producer writes, kill -9 the container at a random point,
# repeat. Callers pass a producer command in PRODUCER (run per cycle).
kill_cycles() {
  local test="$1" name="$2" cycles="$3"; shift 3
  local i
  for i in $(seq 1 "$cycles"); do
    eval "$PRODUCER $i" >/dev/null &
    local ppid=$!
    STREAM_RUN_EXPECT_KILL=1 stream_run "$name" "$@" --trigger-seconds 3 --optimize-every-seconds 0 >/dev/null &
    local spid=$!
    local wait_s=$((10 + RANDOM % 26))
    sleep "$wait_s"
    docker kill "${STREAM_CONTAINER_PREFIX}${name}" >/dev/null 2>&1 || true
    wait "$spid" 2>/dev/null || true
    wait "$ppid" || true
    log "$test: cycle $i/$cycles killed after ${wait_s}s"
  done
}

# --- setup -------------------------------------------------------------------

log "build: bin/clavesa (make build ran as a dependency)"
docker network create "$NET" >/dev/null 2>&1 || true

log "start: LocalStack Kinesis and Kafka"
docker rm -f "$LS" "$KAFKA" >/dev/null 2>&1 || true
# The connector parses the region out of the endpoint host and fails on a
# host with no dots, even when kinesis.region is set (KinesisOptions at
# v2.0.0). The alias gives LocalStack an AWS-shaped host name.
docker run -d --name "$LS" --network "$NET" --network-alias localstack \
  --network-alias kinesis.us-east-1.localstack \
  -e SERVICES=kinesis -e KINESIS_SHARD_LIMIT=100 "$LOCALSTACK_IMAGE" >/dev/null
docker run -d --name "$KAFKA" --network "$NET" --network-alias kafka \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_NUM_PARTITIONS=3 -e "KAFKA_HEAP_OPTS=-Xmx512m -Xms512m" \
  "$KAFKA_IMAGE" >/dev/null

log "workspace: $WS (builds the runner image; cached after the first run)"
mkdir -p "$WS"
: >"$RUN_FAILURES"
"$BIN" workspace init "$WS_NAME" --workspace "$WS" >"$WS/../init.log" 2>&1 || {
  tail -30 "$WS/../init.log"; exit 1; }

for _ in $(seq 1 60); do
  docker exec "$LS" curl -s localhost:4566/_localstack/health 2>/dev/null | grep -q '"kinesis": "\(available\|running\)"' && break
  sleep 2
done
for _ in $(seq 1 60); do
  docker exec "$KAFKA" /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list >/dev/null 2>&1 && break
  sleep 2
done

# --- 1. kinesis basic ----------------------------------------------------------
T=kinesis-basic
log "$T"
harness kinesis-create --stream s1 --shards 2 >/dev/null
harness kinesis-put --stream s1 --count 10000 >/dev/null
out="$(stream_run k1 $(kinesis_flags s1) --trigger-seconds 5 --drain)"
log "$T: $out"
count_table "$T" k1 10000

# --- 2. kinesis kill x N -------------------------------------------------------
T="kinesis-kill-x${KILL_CYCLES}"
log "$T"
harness kinesis-create --stream s2 --shards 2 >/dev/null
# The producer for cycle i writes events (i-1)*1000 .. i*1000-1.
produce_s2() { harness kinesis-put --stream s2 --rate 150 --count 1000 --start $(( ($1 - 1) * 1000 )); }
PRODUCER="produce_s2"
kill_cycles "$T" k2 "$KILL_CYCLES" $(kinesis_flags s2)
out="$(stream_run k2 $(kinesis_flags s2) --trigger-seconds 5 --drain)"
log "$T: final drain $out"
count_table "$T" k2 $((KILL_CYCLES * 1000))

# --- 3. kinesis shard split ----------------------------------------------------
T=kinesis-shard-split
log "$T"
harness kinesis-create --stream s3 --shards 2 >/dev/null
harness kinesis-put --stream s3 --count 5000 >/dev/null
STREAM_RUN_EXPECT_KILL=1 stream_run k3 $(kinesis_flags s3) --trigger-seconds 3 --optimize-every-seconds 0 >/dev/null &
spid=$!
sleep 25
log "$T: $(harness kinesis-split --stream s3)"
harness kinesis-put --stream s3 --start 5000 --count 5000 --rate 500 >/dev/null
sleep 15
docker stop -t 30 "${STREAM_CONTAINER_PREFIX}k3" >/dev/null 2>&1 || true
wait "$spid" 2>/dev/null || true
out="$(stream_run k3 $(kinesis_flags s3) --trigger-seconds 5 --drain)"
log "$T: final drain $out"
count_table "$T" k3 10000 --order
# A new query on a stream that already has a closed shard must read the
# closed shard too (upstream v2.0.0 skipped it; runner/patches/ fixes it).
T=kinesis-start-after-split
log "$T"
out="$(stream_run k3b $(kinesis_flags s3) --trigger-seconds 5 --drain)"
log "$T: $out"
count_table "$T" k3b 10000 --order

# --- 4. producer duplicate -----------------------------------------------------
T=producer-duplicate
log "$T"
harness kinesis-create --stream s4 --shards 2 >/dev/null
harness kinesis-put --stream s4 --count 1000 --repeat-from 200 --repeat-count 100 >/dev/null
DEDUP_SQL="SELECT *, get_json_object(raw, '\$.event_id') AS event_id FROM batch"
out="$(stream_run k4 $(kinesis_flags s4) --trigger-seconds 5 --drain \
  --sql "$DEDUP_SQL" --record-key event_id)"
log "$T: $out"
count_table "$T" k4 1000

# --- 5. checkpoint reset + recovery MERGE ----------------------------------------
T=checkpoint-reset
log "$T"
harness kinesis-create --stream s5 --shards 2 >/dev/null
harness kinesis-put --stream s5 --count 3000 >/dev/null
stream_run k5 $(kinesis_flags s5) --trigger-seconds 5 --drain >/dev/null
count_table "$T (before reset)" k5 3000
ckpt="$(find "$WAREHOUSE/_streams" -maxdepth 1 -type d -name '*.k5' -print -quit)"
log "$T: deleting checkpoint $ckpt"
rm -rf "$ckpt"
harness kinesis-put --stream s5 --start 3000 --count 2000 >/dev/null
out="$(stream_run k5 $(kinesis_flags s5) --trigger-seconds 5 --drain --recover)"
log "$T: recovery $out"
check "$T" "recovery mode" "$(json_get "$out" mode)" "merge"
count_table "$T (after recovery)" k5 5000
harness kinesis-put --stream s5 --start 5000 --count 1000 >/dev/null
out="$(stream_run k5 $(kinesis_flags s5) --trigger-seconds 5 --drain)"
log "$T: after recovery, normal run $out"
check "$T" "mode after recovery" "$(json_get "$out" mode)" "append"
count_table "$T (normal run after recovery)" k5 6000

# --- 6. kafka basic --------------------------------------------------------------
T=kafka-basic
log "$T"
docker exec "$KAFKA" /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --topic t1 --partitions 3 >/dev/null
harness kafka-put --bootstrap kafka:9092 --topic t1 --count 10000 >/dev/null
out="$(stream_run f1 $(kafka_flags t1) --trigger-seconds 5 --drain)"
log "$T: $out"
count_table "$T" f1 10000 --order

# --- 7. kafka kill x N -----------------------------------------------------------
T="kafka-kill-x${KAFKA_KILL_CYCLES}"
log "$T"
docker exec "$KAFKA" /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --topic t2 --partitions 3 >/dev/null
produce_t2() { harness kafka-put --bootstrap kafka:9092 --topic t2 --count 1000 --start $(( ($1 - 1) * 1000 )); }
PRODUCER="produce_t2"
kill_cycles "$T" f2 "$KAFKA_KILL_CYCLES" $(kafka_flags t2)
out="$(stream_run f2 $(kafka_flags t2) --trigger-seconds 5 --drain)"
log "$T: final drain $out"
count_table "$T" f2 $((KAFKA_KILL_CYCLES * 1000))

# --- report ------------------------------------------------------------------------
if [ -s "$RUN_FAILURES" ]; then
  while IFS= read -r line; do RESULTS+=("$line"); done <"$RUN_FAILURES"
  FAILED=1
fi
echo
echo "stream tests:"
printf '  %s\n' "${RESULTS[@]}"
if [ "$FAILED" != "0" ]; then
  echo "test-stream: FAILED (logs: rerun with KEEP=1 and read $WS/stream-*.log)"
  exit 1
fi
echo "test-stream: all green"
