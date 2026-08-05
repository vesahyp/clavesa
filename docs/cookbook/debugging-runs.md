# Debugging failed runs

> **When you have one:** a pipeline scheduled via external cron wrapping `clavesa pipeline run` (the local equivalent of the deployed EventBridge schedule — see [scheduled-rollup](scheduled-rollup.md)). Cron runs unattended overnight. Some morning one of them fails, and cron's own log just shows a non-zero exit. You want to know which run failed, which node broke, why, and the full Spark stack trace — from the terminal, without opening the UI.

`clavesa pipeline runs`, `clavesa pipeline logs`, and `clavesa pipeline status` are the CLI's run-inspection trio. They read the same run history and captured logs the dashboard's Runs tab shows, so a cron-driven pipeline is exactly as debuggable as one you click through in the browser (ADR-015).

> **Continues from** the [README quick-start](../../README.md#quick-start) — it assumes the `cookbook` workspace with `src_trips` registered. If you don't have that state, run the **Setup** block below.

## Setup (self-contained)

```bash
make build                                   # produces ./bin/clavesa
export WS=/tmp/clavesa-cookbook
mkdir -p $WS
bin/clavesa workspace init cookbook --workspace $WS    # no-op if it already exists
bin/clavesa source register src_trips \
  --from https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet \
  --workspace $WS

bin/clavesa pipeline create nightly --workspace $WS
bin/clavesa node add nightly --type transform --name trips --workspace $WS
bin/clavesa source attach nightly src_trips --to trips --as src_trips --workspace $WS
bin/clavesa node edit nightly trips \
  --set "sql=SELECT VendorID, tpep_pickup_datetime, payment_type, total_amount FROM src_trips" \
  --workspace $WS
```

Set `export CLAVESA_WORKSPACE=$WS` to drop the `--workspace` flag from the commands below.

## A run that succeeds

```bash
bin/clavesa pipeline run nightly --workspace $WS
```

```
Workdir: /tmp/clavesa-cookbook/nightly
Run: 3f8a1c2e-...
NODE   TYPE       STATUS  OUTPUT
trips  transform  ok      clavesa_cookbook__nightly.trips
```

The `Run: <id>` line is new (GH #94) — a cron wrapper that captures stdout gets a stable handle for this exact execution, without a separate query. Note it down; it's the id every command below can target with `--run`.

## Break it

Simulate the kind of change that ships without a test: point the transform at a column that doesn't exist.

```bash
bin/clavesa node edit nightly trips \
  --set "sql=SELECT nonexistent_column FROM src_trips" \
  --workspace $WS
```

## A run that fails

```bash
bin/clavesa pipeline run nightly --workspace $WS
```

```
Workdir: /tmp/clavesa-cookbook/nightly
Error: pipeline failed at node "trips"
stderr: ...UNRESOLVED_COLUMN... `nonexistent_column`...
full log: /tmp/clavesa-cookbook/nightly/.clavesa/runs/<runID>/_bundle.log
```

Exit code is non-zero — this is the thing cron's own log shows, and nothing more. Everything below turns that one flag into a full picture, without re-running the pipeline.

## See what happened: `pipeline runs`

```bash
bin/clavesa pipeline runs nightly --workspace $WS
```

```
RUN                                   STATUS   TRIGGER  STARTED              DURATION  FAILED STEP  ERROR
9c1e...                               FAILED   manual   2026-08-05 07:02:11  4s        trips        [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column...
3f8a1c2e-...                          SUCCEEDED manual  2026-08-05 07:00:48  6s        —            —
```

One line per run, newest first: id, status, trigger, when it started, how long it took, the node that broke it, and enough of the error to recognize it. That's the "which of last night's runs failed, and why" answer in one command — exactly what a cron-driven pipeline needs, since there's no dashboard tab open to notice the red run.

Filter to just the failures:

```bash
bin/clavesa pipeline runs nightly --status failed --workspace $WS
```

Add `--json` for the untruncated error text and for piping into a script or alert:

```bash
bin/clavesa pipeline runs nightly --status failed --json --workspace $WS
```

```json
{"rows":[{"run_id":"9c1e...","pipeline":"nightly","status":"FAILED","trigger":"manual","started_at":"2026-08-05T07:02:11Z","ended_at":"2026-08-05T07:02:15Z","duration_ms":4123,"failed_step":"trips","error_class":"transform_error","error_msg":"[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `nonexistent_column` cannot be resolved. Did you mean one of the following? ..."}],"truncated":false}
```

A cron wrapper that checks `jq '.rows[0].status == "FAILED"'` after every `pipeline run` is a five-line alert.

## Per-node detail: `pipeline status`

`pipeline runs` answers "which run, which node." `pipeline status` answers "what exactly did that node say":

```bash
bin/clavesa pipeline status nightly --workspace $WS
```

```
Run: 9c1e... (FAILED)
NODE   STATUS  PROGRESS
trips  FAILED  —
trips: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `nonexistent_column` cannot be resolved. Did you mean one of the following? [`src_trips`.`VendorID`, ...
```

The per-node error line only prints for nodes that actually failed — a healthy multi-node pipeline just shows the status table. Pass `--run <id>` to inspect a run other than the latest; `--json` gives the same shape the dashboard's run-detail DAG renders from.

## The full stack trace: `pipeline logs`

`runs` and `status` give you the diagnosis; `logs` gives you the evidence. A local run captures one bundle log covering every node in that run, and the real Spark stack trace — the part that names the exact line and the exact suggested columns — lives at the *end* of it, which is what `--tail` is for:

```bash
bin/clavesa pipeline logs nightly --tail 40 --workspace $WS
```

```
Log: /tmp/clavesa-cookbook/nightly/.clavesa/runs/9c1e.../_bundle.log
07:02:14.812  pyspark.errors.exceptions.captured.AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `nonexistent_column` cannot be resolved. Did you mean one of the following? [`src_trips`.`VendorID`, `src_trips`.`payment_type`, `src_trips`.`total_amount`, `src_trips`.`tpep_pickup_datetime`]. SQLSTATE: 42703
07:02:14.812    at org.apache.spark.sql.catalyst.analysis...
...
```

`logs` defaults to the latest run; pass `--run <id>` to inspect a specific one (the id `pipeline runs` printed, or the one `pipeline run` echoed at the time):

```bash
bin/clavesa pipeline logs nightly --run 9c1e... --tail 40 --workspace $WS
```

`--json` is the scripting form — one event per line, with the log's location:

```bash
bin/clavesa pipeline logs nightly --tail 3 --json --workspace $WS
```

```json
{"source":"local","log_group":"/tmp/clavesa-cookbook/nightly/.clavesa/runs/9c1e.../_bundle.log","function_name":"","events":[{"timestamp":"2026-08-05T07:02:14.812Z","message":"pyspark.errors.exceptions.captured.AnalysisException: ..."},{"timestamp":"...","message":"..."},{"timestamp":"...","message":"..."}],"truncated":true}
```

## Where the artifacts live on disk

Nothing above is a black box. Every local run writes a durable trail under the pipeline and the workspace: the full captured log sits at `<pipeline>/.clavesa/runs/<runID>/_bundle.log` (what `pipeline logs` reads), and the run/node status markers `pipeline runs` and `pipeline status` read live in the workspace-shared `<workspace>/.clavesa/warehouse/_progress/<runID>/` tree (`_run.json` for the run, one `<node>.json` per node). Both survive the run that wrote them — `grep` them directly if you'd rather not go through the CLI, or point a log shipper at the `_bundle.log` files.

## Fix it and confirm

```bash
bin/clavesa node edit nightly trips \
  --set "sql=SELECT VendorID, tpep_pickup_datetime, payment_type, total_amount FROM src_trips" \
  --workspace $WS
bin/clavesa pipeline run nightly --workspace $WS
```

```
NODE   TYPE       STATUS  OUTPUT
trips  transform  ok      clavesa_cookbook__nightly.trips
```

`pipeline runs nightly --workspace $WS` now shows the newest row `SUCCEEDED` — the pipeline is left healthy for whatever recipe you read next.

## Verify

```bash
# The good run reports Run: <id> and exits 0.
bin/clavesa pipeline run nightly --workspace $WS ; echo "exit=$?"    # exit=0, stdout contains "Run: "

# Break the SQL, then the run fails and exits non-zero.
bin/clavesa node edit nightly trips --set "sql=SELECT nonexistent_column FROM src_trips" --workspace $WS
bin/clavesa pipeline run nightly --workspace $WS ; echo "exit=$?"    # exit=1

# The newest row in `pipeline runs --json` is FAILED, with a failed step
# and an error message naming the bad column.
bin/clavesa pipeline runs nightly --json --workspace $WS | jq '.rows[0] | {status, failed_step, error_msg}'
# → {"status":"FAILED","failed_step":"trips","error_msg":"...nonexistent_column..."}

# --status filters to only FAILED rows.
bin/clavesa pipeline runs nightly --status failed --json --workspace $WS | jq '[.rows[].status] | unique'
# → ["FAILED"]

# pipeline status shows the failing node with a non-empty error.
bin/clavesa pipeline status nightly --json --workspace $WS | jq '.states.trips | {status, error_msg: (.error_msg != "")}'
# → {"status":"FAILED","error_msg":true}

# pipeline logs finds the bundle log, non-empty, sourced "local".
bin/clavesa pipeline logs nightly --json --workspace $WS | jq '{source, has_events: (.events | length > 0), log_group: (.log_group | endswith("_bundle.log"))}'
# → {"source":"local","has_events":true,"log_group":true}

# --tail caps and marks the response truncated.
bin/clavesa pipeline logs nightly --tail 3 --json --workspace $WS | jq '{n: (.events | length), truncated}'
# → {"n":3,"truncated":true}

# Restore the SQL; the pipeline runs clean again.
bin/clavesa node edit nightly trips --set "sql=SELECT VendorID, tpep_pickup_datetime, payment_type, total_amount FROM src_trips" --workspace $WS
bin/clavesa pipeline run nightly --workspace $WS ; echo "exit=$?"    # exit=0
```

## What to expect — and the limits

- **Local runs get the full trio for free.** No CloudWatch, no Athena query — `pipeline runs` and `pipeline status` read the workspace's `_progress` marker tree, and `pipeline logs` reads the run's `_bundle.log` file directly. All three are fast filesystem reads; none of them boot Spark.
- **On cloud (deployed, Step-Functions-driven) runs, `--tail` caps from the head, not the tail.** CloudWatch's `FilterLogEvents` has no "last N lines" mode, so a cloud `pipeline logs --tail 40` returns the *first* 40 events of the stream, not the last 40 where a Spark stack trace usually lands. Ask for a larger `--tail` and search, or read the stream directly in CloudWatch.
- **Cloud-local runs (a cloud warehouse, local compute — ADR-024) have no captured log yet.** `pipeline logs` returns the empty-state hint there; the runner's stdout isn't teed anywhere durable for that mode yet (GH #85).
- **`--node` only matters on cloud.** Locally one bundle log covers every node in the run, so `--node` is accepted but ignored; on a deployed pipeline it selects which node's Lambda CloudWatch stream to read, and omitting it is a user error there.

## Next

- **[scheduled-rollup](scheduled-rollup.md)** — the deployed counterpart to the cron-wrapped `pipeline run` this recipe assumes; when a *scheduled* run fails, `pipeline runs` / `pipeline logs` are the first two commands to reach for.
- **[query-your-data](query-your-data.md)** — `node_runs` and `runs` are themselves queryable Delta tables if you want an aggregate view (failure rate over a week, slowest node) instead of a single run.
