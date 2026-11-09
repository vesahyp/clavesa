# clavesa pipeline run

Execute the pipeline (local: runner container; cloud: SFN StartExecution)

Dispatches by the workspace warehouse:

  - warehouse = local  →  walks the DAG and invokes the runner container
                          for each transform; outputs land in a fresh
                          temp workdir.
  - warehouse = cloud  →  finds the deployed Step Functions state machine
                          (clavesa-<pipeline_name>) and calls
                          StartExecution. Pass --wait to block until the
                          execution terminates.

The warehouse defaults to "local" and is set with `clavesa workspace use --warehouse`.
Pass --warehouse local|cloud to override it for this run only.

--compute local runs the whole pipeline in a local docker container against
the cloud warehouse — same data, watermarks, and SQS cursors as a deployed
run (it DRAINS them), the cost-per-billion win of laptop compute. It needs
source-S3 read + warehouse-S3 read/write + Glue read/write + SQS consume on
the human principal, and honors CLAVESA_JVM_HEAP_MB. On a local warehouse
--compute local is a no-op (compute already equals the warehouse); --compute
cloud against a local warehouse is rejected.

Local filesystem sources only on the local path — S3 sources need cloud
dispatch.

On a local warehouse, CLAVESA_TRIGGER labels the run (default "manual"; any
non-empty string, e.g. "cron") in the run marker, the runs table and the
timing record. Every local run writes a timing record (per-phase wall-clock
time plus the runner's per-node timing) to
<workspace>/.clavesa/warehouse/_progress/<run>/_timing.json and a copy to the
run history dir: $CLAVESA_RUN_HISTORY_DIR, else $XDG_STATE_HOME/clavesa/runs,
else ~/.local/state/clavesa/runs. The run ends with a short phase table;
--json carries the record under "timing".

Pipeline directory:
  Pass the pipeline directory as the first argument, relative to the
  workspace root (e.g. "my-pipeline") or as an absolute path. Omit it to
  use the current directory, which is handy once you have cd'd into the
  pipeline. Run outside any pipeline with no argument and the command
  reports a clear error.

## Usage

```
clavesa pipeline run <pipeline-dir> [flags]
```

## Flags

```
      --compute string       execution placement for this run: local (docker runner against the cloud warehouse) | cloud (SFN, the default on a cloud warehouse)
      --force                Bypass incremental-skip checks for this run; the runner reads the full source range. Watermarks still advance on success.
      --force-node strings   Bypass incremental-skip for the named node only. Repeatable. Implies --force scoped to that node.
      --json                 emit machine-readable output
      --wait                 block until the run terminates (cloud: SFN execution; cloud + --compute local: the local docker bundle)
      --warehouse string     override the workspace warehouse for this run: local | cloud
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa pipeline](clavesa_pipeline.md) — Manage pipelines
- [Command index](README.md)
