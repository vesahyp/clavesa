# clavesa pipeline timings

Show where the time of recent runs went, phase by phase

Show the timing history of recent runs, one row per run, oldest first.

Every local `pipeline run` writes a timing record: wall-clock time
per run-level phase (measured by the CLI) plus the runner's per-node phases.
The record lands in two places:

  <workspace>/.clavesa/warehouse/_progress/<run-id>/_timing.json
  <history dir>/<started>_<workspace>_<pipeline>_<run8>.json

The history dir collects the records of every workspace on this machine:
$CLAVESA_RUN_HISTORY_DIR, else $XDG_STATE_HOME/clavesa/runs, else
~/.local/state/clavesa/runs. --all reads it, so it needs no workspace and
ignores [pipeline-dir].

On a cloud warehouse there are no CLI-side phases; each run is assembled
from the node_runs table, so only the node columns carry time.

Columns (times are wall clock):
  TOTAL      start of the command to the end of the run
  STARTUP    startup + prepare + lock + image + container + boot
  METASTORE  bringing up or reusing the shared metastore
  SPARK      Spark session and catalog setup, summed over nodes
  READS      resolving sources plus every node's input reads
  TRANSFORM  the transform itself, summed over nodes
  WRITES     table writes, summed over nodes
  STATS      column statistics, summed over nodes
  OTHER      TOTAL minus the columns above
A "-" means the runner image predates per-node timing; OTHER then holds
all node time.

--json prints {"generated_at","history_dir","runs":[...]} with every record
in full: run_id, workspace, workspace_dir, pipeline, status, trigger,
compute, started_at, ended_at, total_ms, phases (startup, prepare, lock,
metastore, image, inputs, container, boot, bundle, nodes, record) and
nodes[] (node, status, started_ms, ended_ms, duration_ms, output_rows,
timing). history_dir is "" for a single pipeline.

Pipeline directory:
  Pass the pipeline directory as the first argument, relative to the
  workspace root (e.g. "my-pipeline") or as an absolute path. Omit it to
  use the current directory, which is handy once you have cd'd into the
  pipeline. Run outside any pipeline with no argument and the command
  reports a clear error.

## Usage

```
clavesa pipeline timings [pipeline-dir] [flags]
```

## Examples

```
clavesa pipeline timings my-pipeline
  clavesa pipeline timings my-pipeline --runs 50 --since 7d
  clavesa pipeline timings --all --since 90d
  clavesa pipeline timings my-pipeline --json
```

## Flags

```
      --all            every pipeline in every workspace on this machine, from the history dir
      --json           output as JSON
      --runs int       number of most recent runs to show (0 = all) (default 20)
      --since string   only runs that started within this window: a Go duration (24h) or days (7d)
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa pipeline](clavesa_pipeline.md) — Manage pipelines
- [Command index](README.md)
