# clavesa pipeline status

Show per-node status for the latest (or a given) run

Print the per-node execution state for the latest run of a pipeline.

Nodes still RUNNING show live Spark task progress ("124/300 tasks") as the
runner reports it; finished nodes show their terminal status. Pass --run to
inspect a specific run id instead of the latest.

Pipeline directory:
  Pass the pipeline directory as the first argument, relative to the
  workspace root (e.g. "my-pipeline") or as an absolute path. Omit it to
  use the current directory, which is handy once you have cd'd into the
  pipeline. Run outside any pipeline with no argument and the command
  reports a clear error.

## Usage

```
clavesa pipeline status [pipeline-dir] [flags]
```

## Flags

```
      --json         output as JSON
      --run string   run id to inspect (default: latest)
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa pipeline](clavesa_pipeline.md) — Manage pipelines
- [Command index](README.md)
