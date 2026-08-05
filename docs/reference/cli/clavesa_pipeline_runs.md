# clavesa pipeline runs

List recent runs of a pipeline

List recent executions of a pipeline, newest first.

Each row shows the run id, overall status, trigger, start time, duration,
and (for a failed run) the failed step and a truncated error message. Pass
--status to filter to one status; use --json for the full untruncated
error text.

Pipeline directory:
  Pass the pipeline directory as the first argument, relative to the
  workspace root (e.g. "my-pipeline") or as an absolute path. Omit it to
  use the current directory, which is handy once you have cd'd into the
  pipeline. Run outside any pipeline with no argument and the command
  reports a clear error.

## Usage

```
clavesa pipeline runs [pipeline-dir] [flags]
```

## Flags

```
      --json            output as JSON
      --limit int       maximum number of runs to show (default 20)
      --status string   filter by status: RUNNING, SUCCEEDED, or FAILED
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa pipeline](clavesa_pipeline.md) — Manage pipelines
- [Command index](README.md)
