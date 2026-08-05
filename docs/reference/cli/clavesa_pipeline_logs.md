# clavesa pipeline logs

Print the captured log for a run

Print the captured log for one run of a pipeline.

Defaults to the latest run. On cloud pipelines --node selects which step's
CloudWatch log stream to read; on local pipelines the log is one bundle per
run covering every node, so --node has no effect there.

Pipeline directory:
  Pass the pipeline directory as the first argument, relative to the
  workspace root (e.g. "my-pipeline") or as an absolute path. Omit it to
  use the current directory, which is handy once you have cd'd into the
  pipeline. Run outside any pipeline with no argument and the command
  reports a clear error.

## Usage

```
clavesa pipeline logs [pipeline-dir] [flags]
```

## Flags

```
      --json          output as JSON
      --node string   node/step name; selects the CloudWatch stream on cloud runs, ignored on local runs
      --run string    run id to inspect (default: latest)
      --tail int      maximum log lines; local runs return the last N (default 2000)
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa pipeline](clavesa_pipeline.md) — Manage pipelines
- [Command index](README.md)
