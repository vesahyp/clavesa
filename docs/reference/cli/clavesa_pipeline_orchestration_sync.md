# clavesa pipeline orchestration sync

Generate orchestration.tf for a pipeline

Generate orchestration.tf for a pipeline.

Pipeline directory:
  Pass the pipeline directory as the first argument, relative to the
  workspace root (e.g. "my-pipeline") or as an absolute path. Omit it to
  use the current directory, which is handy once you have cd'd into the
  pipeline. Run outside any pipeline with no argument and the command
  reports a clear error.

## Usage

```
clavesa pipeline orchestration sync [pipeline-dir] [flags]
```

## Flags

```
      --schedule string   EventBridge schedule expression, e.g. "rate(1 hour)"
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa pipeline orchestration](clavesa_pipeline_orchestration.md) — Manage pipeline orchestration
- [Command index](README.md)
