# clavesa pipeline backfill diff

Compare a staging table against its canonical target

Compare a staging table against its canonical target.

Pipeline directory:
  Pass the pipeline directory as the first argument, relative to the
  workspace root (e.g. "my-pipeline") or as an absolute path. Omit it to
  use the current directory, which is handy once you have cd'd into the
  pipeline. Run outside any pipeline with no argument and the command
  reports a clear error.

## Usage

```
clavesa pipeline backfill diff [pipeline-dir] <run_id> [flags]
```

## Flags

```
      --json   emit machine-readable output
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa pipeline backfill](clavesa_pipeline_backfill.md) — Replay a transform over a historical partition window
- [Command index](README.md)
