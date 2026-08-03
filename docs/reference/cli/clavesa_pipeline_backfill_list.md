# clavesa pipeline backfill list

List open (un-promoted/un-discarded) backfill staging tables

List open (un-promoted/un-discarded) backfill staging tables.

Pipeline directory:
  Pass the pipeline directory as the first argument, relative to the
  workspace root (e.g. "my-pipeline") or as an absolute path. Omit it to
  use the current directory, which is handy once you have cd'd into the
  pipeline. Run outside any pipeline with no argument and the command
  reports a clear error.

## Usage

```
clavesa pipeline backfill list [pipeline-dir] [flags]
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
