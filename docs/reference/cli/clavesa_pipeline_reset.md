# clavesa pipeline reset

Drop a pipeline's output tables and watermarks so the next run rebuilds from scratch

Drop the canonical output tables of a pipeline's transforms — and by
default the CDF watermarks feeding them — so the next run rebuilds
everything from source. Reset is a data operation: it never touches the
deployed Lambda / Step Functions / IAM stack — tearing that down is
`pipeline destroy`.

--include-watermarks defaults to true on purpose: for a CDF consumer,
dropping the table but keeping the watermark leaves the table empty on
the next run, because CDF reads only the not-yet-consumed range and
that range is empty after a drop. Clearing the watermark replays
upstream history from version 0, which is exactly the rebuild a reset
is for. Pass --include-watermarks=false to keep the cursors and drop
data only (rare).

In local mode this deletes warehouse table directories and watermark
files; in cloud mode it deletes the S3 warehouse prefixes, the Glue
catalog entries, and the S3 watermark objects.

Pipeline directory:
  Pass the pipeline directory as the first argument, relative to the
  workspace root (e.g. "my-pipeline") or as an absolute path. Omit it to
  use the current directory, which is handy once you have cd'd into the
  pipeline. Run outside any pipeline with no argument and the command
  reports a clear error.

## Usage

```
clavesa pipeline reset [pipeline-dir] [flags]
```

## Flags

```
      --include-watermarks   clear CDF watermark state so the next run replays upstream history from version 0 (default true)
      --json                 emit machine-readable output (requires --yes)
      --node string          reset only this node's output (default: all transform nodes)
  -y, --yes                  skip the interactive confirmation prompt
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa pipeline](clavesa_pipeline.md) — Manage pipelines
- [Command index](README.md)
