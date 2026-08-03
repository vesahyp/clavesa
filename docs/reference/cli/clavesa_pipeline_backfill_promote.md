# clavesa pipeline backfill promote

Merge a staging table into its canonical target, then drop staging

Merge a staging table into its canonical target, then drop staging.

Pipeline directory:
  Pass the pipeline directory as the first argument, relative to the
  workspace root (e.g. "my-pipeline") or as an absolute path. Omit it to
  use the current directory, which is handy once you have cd'd into the
  pipeline. Run outside any pipeline with no argument and the command
  reports a clear error.

## Usage

```
clavesa pipeline backfill promote [pipeline-dir] <run_id> [flags]
```

## Flags

```
      --allow-duplicates     (append outputs) accept duplicates — plain INSERT INTO without dedup
      --compute string       where to run the MERGE: "local" runs it in a local docker container against the cloud warehouse (IAM superset; honors CLAVESA_JVM_HEAP_MB); default runs on the deployed Lambda
      --force-dedup string   (append outputs) column to MERGE on so duplicates are dropped; column must uniquely identify a row
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa pipeline backfill](clavesa_pipeline_backfill.md) — Replay a transform over a historical partition window
- [Command index](README.md)
