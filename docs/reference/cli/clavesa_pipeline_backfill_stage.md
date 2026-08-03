# clavesa pipeline backfill stage

Stage a backfill into a parallel Delta table

Stage a backfill into a parallel Delta table.

Pipeline directory:
  Pass the pipeline directory as the first argument, relative to the
  workspace root (e.g. "my-pipeline") or as an absolute path. Omit it to
  use the current directory, which is handy once you have cd'd into the
  pipeline. Run outside any pipeline with no argument and the command
  reports a clear error.

--compute local runs the heavy Spark staging job in a local docker
container on this machine against the cloud warehouse — the workaround
for the deployed Lambda's 15-minute cap on large historical windows
(GH #43). The staging table still lands in the cloud S3 warehouse and
registers in Glue, so diff / promote / discard are unchanged. Requires
docker, and the human principal needs source-S3 read + warehouse-S3 RW
+ Glue RW. Honors CLAVESA_JVM_HEAP_MB to raise the Spark driver heap for
big windows (defaults to ~1 GB).

## Usage

```
clavesa pipeline backfill stage [pipeline-dir] [flags]
```

## Flags

```
      --compute string   where to run staging compute: "local" runs heavy Spark in a local docker container against the cloud warehouse (GH #43 workaround); default runs on the deployed Lambda
      --direct           skip staging — write straight to the canonical target (escape hatch; non-merge outputs need this carefully)
      --from string      partition cursor lower bound, slash-separated (e.g. 2026/04/26/00) (required)
      --json             emit machine-readable output
      --node string      transform node to backfill (required)
      --to string        partition cursor upper bound, slash-separated (e.g. 2026/04/27/00) (required)
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa pipeline backfill](clavesa_pipeline_backfill.md) — Replay a transform over a historical partition window
- [Command index](README.md)
