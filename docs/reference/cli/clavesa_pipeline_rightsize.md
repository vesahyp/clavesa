# clavesa pipeline rightsize

Recommend per-node Lambda memory from recent run history

Recommend a Lambda memory allocation per node, computed from the
p95 of the node's recent peak RSS and how often it spilled.

Recommend-only: this prints advice; it does not re-deploy or edit the
pipeline. Nodes with no allocated memory on record (local runs) or no
Spark memory metrics yet show confidence "n/a".

Pipeline directory:
  Pass the pipeline directory as the first argument, relative to the
  workspace root (e.g. "my-pipeline") or as an absolute path. Omit it to
  use the current directory, which is handy once you have cd'd into the
  pipeline. Run outside any pipeline with no argument and the command
  reports a clear error.

## Usage

```
clavesa pipeline rightsize [pipeline-dir] [flags]
```

## Flags

```
      --json       output as JSON
      --last int   number of recent runs to consider (default 50)
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa pipeline](clavesa_pipeline.md) — Manage pipelines
- [Command index](README.md)
