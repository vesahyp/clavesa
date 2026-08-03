# clavesa pipeline cost

Report cost per billion records — clavesa's north-star metric

Report clavesa's north-star metric: cost per billion records processed.

Reads the pipeline's recent runner invocations, sums the records processed
and the billed compute, and prints the blended cost-per-billion alongside
sustained throughput. All-local pipelines have zero compute cost; the
throughput half of the metric is still reported.

Report-only: this prints the metric; it does not re-deploy or edit the
pipeline.

Pipeline directory:
  Pass the pipeline directory as the first argument, relative to the
  workspace root (e.g. "my-pipeline") or as an absolute path. Omit it to
  use the current directory, which is handy once you have cd'd into the
  pipeline. Run outside any pipeline with no argument and the command
  reports a clear error.

## Usage

```
clavesa pipeline cost [pipeline-dir] [flags]
```

## Flags

```
      --dir string   pipeline directory (alternative to the positional argument)
      --json         output as JSON
      --last int     number of recent runs to consider (default 50)
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa pipeline](clavesa_pipeline.md) — Manage pipelines
- [Command index](README.md)
