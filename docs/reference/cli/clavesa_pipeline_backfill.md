# clavesa pipeline backfill

Replay a transform over a historical partition window

Backfill a transform's output over a historical partition window.

Default shape — stage → review → promote — gives you a parallel
Delta staging table to inspect before anything lands in the canonical
target:

  clavesa pipeline backfill stage <dir> --node <n> --from <c> --to <c>
  clavesa pipeline backfill diff <dir> <run_id>
  clavesa pipeline backfill promote <dir> <run_id>     # or discard

Every subcommand takes the pipeline directory as the first argument;
omit it to use the current directory once you have cd'd into the
pipeline.

The --direct flag on stage skips staging and writes to the canonical
target — for cases where you know the output is keyed (merge mode) and
want to skip the round-trip.

## Usage

```
clavesa pipeline backfill
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## Subcommands

- [clavesa pipeline backfill diff](clavesa_pipeline_backfill_diff.md) — Compare a staging table against its canonical target
- [clavesa pipeline backfill discard](clavesa_pipeline_backfill_discard.md) — Drop a staging table without promoting
- [clavesa pipeline backfill list](clavesa_pipeline_backfill_list.md) — List open (un-promoted/un-discarded) backfill staging tables
- [clavesa pipeline backfill promote](clavesa_pipeline_backfill_promote.md) — Merge a staging table into its canonical target, then drop staging
- [clavesa pipeline backfill stage](clavesa_pipeline_backfill_stage.md) — Stage a backfill into a parallel Delta table

## See also

- [clavesa pipeline](clavesa_pipeline.md) — Manage pipelines
- [Command index](README.md)
