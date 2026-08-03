# clavesa pipeline optimize

Compact, re-cluster, and vacuum a pipeline's Delta output tables

Run Delta table maintenance over a pipeline's output tables by
invoking the runner's control-plane operations.

By default every transform output table is OPTIMIZEd (compacted). With
--recluster, tables that declare cluster_by (or merge_keys on a merge-mode
output) get ALTER TABLE CLUSTER BY (keys) before the OPTIMIZE — the migration
path for tables created before liquid clustering. With --vacuum, each table is
also VACUUMed past the retention window.

A single-table failure is reported in the results; the sweep continues to the
remaining tables. Exit is non-zero if any table errored.

Pipeline directory:
  Pass the pipeline directory as the first argument, relative to the
  workspace root (e.g. "my-pipeline") or as an absolute path. Omit it to
  use the current directory, which is handy once you have cd'd into the
  pipeline. Run outside any pipeline with no argument and the command
  reports a clear error.

## Usage

```
clavesa pipeline optimize [pipeline-dir] [flags]
```

## Flags

```
      --json               output as JSON
      --node string        optimize only this node's output table(s) (default: all)
      --recluster          re-cluster (ALTER TABLE CLUSTER BY merge/cluster keys + OPTIMIZE); migrates pre-clustering tables
      --retain-hours int   VACUUM retention window in hours (default 168)
      --vacuum             also VACUUM after optimize
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa pipeline](clavesa_pipeline.md) — Manage pipelines
- [Command index](README.md)
