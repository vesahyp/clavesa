# clavesa node edit

Edit node configuration

Edit node configuration using --set key=value flags.

Run without flags to see all settable config keys for a node.

Output shape (transforms): --output-mode and --output-merge-keys edit
the default output's writer behaviour without touching nested HCL.
Use --add-output / --remove-output (repeatable) to manage additional
output keys for multi-output Python transforms that return more than
one DataFrame. New outputs are seeded with replace mode; tune them
with --set output_definitions={...} or by hand-editing the .tf for
now. Pass --output-merge-keys with no value to clear the existing
keys; pass --output-mode "" likewise. --output-cluster-by liquid-clusters
a replace/append output on the given columns without changing its mode
(merge outputs already cluster by their merge_keys); pass it with no value
to clear. --output-merge-update sets per-column merge expressions (col=spec
pairs) so CDF-incremental aggregates accumulate on a mode=merge output
instead of being overwritten; does not change mode. Pass with no value to
clear.

Examples:
  clavesa node edit my-pipeline source1 --set bucket=my-data
  clavesa node edit my-pipeline transform1 --set sql="SELECT * FROM source1"
  clavesa node edit my-pipeline transform1 --set python=file(transforms/enrich.py)
  clavesa node edit my-pipeline dim_customers --output-merge-keys customer_id
  clavesa node edit my-pipeline daily_revenue --output-cluster-by region,day
  clavesa node edit my-pipeline daily_counts --output-merge-update event_count=additive,last_seen=max
  clavesa node edit my-pipeline orders --output-mode append
  clavesa node edit my-pipeline enrich --add-output outliers
  clavesa node edit my-pipeline source1    # show settable keys

Pipeline directory:
  Pass the pipeline directory as the first argument, relative to the
  workspace root (e.g. "my-pipeline") or as an absolute path. Omit it to
  use the current directory, which is handy once you have cd'd into the
  pipeline. Run outside any pipeline with no argument and the command
  reports a clear error.

## Usage

```
clavesa node edit <pipeline-dir> <node-id> [flags]
```

## Flags

```
      --add-output strings                   declare an additional output key on a multi-output transform (repeatable). Seeded with mode=replace; tune via direct .tf edit
      --incremental-input strings            read this input alias incrementally (Delta CDF version range, watermark-tracked). Repeatable. Per-input opt-in; transforms full-read by default
      --non-incremental-input strings        drop an alias from incremental_inputs so it reverts to full-read on every run (repeatable)
      --output-bound-by strings              comma-separated columns to statically bound a merge output's target scan on (must be functionally determined by merge_keys); pass with no value to clear
      --output-cluster-by strings            comma-separated columns to liquid-cluster the output Delta table on (replace/append outputs); does not change mode. Pass with no value to clear
      --output-merge-keys strings            comma-separated columns forming the natural key; sets mode=merge implicitly
      --output-merge-update stringToString   col=spec pairs giving per-column merge expressions for mode=merge outputs (e.g. event_count=additive,last_seen=max). Spec is a keyword (additive, min, max, sketch) or a raw SparkSQL expression. Does not change mode. Pass with no value to clear (default [])
      --output-mode string                   default-output mode: "replace" (default; full overwrite), "append", or "merge"
      --output-stats                         opt this transform's outputs into per-column stats (null %, distinct, top-K, percentiles); pass --output-stats=false to turn off
      --remove-output strings                remove a non-default output key from output_definitions (repeatable)
      --set key=value                        set key=value (repeatable) (default map[])
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa node](clavesa_node.md) — Manage pipeline nodes and edges
- [Command index](README.md)
