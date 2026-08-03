# clavesa node connect

Connect a node, source, or external table to a transform's input

Wire a transform's inputs map. Three forms, mutually exclusive:

  --from <node>                 intra-pipeline edge (this pipeline's source/transform → this transform).
  --from-table <schema>.<table> cross-pipeline / external-table read (ADR-016 slice 2).
                                Resolved against the workspace catalog at orchestration-sync time.

The --input flag sets the SQL table alias for the connection. It defaults
to the from-node ID (--from) or the table-name portion (--from-table) so
the SQL reads naturally — e.g. `FROM dim_customers` over a
`marketing.dim_customers` reference.

Examples:
  clavesa node connect my-pipeline --from source1 --to transform1
  clavesa node connect my-pipeline --from source1 --to transform1 --input raw
  clavesa node connect my-pipeline --from-table marketing.dim_customers --to enrich

Pipeline directory:
  Pass the pipeline directory as the first argument, relative to the
  workspace root (e.g. "my-pipeline") or as an absolute path. Omit it to
  use the current directory, which is handy once you have cd'd into the
  pipeline. Run outside any pipeline with no argument and the command
  reports a clear error.

## Usage

```
clavesa node connect <pipeline-dir> [flags]
```

## Flags

```
      --from string         source node ID (intra-pipeline edge)
      --from-table string   cross-pipeline / external table reference (<schema>.<table>)
      --input string        SQL table alias for this input
      --output string       output port name (intra-pipeline only) (default "default")
      --to string           target node ID
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa node](clavesa_node.md) — Manage pipeline nodes and edges
- [Command index](README.md)
