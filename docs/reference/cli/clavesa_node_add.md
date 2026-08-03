# clavesa node add

Add a node to a pipeline

Add a new node to a pipeline.

Use --name to give the node a meaningful identifier (e.g. "enrich_logs"
or "warehouse"). If omitted, a sequential name like "transform2" is
generated.

ADR-017 slice 4: --type source and --from are gone — sources are
workspace-level registry entries now. Use:

  clavesa source register <name> --from <url> --attach <pipeline> --to <transform>

Examples:
  clavesa node add my-pipeline --type transform --name enrich_logs
  clavesa node add my-pipeline --type destination

Pipeline directory:
  Pass the pipeline directory as the first argument, relative to the
  workspace root (e.g. "my-pipeline") or as an absolute path. Omit it to
  use the current directory, which is handy once you have cd'd into the
  pipeline. Run outside any pipeline with no argument and the command
  reports a clear error.

## Usage

```
clavesa node add <pipeline-dir> [flags]
```

## Flags

```
      --name string                    node name (e.g. enrich_logs); auto-generated if omitted
      --type clavesa source register   node type: transform or destination (sources: see clavesa source register)
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa node](clavesa_node.md) — Manage pipeline nodes and edges
- [Command index](README.md)
