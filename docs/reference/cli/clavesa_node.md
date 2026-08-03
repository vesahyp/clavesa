# clavesa node

Manage pipeline nodes and edges

Manage pipeline nodes and edges (list, show, add, edit, remove, connect,
disconnect, preview).

Nodes are transforms or destinations inside a pipeline. Sources live in
the workspace registry (see `clavesa source --help`) and attach to a
transform's inputs map.

The pipeline directory is the first argument; omit it to use the current
directory once you have cd'd into the pipeline.

Examples:
  clavesa node list my-pipeline
  clavesa node add my-pipeline --type transform --name enrich
  clavesa source attach my-pipeline trips --to enrich --as trips
  clavesa node edit my-pipeline enrich --set sql="SELECT * FROM trips"
  clavesa node preview enrich              # from inside the pipeline dir

## Usage

```
clavesa node
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## Subcommands

- [clavesa node add](clavesa_node_add.md) — Add a node to a pipeline
- [clavesa node connect](clavesa_node_connect.md) — Connect a node, source, or external table to a transform's input
- [clavesa node disable](clavesa_node_disable.md) — Disable a node — skip it in runs without deleting it
- [clavesa node disconnect](clavesa_node_disconnect.md) — Remove an edge between nodes
- [clavesa node edit](clavesa_node_edit.md) — Edit node configuration
- [clavesa node enable](clavesa_node_enable.md) — Re-enable a previously disabled node
- [clavesa node list](clavesa_node_list.md) — List nodes in a pipeline
- [clavesa node preview](clavesa_node_preview.md) — Preview data flowing through a node
- [clavesa node remove](clavesa_node_remove.md) — Remove a node from a pipeline
- [clavesa node rename](clavesa_node_rename.md) — Rename a node
- [clavesa node show](clavesa_node_show.md) — Show node details

## See also

- [clavesa](clavesa.md) — Visual ETL for Terraform pipelines
- [Command index](README.md)
