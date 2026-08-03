# clavesa node rename

Rename a node

Rename a node — the module block, every downstream edge that
reads it, and its SQL/PySpark script files all move to the new
name.

Note: a node's id is also the stem of its Delta output table
(<node>__default), so a rename changes that table's name. Data
already written under the old name is not moved.

Pipeline directory:
  Pass the pipeline directory as the first argument, relative to the
  workspace root (e.g. "my-pipeline") or as an absolute path. Omit it to
  use the current directory, which is handy once you have cd'd into the
  pipeline. Run outside any pipeline with no argument and the command
  reports a clear error.

## Usage

```
clavesa node rename [pipeline-dir] <old-id> <new-id>
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa node](clavesa_node.md) — Manage pipeline nodes and edges
- [Command index](README.md)
