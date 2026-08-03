# clavesa node preview

Preview data flowing through a node

Preview data flowing through a node.

Pipeline directory:
  Pass the pipeline directory as the first argument, relative to the
  workspace root (e.g. "my-pipeline") or as an absolute path. Omit it to
  use the current directory, which is handy once you have cd'd into the
  pipeline. Run outside any pipeline with no argument and the command
  reports a clear error.

## Usage

```
clavesa node preview [pipeline-dir] <node-id> [flags]
```

## Flags

```
      --json         output as JSON
      --offset int   row offset (source nodes)
      --rows int     number of rows to fetch (transform nodes) (default 15)
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa node](clavesa_node.md) — Manage pipeline nodes and edges
- [Command index](README.md)
