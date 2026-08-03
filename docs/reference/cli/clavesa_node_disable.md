# clavesa node disable

Disable a node — skip it in runs without deleting it

Disable a node — skip it in runs without deleting it.

Takes effect on the next local run, and on cloud after `pipeline orchestration sync` / deploy.

Pipeline directory:
  Pass the pipeline directory as the first argument, relative to the
  workspace root (e.g. "my-pipeline") or as an absolute path. Omit it to
  use the current directory, which is handy once you have cd'd into the
  pipeline. Run outside any pipeline with no argument and the command
  reports a clear error.

## Usage

```
clavesa node disable [pipeline-dir] <node-id>
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa node](clavesa_node.md) — Manage pipeline nodes and edges
- [Command index](README.md)
