# clavesa source detach

Detach a named input from a transform

Remove an aliased input from a transform's inputs map.

Covers all three attachment kinds — registry sources, external Glue
tables (`<schema>.<table>` refs), and transform→transform edges — so
one command works regardless of how the input was originally attached.
For transform→transform edges, `clavesa node disconnect` is the
direct equivalent.

Examples:
  clavesa source detach my-pipeline --to t1 --as raw
  clavesa source detach              --to t1 --as raw   # cwd is the pipeline dir

Pipeline directory:
  Pass the pipeline directory as the first argument, relative to the
  workspace root (e.g. "my-pipeline") or as an absolute path. Omit it to
  use the current directory, which is handy once you have cd'd into the
  pipeline. Run outside any pipeline with no argument and the command
  reports a clear error.

## Usage

```
clavesa source detach [pipeline-dir] [flags]
```

## Flags

```
      --as string   input alias to remove
      --to string   transform node id to detach from
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa source](clavesa_source.md) — Manage workspace input sources (registry)
- [Command index](README.md)
