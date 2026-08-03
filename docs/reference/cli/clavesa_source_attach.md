# clavesa source attach

Attach a registered source to a transform input

Attach a registered source to a transform's inputs map.

Writes inputs = { <alias> = "sources.<source>" } into the transform
block; the orchestration emitter resolves the reference at sync time.

Examples:
  clavesa source attach my-pipeline trips --to t1 --as raw
  clavesa source attach my-pipeline trips --to t1   # alias defaults to source name

Pipeline directory:
  Pass the pipeline directory as the first argument, relative to the
  workspace root (e.g. "my-pipeline") or as an absolute path. Omit it to
  use the current directory, which is handy once you have cd'd into the
  pipeline. Run outside any pipeline with no argument and the command
  reports a clear error.

## Usage

```
clavesa source attach [pipeline-dir] <source> [flags]
```

## Flags

```
      --as string   input alias (default: source name)
      --to string   transform node id to attach to
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa source](clavesa_source.md) — Manage workspace input sources (registry)
- [Command index](README.md)
