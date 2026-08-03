# clavesa source preview

Preview a registered source's data

Sample a registered source's data without attaching it to a pipeline.

Fetches the source through the same host-side path preview uses for
transform inputs — http and s3 sources both work. Sources that
reference a credential aren't previewable yet (works in pipeline run).

  clavesa source preview trips
  clavesa source preview trips --limit 50 --json

## Usage

```
clavesa source preview <name> [flags]
```

## Flags

```
      --json         output as JSON
      --limit int    max rows to show (default 15)
      --offset int   row offset
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa source](clavesa_source.md) — Manage workspace input sources (registry)
- [Command index](README.md)
