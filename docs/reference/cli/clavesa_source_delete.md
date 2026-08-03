# clavesa source delete

Delete a registered source

Delete a registered source.

Refuses if any pipeline in the workspace references the source. Use
--force to delete anyway (intended for scripted teardown — manually
clean up the dangling references afterwards).

## Usage

```
clavesa source delete <name> [flags]
```

## Flags

```
      --force   delete even if pipelines reference this source
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa source](clavesa_source.md) — Manage workspace input sources (registry)
- [Command index](README.md)
