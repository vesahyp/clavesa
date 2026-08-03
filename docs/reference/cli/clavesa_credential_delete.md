# clavesa credential delete

Delete a registered credential

Delete a registered credential.

Refuses if any source in the workspace references it. Use --force to
delete anyway (intended for scripted teardown).

## Usage

```
clavesa credential delete <name> [flags]
```

## Flags

```
      --force   delete even if sources reference this credential
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa credential](clavesa_credential.md) — Manage workspace credentials (registry)
- [Command index](README.md)
