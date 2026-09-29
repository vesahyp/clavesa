# clavesa credential register

Register a new credential

## Usage

```
clavesa credential register <name> [flags]
```

## Flags

```
      --header string         HTTP header name to inject (kind=header)
      --kind string           credential kind (slice 2: header) (default "header")
      --secret string         secret reference: arn:aws:secretsmanager:..., env:VAR, or file:<workspace-rel>
      --value-prefix string   value prepended to the resolved secret (e.g. "Bearer ")
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa credential](clavesa_credential.md) — Manage workspace credentials (registry)
- [Command index](README.md)
