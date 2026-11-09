# clavesa credential register

Register a new credential

Register a new credential.

Kinds:
  header            one HTTP header for a source fetch (--header, --value-prefix)
  databricks-oauth  a Databricks service principal for the Unity Catalog
                    publish (--client-id is its application ID; --secret
                    points at its OAuth secret)

Example:
  clavesa credential register dbx-sp --kind databricks-oauth \
    --client-id 6f1c...-... --secret env:DATABRICKS_CLIENT_SECRET

## Usage

```
clavesa credential register <name> [flags]
```

## Flags

```
      --client-id string      Databricks service principal application ID (kind=databricks-oauth)
      --header string         HTTP header name to inject (kind=header)
      --kind string           credential kind: header or databricks-oauth (default "header")
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
