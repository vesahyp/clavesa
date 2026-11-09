# clavesa workspace databricks

Show the workspace's Databricks connection

## Usage

```
clavesa workspace databricks [flags]
```

## Flags

```
      --check   run the admin checks against Databricks again
      --json    output as JSON
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## Subcommands

- [clavesa workspace databricks preflight](clavesa_workspace_databricks_preflight.md) — Check, read-only, what a Databricks and an AWS admin must grant before clavesa can replace a step
- [clavesa workspace databricks tables](clavesa_workspace_databricks_tables.md) — List each pipeline's tables in Unity Catalog, with Catalog Explorer links

## See also

- [clavesa workspace](clavesa_workspace.md) — Manage workspaces
- [Command index](README.md)
