# clavesa workspace tables

List Delta tables in the workspace catalog

List every Delta table the workspace catalog owns.

Filter to one catalog or schema (ADR-016 three-level namespace) — the
CLI twin of the Catalog page's ?catalog=&schema= view:

  clavesa workspace tables --schema taxis
  clavesa workspace tables --catalog clavesa_demo_ws --json

## Usage

```
clavesa workspace tables [flags]
```

## Flags

```
      --catalog string   show only tables in this catalog
      --json             output as JSON
      --schema string    show only tables in this schema
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa workspace](clavesa_workspace.md) — Manage workspaces
- [Command index](README.md)
