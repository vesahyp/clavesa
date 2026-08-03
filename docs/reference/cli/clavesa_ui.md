# clavesa ui

Start the visual editor in your browser

Start the UI server and open it in your browser.

The UI reads and writes .tf files in the workspace directory. It renders
pipelines as interactive DAGs and lets you edit nodes, connect edges,
and preview data visually.

Examples:
  clavesa ui
  clavesa ui --workspace /path/to/project
  clavesa ui --no-browser

## Usage

```
clavesa ui [flags]
```

## Flags

```
      --no-browser   do not open the browser automatically
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa](clavesa.md) — Visual ETL for Terraform pipelines
- [Command index](README.md)
