# clavesa dashboards apply

Create or replace a dashboard from a JSON spec file

Create or replace a dashboard from a JSON file. The file is the
datasets-shaped spec (a title, datasets, widgets); the legacy
per-widget-SQL shape is accepted and migrated automatically.

The slug defaults to the file's base name; override with --slug.

## Usage

```
clavesa dashboards apply <file.json> [flags]
```

## Flags

```
      --slug string   dashboard slug (default: file base name)
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa dashboards](clavesa_dashboards.md) — List, inspect, render, and author workspace dashboards
- [Command index](README.md)
