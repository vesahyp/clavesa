# clavesa dashboards

List, inspect, render, and author workspace dashboards

Manage workspace dashboards — saved SQL widgets over the catalog.

Dashboards are stored as JSON specs under the workspace's
`.clavesa/dashboards/` directory (one file per dashboard), shared with
everyone who has the workspace checked out.

Examples:
  clavesa dashboards list
  clavesa dashboards show pipeline-runs-demo
  clavesa dashboards render pipeline-runs-demo --json
  clavesa dashboards apply revenue.json
  clavesa dashboards delete revenue

## Usage

```
clavesa dashboards
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## Subcommands

- [clavesa dashboards apply](clavesa_dashboards_apply.md) — Create or replace a dashboard from a JSON spec file
- [clavesa dashboards delete](clavesa_dashboards_delete.md) — Delete a dashboard
- [clavesa dashboards list](clavesa_dashboards_list.md) — List dashboards
- [clavesa dashboards render](clavesa_dashboards_render.md) — Execute every widget's dataset and print the results
- [clavesa dashboards show](clavesa_dashboards_show.md) — Show a dashboard's datasets and widgets

## See also

- [clavesa](clavesa.md) — Visual ETL for Terraform pipelines
- [Command index](README.md)
