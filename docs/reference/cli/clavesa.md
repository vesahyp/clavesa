# clavesa

Visual ETL for Terraform pipelines

Clavesa — visual ETL for Terraform pipelines

Pipelines are Terraform. The visual UI reads and writes .tf files directly.

Quick start:
  clavesa workspace init my-project        # scaffold a workspace
  clavesa pipeline create my-pipeline      # add a pipeline
  clavesa source register trips --from https://example.com/data.parquet
  clavesa node add my-pipeline --type transform
  clavesa source attach my-pipeline trips --to transform1 --as trips
  clavesa node edit my-pipeline transform1 --set sql="SELECT * FROM trips"
  clavesa node preview my-pipeline transform1
  clavesa ui                               # open the visual editor

## Flags

```
      --workspace string   workspace root directory (default: current directory)
```

## Subcommands

- [clavesa credential](clavesa_credential.md) — Manage workspace credentials (registry)
- [clavesa dashboards](clavesa_dashboards.md) — List, inspect, render, and author workspace dashboards
- [clavesa deploy](clavesa_deploy.md) — Apply the workspace infra and every pipeline in one pass
- [clavesa node](clavesa_node.md) — Manage pipeline nodes and edges
- [clavesa notebook](clavesa_notebook.md) — Multi-cell SQL + PySpark notebooks (Jupyter .ipynb)
- [clavesa pipeline](clavesa_pipeline.md) — Manage pipelines
- [clavesa plan](clavesa_plan.md) — Plan the workspace infra and every pipeline (no apply)
- [clavesa query](clavesa_query.md) — Run an ad-hoc SQL query against the workspace catalog
- [clavesa runner](clavesa_runner.md) — Manage the PySpark runner image (Python deps, etc.)
- [clavesa source](clavesa_source.md) — Manage workspace input sources (registry)
- [clavesa sql](clavesa_sql.md) — SparkSQL tooling (parse-check, lint)
- [clavesa ui](clavesa_ui.md) — Start the visual editor in your browser
- [clavesa version](clavesa_version.md) — Print the clavesa version
- [clavesa workspace](clavesa_workspace.md) — Manage workspaces

## See also

- [Command index](README.md)
