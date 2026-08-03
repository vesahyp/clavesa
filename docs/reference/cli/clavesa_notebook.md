# clavesa notebook

Multi-cell SQL + PySpark notebooks (Jupyter .ipynb)

Manage workspace notebooks — Databricks-style multi-cell SQL + PySpark.

Notebooks live as .ipynb files under <workspace>/notebooks/, so GitHub
renders them natively and JupyterLab can open them offline. Cells run
against the warm Spark Connect server with per-notebook SparkSession
isolation; Python globals and SQL temp views persist across cells in
the same notebook.

Examples:
  clavesa notebook create exploration
  clavesa notebook list
  clavesa notebook show exploration
  clavesa notebook run exploration
  clavesa notebook run exploration --cell <id> --json
  clavesa notebook session stop exploration
  clavesa notebook clear-outputs exploration   # git-friendly commit prep
  clavesa notebook delete exploration

## Usage

```
clavesa notebook
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## Subcommands

- [clavesa notebook clear-outputs](clavesa_notebook_clear-outputs.md) — Clear all cell outputs (matches `jupyter nbconvert --clear-output`)
- [clavesa notebook create](clavesa_notebook_create.md) — Create an empty notebook
- [clavesa notebook delete](clavesa_notebook_delete.md) — Delete a notebook (also stops its REPL if running)
- [clavesa notebook graduate](clavesa_notebook_graduate.md) — Promote a notebook cell into a transform node
- [clavesa notebook list](clavesa_notebook_list.md) — List notebooks in the workspace
- [clavesa notebook run](clavesa_notebook_run.md) — Run every cell (or one cell with --cell) and persist outputs
- [clavesa notebook session](clavesa_notebook_session.md) — Manage notebook REPL sessions
- [clavesa notebook show](clavesa_notebook_show.md) — Print the notebook (cell sources, no outputs)

## See also

- [clavesa](clavesa.md) — Visual ETL for Terraform pipelines
- [Command index](README.md)
