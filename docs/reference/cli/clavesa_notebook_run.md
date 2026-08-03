# clavesa notebook run

Run every cell (or one cell with --cell) and persist outputs

Run cells through the warm Spark Connect container.

If --cell <id> is given, only that cell runs. Otherwise every code cell
runs sequentially in notebook order, in the SAME REPL subprocess —
SparkSession + Python globals persist across cells, matching what the UI
gives you.

This spawns its own warm worker container if one isn't already running,
which adds ~30s of Spark cold start on first invocation. Subsequent runs
in the same workspace reuse the running container.

## Usage

```
clavesa notebook run <name> [flags]
```

## Flags

```
      --cell string   Run only this cell ID (default: all code cells)
      --json          Emit CellResult[] JSON
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa notebook](clavesa_notebook.md) — Multi-cell SQL + PySpark notebooks (Jupyter .ipynb)
- [Command index](README.md)
