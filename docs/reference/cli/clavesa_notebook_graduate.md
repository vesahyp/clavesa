# clavesa notebook graduate

Promote a notebook cell into a transform node

Turn an explored notebook cell into a pipeline transform.

Writes the cell source to <pipeline>/transforms/<transform>.{sql,py}
(stripping the leading %%magic) and registers a new transform node in
the pipeline's main.tf. The cell's language (SQL vs Python) determines
the file extension and the node's language attribute.

The graduated transform has no inputs wired — attach sources or connect
upstream nodes via the editor afterward.

Examples:
  clavesa notebook graduate exploration --cell c2py --to demo --as enrich_orders
  clavesa notebook graduate scratch    --cell 1a2b  --to demo --as revenue

## Usage

```
clavesa notebook graduate <notebook> --cell <id> --to <pipeline> --as <transform> [flags]
```

## Flags

```
      --as string     Transform node name to create
      --cell string   Cell ID to graduate
      --to string     Target pipeline directory (must exist)
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa notebook](clavesa_notebook.md) — Multi-cell SQL + PySpark notebooks (Jupyter .ipynb)
- [Command index](README.md)
