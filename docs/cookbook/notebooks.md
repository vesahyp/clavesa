# Explore in a notebook

> **When you have one:** a question that takes more than one query — you want to poke at a table, keep some state, try a PySpark transformation, and only *then* decide what becomes a pipeline transform.

`clavesa notebook` gives you a Databricks-style multi-cell scratchpad — mixed SQL and PySpark — that runs against the same warm Spark session your queries and pipelines use. Notebooks are real `.ipynb` files, so GitHub renders them and JupyterLab opens them offline. When a cell earns its place, **graduate** it straight into a pipeline transform.

> **Continues from** [query-your-data](query-your-data.md): same `cookbook` workspace, and it reads the `demo` and `taxis` tables those recipes built. The **Setup** block rebuilds them if you're starting cold.

## Setup (self-contained)

If you don't already have the `cookbook` workspace with the `demo` and `taxis` pipelines, run the setup blocks from [query-your-data](query-your-data.md#setup-self-contained) and [multi-stage-pipeline](multi-stage-pipeline.md#setup-self-contained) first. Then:

```bash
export WS=/tmp/clavesa-cookbook
export CLAVESA_WORKSPACE=$WS
```

## Create a notebook

```bash
bin/clavesa notebook create exploration --workspace $WS
```

This writes an empty `notebooks/exploration.ipynb`.

## Author cells

The natural way to author cells is the **UI editor**: run `bin/clavesa ui --workspace $WS`, open `/notebooks`, pick `exploration`, and add cells — a `%%sql` cell runs SQL, a plain cell runs Python. (There's no CLI command to add a cell today; authoring is UI-or-file. See [Limits](#what-to-expect--and-the-limits).)

For a reproducible, copy-pasteable recipe, here's the same notebook as the `.ipynb` on disk — two cells, one SQL and one PySpark. Note that cell `source` must be a **list of lines** (not a single string):

```json
{
 "nbformat": 4, "nbformat_minor": 5,
 "metadata": {"kernelspec": {"name": "clavesa-pyspark", "display_name": "Clavesa (PySpark)"},
              "clavesa": {"format_version": 1}},
 "cells": [
  {"cell_type": "code", "id": "c1sql", "metadata": {}, "execution_count": null, "outputs": [],
   "source": ["%%sql\n",
              "SELECT payment_type, COUNT(*) AS n\n",
              "FROM clavesa_cookbook__demo.trips\n",
              "GROUP BY payment_type ORDER BY n DESC"]},
  {"cell_type": "code", "id": "c2py", "metadata": {}, "execution_count": null, "outputs": [],
   "source": ["df = spark.table(\"clavesa_cookbook__taxis.revenue_by_payment\")\n",
              "total = df.selectExpr(\"round(sum(revenue), 2) AS r\").collect()[0][\"r\"]\n",
              "print(f\"total revenue: {total}\")"]}
 ]
}
```

`spark` is the live SparkSession, injected into every Python cell. SQL cells start with the `%%sql` magic. Python globals and SQL temp views persist across cells in the same notebook, so cell 2 can use anything cell 1 defined.

## Run it

```bash
bin/clavesa notebook show exploration --workspace $WS
```

```
notebook exploration · 2 cells

# Cell 1 · code/sql · id=c1sql · last=—
%%sql
SELECT payment_type, COUNT(*) AS n
FROM clavesa_cookbook__demo.trips
GROUP BY payment_type ORDER BY n DESC

# Cell 2 · code/python · id=c2py · last=—
df = spark.table("clavesa_cookbook__taxis.revenue_by_payment")
...
```

```bash
bin/clavesa notebook run exploration --workspace $WS
```

```
cell c1sql · ok · 21402ms
cell c2py · ok · 4972ms
```

Outputs are written back into the `.ipynb` (so the file carries its results, like any Jupyter notebook). The first cell pays the one-time Spark warm-up; the rest are quick. Run a single cell with `--cell`, and `--json` to capture the result programmatically:

```bash
bin/clavesa notebook run exploration --cell c2py --json --workspace $WS
```

```json
[{"cell_id":"c2py","result":{"status":"ok","duration_ms":16873,"stdout":"total revenue: 79456384.28\n","stderr":"","display":{"type":"none","text_repr":""}}}]
```

That `total revenue: 79456384.28` matches the gold KPI from [multi-stage-pipeline](multi-stage-pipeline.md) — the notebook and the pipeline run the same engine over the same tables.

## Graduate a cell into a transform

When a cell is worth keeping, promote it into a pipeline transform — no copy-paste:

```bash
bin/clavesa notebook graduate exploration --cell c1sql --to demo --as payment_counts --workspace $WS
```

```
Graduated exploration/c1sql → demo/transforms/payment_counts
```

That strips the `%%sql` magic, writes the cell body to `demo/transforms/payment_counts.sql`, and registers a `payment_counts` transform node in the `demo` pipeline. SQL cells become `.sql` transforms; Python cells become `.py` transforms with `language = "python"`. The new node has **no inputs wired** — attach a source or connect an upstream node in the editor (or with `node connect`) before running it.

```bash
bin/clavesa node list demo --workspace $WS
# → trips, revenue_by_payment, payment_counts
```

## What to expect — and the limits

- **Local-only.** Notebooks run through the warm Spark container against your workspace's local catalog. There's no cloud notebook execution; explore locally, then `graduate` and `pipeline deploy`.
- **Cells run sequentially and block.** `notebook run` executes top-to-bottom; there's no async/REPL stepping. One warm session per notebook keeps Python globals and temp views alive across cells.
- **First run is slow.** The Spark session cold-starts on the first cell (~20s); later cells and re-runs are fast until the session is stopped (`notebook session stop exploration`).
- **Cell authoring is UI-or-file.** `notebook create` makes an empty notebook; there's no `notebook add-cell` command yet. Author in the `/notebooks` editor, or edit the `.ipynb` directly. When editing the file, `source` must be a JSON **array of strings**, one per line — a single string is rejected.
- **`clear-outputs` before committing.** `bin/clavesa notebook clear-outputs exploration` strips outputs so the `.ipynb` diffs cleanly in git, the same as `jupyter nbconvert --clear-output`.

## Verify

```bash
bin/clavesa notebook create scratch --workspace $WS      # creates notebooks/scratch.ipynb
bin/clavesa notebook list --workspace $WS                # → lists exploration, scratch
bin/clavesa notebook run exploration --workspace $WS     # → both cells report `ok`
bin/clavesa notebook run exploration --cell c2py --json --workspace $WS
#   → result.status == "ok", stdout contains "total revenue: 79456384.28"
bin/clavesa notebook graduate exploration --cell c1sql --to demo --as payment_counts --workspace $WS
bin/clavesa node list demo --workspace $WS                # → payment_counts now present
```

Assertable signals: `notebook run` reports `ok` per cell and exits 0; `--json` gives `result.status` per cell; `graduate` creates `<pipeline>/transforms/<name>.{sql,py}` and the node shows up in `node list`.

## Troubleshooting

**`cannot unmarshal string into Go struct field Cell.cells.source`.** A cell's `source` is a plain string; it must be a JSON array of line strings. Re-author in the UI editor (which always writes the right shape) or split the string into `["line1\n", "line2"]`.

**A cell hangs ~20s on the first run.** That's the Spark warm-up, not a stall. Subsequent cells are fast until you `notebook session stop`.

**`graduate` says the target pipeline doesn't exist.** `--to` must name an existing pipeline directory. Create it first with `pipeline create`.

## Next

- **[Build a dashboard](dashboards.md)** — turn the queries you explored here into saved, shareable widgets.

## See also

- [query-your-data](query-your-data.md) — for one-off SQL, skip the notebook and use `clavesa query`.
- [python-transform](python-transform.md) — the `transform(spark, inputs)` contract a graduated Python cell becomes.
