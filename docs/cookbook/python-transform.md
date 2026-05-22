# Python transform

> **When you have one:** logic that's awkward in pure SQL — feature engineering, regex parsing of free-text columns, multi-step DataFrame transforms, anything involving NumPy / pandas operations, or calls into a library that has no SQL equivalent.

Same pipeline skeleton as the SQL recipes; the transform's `language` is `"python"` and the implementation is a function the runner calls with a Spark session and a dict of input DataFrames.

## What you'll end up with

- A pipeline with a Python-backed transform that imports any library on the runner image (pyspark, numpy; add to `runner/requirements.txt` and rebuild for more).
- An Iceberg output table the same shape any SQL transform produces.
- A `transforms/<name>.py` file checked into your workspace alongside the `.tf`, so the logic is reviewable and version-controlled.

## Prerequisites

- Workspace per the [README quick-start](../../README.md#quick-start).
- A source — this recipe reuses the NYC TLC trip data from the README. Substitute your own.

## The recipe

```bash
# 1. Register the source (same as the README quick-start).
bin/clavesa source register trips \
  --from https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet

# 2. Create the pipeline and add a Python transform.
bin/clavesa pipeline create trip_features
bin/clavesa node add trip_features --type transform --name enrich
```

Now write the transform's Python. Save as `<workspace>/trip_features/transforms/enrich.py`:

```python
"""
Trip-feature enrichment — compute z-scored fare amount and a tip-pct
bucket for each trip. The kind of derivation that's painful in SQL
(window function for the z-score, ugly CASE WHEN ladder for the
bucket) but a few lines of pandas-style PySpark.

Clavesa's runner calls transform(spark, inputs) and expects back
{output_key: DataFrame}. Anything you return under "default" lands as
the Iceberg table <node>__default; multiple keys land as separate
tables (see "Multiple outputs" below).
"""

from pyspark.sql import DataFrame, functions as F, Window


def transform(spark, inputs: dict[str, DataFrame]) -> dict[str, DataFrame]:
    trips = inputs["trips"]

    # Window for global stats. With small datasets this is fine; on
    # huge inputs you'd compute mean/std once and broadcast.
    global_stats = Window.partitionBy()

    enriched = (
        trips
        .withColumn("fare_z",
            (F.col("fare_amount") - F.mean("fare_amount").over(global_stats))
            / F.stddev("fare_amount").over(global_stats),
        )
        .withColumn("tip_pct",
            F.when(F.col("fare_amount") > 0,
                   F.col("tip_amount") / F.col("fare_amount")).otherwise(None),
        )
        .withColumn("tip_bucket",
            F.when(F.col("tip_pct").isNull(),             F.lit("unknown"))
             .when(F.col("tip_pct") < 0.05,               F.lit("low"))
             .when(F.col("tip_pct") < 0.15,               F.lit("medium"))
             .when(F.col("tip_pct") < 0.25,               F.lit("generous"))
             .otherwise(                                   F.lit("very_generous")),
        )
    )

    return {"default": enriched}
```

Wire the transform to use that file:

```bash
bin/clavesa node edit trip_features enrich \
  --set "language=python" \
  --set "python=file(transforms/enrich.py)"

bin/clavesa source attach trip_features trips --to enrich --as trips
bin/clavesa pipeline run trip_features
```

`file(transforms/enrich.py)` tells the runner to read the script's contents from that path (relative to the pipeline directory). Inline Python via `--set "python=..."` works for one-liners but quotation-escaping makes it unpleasant past a few lines.

## What you should see

- `pipeline run` reports `enrich` as `ok`.
- `/` (Catalog) shows `enrich__default` with extra columns: `fare_z`, `tip_pct`, `tip_bucket`.
- Click into TableDetail → Sample rows include the derived features. The Lineage panel shows the source flowing in.

## The function contract

The runner imports your script and calls one specific symbol:

```python
def transform(spark, inputs: dict[str, DataFrame]) -> dict[str, DataFrame]:
    ...
```

- `spark`: the `SparkSession` the runner already booted. Reuse it; don't `SparkSession.builder.getOrCreate()` yourself.
- `inputs`: a dict from input alias (the `--as <alias>` you pass to `source attach`, or the `--input <alias>` from `node connect`) to a Spark `DataFrame` representing the upstream source / table. The runner has already done the format-aware reads.
- Return: a dict from output key to `DataFrame`. The runner writes each value to the matching Iceberg table.

Anything Python you can run on the runner image works inside `transform()`. The image includes `pyspark` and `numpy` out of the box. To pull in more — pandas, scikit-learn, your own libraries — add lines to `runner/requirements.txt` and rebuild the image (`make build-runner`).

## Multiple outputs

Return more than one key and each becomes its own Iceberg table. Declare every output key on the transform first so the orchestration emitter passes it through to the runner:

```bash
bin/clavesa node edit trip_features enrich --add-output outliers
```

Then return both keys from `transform()`:

```python
def transform(spark, inputs):
    trips = inputs["trips"]
    return {
        "default":     trips.select("tpep_pickup_datetime", "fare_amount", "fare_z", "tip_bucket"),
        "outliers":    trips.where(F.abs(F.col("fare_z")) > 3),
    }
```

`pipeline run` writes both Iceberg tables (`enrich__default`, `enrich__outliers`); both surface in `/` (Catalog) under your pipeline. The same shape deploys to Lambda or any other cloud target. To tune the write mode per-key (e.g. `outliers` as `append` instead of `replace`), edit `output_definitions` directly in the transform's `.tf`; the CLI flag seeds new entries with the default mode.

The UI equivalent: select the transform in the editor, open the right-panel **Output** section, type the extra key under **Extra outputs**, click **Add output**. Same surface for adding more keys or removing them.

## Extending SQL with a UDF

The DataFrame style above is one flavour of Python transform. The other is: keep the bulk of your logic as SQL, register a Python function so that SQL can call it, run `spark.sql(...)` from inside `transform()`. Useful when one column needs custom logic but the rest of the query is naturally a `SELECT … FROM … JOIN …`.

**First — check the built-ins.** Spark's standard library is bigger than people remember: `regexp_extract`, `regexp_replace`, `from_json`, `to_json`, `sha2`, `md5`, `hash`, `levenshtein`, `aggregate`, `transform` (on arrays), date math, window functions. Most "I need a UDF for X" turns out to be one or two built-ins you didn't know existed. If a built-in fits, use it — it's the fast path, no Python in the data path at all.

**When a built-in doesn't fit, prefer a vectorized UDF.** Spark batches rows into Arrow chunks, ships them to a Python worker, the worker gets a pandas `Series` in and returns one back. Roughly 2–3× slower than native. **Don't** reach for plain `spark.udf.register("foo", lambda x: ...)` — that's per-row Python interpreter overhead, 10–100× slower, useful only when you genuinely need per-row Python state a vectorized UDF can't carry.

Example: parse human-formatted denominations (`"1.5K"`, `"200M"`, `"3.2B"`) into a numeric column. SQL can do it with a stack of `CASE WHEN`s; it's tedious. Vectorized UDF:

```python
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType


@pandas_udf(DoubleType())
def parse_denom(values: pd.Series) -> pd.Series:
    multipliers = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}

    def one(s):
        if not isinstance(s, str) or not s.strip():
            return None
        s = s.strip().upper()
        suffix = s[-1] if s[-1] in multipliers else ""
        try:
            base = float(s[:-1] if suffix else s)
        except ValueError:
            return None
        return base * multipliers.get(suffix, 1)

    return values.map(one)


def transform(spark, inputs: dict[str, DataFrame]) -> dict[str, DataFrame]:
    inputs["sales"].createOrReplaceTempView("sales")
    spark.udf.register("parse_denom", parse_denom)

    out = spark.sql("""
        SELECT
          region,
          ROUND(SUM(parse_denom(revenue_str)), 2) AS revenue_total
        FROM sales
        GROUP BY region
        ORDER BY revenue_total DESC
    """)
    return {"default": out}
```

Two things to notice: the input DataFrame gets registered as a temp view before the SQL runs (`createOrReplaceTempView`), and the UDF gets registered against the active session. After that the body is plain SQL — readable, joinable with other registered functions, easy to extend.

### When to pick this over the DataFrame style

Both run at the same speed, both deploy the same way, both ship in the same `transforms/<name>.py` file. The choice is purely about readability:

- **`spark.sql(...)` with UDFs:** when the transform is mostly a joins/aggregates/filters SQL query and you need one or two custom scalar functions inside it.
- **DataFrame chain (the recipe earlier):** when the transform is multi-step composition — `.withColumn(...).withColumn(...).join(...).agg(...)` — and naming intermediate DataFrames clarifies intent.

Both are real Python transforms. Pick whichever reads better for the shape of the logic.

## When to reach for Python over SQL

- **Multi-step transforms that compose.** SQL forces you into nested subqueries or CTEs; Python lets you name intermediate DataFrames.
- **Regex / free-text parsing.** `F.regexp_extract` works in both, but composing several regexes against the same column is cleaner as a function.
- **NumPy / pandas math.** Anything involving FFT, geospatial, image, embedding, or scikit-learn-style preprocessing.
- **Calls to external libraries.** Loading a serialized model, computing hashes, calling out to an in-process scoring function.

Stick with SQL when the transform is a join / filter / aggregate. SQL is shorter, every database engineer reads it, and the planner has more freedom to optimize.

## Troubleshooting

**`transform() missing 1 required positional argument: 'inputs'`.** Either the signature has the wrong shape (check it matches `transform(spark, inputs)`) or you accidentally defined `def transform(inputs)` without `spark`. The runner passes both positionally.

**`ModuleNotFoundError: No module named 'sklearn'`.** The runner image only ships what's in `runner/requirements.txt` — pyspark, numpy, the AWS Iceberg JARs. Add `scikit-learn` (or whatever) to that file, run `make build-runner`, and re-run.

**Schema-evolution surprises.** Iceberg writes infer the schema from the output DataFrame. If you rename or drop a column, the next write commits a schema-evolution snapshot — downstream readers see the new shape immediately. If that's not what you want, project to the existing schema (`enriched.select("col1", "col2", ...)`) before returning.

**The runner crashes with an OOM.** PySpark on Lambda is memory-bound — the default of 1.5 GB is generous for one transform but tight if you're materializing a wide DataFrame to driver memory. Either avoid `.toPandas()` / `.collect()` (they pull all rows to the driver), or bump `lambda_memory_mb` on the transform module.

## See also

- [multi-stage-pipeline](multi-stage-pipeline.md) — chain a Python silver transform after a SQL bronze ingest.
- [merge-dim-table](merge-dim-table.md) — Python transforms support all the same output modes, including `mode = "merge"` with merge keys.
