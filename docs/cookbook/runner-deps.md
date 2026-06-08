# Add a Python dependency for your UDFs

> **When you have one:** a transform that needs a third-party Python package the runner image doesn't ship — a UA parser like `crawlerdetect`, an IP→ASN lookup like `pyasn`, `humanize`, `python-slugify`, anything on PyPI. SQL UDFs and PySpark transforms both run inside the runner container, so the package has to be installed *in the image*.

Clavesa's runner image ships a fixed base (pyspark, pandas, boto3). Extra packages are workspace config: list them in `.clavesa/runner-requirements.txt` (standard pip `requirements.txt` format) and they install into the image on the next build — local and cloud alike.

> Editing `<workspace>/runner/requirements.txt` does **not** work: that directory is a regenerated mirror of the binary's embedded runner source and is overwritten on every build. Use the requirements file under `.clavesa/` (or the CLI / `/runner` UI page below), which is yours and is version-controlled with the rest of the workspace.

## Prerequisites

- Workspace per the [README quick-start](../../README.md#quick-start) (this recipe reuses the `cookbook` workspace + NYC TLC `src_trips` source). The **Setup** block in [query-your-data](query-your-data.md) lays it down if you don't have it.

## The recipe

Add a dependency. We'll use [`humanize`](https://pypi.org/project/humanize/) — a tiny, dependency-free package not in the base image — to format trip distances into human-readable strings:

```bash
bin/clavesa runner requirements add humanize --workspace $WS
# → Added humanize
#   Applies on the next runner build (clavesa pipeline run locally, or clavesa workspace deploy for cloud).

bin/clavesa runner requirements list --workspace $WS
# REQUIREMENT
# humanize
```

Write a Python transform that imports it. Save as `$WS/demo/transforms/trip_summary.py`:

```python
"""Human-readable trip summaries — proves a third-party import works
inside the runner. `humanize` is not in the base image; it's available
because we added it to .clavesa/runner-requirements.txt."""

import humanize
from pyspark.sql import DataFrame, functions as F, types as T


def transform(spark, inputs) -> dict[str, DataFrame]:
    trips = inputs["trips"]

    @F.udf(returnType=T.StringType())
    def humanize_miles(d):
        if d is None:
            return None
        return humanize.intcomma(round(float(d), 1)) + " mi"

    out = (
        trips.select("trip_distance", "total_amount")
        .where(F.col("trip_distance").isNotNull())
        .withColumn("distance_human", humanize_miles(F.col("trip_distance")))
        .limit(1000)
    )
    return {"default": out}
```

Wire it into the `demo` pipeline downstream of `trips`, point the node at the `.py` file, and run:

```bash
bin/clavesa node add demo --type transform --name trip_summary --workspace $WS
bin/clavesa node connect demo --from trips --to trip_summary --workspace $WS
bin/clavesa node edit demo trip_summary \
  --set "language=python" \
  --set "python=file(transforms/trip_summary.py)" \
  --workspace $WS
bin/clavesa pipeline run demo --workspace $WS
```

`pipeline run` rebuilds the runner image first (docker's layer cache makes the rebuild a fast no-op except for the new `pip install humanize` layer), so the `import humanize` resolves and the transform reports `ok`.

## Verify

```bash
# The dependency is recorded and version-controlled.
bin/clavesa runner requirements show --workspace $WS          # prints `humanize`
test -f $WS/.clavesa/runner-requirements.txt && echo "tracked file present"

# The transform ran with the third-party import available.
bin/clavesa pipeline run demo --workspace $WS                  # trip_summary → ok

# The output table has the humanized column.
bin/clavesa query "SELECT distance_human FROM clavesa_cookbook__demo.trip_summary LIMIT 3" --workspace $WS
# distance_human
# 1.7 mi
# ...
```

If `import humanize` had failed (package not installed), `trip_summary` would report `error` with a `ModuleNotFoundError` — the proof that the dependency reached the image.

## Deploying to the cloud

For a deployed pipeline the same file drives the Lambda image: `clavesa workspace deploy` rebuilds the runner image with your extra requirements and pushes it to ECR, then `clavesa pipeline deploy <pipeline>` re-pins the Lambda to the new digest. Native-extension packages (e.g. `pyasn`) compile for the image's architecture at build time — build on the same architecture your Lambda uses (x86_64 by default).

## UI

The same list is editable at `/runner` in the UI: a textarea in pip `requirements.txt` format with a parsed-package preview. **Save** writes `.clavesa/runner-requirements.txt`; the change applies on the next build, exactly like the CLI.

## Notes

- **Format:** standard pip `requirements.txt` — one spec per line (`pyasn>=1.6`), `#` comments allowed. Paste an existing `requirements.txt` with `clavesa runner requirements import <file>`.
- **Dedupe:** `add` is idempotent per package name — `add pyasn==1.5` after `add pyasn>=1.6` is a no-op; edit the line (or `import`) to change a pin.
- **Reference data vs. code:** ship *libraries* here; ship large *data* files (model weights, IP→ASN databases) to S3 and pull them at runtime with `boto3` (already in the base image) so refreshing them never needs an image rebuild.
