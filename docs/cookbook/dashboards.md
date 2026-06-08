# Build a dashboard

> **When you have one:** a handful of queries you keep re-running, and you want them saved, laid out, and shareable — charts over your pipeline's tables, refreshed on demand.

A clavesa dashboard is a JSON spec: a set of **datasets** (named SQL queries) and **widgets** (charts bound to those datasets). You author it with `dashboards apply`, smoke-test it from the CLI with `dashboards render`, and view it in the UI at `/dashboards/<slug>`. It runs against the same catalog your pipelines write to — local now, Athena once deployed (same spec, [ADR-014](../decisions/014-local-cloud-parity.md)).

> **Continues from** [multi-stage-pipeline](multi-stage-pipeline.md) and [merge-cdf](merge-cdf.md) — it charts the `taxis` and `daily` tables those built. Run their **Setup** blocks first if you're starting cold.

## Setup (self-contained)

You need the `cookbook` workspace with the `taxis` pipeline (`revenue_kpis`, `revenue_by_payment`) and the `daily` pipeline (`daily_revenue`). If you don't have them, run the recipes above, then:

```bash
export WS=/tmp/clavesa-cookbook
export CLAVESA_WORKSPACE=$WS
```

## Write the spec

A dashboard is one JSON file. Save this as `taxi-revenue.json` — **the file name is the slug** (override with `--slug`; the `slug` field inside the JSON is not used by `apply`):

```json
{
  "title": "Taxi Revenue",
  "datasets": [
    {"name": "totals",     "dir": "taxis", "sql": "SELECT total_trips, total_revenue, revenue_per_trip FROM clavesa_cookbook__taxis.revenue_kpis"},
    {"name": "by_payment", "dir": "taxis", "sql": "SELECT CAST(payment_type AS STRING) AS payment_type, revenue FROM clavesa_cookbook__taxis.revenue_by_payment ORDER BY revenue DESC"},
    {"name": "daily",      "dir": "daily", "sql": "SELECT trip_date, revenue FROM clavesa_cookbook__daily.daily_revenue ORDER BY trip_date"}
  ],
  "widgets": [
    {"id": "w_rev",   "type": "big_number", "title": "Total revenue",          "dataset": "totals",     "value_field": "total_revenue", "layout": {"x": 0, "y": 0, "w": 3, "h": 2}},
    {"id": "w_trips", "type": "big_number", "title": "Total trips",            "dataset": "totals",     "value_field": "total_trips",   "layout": {"x": 3, "y": 0, "w": 3, "h": 2}},
    {"id": "w_pay",   "type": "bar",        "title": "Revenue by payment type", "dataset": "by_payment", "x_field": "payment_type", "y_field": "revenue", "layout": {"x": 0, "y": 2, "w": 6, "h": 4}},
    {"id": "w_daily", "type": "line",       "title": "Daily revenue",          "dataset": "daily",      "x_field": "trip_date",    "y_field": "revenue", "layout": {"x": 6, "y": 0, "w": 6, "h": 6}}
  ]
}
```

- **Datasets** are named SQL queries. `dir` is the pipeline whose context the query runs in (it also picks local vs cloud mode); the SQL itself is fully qualified, so a dashboard can pull from several pipelines at once — this one spans `taxis` and `daily`.
- **Widgets** bind a `type` to a `dataset` and map columns to roles. The role fields depend on the type:
  - `big_number` → `value_field`
  - `line` / `bar` / `stacked_bar` / `bar_line` → `x_field`, `y_field` (+ `series_fields` for stacked, `line_field` for bar_line)
  - `pie` / `donut` → `y_field`
  - `table` → no field mapping; shows the dataset as-is
  - `world_map` → `value_field`, `region_field` (ISO country code), optional `tooltip_field`
- **Layout** is a 12-column grid: `x` + `w` must be ≤ 12. Widgets flow into the columns you give them.

## Apply and render

```bash
bin/clavesa dashboards apply taxi-revenue.json --workspace $WS
```

```
Applied dashboard taxi-revenue (3 dataset(s), 4 widget(s))
```

`dashboards render` executes every widget's SQL and prints the results — a CLI smoke test that the queries all run. It **exits non-zero if any widget errors**, so it's safe in CI:

```bash
bin/clavesa dashboards render taxi-revenue --workspace $WS
```

```
Taxi Revenue  (taxi-revenue)

• Total revenue  [big_number]
total_trips  total_revenue  revenue_per_trip
2964624      79456384.28    26.8

• Revenue by payment type  [bar]
payment_type  revenue
1             65533599.31
2             10050669.22
...

• Daily revenue  [line]
trip_date                revenue
2024-01-01T00:00:00.000  2442843.25
...
```

## View it

```bash
bin/clavesa ui --workspace $WS
```

Open `/dashboards/taxi-revenue`. The four widgets render on the grid: **Total revenue** shows `79.46M`, **Total trips** `2.96M`, the bar chart breaks revenue down by payment type (axis to 80M), and the line chart plots all 60 days from January through February. The header reads "3 datasets over 2 pipelines". The same page is the editor — click **Edit** to rearrange widgets or tweak SQL, which writes the spec back to `.clavesa/dashboards/taxi-revenue.json`.

## Controls (optional)

Add interactive filters with a `controls` array. A `time_range` control exposes `{{name.start}}` / `{{name.end}}` placeholders to every dataset's SQL; a `select` control exposes `{{name}}`:

```json
"controls": [
  {"name": "tr", "type": "time_range", "label": "Date range", "default": "last_30d"}
]
```

Then reference it: `... WHERE trip_date BETWEEN '{{tr.start}}' AND '{{tr.end}}'`. From the CLI, pass values with `--param`:

```bash
bin/clavesa dashboards render taxi-revenue --param tr.start=2024-02-01 --param tr.end=2024-02-29 --workspace $WS
```

Missing params fall back to the control's `default`. Select controls take a static `options` list or a `sql` query that populates them.

## Verify

```bash
bin/clavesa dashboards apply taxi-revenue.json --workspace $WS    # → "Applied dashboard taxi-revenue (3 dataset(s), 4 widget(s))"
bin/clavesa dashboards list --workspace $WS                       # → taxi-revenue present
bin/clavesa dashboards render taxi-revenue --workspace $WS ; echo "exit=$?"   # → exit=0, all widgets print rows
```

In the UI: `/dashboards/taxi-revenue` shows all four widgets populated (big numbers `79.46M` / `2.96M`, bar + line charts), and `playwright-cli console error` reports 0.

Assertable signals: `render` exits 0 when every widget's SQL succeeds and **non-zero if any fails** (point a dataset at a missing table to see exit 1); `apply` reports the dataset/widget counts; the spec round-trips through `.clavesa/dashboards/<slug>.json`.

## What to expect — and the limits

- **Slug = file name (or `--slug`).** The `slug` field in the JSON is ignored on `apply`; name the file what you want the slug to be.
- **Local renders through warm Spark; deployed pipelines render through Athena.** The spec is portable, but keep widget SQL Trino-compatible if the dashboard will run against a deployed pipeline (see the dialect note in [query-your-data](query-your-data.md#what-to-expect--and-the-limits)).
- **No automatic row cap.** A `table` widget over a million-row dataset will try to return all of it — add `LIMIT` / aggregate in the dataset SQL.
- **First render pays the Spark warm-up** (tens of seconds), then it's quick.

## Troubleshooting

**`render` exits non-zero / a widget shows an error.** One dataset's SQL failed — `render` prints the failing widget's error. Run that SQL through `clavesa query` to debug it in isolation.

**Widget renders blank in the UI but `render` works on the CLI.** Usually a field-mapping mismatch — e.g. a `line` widget whose `y_field` names a column the dataset doesn't return. Check the widget's role fields against the dataset's columns.

**`apply` created a dashboard with the wrong name.** The slug came from the file name. Rename the file or pass `--slug`.

## Next

You've now gone from a raw URL to a multi-stage pipeline, folded in new data with merge + CDF, explored it in SQL and a notebook, and charted it. That's the round trip. From here, branch into the **real-world ingestion** recipes (S3, HTTP, triggers) in the [cookbook index](README.md).

## See also

- [query-your-data](query-your-data.md) — every widget is a saved query; debug them with `clavesa query`.
- The auto-seeded `pipeline-runs-demo` dashboard (`bin/clavesa dashboards show pipeline-runs-demo`) is a worked example over the `runs` / `node_runs` system tables.
