# Query your data

> **When you have one:** tables built by a pipeline and a question you want to answer right now — no BI tool, no notebook, just SQL in the terminal.

`clavesa query` runs ad-hoc SparkSQL against the tables your pipelines have written, straight from the command line. It's the fastest way to sanity-check what a transform produced, explore a new table, or pull a number for a ticket.

> **Continues from** the [README quick-start](../../README.md#quick-start) — it assumes the `cookbook` workspace with the `demo` pipeline run at least once, so `clavesa_cookbook__demo.trips` and `clavesa_cookbook__demo.revenue_by_payment` exist. If you don't have that state, run the **Setup** block below; if you just finished the quick-start (or [multi-stage-pipeline](multi-stage-pipeline.md)), skip it.

## Setup (self-contained)

```bash
make build                                   # produces ./bin/clavesa
export WS=/tmp/clavesa-cookbook
rm -rf $WS && mkdir -p $WS

bin/clavesa workspace init cookbook --workspace $WS
bin/clavesa source register src_trips \
  --from https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet \
  --workspace $WS

bin/clavesa pipeline create demo --workspace $WS
bin/clavesa node add demo --type transform --name trips --workspace $WS
bin/clavesa source attach demo src_trips --to trips --as src_trips --workspace $WS
bin/clavesa node edit demo trips --set "sql=SELECT * FROM src_trips" --output-stats --workspace $WS
bin/clavesa node add demo --type transform --name revenue_by_payment --workspace $WS
bin/clavesa node connect demo --from trips --to revenue_by_payment --workspace $WS
bin/clavesa node edit demo revenue_by_payment --set "sql=SELECT payment_type, COUNT(*) AS trips, ROUND(SUM(total_amount), 2) AS revenue FROM trips GROUP BY payment_type ORDER BY revenue DESC" --workspace $WS

bin/clavesa pipeline run demo --workspace $WS
```

The rest of this recipe uses `--workspace $WS`. Set `export CLAVESA_WORKSPACE=$WS` once and you can drop the flag.

## See what's there

```bash
bin/clavesa query "SHOW DATABASES" --workspace $WS
```

```
namespace
clavesa_cookbook__demo
clavesa_cookbook_system__pipelines
default
```

Tables address as **`<workspace>__<schema>.<table>`** — three levels flattened into Spark's two-level namespace at the catalog seam ([ADR-016](../decisions/016-catalog-schema-namespace.md), [ADR-018](../decisions/018-delta-table-format.md)). Your workspace is the catalog (`clavesa_cookbook`), your pipeline is the schema (`demo`), so the pipeline's tables live in `clavesa_cookbook__demo`. There's no `__default` suffix on table names — a transform's primary output is just the node name.

```bash
bin/clavesa query "SHOW TABLES IN clavesa_cookbook__demo" --workspace $WS
```

```
namespace               tableName           isTemporary
clavesa_cookbook__demo  revenue_by_payment  false
clavesa_cookbook__demo  trips               false
```

## Run a real query

```bash
bin/clavesa query "
  SELECT payment_type, COUNT(*) AS n
  FROM clavesa_cookbook__demo.trips
  GROUP BY payment_type ORDER BY n DESC" --workspace $WS
```

```
payment_type  n
1             2319046
2             439191
0             140162
4             46628
3             19597
```

Add `--json` when you want to pipe the result into `jq`, a script, or a test assertion. The JSON carries the column types alongside the rows — the same shape the UI's `/query` page consumes ([ADR-015](../decisions/015-cli-ui-parity.md)):

```bash
bin/clavesa query "SELECT payment_type, COUNT(*) AS n FROM clavesa_cookbook__demo.trips GROUP BY payment_type ORDER BY n DESC" --json --workspace $WS
```

```json
{"columns":[{"name":"payment_type","type":"bigint"},{"name":"n","type":"bigint"}],"rows":[["1","2319046"],["2","439191"],["0","140162"],["4","46628"],["3","19597"]],"row_count":5,"truncated":false}
```

You can also pipe SQL in on stdin — handy for longer queries kept in a file:

```bash
bin/clavesa query --workspace $WS < analysis.sql
```

## Join across tables — and across pipelines

Fully-qualified names mean a join is just a join, whether the tables come from the same pipeline or different ones:

```bash
bin/clavesa query "
  SELECT r.payment_type, r.revenue,
         ROUND(100.0 * r.trips / t.total, 1) AS pct_of_trips
  FROM clavesa_cookbook__demo.revenue_by_payment r
  CROSS JOIN (SELECT COUNT(*) AS total FROM clavesa_cookbook__demo.trips) t
  ORDER BY r.revenue DESC" --workspace $WS
```

```
payment_type  revenue      pct_of_trips
1             65533599.31  78.2
2             10050669.22  14.8
0             3617824.63   4.7
3             171581.04    0.7
4             82710.08     1.6
```

A **cross-pipeline** join looks identical — qualify the other table with its own pipeline's schema, e.g. `clavesa_cookbook__marketing.campaigns`. Cross-pipeline *reads* are first-class; only cross-pipeline *writes* are disallowed ([ADR-016](../decisions/016-catalog-schema-namespace.md)).

The workspace's own observability tables are queryable the same way — they live in `clavesa_cookbook_system__pipelines`:

```bash
bin/clavesa query "SHOW TABLES IN clavesa_cookbook_system__pipelines" --workspace $WS
```

```
namespace                           tableName     isTemporary
clavesa_cookbook_system__pipelines  column_stats  false
clavesa_cookbook_system__pipelines  node_runs     false
clavesa_cookbook_system__pipelines  runs          false
clavesa_cookbook_system__pipelines  tables        false
```

So "how long did each node take on the last run?" is a query, not a separate stack: `SELECT node, status, duration_ms FROM clavesa_cookbook_system__pipelines.node_runs ORDER BY started_at DESC`.

## Check a query parses without running it

`clavesa sql lint` parse-checks a `.sql` file against the same Spark parser, without executing it. Exit 0 and silent on success; exit 1 with a caret pointing at the problem on failure:

```bash
bin/clavesa sql lint analysis.sql --workspace $WS
```

```
analysis.sql: SQL parse failed

[PARSE_SYNTAX_ERROR] Syntax error at or near '('. SQLSTATE: 42601 (line 1, pos 25)

== SQL ==
SELECT payment_type COUNT(*) FROM trips GROUP forgot BY
-------------------------^^^
```

It's a good pre-commit / CI gate for transform SQL: `find . -name '*.sql' -exec bin/clavesa sql lint {} --workspace $WS \;` fails the build on the first unparseable file.

## What to expect — and the limits

- **The engine follows the workspace warehouse** ([ADR-024](../decisions/024-warehouse-compute-split.md)). On a local warehouse (the default), `clavesa query` runs through the warm Spark container against your workspace's local Hadoop catalog — the full SparkSQL/Databricks dialect: `MERGE`, Delta time-travel (`SELECT … VERSION AS OF 3`), `read_files`, the lot. On a cloud warehouse, the same command queries the deployed tables through Athena — exactly what the UI's `/query` page does. Pass `--warehouse local|cloud` to override for one invocation.
- **You author SparkSQL either way.** Athena speaks Trino, not Spark, but you don't hand-maintain a second dialect: on a cloud warehouse your SparkSQL is transpiled to the Trino serving dialect automatically ([ADR-023](../decisions/023-single-serving-dialect-spark-transpile.md)). A construct with no Trino mapping fails with a clear dialect error (pointing at the offending spot) instead of an Athena syntax error — keep truly Spark-only operations like `MERGE` in transforms, where the engine is always Spark.
- **No automatic row cap.** Results stream back in full — your SQL supplies the `LIMIT`. `SELECT * FROM clavesa_cookbook__demo.trips` will try to print 3 million rows. Add `LIMIT`.
- **First local query pays a warm-up.** On a local warehouse the Spark container spins up on the first query in a session (tens of seconds); every query after that is sub-second until the workspace's `clavesa ui`/query worker is torn down. Cloud queries have no Spark warm-up — Athena charges per query instead.
- **Errors lead with the Spark message and exit non-zero** (see below) — so `clavesa query` is safe to use as a check in a script.

## Verify

```bash
# Databases include the demo schema and the system schema → exit 0.
bin/clavesa query "SHOW TABLES IN clavesa_cookbook__demo" --json --workspace $WS
# Expect rows for `trips` and `revenue_by_payment`.

# Aggregate returns the five payment types, bigint counts, no scientific notation.
bin/clavesa query "SELECT COUNT(*) AS c FROM clavesa_cookbook__demo.trips" --json --workspace $WS
# Expect: {"columns":[{"name":"c","type":"bigint"}],"rows":[["2964624"]],"row_count":1,"truncated":false}

# Good SQL lints clean and silent.
printf 'SELECT 1\n' > /tmp/ok.sql
bin/clavesa sql lint /tmp/ok.sql --workspace $WS ; echo "exit=$?"     # exit=0

# Bad SQL fails with a caret and a non-zero exit.
printf 'SELECT a b c FROM\n' > /tmp/bad.sql
bin/clavesa sql lint /tmp/bad.sql --workspace $WS ; echo "exit=$?"    # exit=1

# A missing table errors with the Spark message (not a Java stack dump) and exits non-zero.
bin/clavesa query "SELECT * FROM clavesa_cookbook__demo.nope LIMIT 1" --workspace $WS ; echo "exit=$?"  # exit=1
```

Assertable signals an agent or CI can rely on: `sql lint` exits 0 on parse-clean SQL and 1 otherwise; `query` exits non-zero on any execution error; `--json` output is stable `{columns: [{name, type}], rows, row_count, truncated}` — the same shape from both warehouses and from the UI's `/query` endpoint. Row values are strings (`jq` tip: `.rows[0][0] | tonumber`). The exact row counts above are deterministic for `yellow_tripdata_2024-01.parquet` (2,964,624 rows); a different month will differ.

## Troubleshooting

**`[TABLE_OR_VIEW_NOT_FOUND]`.** The name is wrong or unqualified. Run `SHOW DATABASES` then `SHOW TABLES IN <db>` to get the exact `<workspace>__<schema>.<table>` spelling. Remember there's no `__default` suffix.

**First query hangs for ~30s, then works.** That's the one-time Spark warm-up, not a stall. Subsequent queries in the same session are instant.

**`SELECT *` floods the terminal.** No implicit row cap — add `LIMIT`.

**A query works on the local warehouse but fails with a dialect error on cloud.** The transpiler maps most SparkSQL to Trino automatically ([ADR-023](../decisions/023-single-serving-dialect-spark-transpile.md)); a dialect error means that construct has no Trino equivalent on Athena. Reserve those (e.g. `MERGE`, Delta time-travel) for transforms — the transform engine is always Spark — or run the query with `--warehouse local`.

## Next

- **[Explore in a notebook](notebooks.md)** — when one query becomes a session of them, with Python in the mix.
- **[Build a dashboard](dashboards.md)** — when a query is worth keeping and sharing as a widget.
