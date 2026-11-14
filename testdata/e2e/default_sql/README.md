# End-to-end bundle: Databricks' default-sql example, both ways

The bundle of ADR-031's end-to-end test (`make e2e-bundle`,
[docs/cookbook/bundle-both-ways.md](../../../docs/cookbook/bundle-both-ways.md)).

**Source.** The job and its SQL are Databricks' own example, `default_sql` of
<https://github.com/databricks/bundle-examples/tree/1b358c3d3d82c1560aaed60a68647e1b2d5f0fd2/default_sql>
(the bundle `databricks bundle init default-sql` creates), at that pinned
commit. They are under the Databricks License, which allows using and
changing them with the Databricks Services, so they are not copied into this
repository: `prepare.py` downloads them (and the LICENSE) into a work folder
and applies the edits below, each marked in the file it changes.

**What is ours here.** `databricks.yml` (the example's targets point at
`company.databricks.com`; ours deploy to Databricks, to clavesa, and mixed)
and `prepare.py`.

## The edits

1. `resources/default_sql_sql.job.yml`: the `email_notifications` block to
   the placeholder `user@company.com` goes (clavesa would subscribe it to
   SNS, Databricks would mail it).
2. `src/*.sql`: `USE CATALOG \`{{catalog}}\`` becomes
   `USE CATALOG IDENTIFIER({{catalog}})`. Databricks fills a `{{name}}` in a
   SQL file in as a quoted string, so the published line names a catalog
   `'team_ucpub'` with the quotes. (Found on the first live run.)
3. `src/orders_raw.sql`: `CREATE OR REFRESH STREAMING TABLE ... FROM STREAM
   READ_FILES("/databricks-datasets/...", ...)` becomes `CREATE OR REPLACE
   TABLE ... FROM READ_FILES({{source}}, ...)`. A streaming table needs a
   Lakeflow pipeline (clavesa refuses it at plan); the batch read runs on
   both. `/databricks-datasets` exists only inside Databricks, so the input
   is a copy of the same file in S3, the job parameter `source`.
4. `src/orders_daily.sql`: `CREATE OR REPLACE MATERIALIZED VIEW` becomes
   `CREATE OR REPLACE TABLE` (a materialized view runs only in Databricks).

`prepare.py --as-published` skips edits 2 to 4, to see what `clavesa bundle
plan` says about the example as published.

## The targets

- `dbx`: Databricks, the job's SQL tasks on a SQL warehouse (`--var
  warehouse_id=...`), schedule paused.
- `aws`: clavesa in the AWS account of a workspace connected to Databricks,
  `catalog: unity`, shadow mode: the tables come out as `<table>_clavesa`
  next to the Databricks ones, registered in Unity Catalog. Both tasks on
  Lambda.
- `aws-mixed`: `orders_raw` on clavesa, `orders_daily` on Databricks (run-now
  on this target's Databricks job with `only`); `--var
  clavesa_principal=<the service principal's application ID>` lets it run
  that job.

The SQL picks its catalog and schema at run time (`USE ... IDENTIFIER`), so
the clavesa targets list the tables in `clavesa.tables`.
