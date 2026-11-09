# Feed a Databricks job from clavesa

> **When you have one:** a team on Databricks whose pipelines have cheap steps (a daily aggregate, a reshape, a source pull) that cost DBUs for no reason. Clavesa runs those steps on Lambda or Fargate, writes the tables into Unity Catalog under the same names, and starts the Databricks jobs that come next. DBUs are paid only for the steps that stay on Databricks ([ADR-027](../decisions/027-unity-catalog-publish.md)).

New to this? Start with the [getting-started guide](../databricks/getting-started.md). [ADR-027](../decisions/027-unity-catalog-publish.md) lists what is proven and what is still open. Table registration and lineage use Databricks Public Preview APIs.

## What you'll end up with

- A clavesa pipeline whose output is the Unity Catalog table `<catalog>.<schema>.<table>`: registered as a new external Delta table, or, when that table already exists as an external table, written in place (owner, grants, table ID and history stay).
- Lineage in Catalog Explorer: the input (a table, an S3 or HTTP source) → the clavesa step → the output table, with the column mapping.
- A Databricks job that clavesa starts once after every successful run, linked from the clavesa run page.

## Prerequisites

- A deployed clavesa workspace on the **cloud warehouse** (`clavesa workspace use --warehouse cloud`). A local warehouse never reaches Unity Catalog.
- A Databricks workspace with Unity Catalog, and a Databricks admin who does the admin's part once. `clavesa workspace connect databricks` checks each step and prints the exact fix for anything missing; nothing below needs you to guess a grant.
  - A **service principal** with an OAuth secret, added to the workspace with the **Workspace access** entitlement.
  - On the metastore: **external data access** on, and `CREATE EXTERNAL METADATA` for the service principal (lineage).
  - An **existing catalog** the service principal can use (`USE CATALOG`, and `USE SCHEMA` / `MODIFY` on what it writes), and an external location over the clavesa bucket for new tables (`--external-location`, with `EXTERNAL USE LOCATION`). **Or**, for a Databricks workspace with no real catalog yet, `--create-catalog` and `CREATE CATALOG`, `CREATE STORAGE CREDENTIAL`, `CREATE EXTERNAL LOCATION` on the metastore: clavesa then creates the catalog, its storage on the workspace bucket and the pipelines' schemas.
- The OAuth secret in AWS Secrets Manager (recommended; the runner reads it there) or in an environment variable (deploy copies it to Secrets Manager for the runner).
- A Databricks job to start, on which the service principal has `CAN MANAGE RUN` (or which it owns).

## The recipe

```bash
# 1. The service principal, by reference: the secret itself stays in
#    Secrets Manager (or use --secret env:DATABRICKS_CLIENT_SECRET).
bin/clavesa credential register dbx-sp --kind databricks-oauth \
  --client-id <service principal application ID> \
  --secret arn:aws:secretsmanager:<region>:<account>:secret:<name>

# 2. Connect. Every admin step is checked; nothing is written until all
#    pass. Drop --create-catalog to use your existing catalog.
bin/clavesa workspace connect databricks \
  --host https://<your-workspace>.cloud.databricks.com \
  --catalog clavesa_demo --credential dbx-sp --create-catalog

# 3. Deploy the workspace: with --create-catalog this creates the catalog,
#    its storage credential and external location, and the grants.
bin/clavesa workspace deploy --yes

# 4. The step clavesa takes over. --schema is the Unity Catalog schema;
#    the node name is the table name: this writes clavesa_demo.orders.daily_orders.
bin/clavesa pipeline create uc_orders --schema orders
bin/clavesa node add uc_orders --type transform --name daily_orders
bin/clavesa node edit uc_orders daily_orders --set "sql=SELECT * FROM VALUES (1, DATE'2026-11-01', CAST(10.50 AS DECIMAL(5,2))), (2, DATE'2026-11-01', CAST(4.25 AS DECIMAL(5,2))), (3, DATE'2026-11-02', CAST(7.00 AS DECIMAL(5,2))) AS t(order_id, order_date, amount)"

# 5. The Databricks job that comes next, started after every successful run.
bin/clavesa pipeline databricks-jobs uc_orders --set <job id>

# 6. Deploy and run.
bin/clavesa pipeline deploy uc_orders --yes
bin/clavesa pipeline run uc_orders --wait
```

The run ends with what it did in Databricks:

```
Status: SUCCEEDED
Unity Catalog: clavesa_demo.orders.daily_orders registered  https://<host>/explore/data/clavesa_demo/orders/daily_orders
Databricks job <job id>: run <run id>  https://<host>/?o=<workspace id>#job/<job id>/run/<run id>
```

A second run says `written` instead of `registered`: the table is clavesa's now and is written in place.

## Taking over an existing table

Give the clavesa output the schema and name of the Databricks table it replaces. Before writing, the runner looks the table up:

| The table in Unity Catalog | What clavesa does |
|---|---|
| Does not exist | Writes its own location and registers a new external table (needs `--external-location`, or `--create-catalog`) |
| An **external** Delta table | Writes **in place**, into the table's own folder. Owner, grants, table ID, history and Databricks lineage stay. The runner role needs write access to that prefix. |
| A **managed** table | The node fails: clavesa cannot write managed tables (ADR-027 section 2). Run in shadow mode and switch by renaming: [replace-a-databricks-step](replace-a-databricks-step.md). |
| Written by another clavesa pipeline | The node fails: one pipeline per table. |

Pause the Databricks step you replace first: two writers corrupt a Delta table. `--output-replaces-databricks-job <job id>` makes every run check that, and write nothing while the job is unpaused or running. For a table partitioned by day use `--output-mode replace_partitions --output-partition-by <column>`: mode `replace` on a partitioned table clavesa did not create is refused, because it would delete every other day. The whole rollout, with a read-only preflight and a daily comparison before anything is switched, is [replace-a-databricks-step](replace-a-databricks-step.md).

When your clavesa step later adds or drops a column, readers see it at once (the schema comes from the Delta log), but Catalog Explorer keeps the old column list. The run prints the one statement an admin runs to refresh it: `REPAIR TABLE <table> SYNC METADATA`.

## Verify

```bash
bin/clavesa workspace databricks --check        # every check ok
bin/clavesa workspace databricks tables         # the table, "written by clavesa", a Catalog Explorer link
bin/clavesa pipeline databricks-run uc_orders <run id from step 6>
```

- **Catalog Explorer** (`https://<host>/explore/data/clavesa_demo/orders/daily_orders`): the table, type External, three columns; the **Lineage** tab shows the clavesa step `clavesa_uc_orders_daily_orders` upstream.
- **Workflows → Job runs**: the run clavesa started, succeeded.
- **clavesa UI**: `/unity-catalog` lists the table with **Published to Unity Catalog** and **Open in Catalog Explorer**; the run page has a **Databricks** card with the table and the job run link.

`make smoke-databricks` walks steps 6 and the checks above against a deployed workspace.

## Limits (first version)

- A **managed** input table is read with credentials Unity Catalog vends for it (`clavesa source register <name> --from uc://<catalog>.<schema>.<table>`). They are set for the table's whole bucket, so the same run must not read or write other paths in that bucket.
- Lineage between two clavesa tables goes through the clavesa step; Databricks has no table-to-table edge for outside writers.
- The column list of a taken-over table goes stale after a column change (above).
- Table registration and lineage are Public Preview; `--publish-tables=false` / `--push-lineage=false` turn either off.
