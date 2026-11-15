# Databricks bundles on clavesa

> **For:** a team whose Databricks jobs are defined in a Databricks Asset Bundle (`databricks.yml`). The same bundle deploys a job to Databricks, to clavesa in your own AWS account, or to both. This page is the user guide for `clavesa bundle`. The design is [ADR-031](../decisions/031-dab-bundles-on-clavesa.md).

Two recipes walk it step by step: [move-a-databricks-job](../cookbook/move-a-databricks-job.md) (an example bundle from the plan to a run on AWS) and [bundle-both-ways](../cookbook/bundle-both-ways.md) (Databricks' own `default-sql` example on Databricks and on clavesa, the tables compared). For the connection to Databricks, the service principal and the grants, read [getting-started](getting-started.md), steps 3 to 6.

## Contents

1. [A clavesa target](#1-a-clavesa-target)
2. [The commands](#2-the-commands)
3. [Plan](#3-plan)
4. [Deploy, run, destroy](#4-deploy-run-destroy)
5. [Unity Catalog and shadow tables](#5-unity-catalog-and-shadow-tables)
6. [Compare and switch](#6-compare-and-switch)
7. [Mixed jobs](#7-mixed-jobs)
8. [Lineage in Catalog Explorer](#8-lineage-in-catalog-explorer)
9. [Table maintenance](#9-table-maintenance)
10. [The end-to-end result](#10-the-end-to-end-result)

## 1. A clavesa target

A clavesa target is an ordinary target of the team's `databricks.yml` that sets the complex bundle variable `clavesa`. Databricks ignores the variable. The bundle declares it once:

```yaml
variables:
  clavesa:
    description: Settings for clavesa deploys (ignored by Databricks)
    type: complex
    default: {}
```

The `aws` target of the end-to-end bundle ([testdata/e2e/default_sql/databricks.yml](../../testdata/e2e/default_sql/databricks.yml)):

```yaml
  aws:
    mode: production
    workspace:
      host: https://<your workspace>.cloud.databricks.com
    presets:
      trigger_pause_status: PAUSED
    variables:
      warehouse_id: "-"
      clavesa:
        catalog: unity        # write to Unity Catalog (glue: Glue Data Catalog only)
        shadow: true          # write <table>_clavesa next to each Databricks table
        tables:               # the tables the SQL picks at run time
          orders_raw:
            writes: [team_ucpub.sales.orders_raw]
          orders_daily:
            reads: [team_ucpub.sales.orders_raw]
            writes: [team_ucpub.sales.orders_daily]
        compare:              # what bundle compare checks, per table
          team_ucpub.sales.orders_raw: { partition: order_date, sums: [order_number] }
          team_ucpub.sales.orders_daily: { partition: order_date, sums: [number_of_orders] }
        compute:
          orders_raw: { kind: lambda }
          orders_daily: { kind: lambda }
```

An unknown key or task name in `clavesa` makes the plan refuse.

## 2. The commands

Every command takes `--bundle <folder>` (default: the current folder), `-t <target>` (default: the target with `default: true`) and `--var name=value`.

| Command | What it does | Reference |
|---|---|---|
| `clavesa bundle plan` | Reads the bundle the way the Databricks CLI does and prints every task: where it runs, and ok, warn or refuse with the file, line and fix. No login, no network: it runs in CI. Exit 1 on a refuse. | [plan](../reference/cli/clavesa_bundle_plan.md) |
| `clavesa bundle deploy` | Generates one clavesa pipeline per job and deploys it with Terraform. `--plan-only`, `--emit-only`, `--yes`. | [deploy](../reference/cli/clavesa_bundle_deploy.md) |
| `clavesa bundle run` | Runs a job: `--compute local` in the Docker runner, or the deployed job (`--wait` blocks until it ends). `--param name=value` sets a job parameter. | [run](../reference/cli/clavesa_bundle_run.md) |
| `clavesa bundle compare` | Compares each table the job writes on Databricks with its shadow table: rows and sums per partition, and the columns. PASS or FAIL, exit 1 on FAIL. No DBUs. | [compare](../reference/cli/clavesa_bundle_compare.md) |
| `clavesa bundle switch` | Prints the switch from Databricks to clavesa, table by table, with the exact SQL. Runs nothing. | [switch](../reference/cli/clavesa_bundle_switch.md) |
| `clavesa bundle destroy` | `terraform destroy` of the jobs' pipelines. Keeps the tables. | [destroy](../reference/cli/clavesa_bundle_destroy.md) |
| `clavesa bundle list` | Lists the workspace's pipelines generated from bundle jobs. | [list](../reference/cli/clavesa_bundle_list.md) |

The clavesa UI has the same plan and list on its **Bundles** page (`clavesa ui`, then **Bundles** in the sidebar).

## 3. Plan

```bash
clavesa bundle plan --bundle . -t aws
```

```
bundle default_sql, target aws (catalog: unity, shadow)

default_sql_sql_job  (trigger: periodic (paused), 1 segment, Lambda 3008 MB)
  orders_raw    sql        clavesa     ok
  orders_daily  sql        clavesa     ok

Tables of default_sql_sql_job:
  orders_raw    writes team_ucpub.sales.orders_raw
  orders_daily  writes team_ucpub.sales.orders_daily; reads team_ucpub.sales.orders_raw

Ready: yes.  0 refuse, 0 warn.
```

The plan refuses Databricks-only code with the rewrite: streaming tables, materialized views, `STREAM read_files`, `ai_*` functions, notebook magics, `import dlt`, Auto Loader and the rest of the list in [`internal/bundle/databricks_only.txt`](../../internal/bundle/databricks_only.txt). It also lists the tables each task reads and writes, sizes the compute from the job's clusters, and lists the secrets with their Secrets Manager names. [ADR-031](../decisions/031-dab-bundles-on-clavesa.md) section 5 has the full list of task types.

## 4. Deploy, run, destroy

```bash
clavesa bundle deploy --bundle . -t aws --plan-only   # read the Terraform plan
clavesa bundle deploy --bundle . -t aws --yes
clavesa bundle run --bundle . -t aws --job default_sql_sql_job --wait
```

- Each job becomes the pipeline `<bundle>_<target>_<job>`, for example `default_sql_aws_default_sql_sql_job`. Clavesa v2.25.0 named it `<bundle>_<job>`. Since v2.25.1 the target is in the name, so two clavesa targets of one bundle are two pipelines. A pipeline from v2.25.0 stays as it is: remove it with `clavesa pipeline destroy <name>` when you no longer need it.
- The generated pipeline is read-only. To change it, change the bundle and deploy again.
- A paused schedule (`trigger_pause_status: PAUSED`) is deployed as a disabled EventBridge Scheduler schedule. Nothing runs until you run it or unpause it.
- The workspace must be deployed first (`clavesa workspace deploy`). After a clavesa upgrade, deploy the workspace again so that the runner image of the new version is in ECR.
- `clavesa bundle destroy -t aws --yes` removes the AWS side. The tables stay, because they are data. It prints the SQL that drops the shadow tables. It does not remove the lineage objects in Unity Catalog ([GH #108](https://github.com/vesahyp/clavesa/issues/108)): section 8 has the command.

## 5. Unity Catalog and shadow tables

With `catalog: unity` the workspace must be connected to Databricks (`clavesa workspace connect databricks`, [getting-started](getting-started.md) step 6).

- In shadow mode (`shadow: true`, the default for `catalog: unity`), every table the job writes is written as `<table>_clavesa` in the same schema, also when a later task of the job reads it. The Databricks job keeps its own tables. Nothing changes for their readers.
- A shadow table is an **external** Delta table at a location in the workspace bucket. Clavesa registers it in Unity Catalog after the task that first writes it, and writes it in place after that. Clavesa can write external Delta tables only. A managed table is refused; the switch (section 6) is the way to replace one.
- Tables the job reads but does not write are read from Unity Catalog. A managed table is read with credentials that Unity Catalog vends.
- One writer per table: a table carries the owner that wrote it (`clavesa.pipeline`), and a second owner is refused. For a job from a bundle, the owner is the job, written `<bundle>_<job>`. So the clavesa targets of one job (a shadow target and a mixed target) write the same shadow tables, and tables written by v2.25.0 keep their owner (since v2.25.2).

## 6. Compare and switch

```bash
clavesa bundle compare --bundle . -t aws --job default_sql_sql_job [--days 7]
```

compares each table with its shadow table: rows and the sums of the `sums` columns for each value of `partition`, and the columns. The job must be deployed: compare runs the runner container on your machine (Docker) with the settings of the deployed pipeline's runner, and reads the Databricks tables with credentials that Unity Catalog vends. So it costs no DBUs. Run it daily; exit status 1 on FAIL can start an alert.

```bash
clavesa bundle switch --bundle . -t aws --job default_sql_sql_job --databricks-target dbx
```

prints, in order, the steps that move the job's tables to clavesa: the history copy, pausing the Databricks job, the name swap, the grants, and the bundle change (`shadow: false`, `replaces_job: <job id>`). Clavesa runs none of them. After the switch, a run refuses to write while the Databricks job is unpaused or running. The roll-back is in [replace-a-databricks-step](../cookbook/replace-a-databricks-step.md).

## 7. Mixed jobs

A task that clavesa does not run (a Lakeflow pipeline, and the other types in ADR-031 section 5) stays on Databricks. `clavesa.place: { <task>: databricks }` puts any task there. The job is then split into segments:

- Deploy the job to Databricks first, from the same target, with its schedule paused (`databricks bundle deploy -t <target>`). Give clavesa's service principal `CAN_RUN` on it (`permissions` in the target).
- `clavesa bundle deploy` finds that job and builds a state machine. It runs each clavesa segment in the runner Lambda. It runs each Databricks segment with run-now on that job, with `only` its tasks, and waits with a Wait and `runs/get` loop (waiting costs nothing).
- Task states go across segments, so `run_if` and outcomes work. Task values cannot go across; the plan refuses that.
- A mixed job's clavesa segments run on Lambda only (Fargate segments are in TODO.md).

In the end-to-end run the Databricks run of the mixed job showed `orders_raw` DISABLED (not in `only`) and `orders_daily` SUCCESS, started by clavesa.

## 8. Lineage in Catalog Explorer

With `push_lineage` on (the default of `clavesa workspace connect databricks`), each clavesa task that writes a table pushes lineage to Unity Catalog. What you see and what it needs:

1. **It uses Databricks external lineage, which is Public Preview.** The Databricks API reference marks every external lineage endpoint "Public Preview" ([API reference](https://docs.databricks.com/api/workspace/externallineage); [user guide](https://docs.databricks.com/aws/data-governance/unity-catalog/external-lineage)). The API can change. `--push-lineage=false` on `clavesa workspace connect databricks` turns the push off.
2. **In the Lineage graph, each clavesa task is an external object of type `clavesa transform`, between the tables.** Its name is `clavesa_<pipeline>_<task>`, for example `clavesa_default_sql_aws_default_sql_sql_job_orders_daily`. The edge from the task to the table it writes has a column mapping: each column of the table comes from the column of the same name on the task. The edges from the input tables to the task have no column mapping. The task is not shown as a Databricks job, because it does not run in Databricks. The table lineage API (`GET /api/2.0/lineage-tracking/table-lineage`) returns nothing for a clavesa table: for `team_ucpub.sales.orders_daily_clavesa` it returned `{}` on 2026-11-15. Only the external lineage API (`GET /api/2.0/lineage-tracking/external-lineage`) returns the edges. Databricks also does not write external lineage to the lineage system tables (`system.access.table_lineage`, `system.access.column_lineage`).
3. **It needs `CREATE EXTERNAL METADATA` on the metastore, and `MODIFY` on each table it writes**, for clavesa's service principal. The preflight (`clavesa workspace databricks preflight`, [getting-started](getting-started.md) step 4) checks the first and prints the GRANT for the admin.

Read the edges of a table with the Databricks CLI:

```bash
databricks api get "/api/2.0/lineage-tracking/external-lineage?object_info.table.name=team_ucpub.sales.orders_daily_clavesa&lineage_direction=UPSTREAM"
```

Destroy does not remove the objects yet ([GH #108](https://github.com/vesahyp/clavesa/issues/108)). Remove an object, and with it its edges:

```bash
databricks api delete /api/2.0/lineage-tracking/external-metadata/clavesa_<pipeline>_<task>
```

Clavesa's service principal creates the objects, so it owns them. Run the call as the service principal (a Databricks CLI profile with its OAuth client ID and secret). A workspace admin without `MANAGE` on the object gets `User does not have MANAGE on External Metadata '<name>'`.

A metastore holds at most 10,000 external metadata objects and 100,000 external lineage relationships.

## 9. Table maintenance

A Delta table needs two kinds of maintenance: OPTIMIZE puts small files together (and with `ZORDER BY`, sorts the data by the columns you give), and VACUUM deletes the files that the table no longer uses. You can do both on a pipeline from a bundle in two ways. Each was tested on the end-to-end pipeline `default_sql_aws_default_sql_sql_job` on 2026-11-15 (v2.25.2):

**`clavesa pipeline optimize`** compacts each table that the job's clavesa tasks write. With `--vacuum`, it also vacuums them. It finds the tables in the job file under their names in the runner, `<catalog>__<schema>.<table>` (with `_clavesa` in shadow mode). Tables that a Databricks task writes are left to Databricks. `--node <task key>` limits it to one task's tables.

```bash
clavesa pipeline optimize default_sql_aws_default_sql_sql_job --vacuum --json
```

```json
[
  { "table": "team_ucpub__sales.orders_daily_clavesa", "node": "orders_daily", "output_key": "default", "operation": "optimize", "vacuumed": true, "status": "ok" },
  { "table": "team_ucpub__sales.orders_raw_clavesa", "node": "orders_raw", "output_key": "default", "operation": "optimize", "vacuumed": true, "status": "ok" }
]
```

Both tables had one data file each, so OPTIMIZE had nothing to put together and wrote no new table version. VACUUM ran and deleted 0 files: by default it keeps the files of the last 7 days. Before v2.25.2, the command found no tables in a pipeline from a bundle and exited 0.

**A `sql_task` in the job** runs OPTIMIZE and VACUUM on the clavesa runner like any other SQL:

```sql
USE CATALOG IDENTIFIER({{catalog}});
USE IDENTIFIER({{schema}});

OPTIMIZE orders_daily ZORDER BY (order_date);
VACUUM orders_daily;
```

In shadow mode the statements act on the shadow table (`orders_daily` becomes `orders_daily_clavesa`, as for every table the job writes). The plan refuses the task until its table is listed in `clavesa.tables.<task>.writes`. In the cloud run, the Delta log of `orders_daily_clavesa` got a new version with operation `OPTIMIZE` and `zOrderBy: ["order_date"]`, and the runner log shows the VACUUM. The local test is `TestBundleRunScriptMode` with the bundle [`testdata/bundles/orders_sql`](../../testdata/bundles/orders_sql) (its `compact` task).

Clavesa treats such a task as a writer of the table ([GH #109](https://github.com/vesahyp/clavesa/issues/109)). After the run, the table's `clavesa.node` property names the maintenance task, and the Lineage graph shows the task as a second `clavesa transform` into the table, with no inputs. Until that is fixed, use `clavesa pipeline optimize`, which changes neither.

- **Databricks predictive optimization does not cover the `_clavesa` tables.** Predictive optimization runs only on Unity Catalog managed tables ([Databricks docs](https://docs.databricks.com/aws/en/optimizations/predictive-optimization)), and the shadow tables are external. Compact and vacuum them yourself, with a task or with `clavesa pipeline optimize`.
- **Scheduled OPTIMIZE and VACUUM are not built yet** ([GH #26](https://github.com/vesahyp/clavesa/issues/26)). Run `clavesa pipeline optimize` by hand or from cron, or put the statements in a task of the job.

## 10. The end-to-end result

The end-to-end test ([bundle-both-ways](../cookbook/bundle-both-ways.md), `make e2e-bundle`) uses Databricks' own `default-sql` bundle example. Its `dbx` target deploys the job to Databricks, and its `aws` target deploys the same job to clavesa in shadow mode. Result of 2026-11-14, with the clavesa run repeated on v2.25.2 on 2026-11-15:

```
PASS  team_ucpub.sales.orders_daily vs team_ucpub.sales.orders_daily_clavesa (31 partitions)
PASS  team_ucpub.sales.orders_raw vs team_ucpub.sales.orders_raw_clavesa (107 partitions)

Job default_sql_sql_job (pipeline default_sql_aws_default_sql_sql_job): PASS
```

- The clavesa run (`bundle run -t aws --wait`) ran both SQL tasks on the runner Lambda: `Status: SUCCEEDED`.
- The mixed target (`aws-mixed`) ran `orders_raw` on clavesa and `orders_daily` on Databricks, started by clavesa.
- AWS cost of one clavesa run: about 0.01 USD (Lambda). Compare costs no DBUs. The cost of the Databricks side is in [bundle-both-ways](../cookbook/bundle-both-ways.md#cost).
- Nothing keeps running: the Databricks job is deployed paused, and the clavesa schedule is a disabled EventBridge Scheduler schedule.
