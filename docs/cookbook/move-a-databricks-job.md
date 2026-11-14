# Move a whole Databricks job to clavesa

> **When you have one:** a Databricks job defined in a bundle (`databricks.yml`, Declarative Automation Bundles, formerly Databricks Asset Bundles) that you want to run in your own AWS account, from the same bundle that keeps deploying it to Databricks.

A clavesa target is an ordinary target of your bundle that sets one complex variable, `clavesa`. `clavesa bundle plan` reads the bundle the way the Databricks CLI does, with no Databricks login, and says task by task what runs where and what has to change. `clavesa bundle run` runs a job on clavesa; `clavesa bundle deploy` deploys it to AWS. The design is [ADR-031](../decisions/031-dab-bundles-on-clavesa.md). To replace one step of a pipeline instead of a whole job, see [replace-a-databricks-step](replace-a-databricks-step.md).

This recipe moves the example bundle in `testdata/bundles/daily_sales`: a job with a notebook, a SQL file, a Python wheel, an if/else condition, a notebook with `%run`, a Lakeflow pipeline and a job that starts another job. Everything below runs on your laptop (Docker); the last section is the AWS side.

## Prerequisites

- `bin/clavesa` built (`make build`) and Docker running.
- A workspace with the local warehouse: `bin/clavesa workspace init cookbook --workspace $WS` (any empty folder for `$WS`).
- `python3` with `pip` on your PATH (the wheel artifact is built on your machine, as `databricks bundle deploy` builds it).

## 1. The clavesa target

Copy the example bundle, so the edits below do not touch the repo:

```bash
export BUNDLE=$(mktemp -d)/daily_sales
cp -R testdata/bundles/daily_sales $BUNDLE
```

Its `databricks.yml` declares the variable and sets it in the targets that deploy to clavesa. `aws-local` is the one this recipe uses: `catalog: glue` (no Databricks at all), the raw files read from the bundle's own `data/` folder, and the Lakeflow pipeline task disabled, because it cannot run outside Databricks:

```yaml
variables:
  clavesa:
    description: Settings for clavesa deploys (ignored by Databricks)
    type: complex
    default: {}

targets:
  aws-local:
    variables:
      source_path: ${workspace.file_path}/data/orders
      clavesa:
        catalog: glue
        tables:
          enrich:
            reads: [main.sales.orders_clean]
            writes: [main.sales.orders_enriched]
    resources:
      jobs:
        daily_sales:
          tasks:
            - task_key: refresh_gold
              disabled: true
```

`clavesa.tables` lists the tables of the `enrich` task, because its code builds the table names at run time (`f"{args.catalog}.sales.orders_clean"`) and the plan cannot see them.

## 2. Plan

```bash
bin/clavesa bundle plan --bundle $BUNDLE -t aws-local
```

```
bundle daily_sales, target aws-local (catalog: glue)

daily_sales  (schedule: 0 30 4 * * ? Europe/Helsinki, 1 segment, Fargate 8 vCPU / 60 GB)
  ingest        notebook   clavesa     ok
  clean         sql        clavesa     ok
  enrich        wheel      clavesa     warn   runtime_engine PHOTON: runs without Photon, with the same results
  check         condition  clavesa     ok
  publish       notebook   clavesa     refuse notebooks/publish.py:6  %pip install tabulate==0.9.0: move tabulate==0.9.0 to the task's libraries (pypi)
  refresh_gold  pipeline   clavesa     ok     disabled: not run
  weekly        run_job    clavesa     ok     starts pipeline daily_sales_aws_local_weekly_report and waits
...
Ready: no.  1 refuse, 1 warn.
```

The plan exits 1. It also lists each task's tables, the runner requirements (`pycountry==24.6.1`) and the secret the code reads (`sales/api_key`).

## 3. Fix what the plan refuses

One edit: the `%pip` cell of `notebooks/publish.py` goes, and the package becomes a library of the task. Delete these lines from `$BUNDLE/notebooks/publish.py`:

```
# MAGIC %pip install tabulate==0.9.0

# COMMAND ----------

```

and add `libraries` to the `publish` task in `$BUNDLE/resources/daily_sales.job.yml`:

```yaml
        - task_key: publish
          ...
          notebook_task:
            notebook_path: ../notebooks/publish.py
          libraries:
            - pypi:
                package: tabulate==0.9.0
```

The bundle still deploys to Databricks unchanged (a task library is how Databricks wants it too). Plan again:

```bash
bin/clavesa bundle plan --bundle $BUNDLE -t aws-local
# ...
# Ready: yes.  0 refuse, 1 warn.
```

## 4. Run it

The ingest notebook reads a Databricks secret. On a local run it comes from the shell:

```bash
export CLAVESA_DBX_SECRET__sales__api_key=any-value
bin/clavesa bundle run --bundle $BUNDLE -t aws-local --job daily_sales --compute local --workspace $WS
```

```
Pipeline: daily_sales_aws_local_daily_sales (.../daily_sales_aws_local_daily_sales)
Run: ...
TASK     TYPE       STATUS
ingest   notebook   ok
check    condition  ok
clean    sql        ok
enrich   wheel      ok
publish  notebook   ok
weekly   run_job    ok
```

What happened: the job became the clavesa pipeline `daily_sales_aws_local_daily_sales` (`<bundle>_<target>_<job>`) (read-only: `node edit` names the bundle file to change). The wheel was built from `src/sales_enrich`, `pycountry` and `tabulate` went into the workspace's runner requirements, the notebooks were translated into scripts, and the runner ran the tasks in order in one Spark session: the condition read the task value `row_count` that `ingest` set, and `weekly` ran job `weekly_report` in the same runner.

## Verify

The tables are in Glue database `main__sales` (`catalog.schema` becomes `<catalog>__<schema>`):

```bash
bin/clavesa query --workspace $WS "SELECT country, country_name, orders, CAST(revenue AS STRING) AS revenue FROM main__sales.daily_revenue ORDER BY country"
```

```
country  country_name  orders  revenue
DE       Germany       1       99.99
EE       Estonia       1       19.90
FI       Finland       2       45.25
```

`main__sales.orders_clean` has 4 rows (the 2026-11-13 orders with an amount), and `main__sales.weekly_revenue` the same three countries. Run another day with a job parameter:

```bash
bin/clavesa bundle run --bundle $BUNDLE -t aws-local --job daily_sales --param run_date=2026-11-12 --workspace $WS
bin/clavesa query --workspace $WS "SELECT count(*) FROM main__sales.orders_clean"
# 7
```

The UI shows the same plan on `/bundles`, and the pipeline on `/pipelines` like any other.

## On AWS

With a workspace on the cloud warehouse (`clavesa workspace deploy`):

```bash
bin/clavesa bundle deploy --bundle $BUNDLE -t aws-local --plan-only   # read the Terraform plan
bin/clavesa bundle deploy --bundle $BUNDLE -t aws-local --yes
bin/clavesa bundle run --bundle $BUNDLE -t aws-local --job daily_sales --wait
```

The deploy puts the bundle's files in S3, sizes the runner from the job's clusters (Fargate here), schedules the job with EventBridge Scheduler in `Europe/Helsinki`, sends the failure emails of `email_notifications` through SNS, and alarms on the `health` rule. Put each secret into Secrets Manager once, as the plan lists it (`clavesa/<workspace>/dbx/sales/api_key`). Deploy the workspace again when the job added runner requirements.

With Databricks next to it (`catalog: unity`, the `aws` target), the job writes `<table>_clavesa` next to each Databricks table while both run (shadow mode), `clavesa bundle compare -t aws --job daily_sales` compares them daily (no DBUs), and `clavesa bundle switch -t aws --job daily_sales` prints the switch, table by table. A task clavesa does not run (the Lakeflow pipeline here) stays on Databricks: clavesa runs the job and starts that task with run-now. Deploy the job to Databricks first (`databricks bundle deploy -t aws`), with its schedule paused. The end-to-end proof of both deploys is [bundle-both-ways](bundle-both-ways.md).
