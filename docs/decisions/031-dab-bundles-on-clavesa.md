---
adr: 31
title: One Databricks bundle, two deploy targets; clavesa deploys DAB jobs as its own infrastructure
date: 2026-11-14
status: Accepted
deciders: Vesa
---

# ADR 031: A Databricks bundle deploys to Databricks or to clavesa

**Status**: Accepted (2026-11-14). Vesa gave the go-ahead to build it on
2026-11-14. The three open questions were decided by Head Jeeves on
2026-11-14 (section "Decided 2026-11-14").

This ADR was first written as ADR-029 in the public `clavesa` repo. That
number was already taken here by ADR-029 (cheaper SQL on Unity Catalog
tables), so it is ADR-031 in this repo.

## Context

Vesa, 2026-11-14: "I would like a plan for a direct switch. Ie: define dab
jobs in dbx format but deploy with clavesa as own infra. That way the deploy
could be to a dbx job and/or the exact same as clavesa."

ADR-027 replaces **one step** of a Databricks pipeline: the engineer copies
the step's SQL into a clavesa node by hand, proves it with the daily
comparison and switches the table. Its brief put "migrating whole
pipelines" out of scope. This ADR covers that: a team keeps writing its jobs
as a Databricks Asset Bundle (`databricks.yml`, `resources.jobs`, tasks,
`depends_on`, schedules and triggers, job clusters, parameters, targets),
and the same bundle deploys to one of two places, or to both:

1. **Databricks**, unchanged: `databricks bundle deploy -t <target>`.
2. **Clavesa** in the team's own AWS account: the tasks run as clavesa steps
   (Step Functions, Lambda or Fargate, later EMR Serverless; PySpark per
   ADR-012), with Unity Catalog publish and lineage per ADR-027.
3. **Mixed**: some tasks on Databricks and some on clavesa, per task or per
   target, with clavesa starting the Databricks tasks.

### What the bundle format is today

Checked on 2026-11-14 against the Databricks docs and the published JSON
schema (`bundle/schema/jsonschema.json` in `github.com/databricks/cli`), and
with Databricks CLI v1.24.0 locally against a fake host (no workspace
touched):

- The docs now name the format **Declarative Automation Bundles**, formerly
  Databricks Asset Bundles. The file, the CLI and the keys are unchanged.
  This ADR says "bundle" or "DAB".
- Top-level keys: `bundle`, `include`, `variables`, `targets`, `resources`,
  `workspace`, `artifacts`, `sync`, `permissions`, `presets`, `run_as`,
  `python`, `scripts`, `experimental`.
- A job has `tasks`, `job_clusters`, `environments`, `parameters`,
  `schedule` (`quartz_cron_expression`, `timezone_id`, `pause_status`),
  `trigger` (`file_arrival`, `table_update`, `periodic`, and others),
  `continuous`, `timeout_seconds`, `max_concurrent_runs`, `queue`, `health`,
  notifications, `run_as`, `tags`, `git_source`.
- A task has `task_key`, `depends_on` (`task_key`, `outcome`), `run_if`
  (`ALL_SUCCESS`, `ALL_DONE`, `NONE_FAILED`, `AT_LEAST_ONE_SUCCESS`,
  `ALL_FAILED`, `AT_LEAST_ONE_FAILED`), `max_retries`,
  `min_retry_interval_millis`, `retry_on_timeout`, `timeout_seconds`,
  `job_cluster_key` / `existing_cluster_id` / `new_cluster` /
  `environment_key`, `libraries` (`whl`, `jar`, `pypi`, `maven`,
  `requirements`), `disabled`, and exactly one task type.
- Task types: `notebook_task`, `spark_python_task`, `python_wheel_task`,
  `spark_jar_task`, `spark_submit_task`, `sql_task`, `dbt_task`,
  `dbt_cloud_task`, `dbt_platform_task`, `pipeline_task`, `run_job_task`,
  `condition_task`, `for_each_task`, `clean_rooms_notebook_task`,
  `dashboard_task`, `alert_task`, `power_bi_task`, `gen_ai_compute_task`,
  `ai_function_task`, `ai_runtime_task`, `python_operator_task`.
- Run-time values are `{{...}}` references (`{{job.parameters.x}}`,
  `{{job.start_time.iso_date}}`, `{{tasks.<k>.values.<name>}}`,
  `{{tasks.<k>.result_state}}`, `{{input.<alias>}}` in `for_each`, and
  others). Deploy-time values are `${...}` substitutions (`${var.x}`,
  `${bundle.target}`, `${resources.jobs.<k>.id}`).
- **The schema is closed.** A job, a task, a target and the root all reject
  extra keys: `bundle validate` prints `Warning: unknown field` and drops the
  key from the resolved output. There is no documented extension point.
  **A declared variable of `type: complex` passes with no warning**, keeps a
  nested map in the resolved output, and a target's value for it
  **replaces** the default as a whole (tested locally).
- `databricks bundle validate -o json` prints the resolved configuration
  (includes, variables and the target merged), but it needs a working
  Databricks login: with no credentials it prints the JSON and exits 1.
- With a `python:` section, jobs can be defined or changed by Python code
  (`resources`, `mutators`); the YAML is then not the whole job.
- Jobs API 2.2 `run-now` takes `only`, a list of task keys to run in the
  job (a `+` prefix or suffix also runs the upstream or downstream tasks).
  Source: the Databricks Go SDK `RunNow` struct; the rendered REST page did
  not show it.

### What clavesa is today, where it matters here

- A pipeline is generated Terraform (ADR-001) with one Step Functions state
  machine (ADR-002). Since v2.2.0 the state machine is **one Task** that
  hands the ordered transform list to one runner (Lambda or Fargate); the
  runner's `pipeline_handler` runs every transform in one Spark session.
  `clavesa pipeline run` locally runs the same handler with no Step
  Functions (ADR-014, ADR-024).
- A transform returns DataFrames and **clavesa writes them**, with an output
  mode (`replace`, `append`, `merge`, `replace_partitions`). A Databricks
  task writes its tables itself (`saveAsTable`, `INSERT`, `MERGE`).
- Tables are `<catalog>__<schema>.<table>` in Glue (ADR-016, ADR-019,
  ADR-020); in Unity Catalog clavesa writes external tables in place, never
  a managed table, and pushes lineage with the step as its own box
  (ADR-027).
- Spark is 4.0.2 with Delta 4.0.0. EMR Serverless is accepted by validation
  and refused at deploy (not built).
- `clavesa notebook` already runs `.ipynb` notebooks with `%%sql` cells in
  the warm Spark session.

## Decision

### 1. The bundle is the only source; clavesa settings live in it as one complex variable

Every fact about a job (its tasks, their order, code paths, parameters,
schedule, retries, clusters, libraries) is read from the bundle. Clavesa
adds no second file that names tasks again.

The few settings that have no DAB field (where a task runs, a compute size
that overrides the cluster, the comparison columns, the catalog mode) go
into **one declared variable, `clavesa`, of `type: complex`**, set per
target. A clavesa deploy target is an ordinary DAB target:

```yaml
variables:
  clavesa:
    description: Settings for clavesa deploys (ignored by Databricks)
    type: complex
    default: {}

targets:
  prod:                         # Databricks, unchanged
    mode: production
    workspace: { host: https://<workspace>.cloud.databricks.com }
  aws:                          # clavesa
    mode: production
    workspace: { host: https://<workspace>.cloud.databricks.com }
    presets: { trigger_pause_status: PAUSED }   # Databricks never starts it
    variables:
      clavesa:
        workspace: ~/clavesa/team-dbx            # the clavesa workspace
        catalog: unity                            # or glue (section 6)
        place: { refresh_gold: databricks }       # default: clavesa
        compute: { enrich: { kind: fargate, cpu: 8, memory_gb: 32 } }
        compare: { main.sales.orders_clean: { partition: order_date, sums: [amount] } }
        shadow: true                              # section 9
```

Why the bundle holds these settings (the other option is a clavesa-side file that reads the bundle):

- **One fact, one place.** A side file has to name every task key again to
  say where it runs. The day a task is renamed in `databricks.yml`, the side
  file is wrong and nothing fails until the deploy.
- **One review.** A change to a job and the change to where it runs are one
  diff in one file, in the team's repo, reviewed by the same people.
- **Databricks keeps working.** `databricks bundle validate` and
  `deploy` accept the file with no warning, and Databricks ignores a
  variable that no resource uses.
- **The targets mechanism already says "the same job, deployed somewhere
  else"**, with per-target overrides of any job field. Clavesa reuses it
  and adds no mechanism of its own.

Why not other places in the bundle: unknown keys anywhere are warned about
and dropped (tested); job `tags` are deployed to Databricks and are job-level
only; job `parameters` reach the running code.

The cost: the Databricks CLI checks nothing inside `clavesa`. Clavesa
validates it fully at plan time (section 10), and an unknown key or task
name in it is an error. And because a target's value replaces the default
as a whole, each clavesa target writes the whole `clavesa` value.

### 2. Reading the bundle: clavesa's own resolver, kept honest by the Databricks CLI

Clavesa must read a bundle with no Databricks login: a team that moves a
job fully to AWS may have none, and `clavesa bundle plan` must run in CI.
`databricks bundle validate -o json` needs a login, so clavesa has **its own
resolver in Go** for the YAML part: `include` globs, variables (default,
target value, `--var`, `BUNDLE_VAR_*`), target overrides merged by
`task_key` and `job_cluster_key`, `${...}` substitutions, `presets`.

- **Conformance test.** For each bundle in `testdata/bundles/`, a test
  compares clavesa's resolved job with the JSON that `databricks bundle
  validate -o json` prints. The test runs the CLI with an empty config file
  and a fake host, and reads the JSON even though the CLI exits 1 for want
  of auth. A CLI upgrade that changes the merge makes the test fail
  before any deploy uses it.
- **`python:` bundles** need the CLI: with a `python:` section, clavesa runs
  `databricks bundle validate -o json -t <target>` with the team's own login
  and reads its output. With no login, `plan` stops and says so.
- `${workspace.*}` and `${resources.jobs.<k>.id}` resolve only against a
  Databricks workspace. Clavesa resolves `${resources.jobs.<k>.id}` to its
  own pipeline when job `<k>` also runs on clavesa, and refuses the rest in
  a target with no Databricks host.

### 3. One job becomes one clavesa pipeline, generated and read-only

`clavesa bundle deploy -t aws` turns each job of the target into one clavesa
pipeline named `<bundle>_<target>_<job>` (the target in the name since
v2.25.1, so two clavesa targets of one bundle are two pipelines in one
workspace; v2.25.0 named it `<bundle>_<job>`), generated into the clavesa
workspace as the folder of that name, next to the other pipelines (built that way in
slice 3: the run path takes a pipeline's parent folder to be the workspace,
so a deeper `bundles/<bundle>/<job>/` would not run). `bundle-job.json` in
the folder holds the resolved job and marks it as generated, and `bundle/`
holds a copy of the bundle's files. Each task becomes one node; `depends_on` becomes
the node's parents. The generated files are derived, so they are not
committed; `node edit` and the UI editor refuse a bundle pipeline and name
the line in `databricks.yml` to change. Run history, logs, lineage, cost,
backfill and the run page work as for any pipeline.

A bundle node is a new node kind, **task**: its code writes its own tables,
and clavesa does not apply an output mode. Clavesa still knows which tables
a task reads and writes (section 6) for IAM, lineage, the shadow names and
the single-writer check.

### 4. The run: DAB semantics in the runner, Step Functions only between segments

Retries, timeouts, `run_if`, `condition_task`, `for_each_task` and task
values are implemented **in `pipeline_handler`**. Step Functions gets no
state for them. Reason: a local run has no Step Functions (ADR-014, ADR-024), so
this is the only place where a local run and a cloud run behave the same.
It also keeps one Spark session for the whole job, which is the reason for
the v2.2.0 single-Task design.

| DAB | clavesa |
|---|---|
| `depends_on`, `outcome` | parents; `outcome` is matched against the parent `condition_task` result |
| `run_if` (all six values) | evaluated per task from the parents' states; skipped tasks are `skipped` in `node_runs` |
| `max_retries`, `min_retry_interval_millis`, `retry_on_timeout` | a retry loop per task in the handler; `-1` (retry forever) is capped at 10 with a plan warning |
| task `timeout_seconds` | a deadline per task; on expiry the handler cancels the task's Spark job group and fails the task |
| job `timeout_seconds` | the state machine Task's `TimeoutSeconds` (Fargate) |
| job `max_concurrent_runs` | runs are serialized by the existing lease lock (ADR-024); a value above 1 gets a plan warning |
| `queue` | ignored with a plan note: a run that finds the lock taken waits for it |
| `disabled: true` | the node is generated disabled |
| job `email_notifications` | one SNS topic per bundle target, email subscriptions from the bundle (section 7) |
| job `health` rules | CloudWatch alarms that publish to that topic (section 7) |

The state machine stays one Task when every task runs on clavesa. It gets
more states only for a mixed job (section 8).

The Lambda limit of 15 minutes applies to the whole job, because the job is
one Lambda invocation. A job whose task timeouts add up to more, or that
has none, gets Fargate unless the target says `lambda`.

### 5. Task types

**Mapped directly** (the code runs unchanged):

| Task type | How it runs on clavesa |
|---|---|
| `spark_python_task` | The file runs as `__main__` with `sys.argv` set from `parameters`, in the runner's process. `SparkSession.builder.getOrCreate()` returns the runner's session. `source: WORKSPACE` files come from the bundle folder; `source: GIT` is refused (the docs also advise against it in bundles). |
| `python_wheel_task` | The wheel is built from the bundle's `artifacts` (or taken from `libraries[].whl`), uploaded to the clavesa bucket, and installed into a per-run folder. The runner calls `package_name`'s `entry_point` with `parameters` as `argv`, or `named_parameters` as `--key=value`, the way Databricks does. |
| `sql_task` with `file` | The file's statements run one by one through `spark.sql`, with `parameters` bound as named parameter markers (`:name`). `warehouse_id` is ignored. |
| `notebook_task` | Translated to a script at deploy (below). |
| `condition_task` | Evaluated in the handler: the six `op` values, on resolved `{{...}}` values, with the comparison rules of the Databricks docs (slice 5 copies them into tests). |
| `for_each_task` | The inner task runs once per item of `inputs`, with the `{{input...}}` references resolved. v1 runs items one after the other; `concurrency` above 1 gets a plan warning. |
| `run_job_task` | If the job is in the same bundle and its target is clavesa: the handler starts that clavesa pipeline and waits for it (locally: in-process; in AWS: `states:startExecution.sync:2` from a segment state, section 8). If the job runs on Databricks: `run-now` and wait. `job_parameters` are passed. |

**Notebooks.** A notebook task is translated at deploy into one Python
script; the run never parses a notebook. The three Databricks formats are
read: a `.py` source file (`# Databricks notebook source`, cells split at
`# COMMAND ----------`, magics as `# MAGIC`), `.ipynb`, and a `.sql`
notebook. Per cell:

| Notebook content | v1 |
|---|---|
| Python cells | as they are |
| `%sql` cells, `.sql` notebooks | `spark.sql(...)` per statement; widget values bound as `:name`; `${name}` and `{{name}}` text substitution as Databricks does it |
| `%md`, `%md-sandbox` | dropped |
| `%run ./other` | the other notebook is translated and inlined at deploy (a static path only, as Databricks requires) |
| `%fs ls/cp/rm/head/mkdirs` | `dbutils.fs` calls (below) |
| `%pip`, `%conda` | **refused at plan**: move the package to the task's `libraries` (section 7). The plan prints the line. |
| `%sh`, `%scala`, `%r`, `%lsmagic` and other magics | **refused at plan** |
| `display()`, `displayHTML()` | `display(df)` logs the first 20 rows; `displayHTML` is a no-op |
| `dbutils.widgets.get/getArgument/text/dropdown/combobox/multiselect` | `get` returns `base_parameters` or the job parameter of that name; the create calls set a default |
| `dbutils.secrets.get(scope, key)` | Secrets Manager (section 7) |
| `dbutils.jobs.taskValues.set/get` | in the handler's memory for the run; `{{tasks.<k>.values.<name>}}` reads them |
| `dbutils.notebook.exit(value)` | ends the task with success; the value is the task's result |
| `dbutils.fs` on `s3://` paths and external volume paths | S3 calls with the runner's role |
| `dbutils.notebook.run` | **refused at plan**: rewrite as `%run` (same code) or as a `run_job_task` (separate job) |
| `dbutils.library.*`, `dbfs:/` paths, `/dbfs/` paths | **refused at plan** |

`dbutils` is a Python object the runner puts into `builtins` for every task,
also for `spark_python_task` and wheel tasks, so `from
databricks.sdk.runtime import dbutils` is also served by the same object.
The same calls with names the plan cannot see (built at run time) fail at
run time with the same message the plan would print.

**Need a rewrite** (the plan names what to do):

- `sql_task` with `query`, `dashboard` or `alert`: these are objects in a
  Databricks SQL warehouse and have no file in the bundle. Move the query text
  into a `.sql` file and use `file`.
- Notebooks with the refused content above.
- Auto Loader (`spark.readStream.format("cloudFiles")`): use a clavesa `s3`
  source on the task, or a plain `spark.read` over the prefix.
- `COPY INTO`: a `spark.read` plus `INSERT`, or a clavesa `s3` source.

**Out of scope for v1** (the plan puts them on Databricks in a mixed target,
or refuses them in a target with no Databricks host):

- `pipeline_task` (Lakeflow Declarative Pipelines, formerly DLT): its engine
  exists only in Databricks.
- `dashboard_task`, `alert_task`, `power_bi_task`, `clean_rooms_notebook_task`,
  `gen_ai_compute_task`, `ai_function_task`, `ai_runtime_task`,
  `dbt_cloud_task`, `dbt_platform_task`, `python_operator_task`.
- `spark_jar_task`, `spark_submit_task`: JVM code. They come back with EMR
  Serverless, which runs a JAR natively.
- `dbt_task`: dbt-spark could run it on the runner; a later ADR if a team
  asks.
- `continuous` jobs; `model` and `job_completion` triggers.

### 6. Table names: three levels in, the mode decides where they go

Task code uses Unity Catalog names, `catalog.schema.table`, and
`USE CATALOG` / `USE SCHEMA`. The runner resolves every table name a task
reads or writes. That covers SQL text (parsed with `sqlglot`, already in the
runner image) and the Python table calls (`spark.table`,
`spark.read.table`, `saveAsTable`, `insertInto`, `DeltaTable.forName`,
`spark.sql`). The target picks where a name goes:

- **`catalog: unity`** (the default when the target has a Databricks host):
  the tables stay in the team's Unity Catalog, exactly as in ADR-027.
  External tables are written in place; managed tables are read with vended
  credentials and never written (each managed output needs the ADR-027
  switch); lineage is pushed per task with the task as its own box. Glue
  gets the same tables as today so the clavesa UI works.
- **`catalog: glue`** (no Databricks at all): `catalog.schema.table` becomes
  Glue database `<catalog>__<schema>` (the ADR-016 encoding). The target can
  map catalog names (`catalogs: { main: prod }`). A bundle pipeline owns
  **single tables**, as in ADR-027: two bundle jobs may write
  different tables of one schema. That relaxes ADR-016 section 5 for bundle
  pipelines in `catalog: glue` only (decided 2026-11-14; ADR-016 has the
  matching update); a hand-made pipeline still owns its whole schema, and a
  table written by two pipelines is refused at plan.

**Plan knows the tables.** The plan finds each task's reads and writes from
SQL files, `%sql` cells and literal table names in Python. A name it cannot
see (an f-string) must be listed in `clavesa.tables.<task_key>` in the
bundle; the runner refuses, at the call, a write to a table that is in
neither list. Clavesa needs the list before the first run because IAM
grants, the shadow names, the single-writer check and lineage all depend on
it.

### 7. Schedules, triggers, parameters, compute, libraries, secrets

- **`schedule`.** EventBridge Scheduler (`aws_scheduler_schedule`). Clavesa
  uses an EventBridge rule today; Scheduler is used here because it takes a
  time zone (`timezone_id`). Quartz `sec min hour dom month dow [year]` becomes AWS
  `cron(min hour dom month dow year)`; a seconds field other than `0`, and
  Quartz features AWS lacks, are refused at plan. `pause_status: PAUSED`
  and `presets.trigger_pause_status` give a disabled schedule.
- **`trigger.periodic`**: a `rate(...)` schedule.
- **`trigger.file_arrival`**: an `s3://` URL becomes a clavesa `s3` source
  trigger with the existing SQS batching; `min_time_between_triggers_seconds`
  and `wait_after_last_change_seconds` map to the batch window. A `/Volumes/`
  URL works for an external volume (its S3 location, read from Unity
  Catalog at plan) and is refused for a managed volume.
- **`trigger.table_update`**: a table that a clavesa pipeline writes uses the
  existing cross-pipeline trigger (ADR-016 section 6); an external table in
  the team's bucket uses S3 events on its `_delta_log/` prefix (ADR-027
  measured under a minute). `condition: ALL_UPDATED` waits for every table.
  A managed table cannot be watched from outside Databricks: refused at plan
  in v1.
- **Parameters.** Job `parameters` become run parameters with defaults:
  `clavesa bundle run --param k=v`, and the UI run button asks for them.
  `{{job.*}}`, `{{task.*}}`, `{{tasks.<k>.values|result_state|run_id}}` and
  `{{input...}}` are resolved by the handler. `{{tasks.<k>.output.*}}`
  (SQL task results), `{{workspace.*}}`, and date arithmetic (Beta in
  Databricks) are refused at plan in v1.
- **Compute.** A task's cluster (`job_clusters`, `new_cluster`) gives a
  size: the worker count (or `autoscale.max_workers`) plus the driver, times
  the vCPU and memory of `node_type_id` from a table in `internal/pricing`.
  Up to the Fargate maximum (16 vCPU, 120 GB) it runs on Fargate at that
  size, rounded down to a valid Fargate size; above it, it needs EMR
  Serverless and the plan says so. Serverless compute (`environment_key`)
  and a `sql_task` warehouse get Fargate's default size. Because the job is
  one runner, the job's size is that of its largest task. `clavesa.compute`
  overrides it per task. `spark_conf` keys pass through, except
  `spark.databricks.*`, which are dropped with a warning.
  `spark_version`: the plan warns when the Databricks runtime's Spark
  version differs from the runner's (4.0.2).
- **Libraries.** `pypi` and `requirements` go into the workspace's runner
  requirements (`clavesa runner requirements add`), which rebuilds the
  image; two tasks that pin different versions of one package are refused
  at plan. `whl` from the bundle: section 5. `jar` and `maven`: refused in
  v1. `environments[].spec.dependencies` are treated as `pypi`.
- **Secrets.** `dbutils.secrets.get(scope, key)` and `{{secrets/scope/key}}`
  in `spark_conf` or environment variables read the Secrets Manager secret
  `clavesa/<workspace>/dbx/<scope>/<key>`. The plan lists every secret the
  bundle uses and whether it exists (`clavesa bundle plan --check-aws`).
  Clavesa never copies secret values out of Databricks: the team puts each
  one into Secrets Manager once, and the plan prints the commands, as
  getting-started step 3 does.
- **Notifications.** (Built in slice 7 as one SNS topic per job and
  notification kind, in the job's own pipeline stack: a topic shared by
  the target would need a stack of its own.) One SNS topic per bundle target. Each address in a
  job's `email_notifications` becomes an email subscription on it.
  `on_start`, `on_success` and `on_failure` become EventBridge rules on the
  state machine's execution status. Task-level notifications,
  `webhook_notifications` and `notification_settings` are not carried; the
  plan warns.
- **`health`.** A `RUN_DURATION_SECONDS` rule becomes a CloudWatch alarm on
  the state machine's `ExecutionTime` metric that publishes to the same
  topic. Other health metrics are not carried; the plan warns.
- **`run_as`, `permissions`**: Databricks-only. On clavesa the task runs as
  the pipeline's runner role. A plan note says so.

### 8. Mixed jobs: clavesa orchestrates, Databricks runs its tasks

A target can place tasks on Databricks (`clavesa.place`), and some task
types (section 5) are placed there by force. When a job has tasks in both
places, **clavesa is the orchestrator**, and Databricks never starts the
job itself:

1. The team runs `databricks bundle deploy -t aws` first. That deploys the
   whole job and syncs its files, with its schedule and triggers paused
   (`presets.trigger_pause_status: PAUSED`). `clavesa bundle deploy -t aws`
   checks that the deployed job exists and that its task keys match.
2. Clavesa splits the job graph into **segments**: runs of tasks in one
   place. The state machine gets one state per segment. A clavesa segment
   is the runner as today. A Databricks segment is `run-now` on the
   deployed job with `only: [its task keys]`, then a poll loop of Wait and
   `runs/get` states until it ends. Waiting costs nothing, so a long
   Databricks task does not keep a Lambda or Fargate task running.
3. A local run (`--compute local`) does the same from the CLI.

Built in slice 9 with two limits: the clavesa segments of a mixed job run
on Lambda (Fargate segments are not built; the plan warns), and the
deployed job's ID comes from `databricks bundle summary -t <target>` with
the team's own login at `clavesa bundle deploy`. The run-now and runs/get
calls are made by the runner Lambda (`_operation: dbx_run_now /
dbx_run_get`), so the state machine needs no HTTP connection of its own.

Data crosses the boundary as tables only. A `{{tasks.<k>.values...}}`
reference from one side to the other is refused at plan in v1.

The other direction, Databricks first and clavesa after, needs no
orchestration: ADR-027 already starts a clavesa pipeline from a table commit,
and clavesa starts the next Databricks job after a run (`on_success`).
Cost: each Databricks segment pays a cluster start, so a job that goes back
and forth pays it several times. The plan prints the number of segments.

### 9. Proof of sameness, per job

The switch reuses getting-started steps 7 to 11, with the job as the unit:

- **Shadow mode** (`clavesa.shadow: true`, the default for a target with
  `catalog: unity`): every table the job writes gets the name
  `<table>_clavesa`. Reads of a table written earlier in the same job go to
  the shadow name too, so the clavesa job reads its own outputs. Reads of
  any other table go to the real name. The Databricks job keeps running
  unchanged.
- **`clavesa bundle compare -t aws --job <k>`** runs the ADR-027
  comparison (`pipeline databricks-compare`) for each table the job writes,
  with the columns from `clavesa.compare` (row counts per day when none are
  set), and prints one PASS or FAIL for the job. It costs no DBUs.
- **The switch** happens when the comparison passes every day for 7 days.
  Per table, the getting-started steps apply (history copy, rename for a
  managed table, grants). Then the Databricks target pauses the job, and
  clavesa sets `replaces_databricks_job` on every output, so a clavesa run
  writes nothing while the Databricks job is unpaused or running.
  `clavesa bundle switch` prints the exact SQL for each table, in order.
  It runs nothing in Databricks itself, the same as ADR-027.
- **Roll-back**: getting-started step 11, per table, then `shadow: true`
  again.

In `catalog: glue` there is no Databricks table to compare against; there
the proof is a **parity run**: `clavesa bundle run --compute local` and the
cloud run on the same input produce the same tables (ADR-014).

### 10. What cannot run outside Databricks is found at plan time

`clavesa bundle plan` reads the bundle and the code it points at. It
reports every row below with the task, the file and line, and the fix. A
`refuse` row makes the plan exit 1, and `deploy` runs the plan first.

| Found | Result |
|---|---|
| Task types out of scope (section 5) | placed on Databricks in a target with a host; `refuse` without |
| `runtime_engine: PHOTON`, a Photon node type | `warn`: same results, no Photon; the size is from the cluster |
| `spark.databricks.*` conf | `warn`, dropped |
| `resources.pipelines` used by a task | `pipeline_task`, as above |
| `sql_task` `query` / `dashboard` / `alert` | `refuse`, move the SQL into a file |
| Databricks-only SQL, found by parsing every SQL file, `%sql` cell and literal `spark.sql` string: `ai_*` and `ai_query`, `vector_search`, `http_request`, `secret()`, `is_account_group_member`, `is_member`, `current_metastore`, `STREAM(...)`, `CREATE STREAMING TABLE`, `CREATE MATERIALIZED VIEW`, `APPLY CHANGES`, `COPY INTO`, `CLUSTER BY AUTO`, `GRANT`/`REVOKE`, `CREATE FUNCTION ... LANGUAGE PYTHON`, `SET TAGS` | `refuse` with the rewrite where there is one |
| Python: `dlt` / `pyspark.pipelines` imports, `cloudFiles`, `dbutils.notebook.run`, `dbutils.library`, `dbfs:` and `/dbfs/` paths, Databricks Connect | `refuse` |
| Notebook magics `%pip`, `%sh`, `%scala`, `%r` | `refuse` |
| A written table with Delta features clavesa cannot write (deletion vectors, liquid clustering, row filters, column masks) | `refuse` (ADR-027 decision 6; needs `--check-cloud`) |
| A managed output table in `catalog: unity` | `ok` in shadow mode; `refuse` after the switch until the table is renamed |
| `{{...}}` references not listed in section 7 | `refuse` |
| Table names the plan cannot see and that `clavesa.tables` does not list | `refuse` |
| An unknown key or task name in the `clavesa` variable | `refuse` |

`read_files(...)` in batch SQL was on this list and is run instead (built
with the end-to-end test, slice 11): the runner reads the files with
`spark.read` (the `format` and the other options passed through) into a
temporary view the statement then reads. Databricks' own default-sql
example reads its input that way. `STREAM read_files(...)` stays refused.

The list of Databricks-only functions is a file in the repo with a test per
entry, so a missed function is a one-line fix. Code that builds SQL or
paths at run time cannot be checked fully at plan time: the runner's checks
fail the same call at run time, with the same message, before anything is
written.

### 11. The CLI

All commands take `--bundle <dir>` (default: the current directory) and
`-t/--target` (default: the bundle's default target), and run the same
service calls as the UI (ADR-015). The UI gets a read-only bundle page with
the plan report, in a later slice (built after slice 9: `/bundles`, with the
plan report and the list of generated pipelines; `clavesa bundle list` is
its CLI side).

```bash
clavesa bundle plan   -t aws [--json] [--check-aws] [--check-cloud]
clavesa bundle deploy -t aws [--yes]
clavesa bundle run    -t aws --job daily_sales [--param run_date=2026-11-13] [--compute local] [--wait]
clavesa bundle compare -t aws --job daily_sales [--days 7]
clavesa bundle switch -t aws --job daily_sales      # prints the switch SQL, runs nothing
clavesa bundle destroy -t aws --job daily_sales
```

`plan` checks the following. With no flags it makes no network call.

- The bundle resolves, and the conformance rules hold.
- Every task maps to `ok`, `warn` or `refuse`.
- The `clavesa` variable is valid.
- Every table read and written is known; no table has two writers.
- The schedule converts.
- Compute fits.
- Libraries do not conflict.

`--check-aws` adds the secrets, the runner role's access and the Fargate
quota. `--check-cloud` adds Unity Catalog: table kinds, Delta features,
volumes, and the deployed Databricks job for a mixed target. The report is a
table, task by task:

```
daily_sales  (schedule: 04:30 Europe/Helsinki, 3 segments, Fargate 8 vCPU / 32 GB)
  ingest        notebook    clavesa     ok
  clean         sql         clavesa     ok
  enrich        wheel       clavesa     warn   runtime_engine PHOTON: runs without Photon
  check         condition   clavesa     ok
  refresh_gold  pipeline    databricks  ok     Lakeflow pipeline: stays on Databricks
  publish       notebook    clavesa     refuse notebooks/publish.py:3  %pip install tabulate: move to libraries
  weekly        run_job     clavesa     ok     starts pipeline daily_sales_aws_weekly_report and waits
Ready: no.  1 refuse, 1 warn.
```

### 12. The example bundle in testdata

`testdata/bundles/daily_sales/` is a bundle written the way a Databricks
team writes one, and every slice tests against it:

- `databricks.yml` with targets `dev`, `prod`, `aws` (mixed, `catalog: unity`)
  and `aws-local` (`catalog: glue`, no Databricks host).
- Job `daily_sales`: a Quartz schedule in `Europe/Helsinki`, two job
  clusters (one with Photon), job parameters `run_date` and `country`.
- Its tasks:
  - `ingest`: a `.py` source notebook with widgets, a `%sql` cell,
    `dbutils.secrets.get` and `dbutils.jobs.taskValues.set`. It reads JSON
    lines from `testdata/` files.
  - `clean`: a `sql_task` file with `MERGE INTO main.sales.orders_clean` and
    `:run_date`.
  - `enrich`: a `python_wheel_task` built from `src/sales_enrich`, with a
    `pypi` library.
  - `check`: a `condition_task` on `{{tasks.ingest.values.row_count}}`.
  - `publish`: a notebook with `%run ./common`, a `%pip` cell, and
    `depends_on` `check` with outcome `true`.
  - `refresh_gold`: a `pipeline_task` on a `resources.pipelines` entry.
  - `weekly`: a `run_job_task` on job `weekly_report`.
- Job `weekly_report`: one `spark_python_task` with a `file_arrival`
  trigger.
- `testdata/bundles/daily_sales/expected/`: the golden plan output before
  the migration (the `%pip` refuse, the Photon warn, `refresh_gold` on
  Databricks), and the expected tables after a local run.

The migration of this bundle is the acceptance test. It has three steps:

1. **Plan.** `bundle plan -t aws` matches the golden output.
2. **Fix.** One edit moves the `%pip` package to `libraries`. After it,
   `plan` prints `Ready: yes`.
3. **Run.** `bundle run -t aws-local --compute local` with `catalog: glue`
   (that target overrides `refresh_gold` with `disabled: true`, because a
   Lakeflow pipeline cannot run without Databricks) produces the expected tables and task values.

The same bundle passes `databricks bundle validate` (the conformance test,
section 2).

## Delivery slices

Each slice is testable alone with no cloud spend, unless it says otherwise.
Each one ends with a commit. The estimates are agent time.

| # | Slice | Test | Agent time |
|---|---|---|---|
| 1 | Example bundle, Go resolver, CLI conformance test, `bundle plan` with the task-type mapping and the `clavesa` variable check | golden plan output; `databricks bundle validate` JSON equals the resolver's | 4 h |
| 2 | The plan detectors of section 10: SQL function list, notebook magics, Python constructs, clusters and Photon, libraries, secrets, `{{...}}` references, table names | one fixture per detector; golden output | 3 h |
| 3 | Runner: the task node kind, script mode (`spark_python_task`, `sql_task` file), the name resolver for SQL and Python, `dbutils` (widgets, secrets, task values, `notebook.exit`, `fs`), `bundle run --compute local` in `catalog: glue` | the example's `clean` task, run locally, gives the expected table | 6 h |
| 4 | Notebook translation: `.py` source, `.ipynb`, `.sql`; `%run` inlining | `ingest` and `publish` run locally | 3 h |
| 5 | Handler semantics: `run_if`, `outcome`, retries, timeouts, `condition_task`, `for_each_task`, parameters and `{{...}}` values | unit tests per rule; the whole example runs locally | 4 h |
| 6 | Wheels and libraries: build `artifacts`, stage and install a wheel, `pypi` into runner requirements | `enrich` runs locally | 3 h |
| 7 | `bundle deploy` for AWS: generated pipelines, EventBridge Scheduler with time zone, `periodic`, `file_arrival`, `table_update`, compute from clusters, the SNS topic with email subscriptions and the `health` alarms | `terraform validate` and `plan` on the generated files in tests; no apply | 6 h |
| 8 | `catalog: unity`: ADR-027 publish and lineage per task, shadow names, `bundle compare`, `bundle switch` | unit tests with the fake Databricks client | 4 h |
| 9 | Mixed jobs: segments, `run-now` with `only`, the poll loop, `run_job_task` both ways | the fake Databricks client; generated ASL golden files | 5 h |
| 10 | Docs: "Move a whole Databricks job" guide, CLI reference, a cookbook recipe in the verify-cookbook run | `make verify-cookbook` | 2 h |
| 11 | **Live proof** on the test workspace (`ucpub`, us-east-2, Vesa's own account 699166197771): the example bundle deployed both ways, shadow, compare, a mixed run | `make smoke-bundle` | 3 h, **cloud spend: needs Vesa's go-ahead** |

About 43 hours of agent time in all. Slices 1 to 6 give a team a working
local answer to "can this job run outside Databricks?" before any AWS
work. Slices 1 and 2 alone already give the plan report, which is useful by
itself as an inventory of a team's jobs.

## Consequences

**Positive:**

- A team keeps one bundle in the Databricks format and gets a second deploy
  target. Moving a job back is a change of target, with no code change.
- The plan report answers, per job and before any deploy, what runs where
  and what has to change.
- The proof and the switch are the ones ADR-027 already tested live, now
  per job.
- No DBUs for the tasks that move; the waits in a mixed job cost nothing.

**Negative (costs):**

- **A second implementation of bundle semantics.** The resolver, the
  notebook translator, `dbutils` and the `{{...}}` values copy Databricks
  behaviour that Databricks can change at any release. The conformance
  test covers the resolver only; the rest are as good as their tests.
- **Code that builds names at run time escapes the plan.** It fails at run
  time, before writing, but it fails late.
- **Table ownership relaxes for bundle pipelines in `catalog: glue`**
  (section 6): ADR-016's one-pipeline-per-schema rule no longer holds for
  them.
- **Email subscriptions need each recipient to confirm** the SNS email
  once; until then that address gets nothing.
- **One runner per job** means the job's compute is sized for its largest
  task, and Lambda's 15 minutes cover the whole job.
- **Managed tables still need the per-table switch** of ADR-027. A job that
  writes ten managed tables needs ten renames.
- **Mixed jobs need the Databricks job deployed and paused**, and a team
  that forgets `trigger_pause_status: PAUSED` has two orchestrators. The
  plan and the single-writer check catch it only at the first run,
  after the deploy.
- **`run-now` with `only`** is shown in the Databricks SDK; the rendered
  REST docs do not show it. Slice 9 must prove it on the test workspace before a
  mixed job is relied on.
- **Spark version skew**: Databricks runtimes ship Spark versions other
  than clavesa's 4.0.2, and code that depends on that difference behaves
  differently. The plan warns and does not prove anything.

## Live proof, 2026-11-14 (slice 11)

`make e2e-bundle` on the test workspace (`ucpub`, us-east-2) with Databricks'
own default-sql example: the same bundle deployed with `databricks bundle
deploy -t dbx` and `clavesa bundle deploy -t aws`, both runs green, and
`clavesa bundle compare` PASS on both tables (107 and 31 partitions, the
columns equal). The mixed target proved `run-now` with `only`: Databricks ran
`orders_daily` and marked `orders_raw` DISABLED (the Consequences item
below). Found and fixed on the way: `{{name}}` in a SQL file is a quoted
string in Databricks; `read_files` is now run, not refused; a workspace whose
modules were extracted before slice 3 lacked the task module; `catalog:
unity` needs the runner's Glue access to the job's databases too; the runner
image was pushed to the profile's region instead of the workspace's.

Two clavesa targets of one bundle in one workspace shared the pipeline
`<bundle>_<job>` (the e2e's `aws` and `aws-mixed` did). Since v2.25.1 the
target is in the pipeline name. Fargate segments for mixed jobs are in
TODO.md.

## Decided 2026-11-14

Head Jeeves answered the three open questions of the first draft:

1. **EMR Serverless is not next.** It comes after slices 1 to 6, and only
   when a real job needs more than 16 vCPU or a JAR task. Until then the
   plan refuses those tasks and says why.
2. **ADR-016 section 5 is relaxed for bundle pipelines in `catalog: glue`
   only** (section 6). A hand-made pipeline, and the Glue side of a
   `catalog: unity` bundle pipeline, keep the one-pipeline-per-schema rule.
   ADR-016 has a dated update that says the same.
3. **Notifications go to SNS, `health` rules become CloudWatch alarms**,
   kept minimal (section 7): one topic per bundle target, email
   subscriptions from the bundle, alarms on that topic. Built in slice 7.

## Open questions

1. **Concurrency in `for_each_task`**: v1 runs the items one after the
   other. Threads in one Spark session are possible but change failure
   isolation; decide after a real job asks.

## Relationships

- **ADR-001, ADR-002**: generated Terraform and Step Functions, unchanged;
  the single-Task state machine stays for jobs that run fully on clavesa.
- **ADR-012**: one engine; Databricks code runs on the runner's PySpark.
- **ADR-014, ADR-024**: DAB semantics live in the runner so a local run and
  a cloud run are the same; `--compute local` works for bundles.
- **ADR-015**: CLI and UI share the bundle service calls.
- **ADR-016**: the `<catalog>__<schema>` encoding for `catalog: glue`; its
  section 5 is relaxed for bundle pipelines in `catalog: glue` (section 6).
- **ADR-023**: SQL is parsed with `sqlglot`, already in the runner image.
- **ADR-027**: the Unity Catalog write, lineage, shadow mode, comparison,
  switch and roll-back; this ADR widens its scope from one step to a job.
- **ADR-028**: a Kinesis source can feed a bundle task like any other.
