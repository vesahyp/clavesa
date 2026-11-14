# Getting started: clavesa for a Databricks team

> **For:** a team that runs its pipelines on Databricks with Unity Catalog and wants one step of a pipeline to run outside Databricks compute. Clavesa runs that step on AWS (Fargate or Lambda) in your own account, writes its table into your Unity Catalog under the name the step used, keeps lineage in Catalog Explorer, and starts the Databricks jobs that come next. Nothing changes for the readers of the table.

You follow this guide from top to bottom. Nothing is switched until a daily comparison has proved, for a week, that clavesa writes the same data as the Databricks step. Every step has a way back.

The design is [ADR-027](../decisions/027-unity-catalog-publish.md). Two recipes go deeper: [replace-a-databricks-step](../cookbook/replace-a-databricks-step.md) (the switch and the roll-back in detail) and [feed-a-databricks-job](../cookbook/feed-a-databricks-job.md) (a new table, not a replaced one).

**A whole job instead of one step?** When the job is defined in a bundle (`databricks.yml`), the same bundle can deploy it to clavesa: [move-a-databricks-job](../cookbook/move-a-databricks-job.md) ([ADR-031](../decisions/031-dab-bundles-on-clavesa.md)). Steps 3 to 11 below still apply: a bundle target with `catalog: unity` uses this connection, shadow mode, the comparison (`clavesa bundle compare`) and the switch (`clavesa bundle switch`).

**Status.** Table registration and lineage use Databricks Public Preview APIs. ADR-027 lists what is proven and what is still open.

## Contents

1. [Before you start](#1-before-you-start)
2. [Install](#2-install)
3. [The service principal (Databricks admin)](#3-the-service-principal-databricks-admin)
4. [The preflight: what the admins must grant](#4-the-preflight-what-the-admins-must-grant)
5. [Create the clavesa workspace in AWS](#5-create-the-clavesa-workspace-in-aws)
6. [Connect the workspace to Databricks](#6-connect-the-workspace-to-databricks)
7. [The step on your own S3 bucket, in shadow mode](#7-the-step-on-your-own-s3-bucket-in-shadow-mode)
8. [Tables and lineage in Unity Catalog](#8-tables-and-lineage-in-unity-catalog)
9. [The daily comparison](#9-the-daily-comparison)
10. [The switch, and the downstream jobs](#10-the-switch-and-the-downstream-jobs)
11. [Roll-back](#11-roll-back)
12. [Cost](#12-cost)
13. [Troubleshooting](#13-troubleshooting)

## 1. Before you start

**Who does what.** You (the engineer) do every step in this guide. Two people grant rights once: a **Databricks admin** (a metastore or workspace admin) and an **AWS admin** of the account clavesa deploys into. The preflight in step 4 prints the exact list for each of them; nobody has to guess.

**Pick the step.** A good first step is cheap to run outside Databricks: a daily clean-up of raw files into a table partitioned by day, a reshape, an aggregate. Write down:

- the table it writes, `<catalog>.<schema>.<table>`, and the column it is partitioned by;
- the Databricks job that runs it, and its job ID (Workflows > the job > the ID in the URL);
- what it reads: raw files in S3 (their bucket and prefix, and the format: JSON lines, CSV or Parquet), or a Unity Catalog table;
- its SQL;
- the jobs that run after it and read its table;
- one or two numeric columns whose daily sums prove the data is the same (amounts, counts).

**On your machine:**

- macOS or Linux, with **Docker** running (clavesa runs Spark in a container for previews and the comparison).
- **Terraform** 1.10 or newer, the **AWS CLI** v2, and the **Databricks CLI** (`brew install terraform awscli databricks/tap/databricks`).
- An AWS login to the account clavesa deploys into (`aws sso login` or a profile). It needs rights to create Lambda, Step Functions, ECS, EventBridge, IAM roles, Glue, S3, ECR, Secrets Manager and CloudWatch Logs resources; the preflight checks them.

**Set these once** in the terminal you work in. Every command below uses them, so you can copy each block as it is:

```bash
export AWS_PROFILE=<your AWS profile>         # the account clavesa deploys into
export AWS_REGION=<region>                    # where the team's bucket is, e.g. eu-west-1
DBX_HOST=https://<workspace>.cloud.databricks.com
DBX_PROFILE=work                              # your own Databricks CLI login
SP_ID=<service principal application ID>      # from step 3
TABLE=<catalog>.<schema>.<table>              # the table the Databricks step writes
SHADOW=${TABLE}_clavesa                       # the table clavesa writes next to it
PARTITION=event_date                          # the column TABLE is partitioned by
SUMS=amount,quantity                          # numeric columns to compare per day
JOB=<job ID>                                  # the Databricks job that runs the step
TEAM_BUCKET=<the team's bucket>               # where the step's input files land
RAW_PREFIX=raw/events/                        # their prefix, ending in /
SHADOW_LOCATION=s3://$TEAM_BUCKET/clavesa/<table>/   # a new, empty folder for the shadow table
READERS=<group>                               # the group that reads TABLE from SQL warehouses
WS_NAME=<short name>                          # the clavesa workspace, e.g. team-dbx
WS=~/clavesa/$WS_NAME                         # where it lives on your machine
PIPELINE=<pipeline name>                      # e.g. daily_events
```

## 2. Install

```bash
brew install --cask vesahyp/clavesa/clavesa
clavesa version
```

`brew upgrade --cask clavesa` updates it later. On Linux, use the tarball from the [releases page](https://github.com/vesahyp/clavesa/releases) (see the [README](../../README.md#install)).

## 3. The service principal (Databricks admin)

Clavesa signs in to Databricks as a service principal with an OAuth secret. A Databricks admin creates it once:

1. **Settings > Identity and access > Service principals > Add service principal**, named for example `clavesa`. Its **Application ID** is the `SP_ID` above.
2. On the service principal: **Configurations > Entitlements > Workspace access** on. Without it the principal signs in and then gets `403` on every Unity Catalog call.
3. **Secrets > Generate secret**. Copy the secret once; Databricks does not show it again.

Put the secret into AWS Secrets Manager, in the region you deploy to, as a plain string. The runner reads it from there at run time; it never goes into a clavesa file or into Terraform state.

```bash
read -rs DBX_SECRET     # paste the secret, then Enter; nothing is echoed
aws secretsmanager create-secret --name clavesa/$WS_NAME/databricks-sp \
  --secret-string "$DBX_SECRET" --query ARN --output text
unset DBX_SECRET
```

Keep the ARN it prints:

```bash
SECRET_ARN=$(aws secretsmanager describe-secret --secret-id clavesa/$WS_NAME/databricks-sp --query ARN --output text)
```

## 4. The preflight: what the admins must grant

The preflight checks every right and setting clavesa needs, in Databricks and in AWS, and prints the exact `GRANT` statements and steps for whatever is missing. It only reads, and it needs no clavesa workspace, so run it first and hand its output to the admins.

Sign in to Databricks as yourself, then run it:

```bash
databricks auth login --host $DBX_HOST --profile $DBX_PROFILE
clavesa workspace databricks preflight \
  --host $DBX_HOST --profile $DBX_PROFILE --principal $SP_ID \
  --table $TABLE --shadow $SHADOW --shadow-location $SHADOW_LOCATION \
  --job $JOB --reader $READERS --fargate-cpu 8
```

Add `--input <catalog>.<schema>.<table>` for each Unity Catalog table the step reads, and `--start-job <id>,<id>` for the jobs clavesa will start after each run (step 10).

Each row is `ok`, `FAIL` with the fix, or `??` (your login could not read it; the grant is listed anyway). The end of the output is the list for the admins: the `GRANT` statements to run in the Databricks SQL editor, and the AWS steps. Send it as it is. Run the preflight again until it prints:

```
Ready: nothing is missing.
```

and exits with status 0.

What it says about the table decides the switch in step 10:

- `MANAGED` (the usual case): clavesa never writes a managed table. At the switch the two tables swap names, and the old one stays as `<table>_before_clavesa` for the way back.
- `EXTERNAL`: clavesa writes it in place after the switch, if its Delta features are ones clavesa writes (the preflight checks them).
- `partitioned by <column>`: the output settings in step 7 (`replace_partitions`).

The preflight assumes the step runs on Fargate (`--compute fargate`) with the vCPUs of `--fargate-cpu` (8 here, the size step 7 starts with), and also checks the network for the task (the default VPC, or `--subnet`) and the Fargate vCPU quota. See [Troubleshooting](#13-troubleshooting) for an account that has never run Fargate.

## 5. Create the clavesa workspace in AWS

A workspace is a folder with clavesa's Terraform in it, and the AWS resources it deploys: a bucket for clavesa's own tables, an ECR repository for the runner image, a Glue database.

```bash
mkdir -p $(dirname $WS)
clavesa workspace init $WS_NAME --workspace $WS
cd $WS
clavesa workspace use --warehouse cloud --profile $AWS_PROFILE
clavesa workspace deploy --yes
```

`workspace deploy` builds the runner image and pushes it to ECR, so the first one takes several minutes. The AWS resources are named after the workspace (`clavesa-$WS_NAME-…`, bucket `$WS_NAME-clavesa`).

**For a team, keep the Terraform state in S3**, so a second person (or a second laptop) deploys the same resources: [remote state](../remote-state.md). Do it now, before the first pipeline; it is two commands.

Put the workspace folder in git (the state files and `.clavesa/` caches are already in its `.gitignore`).

## 6. Connect the workspace to Databricks

Register the service principal by reference (the secret stays in Secrets Manager), then connect. `connect` checks the admin's part again and records the connection only when nothing fails. `--catalog` is the catalog your tables are in; clavesa creates no catalog and no schema in it.

```bash
clavesa credential register dbx-sp --kind databricks-oauth \
  --client-id $SP_ID --secret $SECRET_ARN
clavesa workspace connect databricks --host $DBX_HOST \
  --catalog ${TABLE%%.*} --credential dbx-sp --reader $READERS
clavesa workspace deploy --yes
clavesa workspace databricks --check
```

Every check prints `ok`. One line says `skip  Storage for new tables: no --external-location`: that is expected. The shadow table's folder is in the team's bucket (`--output-unity-catalog-location` in step 7), under the team's external location, so clavesa needs no storage of its own in Unity Catalog. `--reader` gives the group `SELECT` on each table clavesa registers, so the shadow table is readable from SQL warehouses from its first run.

## 7. The step on your own S3 bucket, in shadow mode

In shadow mode clavesa runs the step every day and writes a **second** table, `$SHADOW`, in the team's bucket. The Databricks step keeps running unchanged and nobody reads the shadow table yet.

**The input.** Raw files in the team's bucket, with the same partition key as the table. (For a Unity Catalog table instead: `clavesa source register raw --from uc://<catalog>.<schema>.<table>`. For a step that reads a **Kinesis stream**, see the box below.)

> **A step that reads Kinesis** ([ADR-028](../decisions/028-kinesis-stream-source.md), recipe: [kinesis-to-unity-catalog](../cookbook/kinesis-to-unity-catalog.md)). Register the stream instead of the files: `clavesa source register raw_events --kind kinesis --stream <stream name>`. Each scheduled run reads what arrived since the last run, from a checkpoint in the clavesa bucket, and appends it to the table; a run that stops part-way writes nothing twice. Your SQL runs per micro-batch, so keep it row-level (parse, cast, filter), keep the columns `_source_partition` and `_source_offset`, and use `--output-mode append` with `--output-partition-by` (no `replace_partitions`). Schedule the runs more often than the stream's retention (24 hours by default), for example every 15 minutes. In shadow mode clavesa is a second reader of the stream next to the Databricks job: a shard serves 5 reads a second and 2 MB/s to all its polling readers together, so check that the stream has that headroom (CloudWatch `ReadProvisionedThroughputExceeded` stays at 0). The daily comparison in step 9 works the same, by the day column your SQL derives.

```bash
clavesa source register raw_events --kind s3 --bucket $TEAM_BUCKET --prefix $RAW_PREFIX \
  --format json --partitions $PARTITION --start-from all --no-trigger
```

- `--format`: `json` (JSON lines), `csv` or `parquet`.
- `--start-from all`: the first run reads every day there is.
- `--no-trigger`: new files do not start a run. The pipeline runs once a day on its schedule, like the step it replaces, and reads the files that arrived since the last run.

The bucket's owner turns on its event notifications once, so clavesa sees new files: **S3 console > the bucket > Properties > Amazon EventBridge > On**. They take a few minutes to start.

**The step.** The same SQL the Databricks step runs, reading the input as `raw`. Write `$PIPELINE` and the SQL into the node; this example is the test team's daily clean-up, put yours in its place:

```bash
clavesa pipeline create $PIPELINE --schema ${PIPELINE}
clavesa node add $PIPELINE --type transform --name daily
clavesa node edit $PIPELINE daily \
  --set "sql=SELECT event_id, user_id, event_type, CAST(amount AS DECIMAL(12,2)) AS amount, CAST(quantity AS INT) AS quantity, CAST(ts AS TIMESTAMP) AS ts, properties.page AS page, CAST(event_date AS DATE) AS event_date FROM raw WHERE event_type <> 'test'" \
  --set compute=fargate \
  --output-mode replace_partitions --output-partition-by $PARTITION \
  --output-unity-catalog-table $SHADOW \
  --output-unity-catalog-location $SHADOW_LOCATION
clavesa source attach $PIPELINE raw_events --to daily --as raw
```

- `compute=fargate`: one ECS Fargate task per run, no time limit. `lambda` is cheaper for small days but stops at 15 minutes and 10 GB of memory.
- `replace_partitions` with `--output-partition-by`: each run replaces only the days in its output, so a re-run of a day is safe and the other days stay.
- `--output-unity-catalog-table`: the full name of the table clavesa writes. `--output-unity-catalog-location`: where a new table's files go.

The first shadow run is the test of the SQL: it writes only the shadow table, so a wrong result costs nothing but a re-run. (`clavesa node preview` does not yet read nested JSON or partition columns from the path the way a run does: [#104](https://github.com/vesahyp/clavesa/issues/104).)

**Size and schedule.** Put them in the pipeline's `terraform.tfvars`. Schedule the run after the Databricks step usually finishes:

```bash
cat > $PIPELINE/terraform.tfvars <<'EOF'
fargate_cpu       = 8
fargate_memory_gb = 16
trigger_schedule  = "cron(30 4 * * ? *)"   # 04:30 UTC every day
EOF
```

8 vCPU and 16 GB is the place to start: a test day of 1 GB took 34 seconds and peaked at 2.7 GB of memory. Each run's log ends with its time and peak memory; raise the size only if a day takes too long.

**Deploy and run once:**

```bash
clavesa pipeline deploy $PIPELINE --yes
clavesa pipeline run $PIPELINE --wait
```

The run ends with:

```
Status: SUCCEEDED
Unity Catalog: <catalog>.<schema>.<table>_clavesa registered  https://<host>/explore/data/…
```

A Fargate run takes a minute or two more than its task: AWS starts the task and pulls the image first. The next runs say `written` instead of `registered`.

## 8. Tables and lineage in Unity Catalog

```bash
clavesa workspace databricks tables
```

lists each pipeline's tables with a Catalog Explorer link:

```
daily_events  →  main.daily_events
    main.sales.daily_events_clavesa      EXTERNAL written by clavesa
      https://<host>/explore/data/main/sales/daily_events_clavesa
``` In **Catalog Explorer**, the shadow table is an External table in the team's bucket; its **Lineage** tab shows the input, the clavesa step (`clavesa_<pipeline>_<node>`) and the table, with the column mapping. The step is a box of its own in the graph because it runs outside Databricks.

The clavesa UI shows the same, and the run history:

```bash
clavesa ui
```

opens `http://localhost:8080`. **Unity Catalog** in the nav lists each pipeline's tables (**Published to Unity Catalog**, **Open in Catalog Explorer**), the connection's checks, the preflight form and the comparison form. A pipeline's run page has a **Databricks** card with the tables the run wrote and the jobs it started.

## 9. The daily comparison

The comparison reads both tables and prints, for each day, the rows and the exact sum of each `SUMS` column in both, and the columns of both:

```bash
clavesa pipeline databricks-compare $PIPELINE --existing $TABLE --shadow $SHADOW \
  --partition $PARTITION --sum $SUMS --where "$PARTITION >= date_sub(current_date(), 7)"
```

```
event_date=2026-11-01  rows 49 / 49  sum(amount) 505.310000 / 505.310000  sum(quantity) 84.000000 / 84.000000  match
event_date=2026-11-04  rows - / 49  sum(amount) - / 505.310000  sum(quantity) - / 84.000000  missing in existing
FAIL
```

- **PASS** (exit status 0): every day is in both tables, every number is equal, and the columns match.
- **FAIL** (exit status 1): anything else. Sums are exact (`decimal(38,6)`), so a difference is real. A `missing in existing` or `missing in shadow` for today usually means one side has not run yet.
- **No DBUs**: Spark in the local runner container reads both Delta tables directly, a managed table with credentials Unity Catalog vends for it. Docker must be running.

Run it every day after both steps have run. From cron on a machine that has Docker, your AWS login and the workspace (fill in the values; cron does not see your shell's variables):

```
0 7 * * * cd ~/clavesa/team-dbx && AWS_PROFILE=… AWS_REGION=… clavesa pipeline databricks-compare daily_events --existing … --shadow … --partition event_date --sum amount,quantity --where "event_date >= date_sub(current_date(), 7)" || <your alert>
```

Leave out `--where` to compare the whole table.

**Ready to switch** when the comparison has passed every day for at least 7 days in a row, including the days the step behaves differently on (month ends, weekends).

## 10. The switch, and the downstream jobs

The switch has three parts, done in order by the person who can run SQL in Databricks:

1. **History.** The shadow table has the days since shadow mode began. Copy the older days in once, with one `INSERT … SELECT` between two clavesa runs, then compare the whole table.
2. **The switch.** Pause the Databricks job and wait until it has no active run; compare the whole table once more; swap the names (a managed table) or skip that (an external table, which clavesa takes over in place); copy the old table's grants to the new one.
3. **Point clavesa at the real name** and name the paused job, so that clavesa refuses to write while that job is unpaused or running:

   ```bash
   clavesa node edit $PIPELINE daily \
     --output-unity-catalog-table $TABLE \
     --output-replaces-databricks-job $JOB
   clavesa pipeline deploy $PIPELINE --yes
   clavesa pipeline run $PIPELINE --wait
   ```

   The run says `Unity Catalog: <table> written`.

The exact SQL for 1 and 2, the grants the person needs first, and the grant copy with the Databricks CLI are in [replace-a-databricks-step, steps 3 and 4](../cookbook/replace-a-databricks-step.md#3-history). Follow them there.

**The jobs that come next.** The jobs that ran after the Databricks step (or the tasks after it, split out into a job of their own if the step was one task of a bigger job) now start when clavesa's run succeeds. The service principal needs `CAN MANAGE RUN` on each:

```bash
clavesa pipeline databricks-jobs $PIPELINE --set <job id>,<job id>
clavesa pipeline deploy $PIPELINE --yes
```

Each job starts once after every successful run, and the run page links to the Databricks run. Set them at the switch, not before: in shadow mode those jobs still read the Databricks step's table.

**Check after the switch:** a SQL warehouse query as one of the readers, the table's Lineage tab (the clavesa step upstream), and the next runs of the downstream jobs. Keep `<table>_before_clavesa` for a month, then drop it.

## 11. Roll-back

For a managed table (the names were swapped):

1. In the Databricks SQL editor, swap the names back (the statements are in [replace-a-databricks-step, step 5](../cookbook/replace-a-databricks-step.md#5-roll-back)).
2. Put clavesa back in shadow mode:

   ```bash
   clavesa node edit $PIPELINE daily \
     --output-unity-catalog-table $SHADOW \
     --output-replaces-databricks-job 0
   clavesa pipeline databricks-jobs $PIPELINE --clear
   clavesa pipeline deploy $PIPELINE --yes
   ```

3. Unpause the Databricks job and run it for the days clavesa wrote since the switch.
4. Run the comparison until it passes again.

For an external table taken over in place, the same recipe step has the `RESTORE TABLE` way back.

## 12. Cost

Clavesa pays AWS, in your account; there is no clavesa service to pay.

- **Measured**, on synthetic events at about 1 KB an event, ARM Fargate in us-east-2: a 1 GB day took 34 seconds of task time at 8 vCPU and 16 GB (about 0.003 USD), plus a minute or two of start-up. Time grew linearly with the data in the measured range (up to 1.25 GB a run).
- **Estimated, not measured:** a step that reads 2 TB a month (67 GB a day) extrapolates to about 11 minutes a day at 8 vCPU, about 2 USD a month of Fargate, about 3 to 4 USD a month with the fixed AWS costs. 67 GB is 54 times the largest measured run, so treat this as an estimate. Check it against the first real runs: each run's log ends with its time and peak memory.
- **A Kinesis step, estimated, not measured at volume:** 2 billion events and 2 TB a month, read every 15 minutes (about 23,000 events and 23 MB a run) on a 2 vCPU, 8 GB Fargate task. Most of each run is start-up (about a minute) and the drain is seconds, so about 1.5 to 2 minutes of task time per run: about 10 to 15 USD a month of Fargate, plus a few USD of S3 requests and logs. Lambda costs more at this rate (a 10 GB function for about 90 seconds, 2,900 times a month: about 45 USD). The stream itself is the team's and costs the same either way. The proof (`make smoke-kinesis`) ran 4,000 events; it proves correctness, not throughput.
- **The Databricks side:** the step's DBUs stop after the switch. Look up what the step costs today before you decide (`system.billing.usage`, filtered to the job's ID).
- The preflight and the comparison cost no DBUs.

## 13. Troubleshooting

- **Preflight rows say `??`.** Your login cannot read another principal's rights. The grants are listed anyway; a metastore admin can run the preflight to see every row.
- **`You've reached the limit on the number of vCPUs`** on the first Fargate run. AWS can enforce a lower vCPU limit than the quota shows on an account that has not run Fargate yet. Run at 4 or 8 vCPU for a few days, or ask AWS support.
- **The first run after `--start-from all` reads nothing new later.** New files arrive through the bucket's EventBridge notifications; check they are on (step 7) and give them a few minutes after turning them on.
- **`… still has an unpaused schedule … Nothing was written`.** The job named with `--output-replaces-databricks-job` is not paused, or has an active run. Pause it and run again.
- **A SQL warehouse gets `AmazonS3Exception: Forbidden; … Proxy: …`** on a table in the team's bucket. That answer comes from Databricks' serverless proxy, not from S3 or clavesa: the test workspace hit it on Databricks' own managed tables in the same bucket. Check that your warehouses already read the table's bucket before the switch.
- **Catalog Explorer shows an old column list** after you add a column to the step. Readers already see the new column. The run prints the statement an admin runs to refresh the list: `REPAIR TABLE <table> SYNC METADATA`.
- **A run fails.** `clavesa pipeline runs $PIPELINE`, then `clavesa pipeline logs $PIPELINE --tail 200`, show which node and the full error: [debugging-runs](../cookbook/debugging-runs.md).
