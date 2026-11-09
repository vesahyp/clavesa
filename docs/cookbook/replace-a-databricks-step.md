# Replace a Databricks step with clavesa

> **When you have one:** a Databricks pipeline with a step that is cheap to run outside Databricks (a daily clean-up of raw events into a table partitioned by day, a reshape, an aggregate). Clavesa runs that step on its own compute, writes the same table under the same name, and the rest of the pipeline keeps running on Databricks ([ADR-027](../decisions/027-unity-catalog-publish.md)). DBUs stop for that step.

This is a recipe, not a design: every command below was run against the test workspace (`ucpub`, catalog `team_ucpub`, docs/ideas/unity-catalog-publish.md "Shadow-mode recipe test"). Nothing is switched until the numbers say so, and every step has a way back.

The order:

1. **Preflight** (read-only): what the admins must grant.
2. **Shadow mode**: clavesa writes a second table next to the step's table. The Databricks step keeps running unchanged.
3. **Daily comparison**: rows and key sums per day, both tables, pass or fail.
4. **History**: copy the days before shadow mode into the shadow table.
5. **Switch**: pause the Databricks step; the shadow table takes the name.
6. **Roll-back**, if needed: the names go back; the Databricks step runs again.

The names used below: the step's table `team_ucpub.sales.daily_events`, partitioned by `event_date`, written by Databricks job `923072819710352`; the team's bucket `ucpub-team-699166197771`; the clavesa workspace `ucpub` connected to Databricks; the service principal clavesa runs as `d7775f24-…`. Put yours in their place.

## 0. Preflight

Run it as yourself (a Databricks CLI login) against the work workspace, with your AWS login to the account clavesa deploys into. It only reads.

```bash
databricks auth login --host https://<work workspace> --profile work
clavesa workspace databricks preflight \
  --host https://<work workspace> --profile work \
  --principal <service principal application ID> \
  --table team_ucpub.sales.daily_events \
  --shadow-location s3://ucpub-team-699166197771/clavesa/daily_events/ \
  --job 923072819710352 --reader <group that reads the table>
```

It assumes the step runs on Fargate (`--compute fargate`, the default; `--fargate-cpu 16`), and then also checks the Fargate deploy rights, a network for the task (the default VPC, or the subnets you give with `--subnet`) and the Fargate vCPU quota. AWS can enforce a lower vCPU limit than the quota shows on an account that has not used Fargate yet: the test account refused its first 16 vCPU task and took 8, then 16 after a few runs. If the first run fails with "You've reached the limit on the number of vCPUs", run at 8 for a few days or ask AWS support.

Each row is `ok`, `FAIL` (with the fix) or `??` (could not be read with your rights). The end of the output is the list for the admins: the `GRANT` statements to run in the SQL editor, and the other steps. Send it as it is, and run the preflight again until it prints `Ready: nothing is missing.` and exits 0.

What it tells you about the table decides the switch in step 5:

- `MANAGED`: clavesa cannot write it. The switch renames: the shadow table takes the name, the old one stays as `<table>_before_clavesa`.
- `EXTERNAL`: clavesa writes it in place after the switch. Its folder needs the runner role's write access (pipeline deploy adds it) and its Delta features must be ones clavesa writes (the preflight checks them).
- `partitioned by event_date`: use the output settings it prints (`mode = "replace_partitions"`, `partition_by`).

## 1. Shadow mode

Connect the workspace once, with the people who read the table from SQL warehouses as readers (they get `SELECT` on the tables clavesa registers):

```bash
clavesa workspace connect databricks --host https://<work workspace> \
  --catalog team_ucpub --credential dbx-sp --reader <group>
clavesa workspace deploy --yes
```

The step's input. Raw files: an S3 source with the same partition key (`json`, `csv` and `parquet` all work). A table Databricks writes: `clavesa source register <name> --from uc://<catalog>.<schema>.<table>`.

```bash
clavesa source register events_raw --kind s3 --bucket ucpub-team-699166197771 \
  --prefix raw/events/ --format json --partitions event_date --start-from all --no-trigger
```

`--no-trigger`: new files do not start a run; the pipeline runs on its schedule, once a day like the step it replaces. Without it, every batch of files landing during the day starts a run, and with `replace_partitions` each run re-reads the whole day.

The step itself: the same SQL the Databricks step runs on Fargate, writing a **separate** table, `<table>_clavesa`, in the team's bucket:

```bash
clavesa pipeline create uc_events --schema events
clavesa node add uc_events --type transform --name daily_events
clavesa node edit uc_events daily_events \
  --set "sql=SELECT event_id, user_id, event_type, CAST(amount AS DECIMAL(12,2)) AS amount, CAST(quantity AS INT) AS quantity, CAST(ts AS TIMESTAMP) AS ts, properties.page AS page, CAST(event_date AS DATE) AS event_date FROM raw WHERE event_type <> 'test'" \
  --set compute=fargate \
  --output-mode replace_partitions --output-partition-by event_date \
  --output-unity-catalog-table team_ucpub.sales.daily_events_clavesa \
  --output-unity-catalog-location s3://ucpub-team-699166197771/clavesa/daily_events/
clavesa source attach uc_events events_raw --to daily_events --as raw
```

The task size and the schedule go in the pipeline's `terraform.tfvars` (a new file in `<workspace>/uc_events/`). 16 vCPU and 32 GB are the defaults; the 1 GB test day peaked at 4 GB of memory, so memory is not the limit. Schedule the run after the Databricks step's usual finish time:

```hcl
fargate_cpu       = 16
fargate_memory_gb = 32
trigger_schedule  = "cron(30 4 * * ? *)"   # 04:30 UTC every day
```

```bash
clavesa pipeline deploy uc_events --yes
clavesa pipeline run uc_events --wait
```

The first run reads every day there is (`--start-from all`) and ends with `Unity Catalog: team_ucpub.sales.daily_events_clavesa registered`. After it, each run reads the files that arrived since the last run, through the bucket's S3 event notifications to EventBridge: the bucket's owner turns them on once (S3 console > the bucket > Properties > Amazon EventBridge > On; they take a few minutes to start). With `replace_partitions`, a run re-reads every day those files belong to, whole, so a day that landed in several batches is complete.

`replace_partitions` replaces only the days in the run's output, so a re-run of a day is safe and the other days stay. The run's log (CloudWatch, the runner Lambda's log group, streams `fargate/…`) ends with its time and peak memory: `fargate: run took 33.7s, peak memory 3750 MB of 32768 MB`.

## 2. The daily comparison

```bash
clavesa pipeline databricks-compare uc_events \
  --existing team_ucpub.sales.daily_events --shadow team_ucpub.sales.daily_events_clavesa \
  --partition event_date --sum amount,quantity --where "event_date >= date_sub(current_date(), 7)"
```

(The Unity Catalog page in `clavesa ui` has the same form. In the clavesa repo, `make shadow-compare` wraps the command.) For each day: rows, and the exact sum of each key column, in the existing table and the shadow table, and the column names and types of both.

```
event_date=2026-11-01  rows 49 / 49  sum(amount) 505.310000 / 505.310000  sum(quantity) 84.000000 / 84.000000  match
event_date=2026-11-04  rows - / 49  sum(amount) - / 505.310000  sum(quantity) - / 84.000000  missing in existing
FAIL
```

- **Pass**: every day is in both tables, every number is equal, and the columns match. Exit status 0.
- **Fail**: anything else. Exit status 1, so a cron job can alert on it. Sums are exact (each value is summed as `decimal(38,6)`), so a difference is a real difference.
- **Cost**: it runs in the local runner container (Docker) against the cloud: Spark reads both Delta tables directly, a managed table through credentials Unity Catalog vends. No DBUs, no warehouse.

Run it every day after both steps have run, for example from cron (jeeves' `scripts/cronrun` pushes to the phone when it exits non-zero). A `missing in existing` or `missing in shadow` for today usually means one of the two has not run yet; anything else is a difference to explain before the switch.

**Ready to switch** when the comparison has passed for 7 days in a row, every day of the week the step behaves differently on (month ends, weekends) included.

## 3. History

The shadow table holds the days since shadow mode started. Readers of the switched table expect all of history, so copy the older days in once, before the switch.

The shadow table is owned by clavesa's service principal, so the person who does steps 3 and 4 first needs `MODIFY` (to copy) and `MANAGE` (to rename) on it. A metastore admin can grant them; the owner of the catalog cannot write into a table someone else owns:

```sql
GRANT MODIFY, MANAGE, SELECT ON TABLE team_ucpub.sales.daily_events_clavesa TO `<you>`;
```

Then one statement in the Databricks SQL editor (it costs DBUs once; the copy reads and writes every older day):

```sql
INSERT INTO team_ucpub.sales.daily_events_clavesa
SELECT * FROM team_ucpub.sales.daily_events
WHERE event_date < DATE'<first day of shadow mode>';
```

Do it between two clavesa runs (two writers at the same moment corrupt a Delta table). Clavesa keeps writing the table afterwards: the rehearsal had Databricks delete and insert into the shadow table, and the next clavesa runs wrote it as before. Then compare the whole table, without `--where`:

```bash
clavesa pipeline databricks-compare uc_events --existing … --shadow … --partition event_date --sum amount,quantity
```

## 4. The switch

Do it right after a clavesa run, with the comparison passing.

1. **Pause the Databricks step.** Jobs > the job > Pause (or `databricks jobs update` with `pause_status: PAUSED` on its schedule). Wait until it has no active run. If the step is one task of a bigger job, split the tasks after it into their own job first (clavesa starts it, step 6).

2. **Final comparison**, the whole table: the comparison without `--where`. It must pass.

3. **Swap the names** (managed table). In the SQL editor:

   ```sql
   ALTER TABLE team_ucpub.sales.daily_events RENAME TO team_ucpub.sales.daily_events_before_clavesa;
   ALTER TABLE team_ucpub.sales.daily_events_clavesa RENAME TO team_ucpub.sales.daily_events;
   ```

   For an **external** table skip this step: clavesa takes the table over in place in step 5, and the shadow table can be dropped after the switch.

4. **Give the new table the old table's grants.** Grants stay with the renamed old table. Copy them all at once with the Databricks CLI:

   ```bash
   databricks grants get table team_ucpub.sales.daily_events_before_clavesa -o json \
     | python3 -c 'import json,sys; d=json.load(sys.stdin); print(json.dumps({"changes": [{"principal": a["principal"], "add": a["privileges"]} for a in d.get("privilege_assignments", [])]}))' \
     > grants.json
   databricks grants update table team_ucpub.sales.daily_events --json @grants.json
   ```

   The owner of the new table is clavesa's service principal; keep it (clavesa needs to write it).

5. **Point clavesa at the table's real name**, and name the paused job, so that clavesa refuses to write while that job is unpaused or running:

   ```bash
   clavesa node edit uc_events daily_events \
     --output-unity-catalog-table team_ucpub.sales.daily_events \
     --output-replaces-databricks-job 923072819710352
   clavesa pipeline deploy uc_events --yes
   clavesa pipeline run uc_events --wait
   ```

   The run says `Unity Catalog: team_ucpub.sales.daily_events written`, after `replaced Databricks job 923072819710352 (…) is paused and idle` in its log. A run that finds the job unpaused fails before it writes anything:

   ```
   team_ucpub.sales.daily_events: Databricks job 923072819710352 ('…') still has an unpaused schedule. It writes the table clavesa now writes; pause it in Databricks (Jobs, …, Pause) before clavesa writes team_ucpub.sales.daily_events. Nothing was written.
   ```

6. **The steps after it.** Jobs that ran after the step (or the tasks you split out in 1): let clavesa start them after each successful run, `clavesa pipeline databricks-jobs uc_events --set <job id>,…`, then `clavesa pipeline deploy uc_events --yes`.

7. **Check**: a SQL warehouse query as one of the readers (`SELECT event_date, count(*) FROM team_ucpub.sales.daily_events GROUP BY 1`), the table's Lineage tab in Catalog Explorer (the clavesa step upstream), and the downstream jobs' next runs.

Keep `daily_events_before_clavesa` until you no longer need the way back (a month is reasonable), then drop it.

## 5. Roll-back

Managed table (the names were swapped):

1. In the SQL editor, swap the names back:

   ```sql
   ALTER TABLE team_ucpub.sales.daily_events RENAME TO team_ucpub.sales.daily_events_clavesa;
   ALTER TABLE team_ucpub.sales.daily_events_before_clavesa RENAME TO team_ucpub.sales.daily_events;
   ```

2. Put clavesa back in shadow mode. It keeps running on its schedule and writes the shadow table again, which is harmless:

   ```bash
   clavesa node edit uc_events daily_events \
     --output-unity-catalog-table team_ucpub.sales.daily_events_clavesa \
     --output-replaces-databricks-job 0
   clavesa pipeline databricks-jobs uc_events --clear     # if step 4.6 set jobs
   clavesa pipeline deploy uc_events --yes
   ```

3. Unpause the Databricks job, and run it once for the days clavesa wrote since the switch (the old table has not seen them). The test job takes the days as a parameter; yours may need a backfill run.

4. Run the comparison until it passes again.

External table (taken over in place): pause or stop clavesa's writes to it (step 2 above, with the shadow name), unpause the Databricks job and run it for the days since the switch. If the days clavesa wrote must go entirely, Delta history has them: `DESCRIBE HISTORY <table>` and `RESTORE TABLE <table> TO VERSION AS OF <the version before clavesa's first write>`.

## Gate

`make smoke-databricks-shadow` replays the read-only and clavesa-side steps against the test workspace: the preflight is ready, a shadow run succeeds, and the comparison passes. It costs no DBUs. The switch and the roll-back need SQL and an admin, so they are not in the gate; docs/ideas/unity-catalog-publish.md records their rehearsal.

## Limits

- **Two writers corrupt a Delta table.** Every step above that has Databricks write a clavesa table (history copy) or clavesa write a team table (switch) assumes the other side is not writing at that moment. `--output-replaces-databricks-job` enforces it for the job you name; it cannot know about other writers.
- **Vended credentials are per bucket.** A transform that reads a managed table with vended credentials must not read or write other paths in that bucket in the same run (the comparison reads its two tables one after the other, so it is not affected).
- The column list of a taken-over external table goes stale in Catalog Explorer after a column change; the run prints the `REPAIR TABLE … SYNC METADATA` that refreshes it.
- Grants are copied by hand in step 4.4.
- **SQL warehouse `Forbidden` on a table's storage.** In the test workspace a serverless SQL warehouse could not read any table (Databricks' own managed tables included) in the team bucket: `AmazonS3Exception: Forbidden; Request ID: null; Proxy: 192.168.200.20`, an answer from Databricks' serverless proxy, not from S3, while serverless jobs read and wrote the same tables. It is not clavesa's: if your SQL editor hits it, run the step's SQL in a notebook on serverless jobs compute, as the rehearsal did. At work, check that the team's warehouses already read the table's bucket before the switch.
