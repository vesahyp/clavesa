# One Databricks bundle, deployed to Databricks and to clavesa

> **When you have one:** a Databricks job in a bundle, and you want proof that clavesa computes the same tables before you move it. This is the end-to-end test of [ADR-031](../decisions/031-dab-bundles-on-clavesa.md): `make e2e-bundle`.

The bundle is Databricks' own example: `default_sql` from [databricks/bundle-examples](https://github.com/databricks/bundle-examples/tree/1b358c3d3d82c1560aaed60a68647e1b2d5f0fd2/default_sql), what `databricks bundle init default-sql` creates. Two SQL tasks: `orders_raw` reads the retail sales orders (JSON) into a table, `orders_daily` counts the orders per day. It is fetched at a pinned commit with four small edits, listed in [testdata/e2e/default_sql/README.md](../../testdata/e2e/default_sql/README.md).

## What you need

- A clavesa workspace on the cloud warehouse, connected to the Databricks workspace (getting-started steps 5 and 6), with `bin/clavesa` built (`make build`).
- A Databricks CLI profile for the same workspace (`databricks auth login`), and a SQL warehouse: the smallest serverless one with auto-stop 1 minute is plenty.
- A bucket under a Unity Catalog external location that both the job (you) and clavesa's runner can read: the input goes there once.
- The service principal of the clavesa connection may create tables in the schema (`team_ucpub.sales` in the example; `catalog` and `schema` are bundle variables).

## Run it

```bash
make e2e-bundle E2E_WS=~/clavesa-workspaces/ucpub E2E_PROFILE=dbx-ws \
  E2E_WAREHOUSE=<warehouse id> E2E_SOURCE_BUCKET=<bucket> E2E_DEPLOY_WORKSPACE=1
```

`E2E_DEPLOY_WORKSPACE=1` pushes the runner image first (needed once after a clavesa upgrade). Add `E2E_TEARDOWN=1` to remove what the test made at the end.

The script, step by step:

1. `clavesa bundle plan` on the example as published: it refuses the streaming table, `STREAM`, and the materialized view, with the rewrite for each.
2. The input: Databricks' public `retail-org/sales_orders` file, copied once to `s3://<bucket>/e2e-bundle/retail-org/sales_orders/`.
3. **Databricks:** `databricks bundle deploy -t dbx` and `databricks bundle run -t dbx default_sql_sql_job`: the tables `orders_raw` and `orders_daily` (managed).
4. **Clavesa:** `clavesa bundle deploy -t aws` and `clavesa bundle run -t aws --wait`: the same SQL files on Lambda, in shadow mode, so the tables are `orders_raw_clavesa` and `orders_daily_clavesa`, registered in Unity Catalog next to the Databricks ones, with lineage.
5. **Compare:** `clavesa bundle compare -t aws --job default_sql_sql_job`: rows and sums per day for both tables, and the columns.
6. **Mixed:** target `aws-mixed` runs `orders_raw` on clavesa and starts `orders_daily` on Databricks with run-now `only: [orders_daily]`.

## What you should see

From the run on 2026-11-14 (Databricks test workspace, clavesa workspace `ucpub` in us-east-2):

```
PASS  team_ucpub.sales.orders_daily vs team_ucpub.sales.orders_daily_clavesa (31 partitions)
PASS  team_ucpub.sales.orders_raw vs team_ucpub.sales.orders_raw_clavesa (107 partitions)

Job default_sql_sql_job (pipeline default_sql_default_sql_sql_job): PASS
```

(Since v2.25.1 the pipeline is `default_sql_aws_default_sql_sql_job`: the target is in the name, so `aws` and `aws-mixed` are two pipelines.)

and for the mixed run, the Databricks run of the `aws-mixed` job shows `orders_raw` DISABLED (not in `only`) and `orders_daily` SUCCESS, started by clavesa's state machine (segments `Seg0`, `Seg1Start`, `Seg1Wait`, `Seg1Poll`).

In Catalog Explorer, `team_ucpub.sales` has the four tables; the Lineage tab of `orders_daily_clavesa` shows `orders_raw_clavesa`, then the clavesa task `orders_daily`.

## Cost

One run of the whole test. The run times are measured (2026-11-14); the DBUs are an estimate from them at the 2X-Small serverless rate, because Databricks' billing table fills in hours later:

| | |
|---|---|
| Databricks | the two SQL tasks run about 15 s on a 2X-Small serverless warehouse (twice: the `dbx` job and the mixed run's `orders_daily`), plus the warehouse start and its 1-minute auto-stop each time: about 0.2 to 0.4 DBU, well under 1 USD |
| AWS | runner Lambda about 100 s at 3008 MB per clavesa run, the runs writer about 80 s at 1536 MB: about 0.01 USD; S3, Step Functions and Glue negligible |
| Compare | runs in the local runner container, no DBUs, no Lambda |

Nothing keeps running: both jobs' schedules are deployed paused, and the clavesa schedule is a disabled EventBridge Scheduler schedule.
