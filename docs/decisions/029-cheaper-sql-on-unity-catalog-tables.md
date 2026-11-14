---
adr: 29
title: Scheduled SQL moves into clavesa transforms; ad hoc SQL stays on a right-sized serverless warehouse, with Athena through Glue federation as the later cheap read path
date: 2026-11-09
status: Proposed
deciders: Vesa
---

# ADR 029: Cheaper SQL on clavesa's Unity Catalog tables

**Status**: Proposed (2026-11-09). The claims marked "tested" were run on
the test workspace (`ucpub`, Databricks on AWS us-east-2, AWS account
699166197771) on 2026-11-09. Everything the test made was deleted
(section "The test").

## Context

ADR-027 has clavesa write Delta tables to S3 and register them in Unity
Catalog (UC), to replace steps of a work Databricks pipeline: about 2 TB
and 2 billion events a month, so about 67 GB a day. People and downstream
jobs read those tables through Databricks SQL warehouses, which cost
money while they are awake. In the Kinesis smoke test (2026-11-09) the
2X-Small warehouse used 0.716 DBU, 0.50 USD, for about 15 checks; with
0.11 USD of serverless jobs that is the "about 1 USD" that is more than
the AWS side (0.30 USD). Vesa's question: "lets next plan if there are
cheaper ways to run sql clusters or even use athena for dbx sql".

Two kinds of SQL read these tables, and they have different cost shapes:

- **(a) Scheduled downstream jobs**: known SQL, at a known time, that
  usually reads the newest day and writes a result table.
- **(b) Ad hoc analyst queries**: unknown SQL, during working hours, in
  the Databricks SQL editor, dashboards and Genie.

### Prices

Databricks rates are read from the test workspace's own
`system.billing.list_prices` (2026-11-09, no end date), because the
vendor pages (<https://www.databricks.com/product/pricing/databricks-sql>,
<https://www.databricks.com/product/pricing/jobs>) render their numbers
with JavaScript and show none to a fetch. Premium tier, AWS us-east
(Ohio and N. Virginia are equal):

| SKU | USD per DBU | EC2 on top |
|---|---|---|
| SQL Classic | 0.22 | yes |
| SQL Pro | 0.55 | yes |
| SQL Serverless | 0.70 | no, included |
| Jobs Compute (classic) | 0.15 | yes |
| Jobs Serverless | 0.35 | no, included |
| All-Purpose Compute (classic) | 0.55 | yes |
| All-Purpose Serverless | 0.75 | no, included |

EU regions cost more: SQL Serverless is 0.91 in Ireland and Frankfurt,
SQL Pro 0.72. The work region is not stated here; scale the Databricks
rows by about 1.3 for the EU. Enterprise tier is the same as Premium
for SQL and 0.20 for Jobs Compute.

Warehouse behaviour, from
<https://docs.databricks.com/aws/en/compute/sql-warehouse/warehouse-types>
and <https://docs.databricks.com/aws/en/compute/sql-warehouse/create>:
serverless starts in about 2 to 6 s and can auto-stop after 1 minute
when set through the API (5 minutes is the UI minimum); Pro and Classic
start in about 4 minutes and auto-stop after 10 minutes at the soonest.
A 2X-Small warehouse is 4 DBU an hour (Databricks pricing calculator;
the docs give the cluster shape, a driver and 1 worker, not the DBU
rate), X-Small 6, Small 12.

AWS rates, us-east-2, from the AWS Price List API (2026-11-09; the public
pages are <https://aws.amazon.com/athena/pricing/>,
<https://aws.amazon.com/fargate/pricing/>,
<https://aws.amazon.com/lambda/pricing/>,
<https://aws.amazon.com/ec2/pricing/on-demand/>):

| Item | Price |
|---|---|
| Athena, data scanned | 5.00 USD per TB (10 MB minimum per query) |
| Athena provisioned capacity | 0.30 USD per DPU-hour |
| Fargate arm64 | 0.03238 USD per vCPU-hour, 0.00356 USD per GB-hour |
| Lambda arm64 | 0.0000133334 USD per GB-second, 0.20 USD per million requests |
| EC2 i3.xlarge / i3.2xlarge / m6gd.large | 0.312 / 0.624 / 0.0904 USD per hour |
| S3 reads in the same region | no transfer charge; GET 0.0004 USD per 1,000 |

### The volume model

The work team's real query list is not known. The numbers below use
this model; replace them with `system.query.history` from the work
workspace before acting on the totals. The order of the options holds
over a wide range.

- (a) **Scheduled**: 20 jobs a day, each reads one day (67 GB) and 40 %
  of its columns, so 27 GB a query, 540 GB a day, **16 TB a month**.
- (b) **Ad hoc**: 5 analysts, 20 working days, 3,000 queries a month,
  about 5 GB each after partition pruning, so **15 TB a month**. The
  warehouse is awake about 6 hours of each working day.

The throughput guesses (a 2X-Small warehouse or a 4 vCPU DuckDB reads a
day's 27 GB in about 1 to 1.5 minutes; Spark on the same Fargate size
in about 3) were not measured at this size; the test table is 125 KB.
Measure them in next slice 2.

## Options

### 1. Cheaper Databricks SQL

| Option | (a) scheduled, USD/month | (b) ad hoc, USD/month |
|---|---|---|
| Serverless 2X-Small, auto-stop 1 min, jobs run in one window | 28 (20 min a day) | 336 (120 h) |
| Serverless 2X-Small, auto-stop 1 min, jobs spread over the day | 56 (40 min a day) | as above |
| Serverless 2X-Small, auto-stop 10 min (the UI default), spread | 310 (3.7 h a day) | as above |
| Serverless Small (12 DBU) | about the same DBU-seconds for scans, more for idle | 1,008 |
| Pro 2X-Small (2.20 DBU + 1.25 EC2 = 3.45 an hour), one window | 59 (34 min incl. start and stop) | 552 (stays up 8 h, because of 4 min starts) |
| Classic 2X-Small (0.88 + 1.25 = 2.13 an hour), one window | 47 (44 min, slower, no IWM) | 341, with 4 min cold starts |
| Jobs Compute cluster, driver + 2 i3.xlarge, one nightly run | about 24 (35 min incl. start) | not interactive |
| All-Purpose cluster | never cheaper than Jobs Compute for (a) or serverless SQL for (b) | |

The i3.xlarge DBU rate (about 1 DBU an hour) is from the pricing
calculator and is not confirmed; the Jobs row moves with it.

**The biggest lever is auto-stop, not the warehouse type.** A spread
schedule on the default 10-minute auto-stop costs 5.5 times the same
schedule at 1 minute. Serverless at 1 minute beats Pro and Classic for
anything that wakes more than once a day, because they bill 4 minutes
of start and 10 of idle on each wake. The smallest size that finishes a
query is the cheapest: a scan costs about the same DBU-seconds on a
bigger size, and the idle minutes cost more.

What changes for people: nothing, beyond a warehouse that may need a
few seconds to start. What clavesa builds: nothing. It is warehouse
configuration the work team sets (size 2X-Small, max clusters 1,
`auto_stop_mins = 1` through the API or Terraform).

### 2. Athena reading the same tables

Two routes, tested both.

**2a. Athena through the Glue registration clavesa already writes.**
Clavesa registers every output in Glue as well as UC (ADR-027 section
1). Tested: Athena read `clavesa_ucpub__events.daily_events` natively
as Delta, scanned 692 bytes, and returned the same five days and totals
as DuckDB (both read only the current snapshot, not the 37 files left
by older commits). Nothing to build. But UC is out of the picture: no
UC grants, no UC audit, no lineage, and only clavesa-written external
tables, because Databricks-managed tables are not in Glue and their S3
prefix is UC's own.

**2b. Athena through Glue catalog federation to UC.** AWS Glue
federates to UC's Iceberg REST endpoint as a service principal and
Lake Formation (LF) hands the query engine credentials that UC vends
(<https://docs.aws.amazon.com/lake-formation/latest/dg/catalog-federation-databricks.html>,
<https://docs.databricks.com/aws/en/external-access/iceberg>,
<https://docs.databricks.com/aws/en/external-access/credential-vending>).
UC stays the catalog: its grants decide what the service principal sees.

Tested, in order:

1. UC's Iceberg REST endpoint listed the plain Delta tables but refused
   to load them: "Table 'team_ucpub.sales.daily_events_clavesa' is not
   an Iceberg compatible table". **UniForm is required**, on managed and
   external tables alike.
2. A UniForm copy (`delta.enableIcebergCompatV2`,
   `delta.universalFormat.enabledFormats = iceberg`, column mapping
   `name`), made by CTAS on the warehouse, loaded through Iceberg REST
   with S3 credentials in the response.
3. A Glue connection of type `DATABRICKSICEBERGRESTCATALOG` (OAuth
   client credentials of a new service principal, `SOURCE_VENDED`), LF
   registration and a federated catalog: Glue listed only the UniForm
   table, as type `iceberg`. Athena read it: 230 rows, 4,942.32, the
   same as the source, 469 bytes scanned.
4. Revoking the service principal's UC `SELECT` made the same Athena
   query fail at once with `TABLE_NOT_FOUND`. **UC grants hold through
   Athena.**

What the service principal needs in UC: `USE CATALOG`, `USE SCHEMA`,
`EXTERNAL USE SCHEMA` (not implied by ownership: the owner was refused
vending until it was granted explicitly), `SELECT` on each table, the
`workspace-access` entitlement (without it Glue got 403 "This API is
disabled for users without the databricks-sql-access or
workspace-access or workspace-consume entitlements"), and the metastore
setting "external data access" on. On AWS: a Secrets Manager secret with
the client secret, an IAM role that reads it, and an LF admin to
register the connection and grant `DESCRIBE` and `SELECT` per IAM
principal. Gotcha: Glue keeps a cached OAuth token in the secret; an
entitlement added later is not seen until the connection is recreated,
and hand-editing the secret breaks it.

Cost: (a) 16 TB × 5 = **80 USD/month**; (b) 15 TB × 5 = **75 USD/month**.
Athena bills scans, not time, so it beats a warehouse whenever the
warehouse scans less than 0.56 TB per awake hour (2.80 / 5.00); idle ad
hoc warehouses always do, busy scheduled windows usually do not.

What is lost, and how it is kept:

- **Per-person UC grants.** UC sees one service principal; Athena users
  are IAM principals governed by LF. Keep it by mapping UC groups to LF
  grants (one LF grant per role, made by Terraform), or by one
  federated catalog per group, each with its own service principal.
  Either way grants live in two places.
- **Lineage.** Databricks records lineage only for queries that run on
  Databricks; reads by an outside engine need the external lineage API
  (<https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-lineage>).
  The test could not confirm this either way: 1.5 hours after the test,
  `system.access.table_lineage` had not yet received even the lineage of
  the CTAS that ran on the warehouse. Audit does record vending
  (`generateTemporaryTableCredential`, with the principal and the table;
  seen for clavesa's own service principal the same morning), so
  who-read-what is known per federation service principal, not per
  person.
  Keep table-level lineage for scheduled SQL by running it in clavesa,
  which pushes edges (ADR-027 section 5).
- **Databricks dashboards, Genie, the SQL editor's history.** These run
  on a warehouse only. Keep the warehouse for them (option 1); Athena
  serves other tools (BI over JDBC, clavesa's own UI, notebooks).
- **Every table needs UniForm.** That means column mapping, no deletion
  vectors, and Iceberg metadata written on each commit. For
  Databricks-managed tables it is one `ALTER TABLE`. For clavesa-written
  external tables it is unproven whether UC serves Iceberg metadata that
  open-source Delta wrote; this is next slice 3.
- **SQL dialect.** Athena is Trino. Clavesa already transpiles serving
  SQL from Spark to Trino (ADR-023); analysts write Trino by hand.

### 3. Engines clavesa runs itself

**DuckDB with the delta extension.** Tested: DuckDB 2.0 read the
clavesa table with `delta_scan` and the same totals as Athena, and read
both an external table and a Databricks-managed table (with the
deletion-vectors feature on) using only the S3 credentials UC vended
for that table; with no credentials it failed. So UC grants decide what
DuckDB can read, through the same vending clavesa's runner already
uses (`runner/unity_catalog.py`). DuckDB's `unity_catalog` extension
does not use vending (<https://duckdb.org/docs/2.0/core_extensions/unity_catalog.html>),
so clavesa would fetch credentials itself, as the test did.

- On Fargate (4 vCPU, 16 GB arm64, 0.186 USD an hour): (a) 20 × 1.5 min
  = **about 3 to 5 USD/month**.
- On Lambda (10 GB, arm64): 0.012 USD a query, (a) **about 7 USD/month**;
  the 15-minute and 10 GB limits fit a day's partition, not a month.

**Spark on Fargate** is clavesa's runner today: a scheduled downstream
SQL job is a clavesa transform (SparkSQL), and its output already lands
in UC (ADR-027). At about 4 minutes a query including start: (a) **about
7.5 USD/month**. Nothing new to build; managed-table inputs are read
through vending already.

For (b), neither fits: they are per-run processes with no shared
endpoint, and DuckDB on a laptop pays S3 internet egress (about 0.09 USD
per GB, so 15 TB is about 1,350 USD/month).

## Summary

| Option | (a) scheduled | (b) ad hoc | UC grants | UC lineage | Dashboards | Build in clavesa |
|---|---|---|---|---|---|---|
| Serverless 2X-Small, auto-stop 1 min | 28–56 | 336 | yes | yes | yes | none |
| Serverless, today's 10 min default | 310 | 336+ | yes | yes | yes | none |
| Pro / Classic 2X-Small | 47–59 | 341–552 | yes | yes | yes | none |
| Jobs Compute cluster | ~24 | n/a | yes | yes | n/a | none |
| Athena via clavesa's Glue table (2a) | 80 | 75 | **no** | no | no | none |
| Athena via Glue federation to UC (2b) | 80 | 75 | yes, plus LF | no (audit only) | no | UniForm + Terraform |
| DuckDB on Fargate / Lambda | 3–7 | n/a | yes, via vending | edges pushed by clavesa | n/a | an engine |
| Spark on Fargate (clavesa transform) | ~7.5 | n/a | yes, via vending | edges pushed by clavesa | n/a | none |

USD per month at the model volume, us-east prices.

## Decision

1. **Scheduled SQL moves into clavesa as transforms on Fargate.** It is
   the cheapest option that keeps UC as the catalog (about 7.5 USD a
   month against 28 to 310 on a warehouse) and needs no new code: the
   transform reads managed inputs through vending, writes its output
   table in place (ADR-027), and pushes the lineage edge. It also fits
   the north star: the SQL runs on clavesa compute and shows up in cost
   per billion records.
2. **Ad hoc SQL stays on one serverless SQL warehouse, 2X-Small, max 1
   cluster, auto-stop 1 minute.** It keeps per-person grants, lineage,
   dashboards and Genie, and the auto-stop alone removes most of the
   idle cost. This is a recommendation to the work team, not clavesa
   code.
3. **Athena through Glue federation (2b) is the cheap read path for
   tools outside Databricks, built later and only on evidence**: when
   the right-sized ad hoc warehouse still costs more than about 300 USD
   a month in `system.billing.usage`, or a consumer outside Databricks
   needs the tables. Route 2a (clavesa's own Glue registration) stays
   for clavesa's UI only and is not offered to analysts, because it
   bypasses UC grants.
4. **DuckDB is not built now.** Against Spark on Fargate it saves about
   3 USD a month at this volume. Revisit if per-query start time, not
   cost, becomes the problem.

## Next slices (not built)

1. **Measure the work queries.** A read-only `system.query.history` and
   `system.billing.usage` summary from the work workspace, run by Vesa
   at work if he chooses; no session from the dev machine touches the
   work workspace (Head Jeeves, 2026-11-09). It replaces the volume model: which
   scheduled queries exist, their bytes read, and the warehouse's awake
   hours. No code.
2. **Port one scheduled downstream query as a clavesa transform**, in
   shadow mode per the ADR-027 recipe, at work size (67 GB a day), and
   record cost per run and runtime. This measures the Fargate
   throughput guessed above.
3. **UniForm on clavesa outputs.** An output option that writes
   `delta.universalFormat.enabledFormats = iceberg` with column mapping
   (delta-iceberg in the runner image), and a test that UC's Iceberg
   REST serves an external UniForm table that clavesa, not Databricks,
   wrote. Only after decision 3's trigger.
4. **`clavesa workspace connect databricks --athena`**: a Terraform
   module for the federation (service principal secret, IAM role, Glue
   connection, LF registration, federated catalog, LF grants per role),
   plus a preflight for `workspace-access` and `EXTERNAL USE SCHEMA`.
   After slice 3.

## Consequences

- The scheduled SQL that moves leaves the team's Databricks job list and
  shows up in clavesa instead; people who looked for it in Databricks
  Jobs look in clavesa. Its output, grants and table names do not
  change.
- Each moved query is a clavesa pipeline to keep running, with
  clavesa's failure alerts in place of the Databricks job's.
- Ad hoc costs only fall as far as auto-stop allows; a busy team still
  pays for its warehouse hours. Athena would cut that by up to about
  four fifths, and this ADR defers that saving on purpose, to keep one
  set of grants and the dashboards.
- If 2b is built later, grants live in UC and LF, and Athena reads
  appear in UC audit only as the federation's service principal.
- The cost numbers rest on a volume model and on unmeasured throughput;
  slices 1 and 2 replace them before anything is presented as a saving.

## The test

Run 2026-11-09 in AWS account 699166197771, us-east-2, and the `ucpub`
Databricks test workspace. No work workspace or work account was used.

- Athena workgroup `adr029-test` with a 10 MB per-query scan cutoff:
  three queries: 692 and 469 bytes scanned and one query refused after
  the revoke, each billed at the 10 MB minimum, about 0.0002 USD.
- DuckDB on the Mac, reading S3 with the profile's credentials and with
  UC-vended credentials.
- Databricks: one CTAS of a 230-row UniForm table and four system-table
  queries on the 2X-Small serverless warehouse (auto-stop 1 minute).
- Glue federation: a test service principal and OAuth secret, a Secrets
  Manager secret, an IAM role, a Glue connection, an LF registration, a
  federated catalog, LF grants.

Cost: the warehouse woke four times, about 3 minutes each with the
1-minute auto-stop, about 0.8 DBU at 0.70 USD, so **about 0.55 USD**
(estimate: `system.billing.usage` had not yet received these hours).
AWS costs are under 0.01 USD (Athena about 0.0003 USD, Secrets Manager
pro rata for one hour). Total **under 0.60 USD**.

Deleted afterwards, and checked as gone: the federated catalog, the LF
registration and grants, the Glue connection, the IAM role, the secret
(without recovery window), the workgroup and its result prefix
`s3://ucpub-clavesa/_adr029_test/`, the UniForm table, the test service
principal (at account level; deleting it in the workspace only is not
enough), its UC grants, and the `EXTERNAL USE SCHEMA` grant made to the
owner for the test. Lake Formation settings were compared before and
after and are unchanged. Left: the dropped managed table's files (about
100 KB) under UC's managed storage, which UC purges itself after its
7-day undrop window.
