# Clavesa as a compute engine for Unity Catalog

Study, 2026-11-01. No code changes. No Databricks workspace was created.

## The answer

Yes, this is possible, with three limits. Clavesa can do the compute on its own
AWS engine and write Delta tables that Unity Catalog (UC) registers and
governs. Databricks documents this path: an external Apache Spark client uses
the Unity REST API, and UC vends short-lived storage credentials after it
checks the grants.

The three limits:

1. **Lineage is not automatic.** UC captures lineage only for work that runs on
   Databricks compute. Clavesa must push its lineage through the external
   lineage API (Public Preview).
2. **Tables with row filters, column masks or ABAC policies are read-only** for
   an external writer. Those tables must stay on Databricks.
3. **The DBU saving is real but smaller than it looks for large jobs.** The
   large win is on many small jobs and on jobs that now run on serverless or
   all-purpose compute.

Recommendation: clavesa writes **UC external Delta tables** through the UC
Spark connector, and pushes one lineage record per run. The first slice is in
the last section.

## What clavesa is today

- **Engine:** PySpark 4.0.2 with Delta Lake 4.0.0, one runner image for every
  target (ADR-012, ADR-018).
- **Compute:** local Docker, AWS Lambda, Fargate, EMR Serverless. The user
  picks the target per transform. The north star is cost per billion records.
- **Storage and catalog:** Delta tables on S3, registered in AWS Glue Data
  Catalog. Glue is two-level, so the three-level name `<catalog>.<schema>`
  is encoded as one Glue database `<catalog>__<schema>` (ADR-016, ADR-020).
- **Orchestration and deploy:** pipelines are Terraform. Step Functions runs a
  pipeline. `clavesa deploy` applies the Terraform. Schedules are opt-in.
- **Own observability:** runs, node runs and lineage are Delta tables in the
  same catalog. Serving reads in cloud go to Athena (ADR-022, ADR-023).

## Question 1: can clavesa write UC tables from outside Databricks?

Yes. Each line is marked **[docs]** (verified in the Databricks docs on
2026-11-01) or **[assumed]** (my inference, not tested).

| Need | How | Source |
|---|---|---|
| A Spark client that reads, writes and creates UC Delta tables | Unity REST API with the `io.unitycatalog:unitycatalog-spark` connector 0.5.0+, Spark 4.0 or 4.1, Delta Spark 4.3.0+. The catalog is configured as `io.unitycatalog.spark.UCSingleCatalog` with the workspace URL as `uri`. | [docs] [Unity REST](https://docs.databricks.com/aws/en/external-access/unity-rest) |
| External Delta tables | Read, write and create: supported. | [docs] same page |
| Managed Delta tables | Read: supported. Write and create: only with catalog commits (`delta.feature.catalogManaged`), and creating and writing managed tables from Delta clients is Public Preview. | [docs] same page, [external access](https://docs.databricks.com/aws/en/external-access/) |
| Storage credentials | Credential vending: `POST /api/2.1/unity-catalog/temporary-table-credentials` with `READ` or `READ_WRITE`, or path operations. UC checks permissions before it issues a short-lived token. The connector does this for you. | [docs] [credential vending](https://docs.databricks.com/aws/en/external-access/credential-vending) |
| Authentication | Personal access token, or OAuth M2M with a service principal. OAuth renews tokens, so use it for jobs longer than one hour. | [docs] Unity REST |
| Prerequisites | Metastore setting "External data access" on. Grant `EXTERNAL USE SCHEMA` on the schema, `EXTERNAL USE LOCATION` for path access, and `SELECT`, `MODIFY`, `CREATE` as needed. | [docs] Unity REST |
| Iceberg path | UC also has an Iceberg REST catalog with read, write and create. Clavesa is Delta-only (ADR-018), so this path is out of scope. | [docs] external access |
| No-change path | UC can federate an AWS Glue catalog (catalog federation). Writes then stay outside UC. | [docs] page listed; [assumed] that the foreign tables are read-only in UC |
| Delta 4.3.0 ships a build for Spark 4.0 | The docs example uses `delta-spark_4.1_2.13:4.3.0`. Clavesa may need Spark 4.1. | [assumed] |

Limits on external writers **[docs]**:

- No change to `delta.*` or `databricks.*` table properties, no add or remove
  of table features, no column rename, no `CHECK` constraints.
- No `OPTIMIZE`, `VACUUM` or `ANALYZE` on managed tables.
- Databricks warns against more than one writer client on the same table.

## Question 2: can clavesa push lineage into UC?

Yes, with a set pattern. **[docs]** [external lineage](https://docs.databricks.com/aws/en/data-governance/unity-catalog/external-lineage)

- `POST /api/2.0/lineage-tracking/external-metadata` creates an object for an
  outside entity. Clavesa makes one per transform (for example, the system
  `clavesa` and the entity `<pipeline>.<node>`).
- `POST /api/2.0/lineage-tracking/external-lineage` links the object to UC
  tables. Source and target can be a `table`, `path`, `model_version` or
  `external_metadata`. A `columns` list maps source columns to target columns.
- For two UC tables, the documented pattern is to put an external metadata
  object between them: `input table -> clavesa node -> output table`.
- The result shows in the Lineage tab of Catalog Explorer.
- Privileges: `CREATE EXTERNAL METADATA` on the metastore, `MODIFY` on the
  object, and `SELECT` or `MODIFY` on the linked tables.
- Limits: 10,000 external metadata objects and 100,000 relationships per
  metastore.
- The API reference marks the endpoint as **Public Preview**.

Gaps:

- External lineage **is not written to the lineage system tables**
  (`system.access.table_lineage`). Lineage queries in SQL and audit reports
  built on those tables do not see clavesa. **[docs]**
- No native OpenLineage endpoint was found in the Databricks docs. Clavesa must
  call the REST API itself. **[assumed]** after a search on 2026-11-01.
- Column lineage needs clavesa to compute the column map. Clavesa can get it
  from the analyzed Spark plan of each transform. **[assumed]**
- A direct table-to-table edge with no object between them is not documented.
  **[assumed]** that it is not supported.
- UC lineage for reads and writes through the Unity REST API is not
  documented. The lineage page names only Databricks Spark and Databricks SQL.
  **[assumed]** that clavesa writes produce no automatic lineage.

## Question 3: what governance holds when the writer is outside

**Holds:**

- **Grants.** UC checks `EXTERNAL USE SCHEMA`, `SELECT`, `MODIFY` and `CREATE`
  for the clavesa service principal before it vends a credential. Revoke a
  grant and the next run fails. **[docs]**
- **Discovery.** The table is a normal UC securable: it shows in Catalog
  Explorer with schema, history and owner. **[assumed]** for comments and tags
  set from Databricks.
- **All reads on Databricks.** Users, jobs and SQL warehouses that read
  clavesa tables get full UC governance, and UC captures lineage for those
  reads automatically. **[docs]** lineage page
- **Scope of the token.** A path credential covers only that path. **[docs]**
- **Audit.** The credential request is a UC API call. **[assumed]** that it
  lands in the audit log.

**Does not hold, or holds only partly:**

- **What the job did with the token.** UC sees the credential request, not the
  rows the job wrote. The token is S3 access for its lifetime. **[docs]** for
  "short-lived"; the lifetime is not stated.
- **Fine-grained access control.** No writes to tables with row filters,
  column masks or ABAC policies, unless the writer is exempt from the policy.
  Those tables are also not vended for reads; reads go through a billed
  serverless filter layer. **[docs]**
  [cross-engine ABAC](https://docs.databricks.com/aws/en/external-access/cross-engine-abac)
- **Automatic lineage.** See question 2.
- **Data that clavesa reads from outside UC** (HTTP sources, raw S3 drops).
  UC does not govern it. Clavesa can register it as external metadata.
- **Managed-table maintenance.** On managed tables, Databricks does the
  `OPTIMIZE` and `VACUUM`, on Databricks compute. **[assumed]** that this is
  predictive optimization, billed as serverless.

## Clavesa design choices that break this

| Choice | Effect | Change |
|---|---|---|
| Glue as the metastore (`spark_catalog` through the Glue Hive client) | UC does not see the table at all, or sees it only as a federated foreign table. | A UC catalog provider: `UCSingleCatalog` beside or in place of Glue. |
| `<catalog>__<schema>` flat encoding (ADR-016) | Wrong in UC. UC is natively three-level. | Use the three-level name as is. ADR-019 was the native plan; UC is the backend that makes it work. |
| Writes by S3 path with `S3SingleDriverLogStore` | Fine for external tables. A managed table needs catalog commits through the UC client. | External tables only in the first version. |
| Clavesa's own `OPTIMIZE`, `VACUUM`, `ALTER TABLE ... CLUSTER BY` (runner maintenance ops) | Not allowed on managed tables. | Keep them for external tables; switch them off for managed tables. |
| Change Data Feed set as a `delta.*` default property | External clients cannot change `delta.*` properties after create. | Set it in `CREATE TABLE`. **[assumed]** that this is allowed. |
| Delta 4.0.0 | Below the 4.3.0 minimum. | Upgrade Delta, and maybe Spark to 4.1. |
| Lineage held only in clavesa's own tables | Databricks UI shows no edge into clavesa tables. | Push external lineage after each successful node. Keep clavesa's tables too. |
| Any second writer on a clavesa table | Databricks warns of corruption. | ADR-016 schema ownership already gives one writer per schema. Make that a rule: a clavesa schema in UC takes no Databricks writes. |

## Question 4: which workloads suit clavesa compute

**Move to clavesa:**

- Scheduled batch SQL and PySpark transforms: bronze, silver and gold rebuilds,
  daily rollups, MERGE upserts.
- Ingest of files and HTTP sources into bronze.
- Many small jobs. A job that finishes in one minute on Lambda costs cents. On
  classic jobs compute it pays for cluster start, a driver and at least one
  worker.
- Jobs that now run on all-purpose or serverless compute only because that is
  easy.

**Keep on Databricks:**

- Tables with row filters, column masks or ABAC policies (no external writes).
- Lakeflow Declarative Pipelines (DLT) streaming tables, materialized views and
  expectations. These are read-only from outside.
- Tables with more than one writer.
- Work where compliance needs lineage in the system tables.
- Very large shuffles where Photon on Databricks finishes much faster. Measure
  before you move them.
- ML, feature serving, notebooks and other interactive work.

## Question 5: rough cost comparison

All prices are AWS list prices for us-east-1 in USD, without discounts.
**[assumed]** for every number here: the Databricks pricing page did not render,
and the DBU rates come from third-party guides
([mammoth.io](https://mammoth.io/blog/databricks-pricing/),
[doit.com](https://www.doit.com/de/blog/databricks-pricing-explained-dbus-tiers-and-cost-control)).
The AWS rates come from my knowledge. Check both before a budget.

Rates:

- Databricks classic Jobs Compute: about $0.15 to $0.20 per DBU, plus the EC2
  bill. Serverless jobs: about $0.35 to $0.45 per DBU, compute included.
  All-purpose: about $0.55 per DBU, plus EC2.
- One m5.xlarge (4 vCPU, 16 GB) is about 0.7 DBU per hour and $0.19 per hour
  of EC2.
- Lambda: $0.0000166667 per GB-second, so 10 GB for one hour is $0.60.
- Fargate: $0.04048 per vCPU-hour and $0.004445 per GB-hour, so 4 vCPU and
  16 GB is $0.23 per hour.
- EMR Serverless: $0.052624 per vCPU-hour and $0.0057785 per GB-hour, so
  4 vCPU and 16 GB is $0.30 per hour.

One node-hour of 4 vCPU and 16 GB:

| Where it runs | Cost per hour |
|---|---|
| Databricks classic jobs: EC2 $0.19 + 0.7 DBU x $0.15 | about $0.30 |
| Databricks serverless jobs, same work (about 1 to 2 DBU) | about $0.45 to $0.90 |
| Databricks all-purpose: EC2 $0.19 + 0.7 DBU x $0.55 | about $0.57 |
| Clavesa on Fargate | about $0.23 |
| Clavesa on EMR Serverless | about $0.30 |

What this means:

- **A large job on classic jobs compute saves little**, about 0 to 25 percent.
  The DBU is about a third of the bill there.
- **Serverless or all-purpose jobs save about 50 to 75 percent.**
- **Small jobs save the most.** Example: 50 jobs a day, 2 minutes of real work
  each. On classic jobs compute, each job holds a two-node cluster for about
  8 minutes with start-up: 50 x 8/60 x 2 x $0.30, about $4 a day. On clavesa
  Lambda with 4 GB: 50 x 120 s x 4 GB x $0.0000167, about $0.40 a day.
- **New costs on the Databricks side:** a governed read of a table with row
  filters or masks uses billed serverless compute. Managed tables get
  Databricks maintenance on Databricks compute. The external lineage and
  credential APIs have no listed price. **[assumed]**

The next step for a real number: take one month of `system.billing.usage` for
the candidate jobs, and compare it with clavesa's own cost per billion records
for the same pipelines.

## Options

### A. Federate the Glue catalog into UC (no clavesa change)

- **Effort:** none in clavesa. Configure catalog federation in Databricks.
- **Lost:** writes stay outside UC. No UC grants on the write. No lineage into
  clavesa tables. Tables are foreign in UC.
- **Kept:** all of clavesa as it is.
- Good as a first look. It does not meet the goal.

### B. Clavesa writes UC external Delta tables (recommended)

- **Effort:** medium. A UC catalog provider in the runner (`UCSingleCatalog`,
  OAuth M2M, credentials in the source registry from ADR-017), a Delta upgrade,
  native three-level names on this backend, and a lineage push after each
  node. Terraform for the UC side can use the Databricks provider: schema,
  external location, grants. **[assumed]** for the provider resource names.
- **Lost:** Databricks predictive optimization on these tables. Writes to
  tables with row filters or masks. Lineage in the system tables.
- **Kept, and Databricks does not give it:** the cost ladder per transform
  (Lambda to EMR Serverless), local and cloud parity on a laptop, pipelines as
  Terraform in the repo, clavesa's own maintenance ops, and the cost per
  billion records metric.

### C. Clavesa writes UC managed tables with catalog commits

- **Effort:** medium to high. Same as B, plus catalog commits, and the runner
  must skip every property change and maintenance op on these tables.
- **Lost:** clavesa's `OPTIMIZE` and `VACUUM`. Some schema operations. Public
  Preview status today.
- **Gained over B:** UC owns the storage and coordinates commits, so a
  concurrent writer cannot corrupt the table. Databricks does the
  maintenance, at a DBU cost.
- Revisit when external writes to managed tables are GA.

## Recommendation and first slice

Build option B.

**First slice: one transform writes one UC external table, with lineage.**

1. The workspace takes a `catalog_backend = "unity"` setting with a workspace
   URL, a service principal, and a UC external location.
2. The runner upgrades to Delta 4.3+ and the UC Spark connector, and
   configures `UCSingleCatalog` with OAuth M2M when the backend is `unity`.
3. One transform writes `<catalog>.<schema>.<table>` as an external Delta
   table. Native three-level name, no `__` encoding.
4. After the write, the runner creates one external metadata object for the
   node and two lineage relationships: input to node, node to output.
5. Test: the table shows in Catalog Explorer, the Lineage tab shows the clavesa
   node, and a revoke of `MODIFY` makes the next run fail.

The test needs a Databricks workspace with external data access and a service
principal. This study did not check whether the Free Edition allows that.
Vesa must supply a workspace before the slice can be verified.
