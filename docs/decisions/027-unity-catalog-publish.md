---
adr: 27
title: Clavesa replaces steps in existing Databricks pipelines, writing their existing Unity Catalog tables
date: 2026-11-08
status: Accepted
deciders: Vesa
---

# ADR 027: Clavesa replaces steps in existing Databricks pipelines

**Status**: Accepted (2026-11-09) for decisions 1 to 5, 7 and 8, which
the end-to-end tests on the test workspace proved (section "Settled
2026-11-09"). Decision 6 and the points listed under "Still open" are not
proven and stay open. Proposed 2026-11-08 and reworked after Vesa's
review of the first draft: "i'm targeting existing pipelines and
replacing bits for now". The sections below keep the reasoning; where a
section still says "open decision", the settled answer is in "Settled
2026-11-09".

## Context

The target is a team that already runs its pipelines on Databricks: an
existing catalog, existing schemas, existing tables, existing jobs. Some
steps of those pipelines are cheap to run outside Databricks. Clavesa
replaces those steps: it runs them on its own compute (Lambda, Fargate,
EMR Serverless), which costs no DBUs, and the rest of the pipeline keeps
running on Databricks unchanged. The north star (cost per billion
records) applied to a Databricks shop: DBUs are paid only for the steps
that stay.

A replaced step has the same contract as before:

- it **reads** the tables (or raw files) the step read before;
- it **writes** the table the step wrote before, under the same name, so
  every downstream reader, job, dashboard and grant keeps working;
- the steps after it start when it has finished.

P0 (2026-11-08, `docs/ideas/unity-catalog-publish.md`, "P0 results")
tested the parts this needs by hand: an outside engine can register an
external Delta table, a Databricks job reads it, a table's Delta schema
comes from its log, one lineage edge can be pushed, and all three ways to
start a downstream job work. P0 also showed what does not work: changing
a registered table's columns through the API, and lineage edges between
two tables.

Out of scope, from the brief: migrating whole pipelines to clavesa, a DBU
savings report. The brief also put "clavesa reads tables Databricks
writes" out of scope; replacing a middle step needs exactly that, so it
is open decision 2.

## Decision

### 1. The existing catalog by default; creating one only on request

**Decided by Vesa (2026-11-08).** By default the team's catalog and
schemas are used as they are, and clavesa creates neither.

Creating a catalog is an explicit opt-in: `clavesa workspace connect
databricks --create-catalog` (and the same checkbox in the UI, ADR-015).
It is for a Databricks workspace that has no real catalog yet; the test
setup uses it now. With the flag, the workspace stack creates the catalog,
and because a new catalog has no schemas, also the schemas the clavesa
outputs name in it. P0 showed what a catalog clavesa creates needs: a
managed location in customer S3 (Default Storage cannot take `EXTERNAL
USE SCHEMA`), so the opt-in also brings back, only under this flag, the
storage credential, a writable external location over
`s3://<workspace bucket>/_unity_catalog/` (IAM write on that prefix
only), and the `CREATE CATALOG` check. Without the flag none of this is
generated or checked.

A clavesa output
names the Unity Catalog table it replaces, by its full name:

```hcl
module "daily_orders" {
  # ...
  unity_catalog_table = "analytics.sales.daily_orders"
}
```

The connection in `clavesa.json` keeps `host` and `credential`; its
`catalog` becomes the default catalog for short names, not a catalog
clavesa owns. ADR-016's schema ownership stays the rule inside clavesa's
own namespace (Glue); in Unity Catalog clavesa owns single tables, not
schemas. Two clavesa outputs may not name the same Unity Catalog table;
the orchestration emitter refuses it, as it refuses two pipelines writing
one schema today.

The output is still registered in Glue as well, at the same location, so
the clavesa UI keeps working through Athena (ADR-022, ADR-023).

### 2. What happens to the table the replaced step wrote

Clavesa looks the table up when the output is attached and at every
deploy. There are two kinds, and they are handled differently:

**An external table** (a Delta folder in the team's S3, registered by
name): **clavesa takes it over in place.** It writes new Delta commits to
the same folder. The registration, the table ID, the grants, the
history (time travel) and the Databricks-recorded lineage all stay as
they are; readers see the new data at the next read. Clavesa makes no
Unity Catalog call per run. It needs:

- write access for its runner's IAM role to that S3 prefix (open
  decision 3);
- a Delta table clavesa's Delta version can write. A table that uses a
  Databricks feature clavesa cannot write (for example deletion vectors
  or liquid clustering, depending on clavesa's Delta version) is refused
  at attach time with the feature named (open decision 6).

**A managed table** (Databricks owns the storage; the default for tables
Databricks jobs create): **clavesa cannot write it.** Outside writes to
managed tables need the Beta write path or credential vending, both out
of scope. Two ways remain, and Vesa picks one (open decision 1):

- **One-time switch, same name.** An admin action, once per table:
  clavesa copies the current data into a Delta folder in the team's
  storage, drops the managed table, registers an external table under
  the same name at the new folder, and puts the grants it read before the
  drop back on it. Readers keep the name. Lost: the managed table's
  history before the switch, and its table ID. The copy is a one-time
  read of the managed table, which itself needs the read access of open
  decision 2.
- **New name.** Clavesa writes a new external table beside the old one,
  and the team repoints the downstream jobs and drops the old table when
  it is ready. Nothing is lost; the team edits every reader.

Either way, while both the Databricks step and the clavesa step are
scheduled, two writers corrupt the table (S3 Delta tolerates one writer).
The switch is: pause the Databricks step, run the clavesa step (open
decision 7).

### 3. Schema changes barely matter here

A replaced step writes the same columns the Databricks step wrote; that
is what "replace" means. The column list in Unity Catalog stays right.

It goes stale only when the team later changes the clavesa step so its
output gains or loses a column. Then readers are still correct (P0: they
take the schema from the Delta log at once) and only the column list
shown in Catalog Explorer lags. Clavesa cannot fix the list through the
API (P0: refused), and the two ways that can (drop and register again,
which loses grants and the table ID; or a refresh on a SQL warehouse,
which costs DBUs) are worse than a lagging list for a change that
happens at edit time. **Clavesa builds nothing for it in this ADR**; it
warns at deploy when an output's columns change, with the one SQL
statement an admin runs to refresh the list (open decision 4).

The first draft's drop-and-register-with-grants logic is dropped.

### 4. Reading the step's inputs

A replaced step's inputs are either raw files (S3, HTTP: the existing
source registry, ADR-017) or tables. A table input:

- **external table**: clavesa reads its Delta folder directly, with read
  access for its IAM role to that prefix. No Databricks call.
- **managed table**: clavesa cannot read it without credential vending
  (Unity Catalog hands out short-lived S3 credentials for one table). The
  brief put this out of scope. Without it, clavesa can replace only steps
  whose inputs are raw files or external tables, which in a typical
  Databricks pipeline means the first steps (open decision 2).

### 5. Lineage: the clavesa step is a box in the Databricks graph

When Databricks ran the step, it recorded lineage input table → output
table itself. After the replacement it no longer runs the step, so that
edge stops being recorded and the lineage graph breaks at the step.

P0: every pushed edge needs an external metadata object on one side, so
table → table cannot be pushed. What can: **the clavesa step itself is
the external metadata object** ("clavesa transform `daily_orders` in
pipeline `orders`", `system_type: OTHER`), with one edge from each input
to it and one from it to the output table. That is not a stand-in; the
step really is an outside system. Raw-file inputs become external
metadata objects too, as P0 did. Pushed at deploy, because the edges
change only when the pipeline changes (open decision 5).

Downstream of the output table, Databricks records the lineage itself
(P0).

### 6. Starting the steps after it

Three ways, all shown working by P0 against a clavesa-written table:

- **A. Clavesa starts the next job** (`on_success { databricks_job = … }`).
  After the whole run succeeded, the runner's pipeline handler calls
  Databricks to start each named job once, and the run page links to the
  Databricks run. The handler makes the call (not a Step Functions HTTP
  task) because it already holds the secret from Secrets Manager, an
  EventBridge connection would put the secret in Terraform state, and a
  local-compute run then starts the job too (ADR-014, ADR-024).
- **C. Table update trigger** on the output table: about 7 minutes after
  the commit (P0). Needs no clavesa code.
- **B. File arrival trigger** on the table's `_delta_log/`: under a minute
  (P0). Needs no clavesa code, and the path.

B and C fire per table commit, not per run: for a step that writes
several tables they can start the next job before the run has finished,
or after it failed. In an existing pipeline the replaced step is often
one task inside a multi-task Databricks job; the tasks after it must then
become their own job that A, B or C starts (open decision 8).

### 7. The connection and the admin's part

Kept from slice 2: the `databricks-oauth` credential kind, `clavesa
workspace connect databricks`, its checks with exact fixes, the UI page.
The checks change to match this ADR:

| Check | Why |
|---|---|
| The service principal can sign in | every call |
| External data access is on for the metastore | registration, lineage |
| `CREATE EXTERNAL METADATA` on the metastore | lineage (section 5) |
| `USE CATALOG` / `USE SCHEMA` on each catalog and schema a clavesa output names, and ownership or `MODIFY` on each output table | look-up at attach, lineage on the table |
| `EXTERNAL USE SCHEMA` on those schemas | only for the one-time switch (registering) and for credential vending, if those are chosen |

With `--create-catalog` only: `CREATE CATALOG`, `CREATE STORAGE
CREDENTIAL` and `CREATE EXTERNAL LOCATION` on the metastore (section 1).
Without it these are not checked. The per-table checks run when an output is attached to
a Unity Catalog table, not only at connect, because that is when clavesa
knows the table.

### 8. Generated Terraform

Clavesa still generates all of its own Terraform (ADR-001), but it owns
far less in Databricks:

- **Not generated by default:** the storage credential, the external
  locations, the catalog, the schemas. They are the team's. With
  `--create-catalog` the workspace stack generates the catalog, its
  schemas, the storage credential and the writable `_unity_catalog/`
  location (section 1).
- **Generated:** the runner IAM role's S3 access to each taken-over
  table's prefix and each external input's prefix (if the team allows it,
  open decision 3); the Secrets Manager secret for the runner (kept from
  slice 3); the external metadata objects for lineage, if the Databricks
  provider supports them, else REST calls at deploy.
- The Databricks provider stays configured inside the clavesa module, and
  the secret still reaches Terraform only as `DATABRICKS_CLIENT_SECRET`
  (slice 3 decisions, unchanged).

## Provisional decisions

Vesa (2026-11-08): "just have the agent implement to-the-end so we can
verify which parts are good and which not". So each open question takes
the recommended answer, **provisionally**: built to be tried end to end,
expected to change after that.

1. **Managed output table: one-time switch, same name** (section 2).
   Provisional build: clavesa refuses to write a managed table and prints
   the switch steps; the automated switch waits for decision 2's reads.
2. **Reading managed tables: yes, read-only, through credential vending**
   (section 4). Provisional build: inputs that are external tables are
   read by path; a managed input fails with a clear message naming the
   gap. Vending is the next slice after this end-to-end pass.
3. **A taken-over table's data stays in the team's storage**; the team
   gives clavesa's runner role access to that prefix (sections 2, 8).
4. **A lagging Catalog Explorer column list after a later edit is
   acceptable**; the runner warns with the refresh statement
   (section 3).
5. **The clavesa step is its own box in the lineage graph** (section 5).
   Pushed by the runner after it registered or found the output table,
   because an edge needs the table to exist, which is after the first
   run, not at deploy.
6. **A table with a Delta feature clavesa cannot write is refused**, with
   the feature named; Delta reports it when clavesa opens the table
   (section 2).
7. **The team pauses the Databricks step**; clavesa does not yet check
   the schedule (section 2). Provisional: documented, not enforced.
8. **The team splits a multi-task job; clavesa starts the rest** (A,
   section 6), declared per pipeline as a list of Databricks job IDs.

## Consequences

**Positive:**

- The team keeps its catalog, schemas, table names, grants and downstream
  jobs. An external table is taken over with no Unity Catalog change at
  all.
- A replaced step stops costing DBUs; nothing else changes cost.
- Far less clavesa-owned Databricks infrastructure than the first draft;
  the catalog, its storage and schemas exist only behind
  `--create-catalog`.

**Negative (costs):**

- **Managed tables need a one-time switch or a rename.** Most Databricks
  tables are managed, so this is the common case, not the edge.
- **Without managed-table reads, only first steps qualify.** That keeps
  the brief's scope but limits which steps clavesa can replace.
- **Clavesa writes into the team's storage.** The team gives clavesa's
  IAM role write access to the prefixes of the tables it takes over; a
  mistake there reaches the team's data, not a clavesa bucket.
- **Delta feature limits.** Tables using Databricks-only Delta features
  cannot be taken over until the feature is turned off.
- **Two writers corrupt a table.** The switch-over must pause the
  Databricks step; a forgotten schedule is a real failure mode.
- **Public Preview dependency** for lineage and registration (the switch).
- **A second Terraform provider**, its credentials at plan time, and a
  Databricks that answers, for every connected workspace.

## Update 2026-11-08: first end-to-end version, tested live

Built on branch `unity-catalog-publish` and run against the test setup
(workspace `ucpub`, us-east-2; service principal `clavesa-ucpub`;
catalog `clavesa_ucpub` created with `--create-catalog`).

Shown working: the connect checks; the workspace stack (catalog with its
managed location, storage credential, external location, grants); a
clavesa output registered as a new external table; a second run writing
it in place; an existing external table taken over in place (same table
ID, new Delta commit in its own folder, stale column list with the
warning, readers seeing the new column); lineage table -> step -> table
and HTTP source -> step -> table, read back with its column mapping;
option A starting a Databricks job after a Lambda run and after a
`--compute local` run, the job reading the clavesa table successfully;
the run page and `/unity-catalog` links; `make smoke-databricks`.

Answers to the [verify] points: a service principal **can** read its
own effective permissions; an owner **may** grant itself `EXTERNAL USE
SCHEMA` and `EXTERNAL USE LOCATION`; one external location over the
bucket root with the catalog's managed location under it is accepted
(no overlap error). New finding: a service principal added to a
workspace without an entitlement signs in and then gets 403 on every
Unity Catalog call; the connect check names the fix.

Not shown: a SQL warehouse reading the tables. A new warehouse was
tried; the query was refused for lack of `USE CATALOG` (Unity Catalog,
not the S3 403 P0 saw), and the grant to Vesa's user that would let it
read was not made in this session. Not built: credential vending
(managed inputs), the automated managed-table switch, the schedule check
of provisional decision 7.

## Update 2026-11-08: second pass, the rollout recipe

Built and tested live (docs/ideas/unity-catalog-publish.md, "Ready for
work"); what changes in the provisional decisions:

- **Decision 1 (managed output table):** the one-time switch is a rename
  after shadow mode, not a copy: clavesa writes `<table>_clavesa` (an
  external table) next to the managed one, a daily comparison proves
  them equal, the history is copied in once, then the two names swap and
  the managed table stays as `<table>_before_clavesa` for roll-back.
  Recipe: docs/cookbook/replace-a-databricks-step.md. Clavesa still
  never writes a managed table.
- **Decision 2 (managed inputs):** built. Source kind `unity-catalog`;
  a managed table is read with credentials from
  `temporary-table-credentials`, set for its bucket.
- **Decision 3 (team storage):** built. `pipeline deploy` derives the
  runner role's grants on the team's folders and KMS keys from Unity
  Catalog.
- **Decision 7 (paused step):** enforced, per output
  (`replaces_databricks_job`), before every write.
- New: SQL warehouse readers (`--reader`), `replace_partitions` with
  `partition_by`, a read-only preflight, workspace-scoped AWS names, and
  disconnect that releases a `--create-catalog` catalog from state.
- Unity Catalog drops REST-set table properties once Databricks writes a
  table; clavesa keeps its ownership marker in the Delta log as well.

## Settled 2026-11-09

The provisional answers were built and run end to end on the test
workspace (`ucpub`, us-east-2), first on Lambda, then on Fargate
(docs/ideas/unity-catalog-publish.md, "First end-to-end version", "Ready
for work" and "Fargate"). Each decision that a test proved is accepted;
the others stay open.

| Decision | Settled answer | Evidence |
|---|---|---|
| 1. Managed output table | **Accepted.** Clavesa never writes a managed table. Shadow mode writes `<table>_clavesa`, a daily comparison proves the two equal, then the names swap and the old table stays as `<table>_before_clavesa` for roll-back. | Rehearsed: preflight, shadow, comparison, history copy, switch, roll-back. `make smoke-databricks-shadow` GREEN. |
| 2. Reading managed tables | **Accepted.** Source kind `unity-catalog`; a managed table is read with credentials Unity Catalog vends. | The comparison read the managed `team_ucpub.sales.daily_events` with vended credentials. |
| 3. Team storage | **Accepted.** The data stays in the team's bucket; `pipeline deploy` derives the runner role's access from Unity Catalog. | Shadow table written into `ucpub-team-699166197771`; after the switch the run wrote the team's external table in place. |
| 4. Lagging column list | **Accepted.** The run prints `REPAIR TABLE … SYNC METADATA`. | First pass: external table taken over, new column, readers saw it, the warning printed. |
| 5. Lineage | **Accepted.** The clavesa step is its own box in the graph, pushed by the runner after the table exists. | Lineage table → step → table and HTTP source → step → table, read back with the column mapping. |
| 7. Paused step | **Accepted, and enforced** (the provisional answer only documented it): `replaces_databricks_job` makes every run check that the job is paused and idle, and write nothing if not. | An unpaused job failed the run with "Nothing was written"; paused, the run wrote. |
| 8. Steps after it | **Accepted.** The team splits a multi-task job; clavesa starts the named jobs once after every successful run. | A Databricks job started after a Lambda run and after a `--compute local` run, and read the clavesa table. |

**Still open:**

- **Decision 6, Delta features.** The preflight reads the table's Delta
  features and refuses the ones clavesa cannot write, but only unit
  tests cover it. No table with such a feature (deletion vectors, liquid
  clustering) was tried live.
- **The work size.** Fargate is measured up to 1.25 GB a run; a 67 GB
  day is extrapolated, not run.
- **SQL warehouse reads from the team bucket.** A serverless SQL
  warehouse got `Forbidden` from Databricks' proxy on every table in the
  test team bucket, Databricks' own tables included. Not clavesa's, not
  explained.
- **Disconnect after `--create-catalog`** is unit-tested only.
- **An automated switch** (clavesa doing the renames and the grant
  copy) is not built; the switch is the documented hand steps.
- Table registration and lineage use Databricks Public Preview APIs.

## Relationships

- **ADR-001**: clavesa's own infrastructure is generated Terraform; the
  team's Databricks objects are not clavesa's to generate.
- **ADR-014 / ADR-024**: a local-compute run against a cloud warehouse
  writes the same table and starts the same jobs.
- **ADR-015**: CLI and UI attach an output to a Unity Catalog table with
  the same service call.
- **ADR-016**: schema ownership holds in Glue; in Unity Catalog clavesa
  owns single tables.
- **ADR-017**: `databricks-oauth` credential kind; registered raw-file
  sources become external metadata for lineage.
- **ADR-018 / ADR-020**: Delta and the Glue registration are unchanged.
- **ADR-022 / ADR-023**: Athena still serves the clavesa UI.
