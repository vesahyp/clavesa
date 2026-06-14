# Clavesλ

**Clavesa is a self-hosted lakehouse platform. You write transforms in SQL, or in PySpark when SQL isn't enough, and Clavesa compiles them to the cheapest AWS compute that fits: Lambda, Fargate, or EMR Serverless. The whole stack is Terraform in your repo.**

![Catalog page showing every Delta table the workspace produced, ops and user data side by side](docs/images/catalog.png)

---

## What it is

A single Go binary that gives a solo engineer or a small team everything needed to operate a production data lakehouse on AWS:

- **Pipeline authoring.** Visual DAG editor and a CLI, both reading and writing the same `.tf` files. Transforms in SQL or PySpark.
- **Distributed execution.** One PySpark runtime, identical on your laptop and on AWS Lambda, Fargate, or EMR Serverless.
- **Data lakehouse.** Every transform output is a Delta table in Glue Data Catalog, queryable from Athena with no DDL.
- **Observability.** Run history, lineage, freshness SLAs, and SQL-driven dashboards over the same catalog as your data.
- **Local-cloud parity.** Every authoring and operating capability works against deployed pipelines and against local-only ones. Develop offline; deploy when ready.

Built on AWS-native primitives (Step Functions, Lambda, S3, Glue Data Catalog) with no premium services in the data path. No Glue Jobs, no Databricks runtime, no Snowflake credits.

---

## Why this exists

Three ways to run a data platform today, each with a real tradeoff:

- **Hosted SaaS** (Fivetran, dbt Cloud, Airbyte Cloud). Fast to start, but data leaves your account, you pay per row, and nothing runs offline.
- **Hosted runtime** (Databricks, Snowflake, Dagster Cloud). Powerful, but you pay DBU- or credit-hour rates for Spark wrapped in a vendor.
- **Roll your own** (Airflow + dbt + Glue + custom catalogs). Cheap at scale, expensive in engineer-weeks, and prone to local-vs-production drift.

Clavesa is a fourth shape: a lakehouse platform you own, with the authoring ergonomics of a hosted runtime and the cost structure of rolling your own.

- **SQL-first, PySpark when SQL isn't enough.** Analytics engineers write SQL and drop to PySpark only for the transforms that need it. One language for most of the warehouse, one runtime for the rest.
- **Right-sized backend per transform.** A small filter runs on Lambda; a 100M-row join on Fargate; a multi-TB shuffle on EMR Serverless (roughly 5× cheaper than Glue at the same scale). You pick the target per transform, in the same Terraform that defines the pipeline.
- **One engine, local and cloud.** PySpark runs on Lambda, on EMR Serverless, and on your laptop. The same SQL produces the same Delta table in every target.
- **Observability without a second stack.** Run history, lineage, and freshness live as Delta tables in the same catalog as your data. Query them with the same SQL: `SELECT * FROM clavesa_<pipeline>.runs WHERE status = 'FAILED'`.

---

## Prerequisites

**To run clavesa locally** (laptop only, no AWS):

- **Docker.** The runner image executes PySpark for previews, local pipeline runs, and ad-hoc dashboard queries. Required even for fully-offline use.
- **macOS or Linux** (x86_64 or arm64). The binary is a single Go executable; no system libraries to install.

**To deploy pipelines to AWS** (in addition to the above):

- **Terraform 1.x** on your PATH.
- **AWS credentials configured** (e.g. `AWS_PROFILE` or `aws sso login`) with permission to create the resources the modules manage in the target account: Lambda, Step Functions, EventBridge, IAM roles, Glue Data Catalog, S3, ECR, CloudWatch Logs. Nothing is centrally hosted. Every resource lives in your account.

### Install

**macOS (recommended):** install via Homebrew tap as a cask, picks up updates with `brew upgrade`.

```bash
brew install --cask vesahyp/clavesa/clavesa
clavesa version
```

**Linux:** download the prebuilt tarball for your architecture, extract, drop the binary on your PATH.

```bash
ARCH=$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
curl -L https://github.com/vesahyp/clavesa/releases/latest/download/clavesa_$(curl -s https://api.github.com/repos/vesahyp/clavesa/releases/latest | grep tag_name | cut -d'"' -f4)_linux_${ARCH}.tar.gz | tar -xz
./clavesa version
```

Direct macOS downloads from the Releases page are **unsigned**, so Gatekeeper will refuse to run them. Use `brew install --cask` on macOS — Homebrew strips the quarantine attribute on cask install.

**From source** (if you'd rather build locally — see [Development](#development) for toolchain notes):

```bash
brew install mise && mise install && make build
```

---

## Quick start

### Local-only: laptop to working dashboard, no AWS

Two terminal commands get the UI up; everything after that is point-and-click. (Prefer the terminal? See the CLI pointer below.)

```bash
# Pick a workspace dir (any path you like).
WS=/tmp/clavesa-demo
mkdir -p $WS

# 1. Init the workspace. Builds the runner Docker image (docker's layer
#    cache makes a no-change rebuild fast), and records this dir as your
#    active workspace so the UI picks it up without --workspace.
clavesa workspace init demo-ws --workspace $WS

# 2. Start the UI. Browser opens to the Catalog at http://localhost:8080/,
#    which shows a 3-step welcome card on an empty workspace.
clavesa ui
```

Now drive the rest from the browser:

1. **Register a source.** Click **Manage sources** on the Catalog welcome card (or **Sources** in the nav), then **Register source**. Name it `src_trips`, paste this URL, click **Register**:

   ```
   https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
   ```

   NYC TLC Yellow Taxi trips for Jan 2024 (~50 MB / ~3M rows, CloudFront-cached, no auth). The runner fetches it at execution time; nothing is staged.

2. **Create a pipeline.** On the welcome card click **Create a pipeline** (or **Pipelines** → **New pipeline**). Name it `demo`. Clavesa drops you in the editor.

3. **Build the landing transform.** Type `trips` into **Node name**, click **+ SQL Transform**, select it. In the right panel open the **settings** tab, tick **Compute column stats** under **Output**, and click **Save Output Config** (this opts the table into a per-column profile on its catalog page). Back on the **code** tab, **Inputs** → **Add**: the **Source** dropdown is pre-filled with `src_trips`, click **Attach**. Paste the SQL and **Save**:

   ```sql
   SELECT * FROM src_trips
   ```

4. **Build the aggregation transform.** Type `revenue_by_payment` → **+ SQL Transform**. Select it, **Inputs** → **Add**, wire `trips`, **Attach**. Paste and **Save**:

   ```sql
   SELECT
     payment_type,
     COUNT(*) AS trips,
     ROUND(SUM(total_amount), 2) AS revenue,
     ROUND(AVG(tip_amount / NULLIF(fare_amount, 0)) * 100, 1) AS avg_tip_pct
   FROM trips
   GROUP BY payment_type
   ORDER BY revenue DESC
   ```

5. **Run it.** Close the node panel, then click **Run pipeline**. A couple of minutes end to end including Spark cold start (the very first run also builds the runner image, which takes longer). When it finishes, open the run from the **Runs** tab: the DAG shows both transforms **ok**, with a per-node breakdown.

   ![Per-execution run detail with DAG, triage strip, and per-node breakdown](docs/images/run-detail.png)

   Run it twice more (back to the dashboard, **Run pipeline**) so the seeded `/dashboards/pipeline-runs-demo` duration chart has more than one point:

   ![Pipeline runs dashboard for the demo pipeline, five widgets driven by the seeded JSON and populated by three local runs](docs/images/dashboard.png)

6. **Browse the result.** Click **Catalog**. Two new Delta tables sit under your `demo` pipeline. Open `trips`: schema, sample rows of real trip data, a snapshot timeline (one commit per run), lineage, and the **Column profile** card with null %, distinct count (a handful for `payment_type`, millions for `tpep_pickup_datetime`), top-K bars, and p50/p95 for numerics like `fare_amount`. `revenue_by_payment` has the same pages without the profile, since that transform didn't opt in.

   ![Per-table view of revenue_by_payment showing schema, sample rows, snapshot history, and lineage](docs/images/table-detail.png)

> **Prefer the terminal?** Every step above has a `clavesa` command, and CLI and UI write the same `.tf` ([ADR-015](docs/decisions/015-cli-ui-parity.md)), so you can mix the two. The full terminal-only walkthrough lives in the [cookbook](docs/cookbook/README.md).

### Build a dashboard from the demo data

Dashboards live in the `dashboards` system Delta table, in the same catalog as your data and shared with everyone who has workspace access. From `/dashboards` click **New dashboard**, name it, and pick **Blank** to start.

Chart the `revenue_by_payment` table you just built. Click **Add widget** → **Bar chart**. In the widget panel, leave it on **Inline query**, choose the `demo` pipeline, and click `revenue_by_payment` in the **Tables** browser to drop its identifier into the editor:

```sql
SELECT payment_type, revenue FROM clavesa_demo_ws__demo.revenue_by_payment ORDER BY revenue DESC
```

The query auto-runs on the Spark runner; when the result preview fills in, set the **X field** to `payment_type` and the **Y field** to `revenue`. **Save**, then drag or resize the widget on the grid. That's a working revenue-by-payment-type chart off the pipeline you ran a minute ago.

![Dashboard editor: a bar widget with its inline SQL query, the Tables browser that inserts table identifiers, and the live result preview](docs/images/dashboard-editor.png)

A widget's inline query can be promoted to a **shared dataset** for reuse across widgets, and **Controls** add dashboard-level filters (a time-range picker or a select), referenced from SQL as `{{name}}` (or `{{name.start}}` / `{{name.end}}` for a range) and round-tripped through URL params so a filtered view is shareable. Eight widget types are available: big number, line, bar, stacked bar, bar + line, pie, donut, table. One dashboard can blend tables from several pipelines and mix local with cloud, since each widget picks its own pipeline.

Widget SQL is plain Spark SQL, the same dialect you write for transforms, and addresses tables by their `<catalog>__<schema>.<table>` identifier (the Tables browser inserts it for you). On a local pipeline it runs on the Spark runner; on a cloud pipeline clavesa transpiles it to Athena/Trino, so one spec serves both (ADR-023). The CLI authors the same dashboards: `clavesa dashboards apply <file>.json`, with `list` / `show` / `render` / `delete` rounding out the surface (ADR-015 parity).

### Deploy to AWS in one command

Everything above runs locally. When you want it in the cloud, `clavesa deploy` applies the workspace infra (its own S3 bucket, ECR repo, runner image push, system catalog) and every pipeline in it, in one pass. Nothing is centrally hosted; nothing leaves your account.

**Lake Formation-enabled accounts** (default on AWS accounts created after Aug 2023): the deploying principal must be a Lake Formation `DataLakeAdmin` before the first deploy, otherwise the per-pipeline Glue database grants will fail to apply. One-time setup per account:

```bash
ACCT=$(aws sts get-caller-identity --query Account --output text)
ME=$(aws sts get-caller-identity --query Arn --output text)
aws lakeformation put-data-lake-settings --data-lake-settings \
  "{\"DataLakeAdmins\":[{\"DataLakePrincipalIdentifier\":\"$ME\"}]}"
```

If `aws lakeformation get-data-lake-settings` shows `CreateDatabaseDefaultPermissions: []`, your account is LF-gated and the step above is required. Older accounts (`IAMAllowedPrincipals` in the default permissions) inherit the IAM-only path and don't need it.

```bash
AWS_PROFILE=my-account clavesa deploy --workspace $WS
```

`clavesa plan` is the no-apply dry run. Both wrap `terraform init -upgrade → plan -out=tfplan → apply tfplan` behind an AWS credential preflight, applying the workspace first (pipelines read its remote state) and then each pipeline. Re-running is a cheap no-op: terraform and the ECR digest decide what actually changed, so there's nothing to track by hand. The saved plan pauses for a `yes` per target; pass `--yes` for non-interactive runs. Tear down with `clavesa pipeline destroy <pipeline>` (sweeps runtime-created Glue tables first) then `clavesa workspace destroy`. (`clavesa workspace deploy` and `clavesa pipeline deploy <name>` still exist for deploying one target at a time.)

**Operate.** A deployed pipeline doesn't run on its own. Trigger it with `clavesa pipeline run demo --warehouse cloud`, or switch the whole workspace to the cloud warehouse so `pipeline run`, the dashboards, and the catalog all read the deployment without a per-command flag: `clavesa workspace use --warehouse cloud` (back with `--warehouse local`). Scheduling is opt-in by design, not a default, since a schedule spends money you didn't ask to spend: set `trigger_schedule` on a pipeline to run it on a fixed interval. The cloud run history, tables, and dashboards all render in the same UI against identical response shapes (ADR-014), so widgets are interchangeable between cloud and local.

---

## Documentation

- **[CHANGELOG.md](CHANGELOG.md)** lists what shipped in each release, in user-facing terms.
- **[docs/architecture.md](docs/architecture.md)** covers system layers and the data model.
- **[docs/decisions/](docs/decisions/)** holds the ADRs. ADR-012 (PySpark engine), ADR-018 (Delta table format), and ADR-014 (local-cloud parity) are the current architectural anchors.

## Development

Toolchain pinned via `.mise.toml`:

```bash
brew install mise
mise install      # installs Go and Node versions
make dev          # backend :8080 + frontend :5173 with hot reload
make test         # all suites: go + python + cli
```

To drive the dev UI against a real workspace, walk the [Quick start](#quick-start) above against a tempdir and point the frontend at it: `http://localhost:5173/?dir=<your workspace path>`.

## Contributions

Issues and bug reports welcome on [GitHub](https://github.com/vesahyp/clavesa/issues). Not accepting code contributions at this time — drive-by PRs will be closed politely.

For security issues see [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE). Copyright (c) 2026 Vesa Hyppönen.
