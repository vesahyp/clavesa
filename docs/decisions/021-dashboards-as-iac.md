# ADR 021: Dashboards as IaC (workspace dashboard registry)

**Status**: Accepted (2026-05-31)

## Context

Dashboards are currently stored as **runtime state**: definitions live in a system Delta table (`<system_catalog>__pipelines.dashboards`), written through the active environment's Provider, and seeded once from `.clavesa/dashboards/*.json` by a one-shot `importLegacyDashboards` that moves the directory aside after it runs. This model has not held up.

1. **The cloud write path is architecturally broken.** `writeDashboard` goes through the cloud Provider, which is Athena. Athena reads Delta but cannot write it (no `MERGE INTO`, no `CREATE TABLE` with a column list for Delta). So in cloud mode a dashboard create/edit never persists. This is a standing TODO with no fix inside the Athena-only path.
2. **State is environment-specific.** The local Provider writes a Delta table in the on-disk warehouse; the cloud Provider would write a different table in S3/Glue. A dashboard authored in one environment does not exist in the other. There is no single source of truth.
3. **It does not survive a rebuild.** The local `dashboards` table lives in the warehouse, so wiping/rebuilding the warehouse loses every dashboard, and re-import requires re-staging files into `.clavesa/dashboards/` (which the importer then consumes again).
4. **Hand-authored definitions do not load.** A `dashboards/heineli.json` a user writes by hand is invisible: the importer only reads `.clavesa/dashboards/`, imports once, and moves it aside.

The deeper cause: a dashboard is a **definition**, not runtime data, and clavesa already has a coherent model for definitions. Pipelines are Terraform you author locally and promote to cloud. Sources and credentials are workspace-level JSON registries under `.clavesa/` (ADR-017). Dashboards diverged from that model into a runtime table. That divergence is what produces every problem above.

This appears to conflict with the "everything in the system catalog, nothing local-only" direction. It does not, once definitions and data are separated. **Runtime data** (`runs`, `node_runs`, `column_stats`, `tables`) stays in system Delta tables, queryable, IAM-governed. **Definitions** (pipelines, sources, credentials, and now dashboards) are code: files in the workspace, version-controlled, dev'd locally, promoted via the repo.

## Decision

**A dashboard is a workspace-level IaC definition, not a catalog table.** Dashboards become a workspace registry of JSON files, exactly the shape ADR-017 established for sources and credentials.

### Storage

One file per dashboard at `.clavesa/dashboards/<slug>.json`. Workspace-level (not pipeline-scoped) because a dashboard's datasets can read across pipelines (each dataset already carries its own `dir`). The file is the single source of truth. There is no system `dashboards` table.

### Lifecycle: dev to promote

The definition file is read directly. Its widget SQL executes through the same Provider seam every other read uses: local mode runs it on the warm Spark worker, cloud mode runs it on Athena. The same definition, a different engine per environment, identical to how pipelines and `/query` already behave (ADR-014).

A dashboard has no infrastructure to apply, so "promote to cloud" needs no deploy or `terraform apply` step. The file travels with the workspace through git, the same way a pipeline's `.tf` does. Switch the environment to cloud, or a teammate opens the workspace, and the dashboard runs against Athena. Promotion is the commit.

### CRUD parity

Create/read/update/delete go through the service layer, with a CLI wrapper and a UI wrapper at equal fidelity (ADR-015), writing and reading the JSON file. This matches `internal/service/source.go` / `internal/cli/source.go` / `internal/api/sources.go`.

### Removed

The system `dashboards` Delta table and its machinery: `ensureDashboardTable`, `writeDashboard`-through-the-Provider, and `importLegacyDashboards`. The read path stops querying a table and reads the registry directory.

## Consequences

- The broken Athena write disappears, because nothing writes a Delta table. Create/edit works identically in both environments by writing a file.
- One source of truth across environments and across rebuilds. A warehouse wipe no longer loses dashboards; they are files.
- Hand-authored `*.json` dashboards load by being in the registry directory.
- **Access control becomes git/file-based, not Lake Formation.** A dashboard definition is governed like a pipeline `.tf`: by who can write the workspace repo, not by a catalog grant. This is the deliberate trade for treating dashboards as code. Dashboard *results* are still gated by the catalog grants on the tables the SQL reads.
- **Migration**: existing definitions in `.clavesa/dashboards.imported/*.json` and `dashboards/*.json` are moved into `.clavesa/dashboards/<slug>.json`. The seeded demo and any user dashboards (e.g. `heineli`) become registry entries. No data loss; these were always files.

### Relationships

- Mirrors **ADR-017** (workspace source/credential registries) as the sibling pattern; dashboards join it.
- Honors **ADR-014** (local/cloud parity): one definition, Provider-dispatched engine.
- Honors **ADR-015** (CLI/UI parity): service-first, then CLI and UI wrappers.
- Supersedes the prior project direction of holding dashboards in a system Iceberg/Delta table. That direction stands for runtime *data*; dashboard *definitions* are code.
