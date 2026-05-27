# Architecture Decision Records

We use lightweight ADRs to document key technical decisions.

## Format

Each ADR follows this structure:

- **Status**: Proposed / Accepted / Superseded
- **Context**: What is the situation and what forces are at play?
- **Decision**: What did we decide?
- **Consequences**: What are the tradeoffs?

## Index

| # | Decision | Status |
|---|----------|--------|
| [001](001-iac-output.md) | Terraform as source of truth | Accepted |
| [002](002-orchestration.md) | Pipeline orchestration engine | Accepted |
| [003](003-transform-runtime.md) | Transform execution runtime | Accepted (revised by 012) |
| [004](004-ui-framework.md) | UI framework and DAG library | Accepted |
| [005](005-deployment-model.md) | Self-hosted deployment model | Withdrawn |
| [006](006-modules-vs-provider.md) | Terraform modules over custom provider | Accepted |
| [007](007-storage-format.md) | Apache Iceberg as storage format | Superseded by 018 |
| [008](008-backend-language.md) | Go backend over Node.js | Accepted |
| [009](009-go-project-layout.md) | Go project layout | Accepted |
| [010](010-local-preview-engine.md) | DuckDB for local SQL preview | Superseded by 012 |
| [011](011-raw-ingestion-strategy.md) | Raw ingestion strategy and transform time travel | Superseded by 012 + 013 |
| [012](012-pyspark-universal-engine.md) | PySpark as universal execution engine | Accepted |
| [013](013-table-format.md) | Apache Iceberg as table format | Superseded by 018 |
| [014](014-local-cloud-parity.md) | Local–cloud parity across the user surface | Accepted (extends 012) |
| [015](015-cli-ui-parity.md) | CLI / UI parity across authoring + operating surfaces | Accepted |
| [016](016-catalog-schema-namespace.md) | Three-level catalog / schema / table namespace | Accepted |
| [017](017-workspace-source-registry.md) | Workspace source registry (External Locations) | Accepted |
| [018](018-delta-table-format.md) | Delta Lake as the table format (supersedes 013) | Accepted (v2.0.0 cutover) |
