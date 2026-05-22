# Architecture Overview

Clavesa is a visual ETL tool where pipelines **are** Terraform — `.tf` files are the source of truth, the UI reads and writes them directly, and `terraform plan` shows every real resource (no custom provider). See [ADR-001](decisions/001-iac-output.md) and [ADR-006](decisions/006-modules-vs-provider.md).

## The model

```
┌─────────────────────────────────────────────────────────────┐
│  Visual UI (React)         CLI (`clavesa …`)              │  Equal-class surfaces (ADR-015)
├─────────────────────────────────────────────────────────────┤
│  Service layer (Go) — HCL read/write, plan/run dispatch     │
├─────────────────────────────────────────────────────────────┤
│  Terraform modules: workspace · source · transform ·        │
│  destination · orchestration                                │
├─────────────────────────────────────────────────────────────┤
│  Runner container (PySpark + Iceberg JARs)                  │
│  Same image runs on local, Lambda, Fargate, EMR Serverless  │
└─────────────────────────────────────────────────────────────┘
```

The frontend uses React + `@xyflow/react` + Tailwind v4 + shadcn primitives ([ADR-004](decisions/004-ui-framework.md)); the backend is Go for native HashiCorp HCL/Terraform tooling ([ADR-008](decisions/008-backend-language.md)).

## One engine: PySpark on Iceberg

**Transform language is SparkSQL or PySpark** — one engine, one set of semantics, regardless of where the job runs ([ADR-012](decisions/012-pyspark-universal-engine.md)). The same runner image powers local preview, Lambda execution, Fargate, and EMR Serverless. Switching compute targets is a Terraform attribute change (`compute = "lambda"|"fargate"|"emr-serverless"|"local"`); the user's code does not change.

**Every transform output is an Apache Iceberg table** registered in the Glue Data Catalog, queryable from Athena with no DDL ([ADR-013](decisions/013-table-format.md)). Iceberg provides atomic writes, snapshot time-travel, and schema evolution. Path-mode plain-Parquet output exists only as an escape hatch for destination overrides.

**No Glue Jobs.** Glue Data Catalog (the free metastore) is in; Glue Jobs (the per-DPU-hour compute) is out. Bigger compute is `fargate` or `emr-serverless`.

## Three-level namespace

Tables are addressed `<catalog>.<schema>.<table>` ([ADR-016](decisions/016-catalog-schema-namespace.md)). The workspace owns its catalog (one per workspace), the pipeline owns its schema, and the table name is derived from `<node>__<key>`. A schema is owned by exactly one producing pipeline; cross-pipeline **reads** are first-class, cross-pipeline **writes** are refused at orchestration-emit time.

Sanitization (Glue's `[A-Za-z_][A-Za-z0-9_]*`) happens automatically at the provider seam, never as a create-time rejection. Glue's flat namespace encodes the address as `<catalog>__<schema>`; the local Hadoop catalog uses nested namespaces natively.

## Local–cloud parity, CLI–UI parity

Every user-facing observability and authoring surface that works against a deployed pipeline also works against a `compute = "local"` pipeline ([ADR-014](decisions/014-local-cloud-parity.md)). Backends differ (Athena vs Hadoop catalog, Step Functions vs filesystem progress, CloudWatch vs stdout); the shapes the UI and CLI consume do not.

CLI and UI are equal-class surfaces — every authoring or operating capability exists on both, at the same fidelity ([ADR-015](decisions/015-cli-ui-parity.md)). New capabilities go into the service layer, then get a CLI wrapper and a UI wrapper in the same slice.

## Input sources

Input sources are registered at the workspace level — a single source can feed many pipelines ([ADR-017](decisions/017-workspace-source-registry.md)). Three kinds: `http` (public URLs), `s3` (same-account paths), and Glue-catalog tables (referenced directly via the three-level address). Credentials live in a separate workspace-level registry; secrets resolve from AWS Secrets Manager (cloud) or `env:`/`file:` (local-only).

## Resource tagging

Every AWS resource a workspace creates carries a uniform set of `clavesa:*` tags so Cost Explorer can group spend per workspace, pipeline, and node.

| Tag key | Source | Meaning |
| --- | --- | --- |
| `clavesa:workspace` | provider `default_tags` (workspace `main.tf`) | Workspace name. Applied to every resource via the AWS provider chokepoint. |
| `clavesa:managed-by` | provider `default_tags` | Always literal `clavesa`. Distinguishes our-managed resources from hand-built ones in the same account. |
| `clavesa:pipeline` | per-node module `tags` | Pipeline name. Set by transform / source / destination / orchestration modules. |
| `clavesa:node` | per-node module `tags` | Transform / source / destination node id. |
| `clavesa:type` | per-node module `tags` | One of `transform` / `source` / `destination`. |
| `clavesa:catalog` | workspace module | ADR-016 catalog identifier (Glue databases). |
| `clavesa:schema` | workspace module | ADR-016 schema identifier (Glue databases). |

The `default_tags` block lives in the workspace-emitted `main.tf` (`provider "aws" { default_tags { tags = {...} } }`), so any resource any module creates picks up workspace + managed-by automatically. Per-node modules merge their own `clavesa:*` tags with the caller's `var.tags` map on top — users can layer their own tags via terraform.tfvars without losing the schema above.

**One-time step:** activate the `clavesa:*` keys as cost-allocation tags in the AWS Billing console (Billing → Cost allocation tags → User-defined). Without this, Cost Explorer ignores them and the per-workspace / per-pipeline spend rollup stays empty. Verification: `aws resourcegroupstaggingapi get-resources --tag-filters Key=clavesa:managed-by,Values=clavesa` should return every workspace-managed resource.

## Preview, run, observe

Preview executes the node's logic through the runner container against upstream data — same engine as production. Every run (local or cloud) appends one row to the pipeline's `node_runs` Iceberg table; terminal Step Function executions append to `runs`. Run history, per-node duration, and lineage all surface from those two tables; the same tables back both the `clavesa pipeline` CLI subcommands and the UI's run-detail / dashboard pages.
