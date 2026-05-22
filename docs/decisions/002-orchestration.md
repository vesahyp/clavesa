# ADR 002: Pipeline Orchestration Engine

**Status**: Accepted

## Context

When a user runs `terraform apply` on a Clavesa pipeline, the resulting infrastructure needs something to execute the pipeline steps in the correct order. The orchestration engine is the component that says "run the source extract, then the validation transform, then the enrichment transform (but only after validation succeeds), then write to the destination."

Requirements from Clavesa's design:

- **Terraform-native** — the orchestrator must be expressible as Terraform resources, visible in `terraform plan`
- **Right-sized compute** — must be able to invoke different compute services per step (Lambda for lightweight transforms, Glue/Spark for heavy ones, Athena for SQL)
- **DAG execution** — pipelines are directed acyclic graphs with fan-out (multi-output transforms) and fan-in (transforms with multiple inputs)
- **Transparency** — execution state and history should be inspectable without Clavesa-specific tooling
- **Low idle cost** — pipelines that run once a day shouldn't cost money the other 23 hours
- **Cross-cloud path** — AWS-first, but the module abstraction should allow a different orchestrator per cloud without changing the pipeline definition in HCL

### Options considered

**AWS Step Functions (Standard Workflows)** — native AWS workflow service. Defines execution as a state machine in Amazon States Language (ASL). Direct integrations with Lambda, Glue, Athena, ECS, and 220+ other services. Visual execution inspector. Exactly-once semantics. Pay-per-state-transition, zero idle cost.

**Lambda Durable Functions** — orchestration logic lives inside a Lambda function using checkpoint/replay. Simpler Terraform footprint (just a Lambda), but the pipeline execution flow is hidden inside code rather than visible as a state machine. Very new, thin ecosystem.

**EventBridge + Lambda (event-driven)** — each step emits a completion event, EventBridge rules route to the next step. Maximally decoupled, but requires building DAG execution (especially fan-in), execution tracking, and error recovery from scratch. Effectively building a custom orchestrator.

**MWAA (Managed Airflow)** — industry-standard data pipeline orchestration. Best cross-cloud story (Cloud Composer on GCP, managed Airflow on Azure). But minimum ~$350/month idle cost, introduces a parallel Python DAG definition alongside Terraform (conflicting with ADR 001), and heavy infrastructure footprint.

**Glue Workflows** — Glue-native orchestration. Tightly couples all compute to Glue, preventing right-sized compute per step. Limited orchestration features. No cross-cloud path.

**AWS Glue Studio** — Visual drag-and-drop ETL builder that generates Glue jobs. Closest surface-area comparison to Clavesa. Rejected for the same reasons as Glue Workflows: forces all compute through Spark (PySpark or Python shell), generates proprietary internal configuration rather than Terraform, is cloud-only (no local dev/preview), and is AWS-only with no cross-cloud path.

## Decision

**AWS Step Functions (Standard Workflows)** as the orchestration engine for AWS pipelines.

Each Clavesa pipeline produces one `aws_sfn_state_machine` resource. The pipeline's Terraform module generates the ASL definition from the node/edge graph — each node becomes a Task state that invokes the appropriate compute service (Lambda, Glue, Athena), and edges become transitions between states.

### How it maps to pipeline concepts

| Pipeline concept | Step Functions construct |
|---|---|
| Pipeline | State machine |
| Pipeline run | Execution |
| Node (source/transform/destination) | Task state |
| Edge | State transition |
| Multi-output transform (fan-out) | Parallel state or Choice state |
| Multi-input transform (fan-in) | Parallel state with convergence |
| Error handling | Retry/Catch on Task states |
| Pipeline schedule | EventBridge rule triggering the state machine |

### Why Standard over Express

Standard Workflows provide exactly-once execution, full execution history (90-day retention), and support long-running pipelines (up to one year). Express Workflows are cheaper for high-volume, short-duration workloads but use at-least-once semantics. ETL pipelines benefit from exactly-once guarantees and auditability — transforms should not run twice unintentionally.

Express Workflows remain an option for specific pipeline patterns (high-frequency, idempotent micro-batches) in the future. The module could expose this as a configuration option.

### Cross-cloud path

The pipeline definition in HCL stays the same across clouds. The orchestration module translates to a different engine per cloud:

| Cloud | Orchestration engine | Terraform resource |
|---|---|---|
| AWS | Step Functions | `aws_sfn_state_machine` |
| GCP | Cloud Workflows | `google_workflows_workflow` |
| Azure | Logic Apps / Durable Functions | `azurerm_logic_app_workflow` |

This is consistent with how compute modules already work — Lambda on AWS, Cloud Functions on GCP, Azure Functions on Azure. The cross-cloud boundary is at the module level, not the pipeline definition level.

## Consequences

**Positive:**
- `terraform plan` shows `aws_sfn_state_machine.pipeline` with the full ASL definition — the execution flow is transparent
- Direct SDK integrations with Lambda, Glue, Athena, ECS — no custom glue code to invoke pipeline steps
- Built-in retry, error handling, timeouts, and catch blocks at the state level
- Visual execution inspector in AWS console — useful for debugging before Clavesa's own UI covers execution monitoring
- Zero idle cost — pay only per state transition when a pipeline runs
- Execution history retained 90 days — auditable without Clavesa infrastructure
- Parallel state natively handles fan-out and fan-in patterns

**Negative:**
- ASL is verbose JSON — the Terraform module must generate it, which adds complexity to the module code
- AWS-specific — each cloud requires a different orchestration module (accepted tradeoff, consistent with compute modules)
- State machine type (Standard/Express) is immutable after creation — switching requires recreating the state machine
- 25,000 event history limit per execution — unlikely for batch ETL but could matter for pipelines with thousands of micro-steps
- No native "wait for all inputs" primitive — fan-in requires structuring Parallel states carefully

**Tradeoffs accepted:**
- Per-cloud orchestration module in exchange for using each cloud's native, best-supported workflow engine
- ASL generation complexity in exchange for full transparency in `terraform plan`
- AWS-only for v1 in exchange for shipping faster with the strongest native integration
