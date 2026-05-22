# ADR 003: Transform Execution Runtime

**Status**: Accepted (revised by [ADR 012](012-pyspark-universal-engine.md))

> **Revision note (post-ADR-012):** The original Phase-0 picture in this document — `athena` for SQL, `lambda` for Python, `glue/spark` as escape hatch, `ecs` for big jobs — has been replaced. Clavesa now uses PySpark as a single universal engine across all compute targets. The decoupling principle below (logic and compute as separate axes) still holds; the *list* of compute targets simplified to `local | lambda | fargate | emr-serverless`, and the *engine* is PySpark on all of them. The history below is retained because the decoupling-axes framing is still load-bearing.

---

## Context

Each node in a Clavesa pipeline (source, transform, destination) needs compute to execute its logic. The orchestration engine (Step Functions, ADR 002) invokes compute per step. The question is which compute services run that logic, and how the architecture relates transform logic to compute target.

Requirements from existing decisions and design principles:

- **Right-sized compute** — core design principle; not Spark-for-everything *(this principle is partially walked back by ADR 012, which observed that one engine across right-sized infra beats multiple engines)*
- **Terraform-native** — compute resources visible in `terraform plan` (ADR 001, ADR 006)
- **Step Functions orchestration** — invokes compute per Task state, supports different services per step (ADR 002)
- **Cross-cloud** — AWS-first, but the module abstraction should allow different compute per cloud

### The decoupling principle

Transform logic (the SQL query, the Python script) must be **independent of where it executes**. Changing the compute target for a pipeline step should be a configuration change, not a rewrite. The transform definition stays the same; only the infrastructure underneath changes.

This means the Terraform module accepts two orthogonal inputs:

1. **Logic** — what the transform does (SQL or Python code/file)
2. **Compute target** — where it runs (`local`, `lambda`, `fargate`, `emr-serverless`)

## Decision

**Compute is decoupled from logic.** The Terraform module treats the transform definition and the compute target as independent configuration axes. The same SparkSQL query or PySpark function can target any compute tier. Switching compute is a Terraform variable change, not a transform rewrite.

### Compute targets

Per ADR 012, all compute targets run the **same engine (PySpark)** with the **same image**. The targets differ in *where* the container runs and how it's billed:

| Compute target | Use when | Status |
|----------------|----------|--------|
| `local` | dev, CI, manual `clavesa pipeline run` | Implemented |
| `lambda` | small batch (≤15 min, ≤10 GB) | Implemented |
| `fargate` | medium batch (long-running, 30 GB+) | Planned |
| `emr-serverless` | distributed Spark (large data) | Planned |

**Removed since the original ADR:**
- `athena` — replaced by SparkSQL on Lambda (and bigger tiers)
- `glue` — replaced by EMR Serverless (~5× cheaper for the same Spark workload)
- `ecs` — replaced/renamed to `fargate` (specifically the one-shot Task model, not a Service)
- `nodejs` transform language — never implemented; quietly dropped in favour of Python on PySpark

### Override path

```hcl
module "heavy_transform" {
  source  = "github.com/vesahyp/clavesa//modules/transform/aws?ref=vX.Y.Z"
  language = "sql"
  sql      = file("transforms/aggregate.sql")
  compute  = "emr-serverless"  # override: distributed for this big join
  # ...
}
```

When no `compute` is specified, the module defaults to `lambda`.

### How it works with Step Functions (ADR 002)

The orchestration module emits one Step Functions Task state per transform node. Every task is a Lambda invocation today; future Fargate / EMR-Serverless targets will use the appropriate Step Functions integration.

| Compute target | Step Functions Task state |
|----------------|--------------------------|
| `lambda` | `arn:aws:states:::lambda:invoke` |
| `fargate` (planned) | `arn:aws:states:::ecs:runTask.sync` |
| `emr-serverless` (planned) | `arn:aws:states:::emr-serverless:startJobRun.sync` |
| `local` | not orchestrated — `clavesa pipeline run` walks the DAG locally |

The orchestration module doesn't care what the transform does — it only needs to know which service to invoke and how to pass `{inputs, outputs}` as the payload. Because the engine is identical across targets, the payload shape is identical too: `runner.handler(event, context)` works the same whether invoked by Lambda, Fargate task, or local CLI.

## Consequences

**Positive:**

- **Logic portability** — changing where a transform runs is a one-variable change; no transform rewrite required.
- **Right-sized infra, single engine** — each pipeline step uses the cheapest/fastest *infra* tier that fits, without changing the *engine*. This is what the original "right-sized compute" principle was after; ADR 012 found that doing it with one engine instead of four is cleaner.
- **Step Functions alignment** — each compute target maps directly to a Step Functions Task integration (ADR 002).
- **Cross-cloud ready** — engine-named compute targets map naturally to each cloud's container/serverless equivalents.

**Negative:**

- **Behavioural differences across SQL engines are no longer a concern** (eliminated by ADR 012's single-engine choice), but **JVM cold-start cost is now uniform** across compute targets. ~5 s adds up for very-low-latency workloads; clavesa doesn't target those.
- **Image size** is uniform too (~1.3 GB). Acceptable for batch ETL.

**Tradeoffs accepted:**

- Per-target Terraform module branches in exchange for logic + compute decoupling.
- One engine's tradeoffs in exchange for engine identity across environments (see ADR 012 for the full case).
