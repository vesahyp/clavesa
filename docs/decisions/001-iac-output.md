# ADR 001: Terraform as Source of Truth

**Status**: Accepted

## Context

Clavesa needs a way to define data pipelines as infrastructure. The key question is the relationship between the pipeline definition and the infrastructure-as-code:

1. **Pipeline definition compiles to IaC** — a separate format (JSON, YAML) is the source of truth, and a compiler generates Terraform/CloudFormation/Pulumi as output.
2. **IaC is the source of truth** — pipelines are defined directly in Terraform using modules or a custom provider. The visual UI reads and writes the `.tf` files.

Option 1 is how most visual-to-code tools work (Prophecy, Glue Studio, Infrastructure Composer). Option 2 means there is no intermediate format — Terraform IS the pipeline definition.

## Decision

**Terraform is the source of truth.** Pipelines are defined in `.tf` files using Clavesa-provided Terraform modules (see [ADR 006](006-modules-vs-provider.md)). The visual UI is a layer on top of Terraform, not a separate definition format.

### Why Terraform specifically (vs CDK, CloudFormation, Pulumi)

<!-- TODO: Rationale for Terraform over alternatives -->

## Consequences

**Positive:**
- No ejection story needed — pipelines are already Terraform, there's nothing proprietary to leave behind
- Users get full Terraform ecosystem for free — state management, workspaces, CI/CD integration, `terraform plan` diffs
- The visual UI is additive, not essential — pipelines work without it
- Infrastructure and pipeline definitions live together naturally

**Negative:**
- HCL is less expressive than a purpose-built pipeline DSL for data-specific concerns
- The UI must parse and write HCL, which is harder than reading/writing a controlled JSON schema
- Users need Terraform knowledge, narrowing the audience
- Module/provider development is a different skill set than building a compiler

**Tradeoffs accepted:**
- Narrower audience (Terraform users) in exchange for stronger no-lock-in story
- Harder UI implementation in exchange for eliminating the intermediate format entirely
