# ADR 006: Terraform Modules over Custom Provider

**Status**: Accepted

## Context

ADR 001 established that Terraform is the source of truth — pipelines are `.tf` files, not compiled to them. The next question is how Clavesa exposes its abstractions within Terraform. There are three options:

1. **Terraform modules only** — HCL modules that expand pipeline concepts (source, transform, destination) into concrete cloud resources. Published to the Terraform Registry.
2. **Custom Terraform provider** — a Go binary implementing `clavesa_*` resources with full lifecycle management, plan-time validation, and custom diff output.
3. **Hybrid** — modules for infrastructure expansion, a lightweight provider for validation or orchestration glue.

These have meaningfully different implications for transparency, build cost, and lock-in.

## Decision

**Terraform modules only.** No custom provider for v1.

A provider remains an option for future versions if specific lifecycle or validation gaps emerge that cannot be addressed in the UI/API layer.

### Rationale

**1. Transparency is the core product promise.**

ADR 001's value proposition is that there is no hidden control plane. When a user runs `terraform plan`, they should see `aws_lambda_function.validate`, `aws_iam_role.validate_exec`, `aws_sfn_state_machine.pipeline` — real cloud resources they can understand, modify, and keep if they stop using Clavesa.

A custom provider breaks this. `clavesa_transform.validate will be created` tells the user nothing about what infrastructure is actually being provisioned. The opacity that Clavesa eliminates at the pipeline level would be reintroduced at the Terraform level.

**2. Validation belongs in the backend API, not the Terraform layer.**

The main capability a provider offers over modules is plan-time validation: checking pipeline topology, schema compatibility, source connectivity. But Clavesa already has a backend API that parses HCL to serve the visual UI. This is the natural place for validation — the API validates before the user ever runs `terraform plan`.

A provider would duplicate validation logic that the API needs regardless. Worse, provider validation only runs during `terraform plan`, while API validation runs interactively as the user edits.

**3. Build cost is disproportionate at this stage.**

A custom provider requires Go, the Terraform Plugin Framework, binary compilation per OS/architecture, signing, and registry publishing. This is months of work before a demo is possible. Modules are HCL — the same language the user writes. A working proof-of-concept can ship in days.

**4. Modules maximize the no-lock-in story.**

Modules are the most ejectable Terraform construct. Users can fork them, inline them, or replace them with hand-written resources. A provider creates `clavesa_*` resources in state that require the provider binary to manage — the ejection story requires state surgery or `terraform import` into raw resources.

## Consequences

**Positive:**
- `terraform plan` shows every real cloud resource — full transparency
- No Go toolchain, plugin SDK, or binary distribution to maintain
- Fast iteration — edit HCL, `terraform init`, test
- Users can read, fork, and modify the modules — strongest possible no-lock-in
- Validation and topology checking centralized in the backend API, available interactively

**Negative:**
- No plan-time validation from Terraform itself — users who skip the UI and run `terraform plan` directly won't get Clavesa-specific checks
- No custom plan diff output — Terraform shows raw resource changes, not pipeline-level summaries
- No lifecycle hooks — can't encode "drain queue before destroying this transform" in the module
- Module syntax is more verbose than purpose-built resources (`module "validate" { source = "clavesa/..." }` vs `resource "clavesa_transform" "validate" {}`)

**Tradeoffs accepted:**
- Verbose module syntax in exchange for full transparency
- Validation outside Terraform in exchange for interactive validation in the UI
- No lifecycle hooks in exchange for eliminating provider build/maintenance cost

## Future considerations

If real-world usage reveals gaps that only a provider can fill — for example, complex destroy ordering that modules can't express, or demand for CLI-only workflows that need plan-time validation — a hybrid approach becomes the natural evolution. The modules would remain; a lightweight provider would fill the specific gaps. This decision does not preclude that path.
