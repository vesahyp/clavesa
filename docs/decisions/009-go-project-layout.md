# ADR 009: Go Project Layout

**Status**: Accepted

## Context

ADR 008 chose Go as the backend language. Five backend components need a home:

| Component | Role | Key dependency |
|---|---|---|
| FILE-OPS-API | Read/write `.tf` files | `hclwrite` |
| HCL-PARSER | Parse `.tf` → Pipeline Graph JSON | `hclwrite`, `hclsyntax` |
| PLAN-RUNNER | Execute `terraform plan` | `terraform-exec` |
| PIPELINE-API | REST API combining the above three | HTTP router |
| DATA-QUERY-API | Read sample data from S3/Iceberg | AWS SDK + Athena (cloud), runner container (local preview, per ADR 012) |

The dependency relationships are:

```
hclwrite ──→ FILE-OPS-API ──┐
hclwrite ──→ HCL-PARSER  ──┤
terraform-exec ──→ PLAN-RUNNER ──┤
                                 ├──→ PIPELINE-API ──→ HTTP
DATA-QUERY-API ─────────────────┘
```

FILE-OPS-API, HCL-PARSER, and PLAN-RUNNER are siblings — none depends on another. PIPELINE-API is the sole integration point. DATA-QUERY-API is orthogonal (different dependencies, no shared code with the HCL layer).

## Decision

### Single binary

One `clavesa` binary serving HTTP. No microservices, no multi-process coordination.

Rationale: Clavesa runs locally; hosted deployment is out of scope (ADR 005, withdrawn). A single binary is the simplest local tool. All backend components share the same trust boundary and there is no operational reason to isolate them.

### Module path

```
github.com/vesahyp/clavesa
```

### HTTP router

`net/http` with a lightweight router (`chi` or equivalent). No frameworks — the API surface is 7 endpoints.

### Package layout

```
cmd/
  clavesa/
    main.go                  Entry point — wires packages, starts HTTP server

internal/
  graph/
    types.go                 PipelineGraph, Node, OutputPort, Schema, Edge, ValidationMessage

  plan/
    types.go                 PlanResult, PlanSummary, NodeChange, ResourceChange, PlanDiagnostic

  hclparser/
    parser.go                Parse .tf files → PipelineGraph (uses hclwrite + hclsyntax)
    validator.go             Topology + schema validation → ValidationMessage

  fileops/
    fileops.go               read / add_block / update_block / remove_block
    attributes.go            AttributeValue, ModuleReference → hclwrite token encoding
    errors.go                Error codes (FILE_NOT_FOUND, BLOCK_NOT_FOUND, etc.)

  planrunner/
    runner.go                terraform-exec integration → PlanResult
    mapper.go                Map Terraform resource addresses → NodeChange (Phase 1b)

  dataquery/
    query.go                 S3/Iceberg read → tabular data rows

  api/
    handler.go               HTTP handlers (calls hclparser, fileops, planrunner, dataquery)
    routes.go                Route registration
```

### Package design rationale

**`internal/graph`** and **`internal/plan`** are shared type packages. `hclparser` produces `graph.PipelineGraph`; `api` JSON-marshals it to the frontend. If graph types lived inside `hclparser`, the API handler would import the parser just for types — unnecessary coupling. Same logic for `plan` types vs `planrunner`.

**`internal/fileops`** owns `AttributeValue` and `ModuleReference` since they are part of the FILE-OPS protocol. `api` already imports `fileops` to call it, so the types come along without adding a dependency edge.

**`internal/dataquery`** imports nothing from `graph`, `fileops`, or `planrunner`. It is isolated by design — different dependencies (AWS SDK, Iceberg reader), no shared code with the HCL layer.

**`internal/`** prefix on everything: none of these packages are meant to be imported externally. This is a server binary, not a library. `internal/` enforces that at the compiler level.

**`internal/api`** (not `internal/pipelineapi`): shorter, and there's only one API layer in the binary. Maps directly to the PIPELINE-API component.

### Testing

Standard `go test`. Test files live next to source (`parser_test.go` alongside `parser.go`). No test framework beyond the standard library — `testing.T` and table-driven tests.

Integration tests that require Terraform or AWS resources go in a separate `test/` directory at the repo root and are gated by build tags (`//go:build integration`).

### Frontend

The React frontend lives in a `ui/` directory at the repo root. The Go binary serves the built frontend assets in production. During development, the frontend runs on its own dev server with API proxy to the Go backend.

```
ui/
  src/
  package.json
  tsconfig.json
```

### Full repo structure

```
cmd/clavesa/main.go
internal/
  graph/
  plan/
  hclparser/
  fileops/
  planrunner/
  dataquery/
  api/
ui/
  src/
  package.json
modules/                     Terraform modules (source, transform, destination, etc.)
test/                        Integration tests (build-tag gated)
docs/                        Project documentation (existing)
go.mod
go.sum
```

## Consequences

**Positive:**
- Every backend component maps to exactly one Go package — agents can work on packages independently without merge conflicts.
- Shared type packages (`graph`, `plan`) prevent circular dependencies and allow API and parser to evolve separately.
- Single binary eliminates deployment and local development complexity.
- `internal/` enforces that package APIs are designed for internal consumption only.

**Negative:**
- `modules/` (Terraform HCL) and `internal/` (Go) and `ui/` (TypeScript) in one repo means three toolchains. CI must handle `go test`, `terraform validate`, and `npm test`.
- Single binary means all components share a process — a crash in DATA-QUERY-API takes down the whole server. Acceptable for local-first MVP.
