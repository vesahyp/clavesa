# ADR 008: Go Backend over Node.js

**Status**: Accepted

## Context

Clavesa's backend API is responsible for:

1. **HCL parsing** — reading `.tf` files, identifying Clavesa module blocks, building a pipeline graph
2. **File operations** — surgically adding, updating, and removing HCL blocks while preserving comments, formatting, and non-Clavesa content
3. **Plan execution** — invoking `terraform plan` and interpreting structured output
4. **API serving** — exposing these capabilities as JSON APIs to the React frontend

The architecture (pre-ADR) assumed a Node.js backend, primarily for language alignment with the React frontend (noted in [ADR 004](004-ui-framework.md)).

### The HCL problem

HCL parsing and round-trip editing is the backend's hardest job. We evaluated three approaches to HCL manipulation in a Node.js backend:

**Go `hclwrite` via WASM** — HashiCorp's official HCL library compiled to WebAssembly and called from Node.js. `hclwrite` is purpose-built for round-trip editing (parse, modify blocks, write back, preserve comments/formatting). However: Go WASM binaries are 15-20MB, `syscall/js` interop requires serializing all data across the boundary, debugging is opaque, and no existing npm package exposes the full hclwrite API — we'd need to write a custom Go→WASM bridge.

**Native JS/TS parser** — No viable option exists. `hcl2-parser` (archived, GopherJS-based) and `@cdktf/hcl2json` (WASM-based) are parse-only — they convert HCL to JSON but cannot write back. There is no pure JavaScript HCL library that supports round-trip editing.

**Tree-sitter HCL grammar** — `@tree-sitter-grammars/tree-sitter-hcl` is actively maintained and produces a concrete syntax tree that preserves all tokens for round-trip editing. But tree-sitter provides syntax, not semantics — we'd need to build the entire HCL manipulation layer (block identification, attribute extraction, reference resolution, block-level CRUD) in TypeScript on top of it. String interpolation support is incomplete.

All three approaches amount to running Go inside Node (WASM), using abandoned libraries (native JS), or rebuilding what `hclwrite` already does (tree-sitter). The common factor: the best HCL tooling is in Go.

### Beyond HCL

The same pattern holds for Terraform integration. `terraform-exec` is HashiCorp's official Go library for running Terraform commands programmatically — structured plan output, state reading, apply execution. PLAN-RUNNER's current design shells out to `terraform plan -json` and parses stdout; `terraform-exec` replaces this with a typed Go API.

## Decision

**Go as the backend language.** The backend API is a Go service exposing JSON endpoints consumed by the React frontend.

### Key libraries

| Capability | Library | Notes |
|---|---|---|
| HCL parse + round-trip write | `github.com/hashicorp/hcl/v2/hclwrite` | Official. Block traversal, attribute manipulation, comment preservation. |
| HCL expression analysis | `github.com/hashicorp/hcl/v2/hclsyntax` | Token-level access for reference extraction. |
| Terraform execution | `github.com/hashicorp/terraform-exec` | Structured plan/apply/state operations. |
| HTTP API | Standard library or lightweight router | `net/http`, `chi`, or similar. |

### What this changes

The frontend/backend boundary does not change. The React frontend communicates with the backend via the same JSON contracts. Only the implementation language of the backend changes.

Components affected:

| Component | Impact |
|---|---|
| HCL-PARSER | Uses `hclwrite` directly — the open question about parser library is resolved. |
| FILE-OPS-API | Uses `hclwrite` directly — round-trip editing with comment/format preservation is built-in. |
| PLAN-RUNNER | Uses `terraform-exec` instead of shelling out — typed plan results, no stdout parsing. |
| PIPELINE-API | Go HTTP handler instead of Node.js — no functional change. |
| DATA-QUERY-API | Go HTTP handler — no functional change. |

## Consequences

**Positive:**
- **HCL parser decision eliminated** — `hclwrite` is the obvious, only choice. No WASM bridge, no semantic layer to build, no abandoned libraries.
- **Round-trip editing is native** — `hclwrite` preserves comments, formatting, and non-targeted blocks by design. This is its entire purpose.
- **Terraform integration is native** — `terraform-exec` provides typed Go APIs for plan, apply, and state operations. No child process management or stdout parsing.
- **Single ecosystem for all backend dependencies** — HCL, Terraform modules, and the backend API all live in the Go ecosystem. Dependency management is unified.
- **Strong runtime characteristics** — compiled binary, low memory footprint, fast startup, built-in concurrency. No Node.js event loop constraints for CPU-bound HCL parsing.

**Negative:**
- **Two languages in the stack** — TypeScript (frontend) and Go (backend). No shared code, separate toolchains, different testing conventions.
- **Smaller contributor pool for backend** — Go is less widely known than JavaScript/TypeScript, though well-established for infrastructure tooling.
- **No code sharing between frontend and backend** — shared types/validation between React and Go require code generation or manual synchronization. The Pipeline Graph JSON contract becomes the source of truth for both sides.
- **Go learning curve** — if the team is primarily JavaScript-experienced, Go introduces a ramp-up period.

**Tradeoffs accepted:**
- Two-language stack in exchange for using HCL and Terraform tooling natively instead of through WASM bridges
- Loss of frontend/backend language alignment in exchange for eliminating the hardest technical risk in Phase 1
- Go learning curve in exchange for aligning with the HashiCorp ecosystem that Clavesa is built on
