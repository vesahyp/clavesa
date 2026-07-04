// Package version holds build-time constants shared across packages
// that can't depend on `internal/service` (where the canonical
// `service.ModuleVersion` re-exports from here).
//
// Leaf package by design — no other internal imports — so every layer
// (workspace, runner, preview, …) can pull the module version without
// creating cycles back through service.
package version

// Module is the Terraform module version tag this binary ships with.
// Bump in lockstep with `internal/service/version.go`'s alias on
// release; see CLAUDE.md "Publishing to clavesa".
const Module = "v2.15.0"
