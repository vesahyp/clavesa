# ADR 015: CLI and UI Parity

**Status**: Accepted (extends ADR 014)

## Context

Clavesa exposes two surfaces:

- A CLI binary (`clavesa`) with subcommands for workspace setup, pipeline authoring, running, and inspection.
- A web UI served by `clavesa ui`, with pages for the Catalog, TableDetail, per-pipeline dashboards, run-detail, dashboards, and a (legacy, lazy-loaded) DAG editor.

Both surfaces read and write the same `.tf` files via the same HCL parser, and call into a shared service layer for non-HTTP business logic. The plumbing is unified; the **contract about what each surface must offer** is not.

Different users will pick different surfaces. Some live in terminals; others won't touch a CLI and will reach for `clavesa ui` as their first and only contact point. There is no correct answer about which surface is "primary."

What's missing is a binding rule that prevents the two from drifting:

- Anyone adding a CLI flag has no current obligation to mirror it in the UI.
- Anyone adding a UI form field has no current obligation to mirror it on the CLI.
- Defaults can drift (CLI defaults `compute=local`; UI form makes it required).
- Names can drift (CLI says `--bucket`; UI form labels it "Data location").
- Whole capabilities can be CLI-only or UI-only by accident.

ADR 014 established that local and cloud must offer the same user-facing behavior across the two **backends**. This ADR establishes the analogue for the two **surfaces**: anything you can do in one, you can do in the other, with the same shape.

### Why now

The active TODO focus is "CLI / UI authoring parity + workspace dashboards." Without a binding rule, the parity work risks landing as a one-time alignment that drifts the moment new features ship. Better to commit to the principle now and design every new capability for both surfaces from the start.

## Decision

**CLI and UI are equal-class surfaces. Every authoring or operating capability available on one must be available on the other, at the same fidelity, with consistent defaults and naming.**

Concretely:

1. **Capability parity.** Every CLI command that mutates state or triggers an action has a UI equivalent that performs the same action via the same service-layer call. Every UI affordance that mutates state or triggers an action has a CLI equivalent.

2. **Default parity.** If the CLI defaults a value (e.g. `compute=local` for transforms in dev), the UI form defaults to the same value. No "the UI requires it explicitly because forms feel incomplete without dropdowns" — the principle is one mental model, two presentations.

3. **Naming parity.** Variables, flags, and form labels match. The CLI's `--bucket` is the UI form's `bucket` field. Where a longer human label helps the UI ("S3 bucket or local path"), keep the underlying field name identical so docs, CLAUDE.md references, and error messages all line up.

4. **Output parity.** CLI commands that return multi-row data accept `--json` for machine consumption. The UI renders the same data in its native shape from the same service-layer call. No CLI surfacing fields that the UI hides, or vice versa.

5. **New work ships both at once.** A new capability adds to the service layer first, then gets a CLI wrapper and a UI wrapper in the same slice. "CLI first, UI later" or "UI first, CLI later" is not allowed — same rule as ADR-014's "local-and-cloud" principle, applied to surfaces instead of backends.

### Backend boundary

The service layer (`internal/service/`) is the shared core. Both surfaces are thin wrappers:

- **CLI** — Cobra commands in `internal/cli/` parse flags/args and call into `internal/service/`. They never reach for HTTP handlers and never duplicate business logic from the service layer.
- **UI HTTP API** — handlers in `internal/api/` decode JSON and call into `internal/service/`. They never reach for CLI commands and never duplicate business logic from the service layer.

When adding a new capability:

1. Add the function to the service layer with a typed input struct and a typed result.
2. Add the CLI wrapper: a Cobra subcommand that builds the input struct from flags/args, calls the service function, formats the result.
3. Add the UI wrapper: an HTTP handler that decodes the input struct from JSON, calls the service function, encodes the result; plus the React surface that calls it.

If a capability "doesn't fit the UI" or "doesn't fit the CLI," that's a sign the service-layer contract is wrong, not a license to skip the wrapper.

### Discovery affordances

Where useful, the UI surfaces the equivalent CLI command (e.g. a "view as CLI" toggle on authoring forms) so power users can learn the CLI from the UI and so docs can reference one canonical command line. Optional, low priority — does not gate parity; can be added per-page as it pays off.

## Consequences

**Positive:**

- **User choice is real.** Pick the surface you prefer; don't lose features. A team mixing terminal and visual users converges on the same `.tf` files and the same outcomes.
- **Clean service-layer contracts.** A function that has to be wrapped by both a CLI and an HTTP handler tends to have cleaner inputs and outputs than one wired directly to either.
- **Documentation simplifies.** One mental model, two presentations. Concept docs (CLAUDE.md, ADRs, README) describe the capability once and reference both surfaces.
- **Predictable user experience.** Users moving between surfaces see the same names and defaults, not two parallel dialects.

**Negative:**

- **Two integration surfaces per feature.** Roughly 1.3–1.5× wrapper code per capability, indefinitely. Real cost; we accept it in exchange for the principle.
- **Naming friction.** Some CLI conventions (positional args, short flags) don't translate cleanly to UI form fields. Resolved case-by-case favoring clarity over symbolic symmetry — but the underlying field name stays consistent so logs, errors, and docs match.
- **Drift risk.** Two wrappers can subtly diverge (CLI accepts a value the UI rejects; UI computes a default the CLI doesn't). Mitigation: shared input-struct types in the service layer (so both wrappers decode into the same Go struct), plus a contract test that exercises both wrappers against the same scenarios.
- **Confirmation flows.** CLI's `--yes`/interactive prompt model doesn't map 1:1 to a UI modal. Each destructive command needs both an interactive CLI path and a UI modal; the underlying service function should be agnostic.

**Tradeoffs accepted:**

- Two wrappers per service function in exchange for surface-agnostic access to every capability.
- Naming conformance across surfaces over surface-native idiom.
- Slight UI awkwardness when a CLI concept (e.g. piping `--json` output) has no native UI analog. Document; don't pretend symmetry that isn't there.

## What this changes elsewhere

- **CLAUDE.md.** New hard rule: "CLI and UI are equal-class surfaces; new capabilities ship on both at once." References this ADR.
- **The active TODO focus** ("CLI / UI authoring parity + workspace dashboards") becomes the first slice that *applies* this principle: aligning the existing surfaces to it. Subsequent slices (any new authoring or operating feature) maintain it.
- **Service-layer organization.** `internal/service/` becomes the canonical home for any function called from both surfaces. CLI-only or HTTP-only helpers are a smell unless they're surface-presentation concerns (flag parsing, JSON encoding, etc.).
- **Review process.** PRs that add a CLI flag without the UI mirror, or vice versa, get a parity-bot equivalent flag — at minimum a review-checklist line.

## Open questions

- **Scripting workflows.** Some CLI uses are batch shell scripts (`for pipeline in ...; do clavesa pipeline run $pipeline; done`). The UI doesn't have a batch equivalent today; building one (multi-select on `/pipelines` + bulk run) is out of scope for this ADR but consistent with the principle.
- **Power-user CLI features the UI shouldn't grow.** A few CLI behaviors (`--verbose` log toggles, `--dry-run` output to stdout) don't need UI analogs because the UI shows them natively (logs in the run-detail view, plan diff in the editor preview). Resolved on a case-by-case basis: if the UI already covers the underlying need, no parity wrapper is required.
- **CLI ergonomics that don't translate.** Tab completion, shell-history recall, pipe composition. These are surface-native and do not need UI analogs.

## References

- ADR 012 — PySpark as universal execution engine. Established engine parity.
- ADR 014 — Local–cloud parity. Established backend parity. This ADR extends parity to surfaces.
