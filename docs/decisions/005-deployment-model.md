# ADR 005: Self-Hosted Deployment Model

**Status**: Withdrawn (never reached a decision; hosted deployment is out of scope).

## Context

This slot was originally a placeholder for "how the UI + API are deployed to a user's AWS account" — i.e. a hosted/managed clavesa offering. The ADR was filed as Proposed in early planning and never written: the body was three TODO comments. It was deleted in commit `8737fc9` ("docs: restructure for post-MVP — 61 files down to 17") on the rationale that it carried no actual decision.

It is reinstated here as a tombstone so the numbering gap is explained and the dangling reference from ADR 009 ("ADR 005 defers hosted deployment") resolves to something concrete.

## Decision

Clavesa runs locally. Users execute `clavesa ui` on their own machine; the binary reads/writes Terraform in their working directory and applies it against their own AWS credentials. There is no hosted product, no multi-tenant control plane, and no Clavesa-operated AWS account.

If a hosted deployment ever becomes in-scope, it should be filed as a new ADR rather than reviving this one.

## Consequences

- Single binary with embedded UI; no service infrastructure to operate (consistent with ADR 009).
- All cloud resources live in the user's AWS account; the local CLI is the only trust boundary.
- "Local-cloud parity" (ADR 014) means parity between `compute = "local"` and the user's own AWS deployment, not between local and a hosted service.
