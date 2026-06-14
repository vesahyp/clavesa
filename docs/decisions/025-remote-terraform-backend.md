# ADR 025: Remote Terraform backend for shared cloud deploys

**Status**: Proposed (2026-06-14).

## Context

Every workspace and pipeline clavesa generates carries a hard-coded `backend "local" {}`, and each pipeline reads the workspace outputs through `data.terraform_remote_state "workspace" { backend = "local" }` pointing at a sibling `terraform.tfstate` on the deployer's disk. The workspace stack writes `pipeline_bucket` and `runner_image` outputs; each pipeline stack reads them through that local-state data source. The cross-stack channel is therefore bound to a file in one developer's working tree.

The consequences are the blocker for multi-developer cloud deploy:

- **Single point of truth, single point of loss.** The live stack's state lives in one gitignored `terraform.tfstate`. Lose the laptop or the directory and the deployed AWS resources are orphaned: no clean `destroy`, no incremental `apply`, only manual console teardown.
- **A second developer cannot `clavesa deploy`.** A fresh clone has no state, so terraform plans to recreate every resource from scratch and collides with the running stack.
- **No locking.** Local state has no lock; two concurrent applies corrupt it.

This is the *only* surface gated on shared state. Clone the workspace and `clavesa ui`, `workspace tables`, `dashboards render`, and ad-hoc Athena already work for any developer against the deployed stack, because they read the cloud catalog, not terraform state. Deploy is the lone exception, and it fails for everyone but the one developer holding the state file.

The pipeline-to-workspace wiring is local-state-bound, not just the workspace root. Moving the backend means moving both halves together: the workspace's own backend block, and every pipeline's `terraform_remote_state` data source that resolves the workspace outputs.

The regen paths make this load-bearing. `CreatePipeline`, `SyncOrchestration`, `UpgradePipeline`, and `UpgradeWorkspace` all rewrite `.tf` by template and regex. Any one of them re-emitting a `backend "local" {}` over a configured remote backend silently reverts the workspace to broken-for-the-team on the next `deploy` or `upgrade`.

## Decision

**The terraform backend becomes a clavesa-configured, manifest-driven workspace property. Remote state is the supported path for any workspace more than one developer deploys.** Absent configuration, the backend stays local and nothing changes; the existing single-developer flow is untouched and fully backward-compatible.

### Manifest field

`clavesa.json` gains an optional `backend` field on the `Manifest` struct:

```json
{
  "name": "analytics",
  "cloud": true,
  "backend": {
    "type": "s3",
    "bucket": "analytics-tfstate",
    "region": "eu-north-1",
    "dynamodb_table": "analytics-tfstate-lock",
    "key_prefix": "clavesa/"
  }
}
```

`dynamodb_table` and `use_lockfile` are mutually exclusive: one names a DynamoDB lock table, the other opts into S3-native conditional-write locking. Locking is not optional; one of the two must be set.

Absent `backend`, the workspace is local-state, exactly as today. `Load()` already backfills missing manifest fields (`Catalog`, `SystemCatalog`) on read; it backfills `backend` as absent by the same mechanism, so old manifests parse unchanged and no migration runs for local-only users. The manifest is the *shared* configuration (committed); local-only settings (warehouse choice, AWS profile) stay in gitignored `.clavesa/*.json` and never touch the backend.

### Emit, both halves

When `backend` is set, generation emits both ends of the wiring from the manifest:

1. **Workspace `main.tf`** gets `backend "s3" {}` as partial config. The bucket, key, region, and lock settings are supplied at `init` time (a `-backend-config` file clavesa writes) rather than inlined, so secrets and per-environment values stay out of the committed `.tf`.
2. **Each pipeline's `data "terraform_remote_state" "workspace"`** is rewritten to `backend = "s3"` with the matching `config` (bucket, key for the workspace state, region). The pipeline now resolves `pipeline_bucket` and `runner_image` from remote state, not `${path.module}/../terraform.tfstate`.

Both halves move together or not at all. A pipeline still reading local state while the workspace writes remote state resolves stale or absent outputs.

### Backend-aware regen

`CreatePipeline`, `SyncOrchestration`, `UpgradePipeline`, and `UpgradeWorkspace` read the manifest `backend` and emit the configured backend, never a literal `backend "local" {}`. Regen preserving the backend is the invariant: `deploy` and `upgrade` must be safe to run repeatedly on a remote-backed workspace without reverting it.

### Migration, not recreate

An existing local-state deployment moves to S3 without destroying anything. `clavesa workspace migrate-state` (equivalently `deploy --migrate-state`) runs `terraform init -migrate-state` against the workspace root and each pipeline in turn, copying the local state into the configured backend. **Ordering is fixed: the workspace root migrates first, then the pipelines**, because each pipeline's remote-state data source reads the workspace's now-remote state. Migration is a one-time operation; after it, `terraform.tfstate` files are dead and gitignored.

### Safety

- **Locking is required.** A backend with neither `dynamodb_table` nor `use_lockfile` is rejected at config time. Concurrent applies must serialize.
- **Server-side encryption on the state bucket.** State carries resource attributes and occasionally secrets; the bucket must have SSE enabled. clavesa asserts this at preflight.
- **No split-brain.** Once `backend` is declared in the manifest, `deploy` refuses to run from a local `terraform.tfstate`. A developer who has not migrated cannot apply against local state and diverge from the shared remote state.

### Where the state bucket lives

The state bucket exists before `terraform init` runs, which is a chicken-and-egg with the workspace bucket terraform itself creates. Two options:

- **User-provided bucket.** The user (or a one-line `aws s3 mb`) creates the state bucket; clavesa records it in the manifest. Simplest, no bootstrap resource, the user owns the lifecycle.
- **Separate bootstrap step.** A tiny `clavesa workspace bootstrap-state` applies a minimal local-state stack that creates the encrypted, locked state bucket, then the main workspace migrates into it.

The user-provided bucket is the default: it sidesteps the bootstrap's own where-does-*its*-state-live regress, and a state bucket is a once-per-workspace artifact a team creates deliberately. The state bucket may be the workspace bucket or a separate one; a separate bucket is cleaner (its lifecycle is independent of any pipeline `destroy`), and is recommended but not required.

## Consequences

**Positive:**

- **Multi-developer cloud deploy works.** A clone plus the manifest's `backend` config is enough to `deploy`, `upgrade`, and `destroy` against the shared stack. The state file stops being a single developer's private artifact.
- **Locking removes the concurrent-apply corruption hazard.** DynamoDB or S3-native lock serializes applies across developers and machines.
- **Losing a laptop is recoverable.** State lives in S3, not a working tree. Any developer reconstructs the deploy capability from a clone.
- **Honors clavesa's "never hand-run terraform" grain.** Remote state becomes a first-class clavesa-configured property instead of forcing the manual `.tf` edit the tool otherwise forbids. The user configures a backend through clavesa; clavesa owns the emit and the migration.

**Negative:**

- **A state bucket is a prerequisite the user provisions.** The chicken-and-egg is real; the default pushes it onto the user (one `aws s3 mb` with SSE), which is honest but is one more setup step than local state.
- **Migration is a state operation with a blast radius.** `terraform init -migrate-state` is safe but irreversible-in-place; a botched migration ordering (pipelines before the root) leaves pipelines reading the wrong state. The fixed ordering and the local-state refusal guard against this.

**Follow-up implementation slices (ordered):**

1. Manifest `Backend` field plus `Load()` backfill (absent = local, no behavior change).
2. Workspace `main.tf` backend emit from the manifest.
3. Pipeline `terraform_remote_state` emit from the manifest (the second half of the wiring).
4. `clavesa workspace migrate-state` (and `deploy --migrate-state`), workspace-root-first ordering.
5. Backend-awareness in `UpgradePipeline` / `UpgradeWorkspace` / `SyncOrchestration` so regen never clobbers the backend.
6. CLI flags: `workspace init --backend s3 --state-bucket … --state-region … --state-lock …` (and the split-brain refusal in `deploy`).
7. Tests across the emit, migrate, and regen paths.

**Interim stopgap (and its limits):** until the slices land, one designated cloud deployer holds the local `terraform.tfstate` and backs it up to S3 by hand; every other developer inspects and runs-local only, never deploys. This is a stopgap, not a fix. It carries none of the locking and keeps the single-point-of-loss: a manual backup is not a shared backend, and two deployers still cannot coordinate.

## Relationships

- **ADR-005** (deployment model): all cloud resources live in the user's AWS account and the local CLI is the only trust boundary. Remote state stays inside that boundary; the state bucket is the user's, applied with the user's credentials. No hosted control plane is introduced.
- **ADR-014** (local/cloud parity): the backend is a deploy-path concern only. It changes nothing about the response shapes the UI consumes, nor about the warehouse/compute resolution. A remote-backed workspace and a local-backed one serve identical reads.
- **ADR-024** (warehouse/compute split): the warehouse already centralizes where *data* state lives (Glue + S3 vs local catalog). This ADR centralizes where *terraform* state lives, analogously. The two are independent axes: a cloud warehouse can still run on a local terraform backend (the single-developer case), and the backend choice does not touch warehouse or compute resolution.
