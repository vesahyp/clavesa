# ADR 025: Remote Terraform backend for shared cloud deploys

**Status**: Accepted (2026-09-23). Proposed 2026-06-14; revised before implementation, see "Revision" at the end.

## Context

Every workspace clavesa generates carries a hard-coded `backend "local" {}`. Each pipeline has no backend block, so its state is also a local file, and it reads the workspace outputs through `data.terraform_remote_state "workspace" { backend = "local" }` pointing at the sibling `../terraform.tfstate`. The workspace stack writes `pipeline_bucket` and `runner_image` outputs; each pipeline stack reads them through that data source. All of the cloud deployment's state is therefore files in one developer's working tree.

The consequences are the blocker for multi-developer cloud deploy:

- **Single point of truth, single point of loss.** The live stack's state lives in gitignored `terraform.tfstate` files. Lose the laptop or the directory and the deployed AWS resources are orphaned: no clean `destroy`, no incremental `apply`, only manual console teardown or a resource-by-resource import.
- **A second developer cannot `clavesa deploy`.** A fresh clone has no state, so terraform plans to recreate every resource and collides with the running stack.
- **No locking.** Local state has no lock; two concurrent applies corrupt it.

Deploy is the only command that writes state. But several read paths also open `terraform.tfstate` straight from disk, and they break the moment the file moves:

- `workspace.PipelineBucket` (`internal/workspace/tfstate.go`), which feeds the cloud warehouse resolution (`ErrWarehouseUndeployed`), Athena defaults in `internal/cli/helpers.go`, and `internal/observability/cloud.go`.
- `readStateMachineARN` (`internal/pipelinestatus/handler.go`), which reads a *resource* attribute, not an output, from the pipeline state.
- `deploy_all` (`internal/cli/deploy_all.go`), which uses an empty `pipeline_bucket` to decide the workspace is undeployed.

So moving the backend is three write-side pieces (the workspace backend, each pipeline's backend, each pipeline's `terraform_remote_state`) and one read-side piece (every reader above).

## Decision

**The terraform backend becomes a manifest-driven workspace property. Remote state is the supported path for any workspace whose deployed stack matters.** Absent configuration the backend stays local and nothing changes: the single-developer flow is untouched and backward-compatible.

### Manifest field

`clavesa.json` gains an optional `backend` field:

```json
{
  "name": "analytics",
  "cloud": "aws",
  "backend": {
    "type": "s3",
    "bucket": "analytics-tfstate",
    "region": "eu-north-1",
    "key_prefix": "clavesa/"
  }
}
```

`type` is `s3`, the only remote type. `bucket` and `region` are required. `key_prefix` is optional and defaults to `clavesa/`.

Absent `backend`, the workspace is local-state, exactly as today. `Load()` leaves it nil for old manifests; no migration runs for local-only users. The manifest is the shared, committed configuration. The AWS profile is per-developer and is never written into the backend config: terraform inherits `AWS_PROFILE` / `AWS_REGION` from the environment, the same credential chain deploy already uses.

### State keys

One key per stack, namespaced by workspace so two workspaces sharing a bucket cannot collide:

- workspace: `<key_prefix><workspace name>/workspace.tfstate`
- pipeline: `<key_prefix><workspace name>/pipelines/<pipeline dir>.tfstate`

### Locking

**S3-native locking (`use_lockfile = true`) only.** It needs Terraform 1.10 or newer and no second resource. DynamoDB locking is not offered: Terraform 1.11 deprecated it, and supporting it would add a config field, a validation branch, and a resource the user must create, for no benefit. Terraform versions below 1.10 are rejected when a backend is configured, at config time and again before deploy.

### Emit: one clavesa-owned `backend.tf`

When `backend` is set, clavesa writes a `backend.tf` file in the workspace root and in every pipeline directory, with the full config inline. It is regenerated from the manifest, never edited by hand, and carries a header saying so.

Workspace `backend.tf`:

```hcl
terraform {
  backend "s3" {
    bucket       = "analytics-tfstate"
    key          = "clavesa/analytics/workspace.tfstate"
    region       = "eu-north-1"
    encrypt      = true
    use_lockfile = true
  }
}
```

Pipeline `backend.tf` holds the pipeline's own `backend "s3"` block with its pipeline key, plus the `data "terraform_remote_state" "workspace"` block with `backend = "s3"` and the workspace key.

Inline config, not partial config with `-backend-config` files: bucket, key and region are not secrets, and inline config means a plain `terraform plan` in the directory works without clavesa. The workspace `main.tf` loses its `backend "local" {}` line and the pipeline `main.tf` loses its `terraform_remote_state` block, because both now live in `backend.tf`. A local-backend workspace is emitted exactly as today, with no `backend.tf`.

### Regen keeps the backend

`CreatePipeline` and workspace init emit `backend.tf` from the manifest. `UpgradePipeline`, `UpgradeWorkspace` and `SyncOrchestration` rewrite `backend.tf` from the manifest and never write a backend or a `terraform_remote_state` block into any other file. Running `deploy` and `upgrade` repeatedly on a remote-backed workspace must never revert it; a test asserts this for each regen path.

### Read side: one function

All state reads go through one function in `internal/workspace` that returns a stack's state JSON: from the local file when the backend is local, from S3 `GetObject` on the stack's key when it is remote. A direct read is used, not `terraform state pull`, because it needs no `terraform init` and runs in milliseconds, and the key layout is clavesa's own. `PipelineBucket`, `readStateMachineARN` and the `deploy_all` check move onto it. "Not deployed" keeps its current meaning: no state object found.

### Migration, not recreate

An existing local-state deployment moves to S3 without destroying anything. `clavesa workspace migrate-state` does, in order:

1. Check preconditions: `backend` set in the manifest, the bucket exists and has versioning and default encryption, terraform is 1.10 or newer, and no state object exists yet at any target key (refuse rather than overwrite).
2. Write `backend.tf` in the workspace root and strip the local backend line from `main.tf`, then `terraform init -migrate-state -force-copy` in the root.
3. For each pipeline: write its `backend.tf`, strip its local `terraform_remote_state` block, `terraform init -migrate-state -force-copy`.
4. Run `terraform plan` in every stack and report any stack that is not "No changes".

**The workspace root migrates first**, because each pipeline's remote-state data source reads the workspace's now-remote state. The local `terraform.tfstate` files are left in place, renamed to `terraform.tfstate.pre-migrate`, so a failed migration can be recovered by hand.

### Safety

- **Encryption and versioning on the state bucket are checked** before migrate-state. State carries resource attributes and can carry secrets; versioning is the undo for a bad apply. Deploy does not repeat the S3 calls: the bucket settings were checked when the state moved in, and deploy checks the Terraform floor and the split-brain rule below.
- **No split-brain.** Once `backend` is set, `deploy` refuses to run in any stack that still has a local `terraform.tfstate` and no `backend.tf`. A developer who has not pulled the migration cannot apply against local state and diverge from the shared state.

### Where the state bucket lives

The state bucket must exist before `terraform init`, and the workspace bucket is created by terraform itself. **The user provides the bucket**, created once with versioning, encryption and a public-access block, and records it with `workspace set-backend` (CLI) or the same action in the UI. A bootstrap stack that creates the bucket would need somewhere to keep its own state, which is the same problem one level down. A separate bucket from the workspace bucket is recommended, because a workspace `destroy` then cannot take the state with it.

### Surfaces (ADR-015)

CLI and UI get the same capability in the same slice:

- `clavesa workspace init --backend s3 --state-bucket <b> --state-region <r> [--state-key-prefix <p>]` for a new workspace.
- `clavesa workspace set-backend …` with the same flags for an existing one, then `clavesa workspace migrate-state`.
- The UI shows the backend beside the workspace warehouse and offers the same set and migrate actions, through the same service calls. There is no workspace settings page today; slice 5 decides where the controls go.

## Consequences

**Positive:**

- **Multi-developer cloud deploy works.** A clone plus the manifest's `backend` is enough to `deploy`, `upgrade`, and `destroy` against the shared stack.
- **Losing a laptop is recoverable.** State lives in a versioned bucket, not a working tree.
- **Locking removes the concurrent-apply corruption hazard**, with no extra resource.
- **A plain `terraform plan` works in any stack directory**, because the backend config is inline.

**Negative:**

- **The user provisions the state bucket.** One more setup step than local state.
- **Terraform 1.10 is the floor for remote-backed workspaces.** Local-backend workspaces keep today's floor.
- **Migration is a state operation with a blast radius.** The fixed ordering, the refuse-to-overwrite check, the kept `.pre-migrate` files and the post-migration plan guard it.

## Implementation slices (ordered)

Each slice carries its own tests; there is no separate test slice.

1. Manifest `backend` field, validation (type, required fields, terraform version), and `Load()` compatibility. No behavior change.
2. The state-read function, and the readers moved onto it. Local behavior unchanged.
3. `backend.tf` emit for workspace init and `CreatePipeline`, plus backend-aware `UpgradePipeline` / `UpgradeWorkspace` / `SyncOrchestration`. These land together: emit without regen protection is a window where the next upgrade reverts the backend.
4. The service layer for set-backend and migrate-state (resumable, `_maintenance` included), and the split-brain refusal in `deploy`.
5. The CLI and the UI for all of it at once: `workspace init` flags, `workspace set-backend`, `workspace migrate-state`, and the UI controls. One slice, because ADR-015 forbids shipping one surface first.
6. Move the cloud smoke workspace to a remote backend and add a migrate leg to `make smoke-cloud`, so the path stays gated.

## Relationships

- **ADR-005** (deployment model): state stays inside the user's AWS account, applied with the user's credentials. No hosted control plane.
- **ADR-014** (local/cloud parity): the backend is a deploy-path concern. It changes no response shape the UI consumes.
- **ADR-015** (CLI/UI parity): set-backend and migrate-state ship on both surfaces.
- **ADR-024** (warehouse/compute split): independent axis. A cloud warehouse can run on a local backend, and the backend choice does not touch warehouse or compute resolution.
- **GH #97** (pipeline resources carry no workspace name): the state keys here are namespaced by workspace, so state does not collide even while #97's resource names still do.

## Revision (2026-09-23)

The 2026-06-14 draft was revised before any code landed:

- **Read side added.** The draft covered only the write side; the readers of the local state file would have reported a migrated workspace as undeployed.
- **Pipeline backend made explicit.** Pipelines have no backend block today, so their own state is local too, not only their read of the workspace state.
- **DynamoDB locking dropped** in favor of `use_lockfile` only, with Terraform 1.10 as the floor.
- **Inline config in a clavesa-owned `backend.tf`** replaced partial config and edits inside `main.tf`.
- **Emit and regen protection merged into one slice**, tests moved into every slice, and the UI surface added for ADR-015.
