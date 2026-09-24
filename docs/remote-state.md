# Remote Terraform state

> **When you need this:** more than one developer runs `clavesa deploy` or
> `clavesa workspace deploy` against the same cloud workspace, or a lost
> laptop must not leave the deployed AWS resources with no state to manage
> them.

By default every clavesa workspace keeps its Terraform state as local files:
`terraform.tfstate` in the workspace root and in each pipeline directory.
That works for one developer. It fails when a second person deploys: their
clone has no state, so `terraform plan` wants to recreate everything that is
already deployed. Local state also has no locking, so two applies at the same
time can corrupt it. And if the laptop that holds it is lost, the only record
of what is deployed is lost with it.

A remote backend fixes all three. State lives in a shared, versioned S3
bucket, every clone reads the same state, applies are locked with
`use_lockfile` (Terraform 1.10 or newer), and a bad apply can be rolled back
to an earlier version of the state object. The design is in
[ADR-025](decisions/025-remote-terraform-backend.md).

## 1. Create the state bucket

Clavesa does not create the state bucket. A stack that creates its own state
bucket needs somewhere to keep its own state, which is the same problem one
level down. Create the bucket once, by hand, with versioning, default
encryption and a public-access block:

```bash
aws s3api create-bucket \
  --bucket my-workspace-tfstate \
  --region eu-north-1 \
  --create-bucket-configuration LocationConstraint=eu-north-1

aws s3api put-bucket-versioning \
  --bucket my-workspace-tfstate \
  --versioning-configuration Status=Enabled

aws s3api put-bucket-encryption \
  --bucket my-workspace-tfstate \
  --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

aws s3api put-public-access-block \
  --bucket my-workspace-tfstate \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
```

Use a **different bucket from the workspace's own pipeline bucket**. A
workspace `destroy` removes the pipeline bucket, and the state must survive
that.

`clavesa workspace migrate-state` checks that the bucket has versioning and
default encryption before it changes anything, and stops if either is
missing.

## 2. Point the workspace at it

**A new workspace** can start with the backend set:

```bash
clavesa workspace init my-project \
  --backend s3 \
  --state-bucket my-workspace-tfstate \
  --state-region eu-north-1
```

`--state-key-prefix` is optional and defaults to `clavesa/`. Without the
backend flags, `workspace init` works as before, with local state.

**An existing workspace with local state** records the backend first, as a
separate step from moving any state:

```bash
clavesa workspace set-backend \
  --backend s3 \
  --state-bucket my-workspace-tfstate \
  --state-region eu-north-1
```

This writes only `clavesa.json`. Nothing deploys and no state moves, so the
change can be committed and reviewed before the next step touches live
state. The **Terraform state** control in the UI header does the same.

`clavesa workspace set-backend --clear` removes the backend again, but only
while no stack has been migrated. See section 4.

## 3. Move the state

```bash
clavesa workspace migrate-state
```

This is the only step that touches live Terraform state. It never destroys or
recreates a resource.

1. The workspace root moves first, because every pipeline reads the
   workspace's outputs from its state. Then each pipeline moves, in name
   order. A deployed `_maintenance` pipeline is included.
2. For each stack, clavesa writes a `backend.tf`, removes the local backend
   wiring from `main.tf`, and runs `terraform init -migrate-state
   -force-copy`.
3. When the state object is confirmed in S3, the old local state is kept as
   `terraform.tfstate.pre-migrate`. It is never deleted.
4. Every stack that has state is then planned, never applied, to confirm
   that nothing changed.

The command prints one row per stack and a summary line, and exits non-zero
if a stack failed or a plan returned an error:

```
DIR   KEY                                        STATUS    PLAN
.     clavesa/my-project/workspace.tfstate       migrated  no changes
demo  clavesa/my-project/pipelines/demo.tfstate  migrated  no changes

2 migrated, 0 already migrated, 0 skipped (no state), 0 failed
```

Statuses:

- **migrated**: the state moved to S3 in this run.
- **already-migrated**: an earlier run moved this stack. The stack has a
  `backend.tf` and its state object is in S3 (or it never had state), so the
  run skips it. This is what makes a failed run resumable.
- **skipped-no-state**: the stack was never deployed. It gets a `backend.tf`,
  so its first deploy uses S3, but there is no state to move and no plan.
- **failed**: the move did not complete. The row's `err` says why, and the
  run stops at this stack.

Plan column: **no changes** is the expected result. **changes** means the
plan found drift that existed before the migration; look at it, but it is
not a migration failure. **error** means the plan could not run, and it
counts as a failure for the exit code.

`--json` prints the same result as JSON (`MigrateResult`). The HTTP API
(`POST /api/workspace/backend/migrate`) and the UI's **Migrate state** action
return the same shape.

## 4. What can go wrong, and how to recover

**A stack fails.** The run stops at that stack. Stacks before it are
migrated and stacks after it are not touched. Fix the cause in the row's
`err` and run `clavesa workspace migrate-state` again. It continues from the
failed stack.

**`terraform init` fails, or reports success but no object appears in S3.**
`migrate-state` puts the stack back as it was: the original `main.tf`, no
`backend.tf`, and `terraform.tfstate` copied back from the
`terraform.tfstate.backup` that Terraform wrote.

**A stack has `backend.tf` but its state is not in S3.** This happens if a
run was interrupted in a way the rollback could not handle, or if files were
edited by hand. `migrate-state` then fails for that stack and names the local
copy (`terraform.tfstate.backup` or `terraform.tfstate.pre-migrate`). It does
not count the stack as migrated, because the next deploy would then plan to
recreate every resource. Copy the named file to `terraform.tfstate`, remove
`backend.tf`, restore the local backend wiring (below), and run
`migrate-state` again.

**Moving a stack back to local state by hand.** Every migrated stack keeps
its old state as `terraform.tfstate.pre-migrate`. Copy it to
`terraform.tfstate`, remove the stack's `backend.tf`, and put back the local
wiring clavesa generated: `backend "local" {}` inside the `terraform` block of
the workspace root's `main.tf`, or for a pipeline, this block in its
`main.tf`:

```hcl
data "terraform_remote_state" "workspace" {
  backend = "local"
  config  = { path = "${path.module}/../terraform.tfstate" }
}
```

State written to S3 after the migration is newer than the `.pre-migrate`
copy. If you deployed after migrating, download the current object from S3
instead of using the `.pre-migrate` file.

**Clearing the backend after migrating.** `workspace set-backend --clear`
refuses once any stack has a `backend.tf`. Clavesa does not move state back
to local. Recover each stack by hand as above first.

## Deploy protection

Once `clavesa.json` names a backend, `deploy` refuses to run in a stack that
has no `backend.tf`, or that still has a non-empty local `terraform.tfstate`.
A developer who has not pulled the migrated files cannot apply against old
local state and diverge from the shared state. `deploy` also checks that
Terraform is 1.10 or newer. Run `clavesa workspace migrate-state` to fix a
refused stack; it is safe to run again.
