# clavesa workspace init

Initialize a new workspace

Initialize a new workspace.

Pass --backend s3 --state-bucket <b> --state-region <r> to start the
workspace on a remote Terraform backend (ADR-025) instead of local
state: the manifest records the backend, main.tf carries no local
backend block, and a clavesa-owned backend.tf is written alongside it.
Omit the flags and the workspace is created exactly as before —
local state, no backend.tf.

The bucket must already exist, with versioning and default encryption
enabled — see docs/remote-state.md. For an existing local-state
workspace, use `clavesa workspace set-backend` followed by
`clavesa workspace migrate-state` instead.

## Usage

```
clavesa workspace init <name> [flags]
```

## Flags

```
      --backend string            remote Terraform backend type (ADR-025; only "s3" is supported)
      --catalog string            three-level-namespace catalog identifier (default: clavesa_<sanitize(name)>)
      --cloud string              cloud provider (aws) (default "aws")
      --state-bucket string       S3 bucket the backend stores state in (required with --backend)
      --state-key-prefix string   key prefix inside the bucket (default "clavesa/")
      --state-region string       AWS region of the state bucket (required with --backend)
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa workspace](clavesa_workspace.md) — Manage workspaces
- [Command index](README.md)
