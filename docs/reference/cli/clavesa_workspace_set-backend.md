# clavesa workspace set-backend

Configure (or clear) the workspace's remote Terraform backend

Record the ADR-025 remote Terraform backend in clavesa.json:

  clavesa workspace set-backend --backend s3 --state-bucket <b> --state-region <r>

--state-key-prefix is optional (default "clavesa/"). The bucket must
already exist with versioning and default encryption enabled — see
docs/remote-state.md for the aws s3api commands to create one.

This only writes the manifest. Nothing is deployed and no Terraform
state moves — run `clavesa workspace migrate-state` next to move
every stack's state onto the new backend.

--clear removes the backend from the manifest, restoring local state.
It refuses once any stack has a backend.tf (i.e. has already migrated):
moving a migrated stack back to local state is a state operation this
command deliberately doesn't perform.

## Usage

```
clavesa workspace set-backend [flags]
```

## Flags

```
      --backend string            remote Terraform backend type (only "s3" is supported)
      --clear                     clear the configured backend (refused once any stack has migrated)
      --state-bucket string       S3 bucket the backend stores state in
      --state-key-prefix string   key prefix inside the bucket (default "clavesa/")
      --state-region string       AWS region of the state bucket
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa workspace](clavesa_workspace.md) — Manage workspaces
- [Command index](README.md)
