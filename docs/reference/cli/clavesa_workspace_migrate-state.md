# clavesa workspace migrate-state

Move the workspace's Terraform state onto the configured remote backend

Move every stack's Terraform state (the workspace root, then each
pipeline) from local files onto the S3 backend recorded by
`clavesa workspace set-backend`. Nothing is destroyed or recreated:
each stack runs `terraform init -migrate-state -force-copy`, and the
pre-migration local state is kept on disk as terraform.tfstate.pre-migrate
so a botched run can be recovered by hand.

Resumable: a stack already migrated on a previous run is reported
"already-migrated" and skipped. Every stack the run reaches is planned
(never applied) afterward and reported "no changes" / "changes" / "error".

Exits non-zero if any stack failed or its post-migration plan errored.

## Usage

```
clavesa workspace migrate-state [flags]
```

## Flags

```
      --json   output the migration result as JSON
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa workspace](clavesa_workspace.md) — Manage workspaces
- [Command index](README.md)
