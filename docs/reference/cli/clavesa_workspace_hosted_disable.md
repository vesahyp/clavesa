# clavesa workspace hosted disable

Remove the hosted block from clavesa.json

Remove the hosted block from clavesa.json and hosted.tf from the
workspace. The next clavesa deploy deletes the reader role, the workgroup
and the forwarding rules. The tree copy under _clavesa/tree/ stays in the
bucket.

## Usage

```
clavesa workspace hosted disable
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa workspace hosted](clavesa_workspace_hosted.md) — Join or leave the hosted read-only UI
- [Command index](README.md)
