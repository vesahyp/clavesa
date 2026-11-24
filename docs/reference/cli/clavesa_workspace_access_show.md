# clavesa workspace access show

List every table and its tags

List every table of the workspace with its table tag and its tagged
columns, as declared in the pipelines. It reads no AWS. It exits non-zero
when a declaration has a problem that the deploy would refuse.

## Usage

```
clavesa workspace access show [flags]
```

## Flags

```
      --json   print JSON
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa workspace access](clavesa_workspace_access.md) — Show and apply the column tags
- [Command index](README.md)
