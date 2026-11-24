# clavesa workspace access

Show and apply the column tags

Show and apply the column tags of a workspace with an "access" block in
clavesa.json (docs/ideas/column-tags.md).

A transform output tags its table with table_tag and its columns with
column_tags. Every value must be in clavesa.json "access" "tags". A table
or column with no tag has the value "unreviewed", which no viewer reads.

  clavesa workspace access show
  clavesa workspace access show --json
  clavesa workspace access sync

## Usage

```
clavesa workspace access
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## Subcommands

- [clavesa workspace access show](clavesa_workspace_access_show.md) — List every table and its tags
- [clavesa workspace access sync](clavesa_workspace_access_sync.md) — Apply the column tags to the tables in Lake Formation

## See also

- [clavesa workspace](clavesa_workspace.md) — Manage workspaces
- [Command index](README.md)
