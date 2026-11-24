# clavesa workspace access sync

Apply the column tags to the tables in Lake Formation

Apply the declared column tags to every table that exists, in Lake
Formation. clavesa deploy does the same after each apply. Run it after a
pipeline's first run has made a new table: until then the table has the
value "unreviewed", which no viewer reads.

It runs as you, and you must be a Lake Formation data lake admin.

## Usage

```
clavesa workspace access sync
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa workspace access](clavesa_workspace_access.md) — Show and apply the column tags
- [Command index](README.md)
