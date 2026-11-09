# clavesa workspace disconnect databricks

Remove the Databricks connection from clavesa.json

Remove the Databricks connection from clavesa.json.

Tables already registered in Unity Catalog stay there. With a catalog
made by --create-catalog, the catalog, its schemas, the storage
credential, the external location and the IAM role behind them are taken
out of clavesa's Terraform state first: they stay as they are, and
clavesa no longer manages them. Every pipeline's orchestration.tf is
re-emitted without Unity Catalog; deploy the workspace and the pipelines
to apply that.

## Usage

```
clavesa workspace disconnect databricks
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa workspace disconnect](clavesa_workspace_disconnect.md) — Remove a connection to an outside catalog
- [Command index](README.md)
