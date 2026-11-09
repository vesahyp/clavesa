# clavesa stream

Run stream nodes (Kinesis, Kafka) into Delta tables

## Usage

```
clavesa stream
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## Subcommands

- [clavesa stream run](clavesa_stream_run.md) — Run one stream node against the local warehouse

## See also

- [clavesa](clavesa.md) — Visual ETL for Terraform pipelines
- [Command index](README.md)
