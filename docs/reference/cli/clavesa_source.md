# clavesa source

Manage workspace input sources (registry)

Manage the workspace-level input source registry (ADR-017).

A source is a named entry recording where raw data lives. Pipelines
reference sources by name in transform inputs:

    inputs = { raw = "sources.<name>" }

Slice 1: only kind=http (no auth).

Examples:
  clavesa source register trips --from https://example.com/trips.parquet
  clavesa source list
  clavesa source show trips
  clavesa source attach my-pipeline trips --to t1 --as raw
  clavesa source delete trips

## Usage

```
clavesa source
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## Subcommands

- [clavesa source attach](clavesa_source_attach.md) — Attach a registered source to a transform input
- [clavesa source delete](clavesa_source_delete.md) — Delete a registered source
- [clavesa source detach](clavesa_source_detach.md) — Detach a named input from a transform
- [clavesa source edit](clavesa_source_edit.md) — Edit a registered source
- [clavesa source list](clavesa_source_list.md) — List registered sources
- [clavesa source preview](clavesa_source_preview.md) — Preview a registered source's data
- [clavesa source register](clavesa_source_register.md) — Register a new source in the workspace registry
- [clavesa source show](clavesa_source_show.md) — Show a source's spec

## See also

- [clavesa](clavesa.md) — Visual ETL for Terraform pipelines
- [Command index](README.md)
