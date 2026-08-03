# clavesa credential

Manage workspace credentials (registry)

Manage the workspace-level credentials registry (ADR-017).

A credential is a named entry recording how to authenticate an outbound
request — never the secret material itself. Sources reference credentials
by name via --credentials.

Slice 2: kind=header only. Three secret backends:
  arn:aws:secretsmanager:...   cloud-native, runtime fetch via Secrets Manager
  env:VAR_NAME                 local-only (rejected for cloud deploys)
  file:<workspace-rel>         local-only, gitignored under .clavesa/credentials/*.secret

Examples:
  clavesa credential register stripe \
    --header Authorization --value-prefix "Bearer " --secret env:STRIPE_KEY

  clavesa credential list
  clavesa credential show stripe
  clavesa credential delete stripe

## Usage

```
clavesa credential
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## Subcommands

- [clavesa credential delete](clavesa_credential_delete.md) — Delete a registered credential
- [clavesa credential list](clavesa_credential_list.md) — List registered credentials
- [clavesa credential register](clavesa_credential_register.md) — Register a new credential
- [clavesa credential show](clavesa_credential_show.md) — Show a credential's spec (never the secret material)

## See also

- [clavesa](clavesa.md) — Visual ETL for Terraform pipelines
- [Command index](README.md)
