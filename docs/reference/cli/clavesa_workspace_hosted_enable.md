# clavesa workspace hosted enable

Write the hosted block into clavesa.json

Write the hosted block into clavesa.json and hosted.tf into the
workspace. Nothing in AWS changes until the next deploy: run
clavesa deploy to create the reader role and the workgroup and to upload
the tree copy.

The hosted UI Lambda's role (the reader principal) and its event bus are
read from <url>/.well-known/clavesa.json. --reader-principal adds more
principals that may assume the reader role, kept beside the Lambda's
(repeat it for more than one; for example the IAM user that runs the
smoke gate's reader leg). --event-bus-arn overrides the bus.

## Usage

```
clavesa workspace hosted enable [flags]
```

## Flags

```
      --event-bus-arn string           the hosted clavesa-ui event bus; empty: the one the hosted UI names
      --json                           output as JSON
      --reader-principal stringArray   extra IAM principal ARN allowed to assume the reader role, beside the hosted UI's own (repeatable)
      --url string                     the hosted UI, such as https://ui.clavesa.dev (required)
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa workspace hosted](clavesa_workspace_hosted.md) — Join or leave the hosted read-only UI
- [Command index](README.md)
