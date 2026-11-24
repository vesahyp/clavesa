# clavesa workspace hosted enable

Write the hosted block into clavesa.json

Write the hosted block into clavesa.json and hosted.tf into the
workspace. Nothing in AWS changes until the next deploy: run
clavesa deploy to create the reader role and the workgroup and to upload
the tree copy.

--kind cloud or --kind local says how the hosted UI reads the workspace,
and is recorded in the block so it never changes with a developer's own
settings. Without --kind: cloud when clavesa.json names a remote
Terraform backend or the workspace stack is deployed (its pipeline_bucket
output is readable); otherwise the workspace's warehouse setting.

The hosted UI Lambda's role (the reader principal) and its event bus are
read from <url>/.well-known/clavesa.json. For a local-warehouse
workspace the command records the publisher role, the data bucket and its
region from that document instead; nothing is deployed, and
--reader-principal and --event-bus-arn do not apply. --reader-principal adds more
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
      --kind string                    cloud (read through the reader role) or local (summary published at run end); default: cloud when the workspace has a remote backend or is deployed, else its warehouse setting
      --no-snapshot                    local workspace: stop publishing the snapshot
      --reader-principal stringArray   extra IAM principal ARN allowed to assume the reader role, beside the hosted UI's own (repeatable)
      --snapshot                       local workspace: publish its catalog and dashboard snapshot at run end (ADR-036; off unless set)
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
