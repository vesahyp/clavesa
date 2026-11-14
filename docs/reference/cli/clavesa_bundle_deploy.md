# clavesa bundle deploy

Deploy the jobs of a bundle target to AWS as clavesa pipelines

Plan the bundle target (it must be ready), generate one clavesa pipeline
per job into the workspace, and deploy each with Terraform the way
clavesa pipeline deploy does: the runner (Lambda, or Fargate sized from the
job's clusters), the Step Functions state machine, the bundle's files in
S3, the schedule (EventBridge Scheduler, with the job's time zone),
periodic and file_arrival triggers, an SNS topic per notification kind
with the job's email addresses, a CloudWatch alarm for a
RUN_DURATION_SECONDS health rule, and the runner's access to the job's
Glue databases and Databricks secrets (Secrets Manager
clavesa/<workspace>/dbx/<scope>/<key>).

The workspace must be deployed (clavesa workspace deploy). When the job
adds runner requirements, deploy the workspace again so the runner image
has them.

  clavesa bundle deploy -t aws --plan-only
  clavesa bundle deploy -t aws --yes

## Usage

```
clavesa bundle deploy [flags]
```

## Flags

```
      --bundle string     the bundle folder (the one with databricks.yml) (default ".")
      --emit-only         write the pipelines and their Terraform, run no terraform (for review)
      --job stringArray   deploy only this job (repeatable; default: every job of the target)
      --plan-only         stop after terraform plan (don't apply)
  -t, --target string     the bundle target (default: the target with default: true)
      --var stringArray   set a bundle variable, name=value (repeatable); wins over BUNDLE_VAR_<name> and the target
  -y, --yes               skip the interactive 'Apply this plan?' confirmation
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa bundle](clavesa_bundle.md) — Deploy Databricks bundle (DAB) jobs to clavesa (ADR-031)
- [Command index](README.md)
