# clavesa bundle

Deploy Databricks bundle (DAB) jobs to clavesa (ADR-031)

Read a Databricks Asset Bundle (databricks.yml) and run its jobs on clavesa,
in your own AWS account, from the same bundle that deploys them to
Databricks. A clavesa target is an ordinary bundle target that sets the
bundle variable "clavesa" (type: complex); see docs/decisions/031-dab-bundles-on-clavesa.md.

Every command takes --bundle (default: the current folder) and -t/--target
(default: the bundle's default target).

## Usage

```
clavesa bundle
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## Subcommands

- [clavesa bundle compare](clavesa_bundle_compare.md) — Compare the tables a job writes on Databricks with clavesa's shadow tables (PASS or FAIL)
- [clavesa bundle deploy](clavesa_bundle_deploy.md) — Deploy the jobs of a bundle target to AWS as clavesa pipelines
- [clavesa bundle destroy](clavesa_bundle_destroy.md) — Remove a bundle target's jobs from AWS (terraform destroy of their pipelines)
- [clavesa bundle list](clavesa_bundle_list.md) — List the workspace's pipelines generated from bundle jobs
- [clavesa bundle plan](clavesa_bundle_plan.md) — Show, task by task, where each job of a bundle target runs and what has to change
- [clavesa bundle run](clavesa_bundle_run.md) — Run one job of a bundle target on clavesa
- [clavesa bundle switch](clavesa_bundle_switch.md) — Print the steps and the exact SQL that switch a job's tables from Databricks to clavesa (runs nothing)

## See also

- [clavesa](clavesa.md) — Visual ETL for Terraform pipelines
- [Command index](README.md)
