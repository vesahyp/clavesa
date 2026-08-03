# clavesa pipeline

Manage pipelines

Manage pipelines (list, show, create, delete, upgrade, plan, deploy, destroy).

A pipeline is a subdirectory containing .tf files that define sources,
transforms, and destinations.

Most pipeline commands take the pipeline directory as the first argument.
Omit it to use the current directory once you have cd'd into the pipeline.

Examples:
  clavesa pipeline list
  clavesa pipeline create my-pipeline
  clavesa pipeline show my-pipeline
  clavesa pipeline show                  # from inside the pipeline dir
  clavesa pipeline upgrade my-pipeline
  clavesa pipeline delete my-pipeline --force

## Usage

```
clavesa pipeline
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## Subcommands

- [clavesa pipeline backfill](clavesa_pipeline_backfill.md) — Replay a transform over a historical partition window
- [clavesa pipeline cost](clavesa_pipeline_cost.md) — Report cost per billion records — clavesa's north-star metric
- [clavesa pipeline create](clavesa_pipeline_create.md) — Create a new pipeline
- [clavesa pipeline delete](clavesa_pipeline_delete.md) — Delete a pipeline
- [clavesa pipeline deploy](clavesa_pipeline_deploy.md) — terraform init -upgrade → plan -out=tfplan → apply tfplan for a pipeline
- [clavesa pipeline destroy](clavesa_pipeline_destroy.md) — terraform destroy on a pipeline (sweeping runtime-created Glue tables first)
- [clavesa pipeline lineage](clavesa_pipeline_lineage.md) — Show the data-lineage graph for a pipeline
- [clavesa pipeline list](clavesa_pipeline_list.md) — List pipelines in the workspace
- [clavesa pipeline optimize](clavesa_pipeline_optimize.md) — Compact, re-cluster, and vacuum a pipeline's Delta output tables
- [clavesa pipeline orchestration](clavesa_pipeline_orchestration.md) — Manage pipeline orchestration
- [clavesa pipeline plan](clavesa_pipeline_plan.md) — Run terraform plan on a pipeline
- [clavesa pipeline reset](clavesa_pipeline_reset.md) — Drop a pipeline's output tables and watermarks so the next run rebuilds from scratch
- [clavesa pipeline rightsize](clavesa_pipeline_rightsize.md) — Recommend per-node Lambda memory from recent run history
- [clavesa pipeline run](clavesa_pipeline_run.md) — Execute the pipeline (local: runner container; cloud: SFN StartExecution)
- [clavesa pipeline show](clavesa_pipeline_show.md) — Show pipeline details
- [clavesa pipeline status](clavesa_pipeline_status.md) — Show per-node status for the latest (or a given) run
- [clavesa pipeline upgrade](clavesa_pipeline_upgrade.md) — Upgrade module versions in a pipeline

## See also

- [clavesa](clavesa.md) — Visual ETL for Terraform pipelines
- [Command index](README.md)
