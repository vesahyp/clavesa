# `clavesa/orchestration/aws`

Terraform module that wires pipeline nodes into an AWS Step Functions
Standard Workflow state machine and (optionally) deploys the Clavesa
observability stack — a tiny Lambda that appends one row per execution
to the pipeline's `runs` Iceberg table.

## Overview

Each Clavesa pipeline produces exactly one `aws_sfn_state_machine`.
Transform nodes are runner Lambdas (the PySpark image, deployed by the
`transform/aws` module); orchestration invokes them via
`states:::lambda:invoke`. Sources and destinations are pass-through path
declarations and never appear in `nodes` — sources feed transforms via
each transform's `inputs` map, destinations consume transform outputs by
threading their `target_path` into the upstream node's `outputs`.

The module also creates the Glue Data Catalog database
(`<catalog>__<schema>` per ADR-016) the runner writes Iceberg tables into.

## Usage

```hcl
module "pipeline" {
  source = "../.clavesa/modules/v0.30.0/orchestration/aws" # written by clavesa CLI; embedded in the binary since v0.30.0

  pipeline_name = "my-etl-pipeline"

  # Three-level namespace (ADR-016): <catalog>.<schema>.<table>.
  # Catalog is the workspace identifier; schema is per-pipeline.
  catalog = var.catalog
  schema  = var.schema

  nodes = {
    validate = {
      lambda_function_arn = module.validate.lambda_function_arn
      inputs = {
        raw = module.raw_events.outputs["default"].table_path
      }
      outputs         = { default = "" } # "" = Iceberg auto-table
      timeout_seconds = 300
    }
    enrich = {
      lambda_function_arn = module.enrich.lambda_function_arn
      inputs              = { rows = module.validate.outputs["default"].table_path }
      outputs             = { default = module.warehouse.target_path }
      timeout_seconds     = 600
    }
  }

  edges = [
    { from = "validate", from_output = "default", to = "enrich" },
  ]

  # Trigger paths (any combination):
  schedule             = "cron(0 2 * * ? *)"                    # daily at 02:00 UTC
  trigger_queue_arns   = [module.raw_events.trigger_queue_arn]  # fire on new data
  trigger_batch_window = "rate(15 minutes)"                     # how often to poll the queue

  # Optional alerting:
  error_notification_topic = aws_sns_topic.alerts.arn

  # Run-history observability (v0.13+). Setting bucket enables a Lambda
  # that appends one row per terminal execution to <database>.runs.
  bucket = aws_s3_bucket.pipeline_bucket.bucket
}
```

## Triggers and the `_trigger` field

Each start path stamps a `_trigger` value into the SFN execution input;
the runs-writer Lambda reads it back and writes it to `runs.trigger` so
queries can filter by how the run was started. Allowed values, kept in
sync with `runs_writer/index.py:TRIGGER_VALUES`:

| Value         | Source path |
|---------------|-------------|
| `"scheduled"` | `aws_cloudwatch_event_target.schedule` (cron / rate) |
| `"event"`     | SQS poller Lambda (`poller.py`), fired by source queues |
| `"manual"`    | Anything else: CLI run, console click, missing/malformed input |

Any new start path must set one of these. Unknown values are coerced to
`"manual"` rather than polluting the column.

## Fan-out (multi-output nodes)

A node with multiple outgoing edges triggers a `<node>_Branches` Parallel
state. Each branch runs one immediate successor simultaneously:

```hcl
edges = [
  { from = "validate", from_output = "valid",   to = "write_valid" },
  { from = "validate", from_output = "invalid", to = "write_invalid" },
]
```

The Parallel state has `Catch` pointing to `PipelineFailed` — any branch
failure terminates the pipeline.

## Fan-in (multi-input convergence)

When a node has multiple predecessors, the module detects the convergence
point automatically and sets the fan-out Parallel state's `Next` to the
convergence node:

```hcl
edges = [
  { from = "branch_a", from_output = "default", to = "merge_node" },
  { from = "branch_b", from_output = "default", to = "merge_node" },
]
```

`<predecessor>_Branches` transitions to `merge_node` only after both
branches complete.

## Variables

### Required

| Name            | Type            | Description |
|-----------------|-----------------|-------------|
| `pipeline_name` | `string`        | Pipeline namespace. Used in state machine name, log group, and tags. |
| `catalog`       | `string`        | Workspace catalog identifier (top of `<catalog>.<schema>.<table>`). |
| `schema`        | `string`        | Pipeline schema identifier (middle of `<catalog>.<schema>.<table>`). |
| `nodes`         | `map(object)`   | Transform nodes. Each entry → one Task state invoking `lambda_function_arn`. |
| `edges`         | `list(object)`  | Directed edges between transform nodes. |

### Optional

| Name                       | Default | Description |
|----------------------------|---------|-------------|
| `schedule`                 | `null`  | EventBridge cron/rate expression. Null = no schedule. |
| `trigger_queue_arns`       | `[]`    | SQS queue ARNs from sources. With `trigger_batch_window`, a Lambda poller starts the pipeline on new messages. |
| `trigger_batch_window`     | `null`  | How often the poller checks queues (e.g. `"rate(15 minutes)"`). |
| `error_notification_topic` | `null`  | SNS topic ARN; sets up `PipelineFailedNotify` before terminal `PipelineFailed`. |
| `bucket`                   | `null`  | Pipeline bucket. Setting this enables the run-history observability stack (runs-writer Lambda + EventBridge rule + Iceberg `runs` table). Null disables observability writes. |
| `tags`                     | `{}`    | Additional tags merged with `clavesa:pipeline` / `clavesa:type`. |

## Outputs

| Name                 | Description |
|----------------------|-------------|
| `state_machine_arn`  | Step Functions state machine ARN. |
| `state_machine_name` | State machine name (CLI/console reference). |
| `execution_role_arn` | IAM execution role ARN. |
| `trigger_rule_arn`   | EventBridge schedule rule ARN. `null` if `schedule` is unset. |

## Resources created

Always:

| Resource                            | Purpose |
|-------------------------------------|---------|
| `aws_sfn_state_machine.pipeline`    | Standard Workflow state machine with the ASL definition. |
| `aws_iam_role.sfn_exec`             | Execution role with Lambda invoke + CloudWatch Logs perms. |
| `aws_iam_role_policy.sfn_exec_policy` | Inline policy. |
| `aws_cloudwatch_log_group.sfn_logs` | `/clavesa/<pipeline>/sfn`, 90-day retention. |
| `aws_glue_catalog_database.pipeline_db` | The encoded `<catalog>__<schema>` Glue database. |

When `schedule` is set:

| Resource                                | Purpose |
|-----------------------------------------|---------|
| `aws_cloudwatch_event_rule.schedule`    | EventBridge cron/rate rule. |
| `aws_iam_role.events_trigger`           | Role EventBridge assumes to start executions. |
| `aws_cloudwatch_event_target.schedule`  | Routes the rule to the state machine; stamps `_trigger = "scheduled"` into the input. |

When `trigger_queue_arns` and `trigger_batch_window` are set:

| Resource                  | Purpose |
|---------------------------|---------|
| `aws_lambda_function.poller` | Polls SQS queues; starts an execution with `_trigger = "event"` when any has messages. |
| Supporting IAM + EventBridge resources | Schedule that invokes the poller. |

When `bucket` is set:

| Resource                       | Purpose |
|--------------------------------|---------|
| `aws_lambda_function.runs_writer`| On every terminal SFN status change, appends one row to `<database>.runs` via Athena INSERT. |
| `aws_cloudwatch_event_rule.runs_writer` | EventBridge subscription on the state machine's status-change events. |
| Supporting IAM + log group resources | |

## Error handling

Every Task and Parallel state includes:

- **Retry:** 3 attempts, exponential backoff (5s, 10s, 20s, capped at 60s), on `States.TaskFailed`.
- **Catch:** all unhandled errors → `PipelineFailedNotify` (if SNS topic set) → `PipelineFailed` (terminal Fail state).

## Notes

- **No Glue Jobs.** Glue *Data Catalog* is the metastore Iceberg tables
  register in (free); Glue *Jobs* (per-DPU-hour priced) was removed in
  favor of Lambda + Fargate / EMR Serverless. See ADR-012.
- **Run-history observability is gated on `bucket`.** Pre-v0.13 pipelines
  that don't set `bucket` keep working; their UI run-list page just shows
  empty until the workspace re-emits orchestration with the bucket wired.

## Requirements

| Name      | Version |
|-----------|---------|
| terraform | >= 1.3  |
| aws       | >= 5.6  |
