variable "pipeline_name" {
  description = "Pipeline-level namespace. Used in state machine naming, CloudWatch log group, and resource tagging. Must be unique within an AWS account and region."
  type        = string
}

# --- Three-level namespace (ADR-016) ---

variable "catalog" {
  description = <<-EOT
    Workspace catalog identifier (top level of <catalog>.<schema>.<table>).
    Required as of v0.18.1 — orchestration creates the Glue DB at the
    encoded `<catalog>__<schema>` name the runner writes Iceberg tables
    into, and points the runs_writer Lambda's CLAVESA_DATABASE env at
    the same DB so Athena INSERT INTO `<db>.runs` lands alongside the
    transform outputs and node_runs the runner produces.
  EOT
  type        = string
}

variable "schema" {
  description = <<-EOT
    Pipeline schema identifier (middle level of <catalog>.<schema>.<table>).
    Required as of v0.18.1 — see `catalog`.
  EOT
  type        = string
}

variable "system_catalog" {
  description = <<-EOT
    Workspace-owned observability catalog (ADR-016 "Workspace system
    catalog"). The runs_writer Lambda appends to
    `<system_catalog>.pipelines.runs` regardless of which pipeline fired
    the SFN execution — multi-writer by design, distinguished by the
    `pipeline` column on each row. As of v0.20.0 this replaces the
    per-pipeline `<catalog>.<schema>.runs` location.
  EOT
  type        = string
}

variable "nodes" {
  description = <<-EOT
    Map of pipeline transform nodes. Each entry becomes one Step Functions Task
    state that invokes the Clavesa runner Lambda for that node.

    The Lambda payload is the event shape runner.handler() expects:
      { "inputs": <inputs>, "outputs": <outputs> }

    Source and destination nodes do not appear here — they are not invoked
    directly by the orchestration; transforms read source S3 paths via their
    inputs map, and destinations consume transform outputs in a follow-up phase.

    - lambda_function_arn: ARN of the runner Lambda for this node (from
                           module.<node>.lambda_function_arn).
    - inputs:              Map of alias → input descriptor. Each value is either
                           a string (S3 URI or Iceberg table id, current shape)
                           or an object describing a partitioned source for
                           incremental reads (v0.12+).
    - outputs:             Map of output_key → output descriptor. Each value is
                           either a string (S3 URI, table id, or "" for
                           Iceberg auto-tables) or an object specifying a write
                           mode (replace|append, v0.12+; merge, Gate 4 — set
                           merge_keys = ["…"] for MERGE INTO semantics).
    - timeout_seconds:     Step Functions task timeout (default: 300).
  EOT
  # `any` rather than `map(object({...}))` so multi-node pipelines can
  # carry heterogeneous input/output descriptors across nodes (string
  # form for Iceberg-table reads, dict form for partitioned sources;
  # dict form for append/merge outputs vs. string for replace). Terraform
  # would otherwise refuse to unify the differing inferred shapes across
  # the outer map.
  type = any
}

variable "edges" {
  description = <<-EOT
    Ordered list of directed edges between transform nodes. Defines execution
    order. Source-to-transform and transform-to-destination edges are not
    represented here — they are encoded in each transform's `inputs` map.

    A node with multiple outgoing edges (same from, different to) triggers a
    Parallel fan-out state. A node with multiple incoming edges (same to,
    different from) triggers a Parallel fan-in state.
  EOT
  type = list(object({
    from        = string
    from_output = string
    to          = string
  }))
}

variable "trigger_queue_arns" {
  description = "SQS queue ARNs from source nodes to poll for new data. Populate with module.<source>.trigger_queue_arn outputs. When non-empty and trigger_batch_window is set, a Lambda poller starts the pipeline when any queue has messages."
  type        = list(string)
  default     = []
}

variable "trigger_batch_window" {
  description = "How often to check source queues for new data, e.g. \"rate(15 minutes)\" or \"rate(1 hour)\". Requires trigger_queue_arns to be set. Null disables SQS-based triggering."
  type        = string
  default     = null
}

variable "schedule" {
  description = <<-EOT
    Optional EventBridge schedule expression to trigger the pipeline automatically.
    Accepts cron or rate expressions:
      cron(0 2 * * ? *)   — daily at 02:00 UTC
      rate(1 day)         — every 24 hours
    When null (default), the pipeline is manual-trigger only (start-execution via CLI/API).
  EOT
  type    = string
  default = null
}

variable "error_notification_topic" {
  description = <<-EOT
    Optional SNS topic ARN to notify when the pipeline fails.
    When set, a PipelineFailedNotify Task state is inserted before the terminal PipelineFailed
    Fail state. The SNS message includes the pipeline name and the failing state's error cause.
  EOT
  type    = string
  default = null
}

variable "tags" {
  description = "Additional tags to apply to all resources created by this module. Merged with Clavesa-managed tags (clavesa:pipeline, clavesa:type)."
  type        = map(string)
  default     = {}
}

variable "bucket" {
  description = <<-EOT
    Optional S3 bucket name (no scheme, no prefix) used by the runs-writer
    Lambda. When set, an EventBridge rule subscribes to this state machine's
    terminal status-change events and a tiny Python Lambda appends one row
    per execution to <database>.runs (Iceberg via Athena). The bucket also
    hosts Athena query results under <pipeline>/_athena-results/ and the
    runs Iceberg table under <pipeline>/_observability/runs/. Leave null to
    disable observability writes.
  EOT
  type    = string
  default = null
}
