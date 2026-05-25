// Package tfgen emits the full orchestration.tf body for a clavesa pipeline.
//
// Replaces the v1.1.4 `module "orchestration"` indirection with standard
// Terraform: the Step Functions state machine + IAM + log group + Glue DB +
// runs_writer + optional poller + optional schedule, all spelled out as
// direct resources. The ASL definition is inlined via jsonencode({...}) so
// references like `module.bronze.lambda_function_arn` stay as HCL
// expressions that Terraform's dependency graph still tracks.
//
// Why inline: HCL can't recurse, so the previous HCL-side ASL builder
// (modules/orchestration/aws/main.tf:1-247) couldn't represent nested
// fanouts or multi-hop branches, leaving downstream states orphaned and
// failing AWS's MISSING_TRANSITION_TARGET validator. Doing it in Go also
// improves the exit story: a user dropping clavesa keeps idiomatic
// Terraform with no module dependency.
package tfgen

import (
	"fmt"
	"sort"
	"strings"

	"github.com/vesahyp/clavesa/internal/orchestration/aslgen"
)

// Pipeline carries everything tfgen needs to emit orchestration.tf.
// Expressions are bare HCL (no quoting); literal-string fields are
// quoted by the emitter.
type Pipeline struct {
	// PipelineNameExpr is the HCL expression for the pipeline-name value
	// used in resource names and tags. Conventionally "var.pipeline_name".
	PipelineNameExpr string

	// Three-level namespace (ADR-016). Catalog and SystemCatalog are
	// literal identifiers; SchemaExpr is an HCL expression (typically
	// "var.schema") because the schema may be overridden per pipeline.
	Catalog       string
	SchemaExpr    string
	SystemCatalog string

	// BucketExpr is the HCL expression for the warehouse bucket name.
	// runs_writer is unconditional today (always emitted) — every pipeline
	// has a bucket. Typically "data.terraform_remote_state.workspace.outputs.pipeline_bucket".
	BucketExpr string

	// ScheduleExpr is the HCL expression for an EventBridge schedule
	// (e.g. "var.trigger_schedule"). The emitter wraps in a conditional
	// at plan time — Terraform's count = X != null ? 1 : 0 pattern.
	ScheduleExpr string

	// BatchWindowExpr is the HCL expression for the SQS-poller cadence
	// (typically "var.trigger_batch_window"). Same conditional treatment.
	BatchWindowExpr string

	// TriggerQueueExprs is the list of HCL expressions resolving to SQS
	// queue ARNs (one per source). Empty disables the poller.
	TriggerQueueExprs []string

	// StateMachine is the graph-derived ASL shape; tfgen materialises it
	// into the resource's `definition = jsonencode({...})` block.
	StateMachine aslgen.StateMachine

	// NodeMeta carries per-transform-node data (Lambda ARN, timeout,
	// already-rendered inputs/outputs HCL expressions). Keyed by node
	// name (matches StateMachine state names for Task states).
	NodeMeta map[string]NodeMeta
}

// NodeMeta describes one transform node for ASL Task emission.
// InputsExpr and OutputsExpr are pre-rendered HCL map literals — the same
// strings the historical orchestration.go emitter already built for the
// `nodes = { ... }` block. They get inlined verbatim inside the Lambda
// Payload as HCL values.
type NodeMeta struct {
	LambdaARNExpr  string // e.g. "module.bronze.lambda_function_arn"
	InputsExpr     string // e.g. "{ bronze = \"clavesa.…\" }"
	OutputsExpr    string // e.g. "{ default = { kind = \"iceberg_table\", … } }"
	TimeoutSeconds int    // typically 300
}

// Emit returns the full orchestration.tf body (excluding the `module "src_*"`
// blocks the caller emits separately for kind=s3 sources).
func Emit(p Pipeline) (string, error) {
	if err := p.validate(); err != nil {
		return "", err
	}

	var b strings.Builder

	emitTagsLocal(&b, p)
	emitGlueCatalogDB(&b, p)
	emitLogGroup(&b, p)
	emitIAMRole(&b, p)
	emitStateMachine(&b, p)
	emitSchedule(&b, p)
	emitPoller(&b, p)
	emitRunsWriter(&b, p)

	return b.String(), nil
}

// emitTagsLocal writes the shared `local.clavesa_tags` block referenced by
// every taggable resource below — pipeline name + a fixed "clavesa:type"
// label so a customer's tag-cost report can attribute spend per pipeline.
func emitTagsLocal(b *strings.Builder, p Pipeline) {
	fmt.Fprintf(b, "# Shared resource tags — every clavesa-owned AWS resource carries the\n")
	fmt.Fprintf(b, "# pipeline name + a `clavesa:type` label so tag-cost reporting can\n")
	fmt.Fprintf(b, "# attribute spend per pipeline.\n")
	fmt.Fprintf(b, "locals {\n")
	fmt.Fprintf(b, "  clavesa_tags = {\n")
	fmt.Fprintf(b, "    \"clavesa:pipeline\" = %s\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "    \"clavesa:type\"     = \"orchestration\"\n")
	fmt.Fprintf(b, "  }\n")
	fmt.Fprintf(b, "}\n\n")
}

func (p Pipeline) validate() error {
	if p.PipelineNameExpr == "" {
		return fmt.Errorf("tfgen: PipelineNameExpr is required")
	}
	if p.Catalog == "" || p.SchemaExpr == "" || p.SystemCatalog == "" {
		return fmt.Errorf("tfgen: Catalog, SchemaExpr, SystemCatalog all required (ADR-016 three-level namespace)")
	}
	if p.BucketExpr == "" {
		return fmt.Errorf("tfgen: BucketExpr is required (runs_writer needs a bucket)")
	}
	for _, s := range p.StateMachine.States {
		if s.Type == aslgen.Task {
			if _, ok := p.NodeMeta[s.Name]; !ok {
				return fmt.Errorf("tfgen: NodeMeta missing for task state %q", s.Name)
			}
		}
	}
	return checkInnerTasks(p.StateMachine.States, p.NodeMeta)
}

func checkInnerTasks(states []aslgen.State, meta map[string]NodeMeta) error {
	for _, s := range states {
		switch s.Type {
		case aslgen.Task:
			if _, ok := meta[s.Name]; !ok {
				return fmt.Errorf("tfgen: NodeMeta missing for task state %q", s.Name)
			}
		case aslgen.Parallel:
			for _, br := range s.Branches {
				if err := checkInnerTasks(br.States, meta); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Glue catalog DB — per-pipeline output namespace (ADR-016 encoded name)
// ---------------------------------------------------------------------------

func emitGlueCatalogDB(b *strings.Builder, p Pipeline) {
	// catalog_db encoding mirrors identutil.EncodeGlueDatabase / the
	// runner's _glue_db / the transform module's local.catalog_db. All
	// encoders must stay byte-identical so the runner writes to the DB
	// terraform created. Schema isn't a literal (it's var.schema), so the
	// encoded name is itself an HCL string with an embedded replace().
	fmt.Fprintf(b, "# Glue catalog database — per-pipeline output namespace (ADR-016).\n")
	fmt.Fprintf(b, "resource \"aws_glue_catalog_database\" \"pipeline\" {\n")
	fmt.Fprintf(b, "  name        = \"%s__${replace(%s, \"-\", \"_\")}\"\n",
		safeCatalogLiteral(p.Catalog), p.SchemaExpr)
	fmt.Fprintf(b, "  description = \"Clavesa pipeline output tables — ${%s}\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  tags        = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")
}

// safeCatalogLiteral applies the same hyphen→underscore replacement the
// runner / identutil / transform module apply, so we can hard-code the
// encoded prefix when the catalog is a literal.
func safeCatalogLiteral(catalog string) string {
	return strings.ReplaceAll(catalog, "-", "_")
}

// ---------------------------------------------------------------------------
// CloudWatch log group — SFN execution logging
// ---------------------------------------------------------------------------

func emitLogGroup(b *strings.Builder, p Pipeline) {
	fmt.Fprintf(b, "# CloudWatch log group — Step Functions execution logging (90-day retention).\n")
	fmt.Fprintf(b, "resource \"aws_cloudwatch_log_group\" \"sfn_logs\" {\n")
	fmt.Fprintf(b, "  name              = \"/clavesa/${%s}/sfn\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  retention_in_days = 90\n")
	fmt.Fprintf(b, "  tags              = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")
}

// ---------------------------------------------------------------------------
// IAM execution role for the Step Functions state machine
// ---------------------------------------------------------------------------

func emitIAMRole(b *strings.Builder, p Pipeline) {
	fmt.Fprintf(b, "# IAM execution role for the Step Functions state machine.\n")
	fmt.Fprintf(b, "data \"aws_iam_policy_document\" \"sfn_assume\" {\n")
	fmt.Fprintf(b, "  statement {\n")
	fmt.Fprintf(b, "    sid     = \"StepFunctionsTrust\"\n")
	fmt.Fprintf(b, "    actions = [\"sts:AssumeRole\"]\n")
	fmt.Fprintf(b, "    principals {\n")
	fmt.Fprintf(b, "      type        = \"Service\"\n")
	fmt.Fprintf(b, "      identifiers = [\"states.amazonaws.com\"]\n")
	fmt.Fprintf(b, "    }\n")
	fmt.Fprintf(b, "  }\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_iam_role\" \"sfn_exec\" {\n")
	fmt.Fprintf(b, "  name               = \"clavesa-${%s}-orchestration\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json\n")
	fmt.Fprintf(b, "  tags               = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_iam_role_policy\" \"sfn_exec_policy\" {\n")
	fmt.Fprintf(b, "  name = \"clavesa-${%s}-orchestration\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  role = aws_iam_role.sfn_exec.id\n\n")
	fmt.Fprintf(b, "  policy = jsonencode({\n")
	fmt.Fprintf(b, "    Version = \"2012-10-17\"\n")
	fmt.Fprintf(b, "    Statement = [\n")
	fmt.Fprintf(b, "      { Sid = \"LambdaInvoke\", Effect = \"Allow\", Action = [\"lambda:InvokeFunction\"], Resource = \"*\" },\n")
	fmt.Fprintf(b, "      { Sid = \"CloudWatchLogsDelivery\", Effect = \"Allow\", Action = [\n")
	fmt.Fprintf(b, "          \"logs:CreateLogDelivery\", \"logs:GetLogDelivery\", \"logs:UpdateLogDelivery\",\n")
	fmt.Fprintf(b, "          \"logs:DeleteLogDelivery\", \"logs:ListLogDeliveries\", \"logs:PutResourcePolicy\",\n")
	fmt.Fprintf(b, "          \"logs:DescribeResourcePolicies\", \"logs:DescribeLogGroups\",\n")
	fmt.Fprintf(b, "      ], Resource = \"*\" },\n")
	fmt.Fprintf(b, "    ]\n")
	fmt.Fprintf(b, "  })\n")
	fmt.Fprintf(b, "}\n\n")
}

// ---------------------------------------------------------------------------
// Step Functions state machine — the actual ASL definition
// ---------------------------------------------------------------------------

func emitStateMachine(b *strings.Builder, p Pipeline) {
	fmt.Fprintf(b, "# Step Functions state machine — pipeline DAG executor.\n")
	fmt.Fprintf(b, "# ASL definition built by internal/orchestration/aslgen + tfgen; HCL\n")
	fmt.Fprintf(b, "# expressions like `module.<node>.lambda_function_arn` resolve at plan\n")
	fmt.Fprintf(b, "# time so Terraform's dependency graph still tracks each transform.\n")
	fmt.Fprintf(b, "resource \"aws_sfn_state_machine\" \"pipeline\" {\n")
	fmt.Fprintf(b, "  name     = \"clavesa-${%s}\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  role_arn = aws_iam_role.sfn_exec.arn\n")
	fmt.Fprintf(b, "  type     = \"STANDARD\"\n\n")
	fmt.Fprintf(b, "  definition = jsonencode({\n")
	fmt.Fprintf(b, "    Comment = \"Clavesa pipeline: ${%s}\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "    StartAt = %q\n", p.StateMachine.StartAt)
	fmt.Fprintf(b, "    States = {\n")
	emitStates(b, p.StateMachine.States, p.NodeMeta, "      ")
	// Plus the terminal Fail state every pipeline shares.
	fmt.Fprintf(b, "      PipelineFailed = {\n")
	fmt.Fprintf(b, "        Type  = \"Fail\"\n")
	fmt.Fprintf(b, "        Error = \"PipelineFailed\"\n")
	fmt.Fprintf(b, "        Cause = \"A pipeline node failed. Check CloudWatch Logs for execution details.\"\n")
	fmt.Fprintf(b, "      }\n")
	fmt.Fprintf(b, "    }\n")
	fmt.Fprintf(b, "  })\n\n")
	fmt.Fprintf(b, "  logging_configuration {\n")
	fmt.Fprintf(b, "    log_destination        = \"${aws_cloudwatch_log_group.sfn_logs.arn}:*\"\n")
	fmt.Fprintf(b, "    include_execution_data = true\n")
	fmt.Fprintf(b, "    level                  = \"ERROR\"\n")
	fmt.Fprintf(b, "  }\n\n")
	fmt.Fprintf(b, "  tags       = local.clavesa_tags\n")
	fmt.Fprintf(b, "  depends_on = [aws_iam_role_policy.sfn_exec_policy]\n")
	fmt.Fprintf(b, "}\n\n")
}

// emitStates recursively writes the `key = { … }` entries for an ASL
// States map. Each Task carries the standard Retry / Catch policies; each
// Parallel inlines its branches with the same recursion. The indent
// argument is the current map-key column for clean output.
func emitStates(b *strings.Builder, states []aslgen.State, meta map[string]NodeMeta, indent string) {
	// Sort states alphabetically so emit is byte-stable across runs.
	sorted := make([]aslgen.State, len(states))
	copy(sorted, states)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	for _, s := range sorted {
		switch s.Type {
		case aslgen.Task:
			emitTask(b, s, meta[s.Name], indent)
		case aslgen.Parallel:
			emitParallel(b, s, meta, indent)
		}
	}
}

func emitTask(b *strings.Builder, s aslgen.State, m NodeMeta, indent string) {
	fmt.Fprintf(b, "%s%s = {\n", indent, s.Name)
	fmt.Fprintf(b, "%s  Type           = \"Task\"\n", indent)
	fmt.Fprintf(b, "%s  Resource       = \"arn:aws:states:::lambda:invoke\"\n", indent)
	fmt.Fprintf(b, "%s  TimeoutSeconds = %d\n", indent, m.TimeoutSeconds)
	fmt.Fprintf(b, "%s  Parameters = {\n", indent)
	fmt.Fprintf(b, "%s    FunctionName = %s\n", indent, m.LambdaARNExpr)
	fmt.Fprintf(b, "%s    Payload = {\n", indent)
	// inputs / outputs are pre-formatted HCL map literals.
	fmt.Fprintf(b, "%s      inputs  = %s\n", indent, m.InputsExpr)
	fmt.Fprintf(b, "%s      outputs = %s\n", indent, m.OutputsExpr)
	// Three SFN context-object fields the runner uses to attribute
	// node_runs rows to the parent execution.
	fmt.Fprintf(b, "%s      \"_sf_execution_arn.$\"        = \"$$.Execution.Id\"\n", indent)
	fmt.Fprintf(b, "%s      \"_sf_execution_started_at.$\" = \"$$.Execution.StartTime\"\n", indent)
	fmt.Fprintf(b, "%s      \"_execution_input.$\"         = \"$$.Execution.Input\"\n", indent)
	fmt.Fprintf(b, "%s    }\n", indent)
	fmt.Fprintf(b, "%s  }\n", indent)
	fmt.Fprintf(b, "%s  ResultPath = \"$.runner_results.%s\"\n", indent, s.Name)
	emitRetryCatch(b, indent+"  ")
	if s.End {
		fmt.Fprintf(b, "%s  End = true\n", indent)
	} else {
		fmt.Fprintf(b, "%s  Next = %q\n", indent, s.Next)
	}
	fmt.Fprintf(b, "%s}\n", indent)
}

func emitParallel(b *strings.Builder, s aslgen.State, meta map[string]NodeMeta, indent string) {
	fmt.Fprintf(b, "%s%s = {\n", indent, s.Name)
	fmt.Fprintf(b, "%s  Type = \"Parallel\"\n", indent)
	fmt.Fprintf(b, "%s  Branches = [\n", indent)
	for _, br := range s.Branches {
		fmt.Fprintf(b, "%s    {\n", indent)
		fmt.Fprintf(b, "%s      StartAt = %q\n", indent, br.StartAt)
		fmt.Fprintf(b, "%s      States = {\n", indent)
		emitStates(b, br.States, meta, indent+"        ")
		fmt.Fprintf(b, "%s      }\n", indent)
		fmt.Fprintf(b, "%s    },\n", indent)
	}
	fmt.Fprintf(b, "%s  ]\n", indent)
	emitRetryCatch(b, indent+"  ")
	if s.End {
		fmt.Fprintf(b, "%s  End = true\n", indent)
	} else {
		fmt.Fprintf(b, "%s  Next = %q\n", indent, s.Next)
	}
	fmt.Fprintf(b, "%s}\n", indent)
}

func emitRetryCatch(b *strings.Builder, indent string) {
	fmt.Fprintf(b, "%sRetry = [{ ErrorEquals = [\"States.TaskFailed\"], IntervalSeconds = 5, MaxAttempts = 3, BackoffRate = 2.0, MaxDelaySeconds = 60 }]\n", indent)
	fmt.Fprintf(b, "%sCatch = [{ ErrorEquals = [\"States.ALL\"], Next = \"PipelineFailed\" }]\n", indent)
}

// ---------------------------------------------------------------------------
// Schedule trigger (optional — gated on the schedule expression at plan time)
// ---------------------------------------------------------------------------

func emitSchedule(b *strings.Builder, p Pipeline) {
	if p.ScheduleExpr == "" {
		return
	}
	fmt.Fprintf(b, "# Schedule trigger — EventBridge rule + IAM role to start the state\n")
	fmt.Fprintf(b, "# machine on a cron / rate cadence. Created only when var.trigger_schedule\n")
	fmt.Fprintf(b, "# is non-null (count pattern).\n")
	fmt.Fprintf(b, "resource \"aws_cloudwatch_event_rule\" \"schedule\" {\n")
	fmt.Fprintf(b, "  count = %s != null ? 1 : 0\n\n", p.ScheduleExpr)
	fmt.Fprintf(b, "  name                = \"clavesa-${%s}-schedule\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  description         = \"Scheduled trigger for Clavesa pipeline: ${%s}\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  schedule_expression = %s\n", p.ScheduleExpr)
	fmt.Fprintf(b, "  tags                = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "data \"aws_iam_policy_document\" \"events_assume\" {\n")
	fmt.Fprintf(b, "  count = %s != null ? 1 : 0\n\n", p.ScheduleExpr)
	fmt.Fprintf(b, "  statement {\n")
	fmt.Fprintf(b, "    sid     = \"EventBridgeTrust\"\n")
	fmt.Fprintf(b, "    actions = [\"sts:AssumeRole\"]\n")
	fmt.Fprintf(b, "    principals {\n")
	fmt.Fprintf(b, "      type        = \"Service\"\n")
	fmt.Fprintf(b, "      identifiers = [\"events.amazonaws.com\"]\n")
	fmt.Fprintf(b, "    }\n")
	fmt.Fprintf(b, "  }\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_iam_role\" \"events_trigger\" {\n")
	fmt.Fprintf(b, "  count = %s != null ? 1 : 0\n\n", p.ScheduleExpr)
	fmt.Fprintf(b, "  name               = \"clavesa-${%s}-trigger\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  assume_role_policy = data.aws_iam_policy_document.events_assume[0].json\n")
	fmt.Fprintf(b, "  tags               = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_iam_role_policy\" \"events_trigger_policy\" {\n")
	fmt.Fprintf(b, "  count = %s != null ? 1 : 0\n\n", p.ScheduleExpr)
	fmt.Fprintf(b, "  name = \"clavesa-${%s}-trigger\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  role = aws_iam_role.events_trigger[0].id\n\n")
	fmt.Fprintf(b, "  policy = jsonencode({\n")
	fmt.Fprintf(b, "    Version   = \"2012-10-17\"\n")
	fmt.Fprintf(b, "    Statement = [{ Sid = \"StartExecution\", Effect = \"Allow\", Action = [\"states:StartExecution\"], Resource = [aws_sfn_state_machine.pipeline.arn] }]\n")
	fmt.Fprintf(b, "  })\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_cloudwatch_event_target\" \"schedule\" {\n")
	fmt.Fprintf(b, "  count = %s != null ? 1 : 0\n\n", p.ScheduleExpr)
	fmt.Fprintf(b, "  rule     = aws_cloudwatch_event_rule.schedule[0].name\n")
	fmt.Fprintf(b, "  arn      = aws_sfn_state_machine.pipeline.arn\n")
	fmt.Fprintf(b, "  role_arn = aws_iam_role.events_trigger[0].arn\n\n")
	fmt.Fprintf(b, "  # _trigger gets read by runs_writer (see runs_writer/index.py:TRIGGER_VALUES)\n")
	fmt.Fprintf(b, "  # to label the row in `<system_catalog>__pipelines.runs.trigger`.\n")
	fmt.Fprintf(b, "  input = jsonencode({\n")
	fmt.Fprintf(b, "    pipeline = %s\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "    _trigger = \"scheduled\"\n")
	fmt.Fprintf(b, "  })\n")
	fmt.Fprintf(b, "}\n\n")
}

// ---------------------------------------------------------------------------
// SQS poller (optional — gated on TriggerQueueExprs + BatchWindowExpr)
// ---------------------------------------------------------------------------

func emitPoller(b *strings.Builder, p Pipeline) {
	if len(p.TriggerQueueExprs) == 0 || p.BatchWindowExpr == "" {
		return
	}
	queueListExpr := "[" + strings.Join(p.TriggerQueueExprs, ", ") + "]"

	fmt.Fprintf(b, "# SQS poller — Lambda that checks source queues on a schedule and starts\n")
	fmt.Fprintf(b, "# the state machine when any queue has messages.\n")
	fmt.Fprintf(b, "locals {\n")
	fmt.Fprintf(b, "  _poller_queue_arns = %s\n", queueListExpr)
	fmt.Fprintf(b, "  _poller_enabled    = length(local._poller_queue_arns) > 0 && %s != null\n", p.BatchWindowExpr)
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "data \"archive_file\" \"poller\" {\n")
	fmt.Fprintf(b, "  count       = local._poller_enabled ? 1 : 0\n")
	fmt.Fprintf(b, "  type        = \"zip\"\n")
	fmt.Fprintf(b, "  source_file = \"${path.module}/%s/poller.py\"\n", sidecarDirName)
	fmt.Fprintf(b, "  output_path = \"${path.module}/%s/poller.zip\"\n", sidecarDirName)
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "data \"aws_iam_policy_document\" \"poller_assume\" {\n")
	fmt.Fprintf(b, "  count = local._poller_enabled ? 1 : 0\n")
	fmt.Fprintf(b, "  statement {\n")
	fmt.Fprintf(b, "    actions = [\"sts:AssumeRole\"]\n")
	fmt.Fprintf(b, "    principals {\n")
	fmt.Fprintf(b, "      type        = \"Service\"\n")
	fmt.Fprintf(b, "      identifiers = [\"lambda.amazonaws.com\"]\n")
	fmt.Fprintf(b, "    }\n")
	fmt.Fprintf(b, "  }\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_iam_role\" \"poller\" {\n")
	fmt.Fprintf(b, "  count              = local._poller_enabled ? 1 : 0\n")
	fmt.Fprintf(b, "  name               = \"clavesa-${%s}-poller\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  assume_role_policy = data.aws_iam_policy_document.poller_assume[0].json\n")
	fmt.Fprintf(b, "  tags               = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_iam_role_policy\" \"poller\" {\n")
	fmt.Fprintf(b, "  count = local._poller_enabled ? 1 : 0\n")
	fmt.Fprintf(b, "  name  = \"clavesa-${%s}-poller\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  role  = aws_iam_role.poller[0].id\n\n")
	fmt.Fprintf(b, "  policy = jsonencode({\n")
	fmt.Fprintf(b, "    Version = \"2012-10-17\"\n")
	fmt.Fprintf(b, "    Statement = [\n")
	fmt.Fprintf(b, "      { Sid = \"SQSPoll\",  Effect = \"Allow\", Action = [\"sqs:ReceiveMessage\", \"sqs:PurgeQueue\"], Resource = local._poller_queue_arns },\n")
	fmt.Fprintf(b, "      { Sid = \"SFNStart\", Effect = \"Allow\", Action = [\"states:StartExecution\"], Resource = [aws_sfn_state_machine.pipeline.arn] },\n")
	fmt.Fprintf(b, "      { Sid = \"Logs\",     Effect = \"Allow\", Action = [\"logs:CreateLogGroup\", \"logs:CreateLogStream\", \"logs:PutLogEvents\"], Resource = [\"arn:aws:logs:*:*:*\"] },\n")
	fmt.Fprintf(b, "    ]\n")
	fmt.Fprintf(b, "  })\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_lambda_function\" \"poller\" {\n")
	fmt.Fprintf(b, "  count            = local._poller_enabled ? 1 : 0\n")
	fmt.Fprintf(b, "  function_name    = \"clavesa-${%s}-poller\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  role             = aws_iam_role.poller[0].arn\n")
	fmt.Fprintf(b, "  runtime          = \"python3.12\"\n")
	fmt.Fprintf(b, "  handler          = \"poller.handler\"\n")
	fmt.Fprintf(b, "  filename         = data.archive_file.poller[0].output_path\n")
	fmt.Fprintf(b, "  source_code_hash = data.archive_file.poller[0].output_base64sha256\n")
	fmt.Fprintf(b, "  timeout          = 30\n\n")
	fmt.Fprintf(b, "  environment {\n")
	fmt.Fprintf(b, "    variables = {\n")
	fmt.Fprintf(b, "      QUEUE_ARNS        = jsonencode(local._poller_queue_arns)\n")
	fmt.Fprintf(b, "      STATE_MACHINE_ARN = aws_sfn_state_machine.pipeline.arn\n")
	fmt.Fprintf(b, "    }\n")
	fmt.Fprintf(b, "  }\n\n")
	fmt.Fprintf(b, "  tags = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_cloudwatch_event_rule\" \"poller\" {\n")
	fmt.Fprintf(b, "  count               = local._poller_enabled ? 1 : 0\n")
	fmt.Fprintf(b, "  name                = \"clavesa-${%s}-poller\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  description         = \"Polls source queues for ${%s} at ${%s}\"\n", p.PipelineNameExpr, p.BatchWindowExpr)
	fmt.Fprintf(b, "  schedule_expression = %s\n", p.BatchWindowExpr)
	fmt.Fprintf(b, "  tags                = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_cloudwatch_event_target\" \"poller\" {\n")
	fmt.Fprintf(b, "  count = local._poller_enabled ? 1 : 0\n")
	fmt.Fprintf(b, "  rule  = aws_cloudwatch_event_rule.poller[0].name\n")
	fmt.Fprintf(b, "  arn   = aws_lambda_function.poller[0].arn\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_lambda_permission\" \"poller\" {\n")
	fmt.Fprintf(b, "  count         = local._poller_enabled ? 1 : 0\n")
	fmt.Fprintf(b, "  statement_id  = \"AllowEventBridgeInvoke\"\n")
	fmt.Fprintf(b, "  action        = \"lambda:InvokeFunction\"\n")
	fmt.Fprintf(b, "  function_name = aws_lambda_function.poller[0].function_name\n")
	fmt.Fprintf(b, "  principal     = \"events.amazonaws.com\"\n")
	fmt.Fprintf(b, "  source_arn    = aws_cloudwatch_event_rule.poller[0].arn\n")
	fmt.Fprintf(b, "}\n\n")
}

// ---------------------------------------------------------------------------
// runs_writer (always emitted — every pipeline has a bucket today)
// ---------------------------------------------------------------------------

func emitRunsWriter(b *strings.Builder, p Pipeline) {
	fmt.Fprintf(b, "# runs_writer — tiny Python Lambda that appends one row per terminal SFN\n")
	fmt.Fprintf(b, "# execution to <system_catalog>__pipelines.runs (Iceberg via Athena INSERT).\n")
	fmt.Fprintf(b, "# Pairs with the runner-populated node_runs table; joining on\n")
	fmt.Fprintf(b, "# sf_execution_arn answers \"which nodes ran in this execution?\".\n")
	fmt.Fprintf(b, "data \"archive_file\" \"runs_writer\" {\n")
	fmt.Fprintf(b, "  type        = \"zip\"\n")
	fmt.Fprintf(b, "  source_dir  = \"${path.module}/%s/runs_writer\"\n", sidecarDirName)
	fmt.Fprintf(b, "  output_path = \"${path.module}/%s/runs_writer.zip\"\n", sidecarDirName)
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "data \"aws_iam_policy_document\" \"runs_writer_assume\" {\n")
	fmt.Fprintf(b, "  statement {\n")
	fmt.Fprintf(b, "    actions = [\"sts:AssumeRole\"]\n")
	fmt.Fprintf(b, "    principals {\n")
	fmt.Fprintf(b, "      type        = \"Service\"\n")
	fmt.Fprintf(b, "      identifiers = [\"lambda.amazonaws.com\"]\n")
	fmt.Fprintf(b, "    }\n")
	fmt.Fprintf(b, "  }\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_iam_role\" \"runs_writer\" {\n")
	fmt.Fprintf(b, "  name               = \"clavesa-${%s}-runs-writer\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  assume_role_policy = data.aws_iam_policy_document.runs_writer_assume.json\n")
	fmt.Fprintf(b, "  tags               = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")

	// IAM scoped to the workspace system catalog DB (ADR-016).
	sysCatalogSafe := safeCatalogLiteral(p.SystemCatalog)
	fmt.Fprintf(b, "resource \"aws_iam_role_policy\" \"runs_writer\" {\n")
	fmt.Fprintf(b, "  name = \"clavesa-${%s}-runs-writer\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  role = aws_iam_role.runs_writer.id\n\n")
	fmt.Fprintf(b, "  policy = jsonencode({\n")
	fmt.Fprintf(b, "    Version = \"2012-10-17\"\n")
	fmt.Fprintf(b, "    Statement = [\n")
	fmt.Fprintf(b, "      { Sid = \"AthenaQuery\", Effect = \"Allow\", Action = [\"athena:StartQueryExecution\", \"athena:GetQueryExecution\", \"athena:GetQueryResults\", \"athena:GetWorkGroup\"], Resource = \"*\" },\n")
	fmt.Fprintf(b, "      { Sid = \"GlueCatalog\", Effect = \"Allow\", Action = [\"glue:GetDatabase\", \"glue:GetTable\", \"glue:GetTables\", \"glue:CreateTable\", \"glue:UpdateTable\", \"glue:GetPartition\", \"glue:GetPartitions\"], Resource = [\n")
	fmt.Fprintf(b, "          \"arn:aws:glue:*:*:catalog\",\n")
	fmt.Fprintf(b, "          \"arn:aws:glue:*:*:database/%s__pipelines\",\n", sysCatalogSafe)
	fmt.Fprintf(b, "          \"arn:aws:glue:*:*:table/%s__pipelines/*\",\n", sysCatalogSafe)
	fmt.Fprintf(b, "      ]},\n")
	fmt.Fprintf(b, "      { Sid = \"S3Warehouse\", Effect = \"Allow\", Action = [\"s3:GetObject\", \"s3:PutObject\", \"s3:DeleteObject\", \"s3:ListBucket\", \"s3:GetBucketLocation\"], Resource = [\n")
	fmt.Fprintf(b, "          \"arn:aws:s3:::${%s}\",\n", p.BucketExpr)
	fmt.Fprintf(b, "          \"arn:aws:s3:::${%s}/*\",\n", p.BucketExpr)
	fmt.Fprintf(b, "      ]},\n")
	fmt.Fprintf(b, "      { Sid = \"Logs\", Effect = \"Allow\", Action = [\"logs:CreateLogGroup\", \"logs:CreateLogStream\", \"logs:PutLogEvents\"], Resource = [\"arn:aws:logs:*:*:*\"] },\n")
	fmt.Fprintf(b, "    ]\n")
	fmt.Fprintf(b, "  })\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_lambda_function\" \"runs_writer\" {\n")
	fmt.Fprintf(b, "  function_name    = \"clavesa-${%s}-runs-writer\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  role             = aws_iam_role.runs_writer.arn\n")
	fmt.Fprintf(b, "  runtime          = \"python3.12\"\n")
	fmt.Fprintf(b, "  handler          = \"index.handler\"\n")
	fmt.Fprintf(b, "  filename         = data.archive_file.runs_writer.output_path\n")
	fmt.Fprintf(b, "  source_code_hash = data.archive_file.runs_writer.output_base64sha256\n")
	fmt.Fprintf(b, "  timeout          = 60\n\n")
	fmt.Fprintf(b, "  environment {\n")
	fmt.Fprintf(b, "    variables = {\n")
	fmt.Fprintf(b, "      CLAVESA_PIPELINE         = %s\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "      CLAVESA_DATABASE         = \"%s__pipelines\"\n", sysCatalogSafe)
	fmt.Fprintf(b, "      CLAVESA_WAREHOUSE_BUCKET = %s\n", p.BucketExpr)
	fmt.Fprintf(b, "    }\n")
	fmt.Fprintf(b, "  }\n\n")
	fmt.Fprintf(b, "  tags = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_cloudwatch_event_rule\" \"runs\" {\n")
	fmt.Fprintf(b, "  name        = \"clavesa-${%s}-runs\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  description = \"Captures terminal Step Functions execution events for ${%s} into the runs Iceberg table\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  event_pattern = jsonencode({\n")
	fmt.Fprintf(b, "    source        = [\"aws.states\"]\n")
	fmt.Fprintf(b, "    \"detail-type\" = [\"Step Functions Execution Status Change\"]\n")
	fmt.Fprintf(b, "    detail = {\n")
	fmt.Fprintf(b, "      stateMachineArn = [aws_sfn_state_machine.pipeline.arn]\n")
	fmt.Fprintf(b, "      status          = [\"SUCCEEDED\", \"FAILED\", \"TIMED_OUT\", \"ABORTED\"]\n")
	fmt.Fprintf(b, "    }\n")
	fmt.Fprintf(b, "  })\n")
	fmt.Fprintf(b, "  tags = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_cloudwatch_event_target\" \"runs\" {\n")
	fmt.Fprintf(b, "  rule = aws_cloudwatch_event_rule.runs.name\n")
	fmt.Fprintf(b, "  arn  = aws_lambda_function.runs_writer.arn\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_lambda_permission\" \"runs\" {\n")
	fmt.Fprintf(b, "  statement_id  = \"AllowEventBridgeInvoke\"\n")
	fmt.Fprintf(b, "  action        = \"lambda:InvokeFunction\"\n")
	fmt.Fprintf(b, "  function_name = aws_lambda_function.runs_writer.function_name\n")
	fmt.Fprintf(b, "  principal     = \"events.amazonaws.com\"\n")
	fmt.Fprintf(b, "  source_arn    = aws_cloudwatch_event_rule.runs.arn\n")
	fmt.Fprintf(b, "}\n\n")
}

// sidecarDirName is the directory inside the pipeline dir where tfgen
// expects the runs_writer/ tree and poller.py to be materialised by the
// caller (the service layer copies from embed.FS at SyncOrchestration time).
// Underscore-prefixed so it sorts before transforms in directory listings
// and reads as "managed sidecar, don't edit".
const sidecarDirName = "_clavesa_sidecar"

// SidecarDirName exposes the constant for the service layer that has to
// materialise the directory contents.
func SidecarDirName() string { return sidecarDirName }
