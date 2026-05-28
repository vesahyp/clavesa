package tfgen

import (
	"strings"
	"testing"
)

// twoTransforms returns a Pipeline literal with two transforms wired
// bronze → silver. Used as a base across most tests; individual tests
// mutate the fields they care about.
func twoTransforms() Pipeline {
	return Pipeline{
		PipelineNameExpr: "var.pipeline_name",
		Catalog:          "clavesa_x",
		SchemaExpr:       "var.schema",
		SystemCatalog:    "clavesa_x_system",
		BucketExpr:       "data.terraform_remote_state.workspace.outputs.pipeline_bucket",
		RunnerImageExpr:  "data.terraform_remote_state.workspace.outputs.runner_image",
		Transforms: []TransformConfig{
			{
				NodeID:      "bronze",
				Language:    "sql",
				LogicS3URI:  `"s3://${data.terraform_remote_state.workspace.outputs.pipeline_bucket}/${var.pipeline_name}/bronze/_runtime/logic.sql"`,
				InputsExpr:  "{}",
				OutputsExpr: `{ default = "" }`,
				Parents:     nil,
			},
			{
				NodeID:      "silver",
				Language:    "python",
				LogicS3URI:  `"s3://${data.terraform_remote_state.workspace.outputs.pipeline_bucket}/${var.pipeline_name}/silver/_runtime/logic.py"`,
				InputsExpr:  `{ bronze = "clavesa_x__default.bronze__default" }`,
				OutputsExpr: `{ default = "" }`,
				Parents:     []string{"bronze"},
			},
		},
	}
}

// TestEmit_PipelineLambda asserts the per-pipeline runner Lambda emits
// with the expected handler, env vars, IAM role + policy, and ECR data
// source. The function itself is what every other tfgen output now
// references (SFN Task FunctionName, IAM LambdaInvoke resource).
func TestEmit_PipelineLambda(t *testing.T) {
	t.Parallel()
	out, err := Emit(twoTransforms())
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}

	wants := []string{
		// Lambda resource + image_config command.
		`resource "aws_lambda_function" "pipeline_runner"`,
		`command = ["runner.pipeline_handler"]`,
		`package_type  = "Image"`,
		`timeout       = 900`,
		`memory_size   = 10240`,

		// Env vars — workspace-level invariants. pipeline_handler sets
		// per-transform CLAVESA_NODE / CLAVESA_LANGUAGE / CLAVESA_LOGIC_S3_PATH
		// from the event payload.
		`CLAVESA_PIPELINE            = var.pipeline_name`,
		`CLAVESA_CATALOG             = "clavesa_x"`,
		`CLAVESA_SCHEMA              = var.schema`,
		`CLAVESA_SYSTEM_CATALOG      = "clavesa_x_system"`,
		`CLAVESA_WAREHOUSE           = "s3://${data.terraform_remote_state.workspace.outputs.pipeline_bucket}/${var.pipeline_name}/_warehouse/"`,
		`CLAVESA_SYSTEM_WAREHOUSE    = "s3://${data.terraform_remote_state.workspace.outputs.pipeline_bucket}/_system/pipelines/"`,
		`CLAVESA_WATERMARKS          = "s3://${data.terraform_remote_state.workspace.outputs.pipeline_bucket}/${var.pipeline_name}/_watermarks/"`,
		`CLAVESA_RUNNER_IMAGE_DIGEST = data.aws_ecr_image.pipeline_runner.image_digest`,
		`CLAVESA_MODULE_VERSION      = "v2.2.0"`,

		// IAM role + assume policy + at least the write statement
		// targeting the warehouse prefix.
		`resource "aws_iam_role" "pipeline_runner"`,
		`resource "aws_iam_role_policy" "pipeline_runner"`,
		`data "aws_iam_policy_document" "pipeline_runner_assume"`,
		`identifiers = ["lambda.amazonaws.com"]`,
		`"s3:PutObject"`,
		`"arn:aws:s3:::${data.terraform_remote_state.workspace.outputs.pipeline_bucket}/${var.pipeline_name}/_warehouse/*"`,
		`"arn:aws:s3:::${data.terraform_remote_state.workspace.outputs.pipeline_bucket}/${var.pipeline_name}/_watermarks/*"`,
		`"arn:aws:s3:::${data.terraform_remote_state.workspace.outputs.pipeline_bucket}/_system/pipelines/*"`,
		// Glue read includes the workspace catalog wildcard + default DB +
		// system catalog wildcard.
		`"arn:aws:glue:*:*:database/default"`,
		`"arn:aws:glue:*:*:database/clavesa_x__*"`,
		`"arn:aws:glue:*:*:database/clavesa_x_system__*"`,
		// Glue write scoped to this pipeline's DB + system pipelines DB.
		`"arn:aws:glue:*:*:database/clavesa_x__${replace(var.schema, "-", "_")}"`,
		`"arn:aws:glue:*:*:database/clavesa_x_system__pipelines"`,
		// First-run CreateDatabase.
		`Action = ["glue:CreateDatabase"]`,

		// ECR data source — image digest pin.
		`data "aws_ecr_image" "pipeline_runner"`,
		`repository_name = local.pipeline_runner_repo_name`,
	}
	for _, w := range wants {
		if !strings.Contains(out, w) {
			t.Errorf("Emit output missing %q\n--- output ---\n%s", w, out)
		}
	}
}

// TestEmit_SingleTaskStateMachine asserts the v2.2.0 ASL collapse: one
// Task ("RunPipeline") that hands the full transform list to the per-
// pipeline Lambda. No PipelineFailed, no per-state Retry/Catch.
func TestEmit_SingleTaskStateMachine(t *testing.T) {
	t.Parallel()
	out, err := Emit(twoTransforms())
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}

	// Slice out the SFN definition block — IAM jsonencode policies use
	// `Catch` for other reasons and we don't want false positives.
	defStart := strings.Index(out, "definition = jsonencode({")
	if defStart == -1 {
		t.Fatal("definition block not found")
	}
	defEnd := strings.Index(out[defStart:], "  })\n\n  logging_configuration")
	if defEnd == -1 {
		t.Fatal("definition block end not found")
	}
	asl := out[defStart : defStart+defEnd]

	wants := []string{
		`StartAt = "RunPipeline"`,
		`RunPipeline = {`,
		`Type           = "Task"`,
		`Resource       = "arn:aws:states:::lambda:invoke"`,
		`FunctionName = aws_lambda_function.pipeline_runner.arn`,
		`_pipeline_run = true`,
		`pipeline      = var.pipeline_name`,
		`End = true`,
		// Transforms render in input order: bronze first, silver second.
		`node       = "bronze"`,
		`language   = "sql"`,
		`logic_path = "s3://${data.terraform_remote_state.workspace.outputs.pipeline_bucket}/${var.pipeline_name}/bronze/_runtime/logic.sql"`,
		`node       = "silver"`,
		`language   = "python"`,
		`parents    = ["bronze"]`,
		// SFN context-object substitutions — pipeline_handler propagates these
		// onto each per-node runs row.
		`"_sf_execution_arn.$"        = "$$.Execution.Id"`,
		`"_sf_execution_started_at.$" = "$$.Execution.StartTime"`,
		`"_execution_input.$"         = "$$.Execution.Input"`,
	}
	for _, w := range wants {
		if !strings.Contains(asl, w) {
			t.Errorf("ASL missing %q\n--- ASL ---\n%s", w, asl)
		}
	}

	// Input order matters — bronze's block must come before silver's.
	bronzeIdx := strings.Index(asl, `node       = "bronze"`)
	silverIdx := strings.Index(asl, `node       = "silver"`)
	if bronzeIdx == -1 || silverIdx == -1 || bronzeIdx >= silverIdx {
		t.Errorf("transforms not rendered in input order (bronze=%d, silver=%d)", bronzeIdx, silverIdx)
	}

	// Parents for the leaf node must render as the empty list, not as
	// missing or null — pipeline_handler relies on the key being present.
	if !strings.Contains(asl, `parents    = []`) {
		t.Errorf("bronze parents not rendered as []\n--- ASL ---\n%s", asl)
	}

	// Multi-state remnants must be gone.
	doesNotContain := []string{
		`PipelineFailed`,
		`Retry = [{ ErrorEquals = ["States.TaskFailed"]`,
		`Catch = [{ ErrorEquals = ["States.ALL"]`,
		`b_Branches`,
		`Parallel`,
	}
	for _, w := range doesNotContain {
		if strings.Contains(asl, w) {
			t.Errorf("ASL unexpectedly contains %q\n--- ASL ---\n%s", w, asl)
		}
	}

	// SFN IAM role policy must scope LambdaInvoke to the pipeline_runner
	// ARN (not "*", as the multi-state version used).
	if !strings.Contains(out, `Action = ["lambda:InvokeFunction"], Resource = [aws_lambda_function.pipeline_runner.arn]`) {
		t.Error("SFN role policy does not scope lambda:InvokeFunction to pipeline_runner.arn")
	}
}

// TestEmit_BaseResources asserts the always-on resources around the
// state machine — the locals tags block, Glue DB, log group, SFN exec
// role, and runs_writer — all still emit unchanged by the v2.2.0
// collapse.
func TestEmit_BaseResources(t *testing.T) {
	t.Parallel()
	out, err := Emit(twoTransforms())
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	wants := []string{
		`local.clavesa_tags`,
		`resource "aws_glue_catalog_database" "pipeline"`,
		`name         = "clavesa_x__${replace(var.schema, "-", "_")}"`,
		`resource "aws_cloudwatch_log_group" "sfn_logs"`,
		`resource "aws_iam_role" "sfn_exec"`,
		`resource "aws_iam_role_policy" "sfn_exec_policy"`,
		`resource "aws_sfn_state_machine" "pipeline"`,
		// runs_writer (always emitted) — ADR-018 image-based Lambda.
		`resource "aws_lambda_function" "runs_writer"`,
		`command = ["runner.runs_writer_handler"]`,
		`CLAVESA_SYSTEM_CATALOG   = "clavesa_x_system"`,
		`data "aws_ecr_image" "runs_writer"`,
	}
	for _, w := range wants {
		if !strings.Contains(out, w) {
			t.Errorf("Emit output missing %q", w)
		}
	}

	doesNotContain := []string{
		// Poller resources should not appear when TriggerQueueExprs is empty.
		`resource "aws_lambda_function" "poller"`,
		// Old module wrapper must not leak in.
		`module "orchestration"`,
	}
	for _, w := range doesNotContain {
		if strings.Contains(out, w) {
			t.Errorf("Emit output unexpectedly contains %q", w)
		}
	}
}

// TestEmit_PollerStillWorks asserts the SQS poller wiring is orthogonal
// to the ASL collapse — it still emits when TriggerQueueExprs is non-
// empty, and still targets aws_sfn_state_machine.pipeline.arn.
func TestEmit_PollerStillWorks(t *testing.T) {
	t.Parallel()
	p := twoTransforms()
	p.BatchWindowExpr = "var.trigger_batch_window"
	p.TriggerQueueExprs = []string{"module.src_x.trigger_queue_arn", "module.src_y.trigger_queue_arn"}
	out, err := Emit(p)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if !strings.Contains(out, `resource "aws_lambda_function" "poller"`) {
		t.Error("poller resource missing when TriggerQueueExprs is non-empty")
	}
	if !strings.Contains(out, `module.src_x.trigger_queue_arn, module.src_y.trigger_queue_arn`) {
		t.Error("queue ARN list not assembled correctly")
	}
	if !strings.Contains(out, `Resource = [aws_sfn_state_machine.pipeline.arn]`) {
		t.Error("poller's SFNStart resource does not target the pipeline state machine")
	}
}

// TestEmit_UpstreamTriggers asserts that when UpstreamPipelines is
// populated, the emitter produces one EventBridge rule + role + role
// policy + target per producer, all with consistent naming and the
// correct cross-account-style ARN literal pointing at the producer's
// state machine.
func TestEmit_UpstreamTriggers(t *testing.T) {
	t.Parallel()
	p := twoTransforms()
	p.UpstreamPipelines = []string{"bronze", "silver-team"}
	out, err := Emit(p)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}

	wants := []string{
		// Shared data sources for account + region (one pair, not per producer).
		`data "aws_caller_identity" "clavesa_upstream"`,
		`data "aws_region" "clavesa_upstream"`,
		// bronze producer: rule + IAM role + policy + target.
		`resource "aws_cloudwatch_event_rule" "upstream_bronze"`,
		`stateMachineArn = ["arn:aws:states:${data.aws_region.clavesa_upstream.region}:${data.aws_caller_identity.clavesa_upstream.account_id}:stateMachine:clavesa-bronze"]`,
		`status          = ["SUCCEEDED"]`,
		`resource "aws_iam_role" "upstream_trigger_bronze"`,
		`resource "aws_iam_role_policy" "upstream_trigger_bronze"`,
		`Action = ["states:StartExecution"], Resource = [aws_sfn_state_machine.pipeline.arn]`,
		`resource "aws_cloudwatch_event_target" "upstream_bronze"`,
		`_trigger           = "upstream"`,
		`_upstream_pipeline = "bronze"`,
		// silver-team producer: dashes survive in the literal ARN and
		// rule name but the Terraform resource address uses underscores.
		`resource "aws_cloudwatch_event_rule" "upstream_silver_team"`,
		`stateMachine:clavesa-silver-team"]`,
		`_upstream_pipeline = "silver-team"`,
	}
	for _, w := range wants {
		if !strings.Contains(out, w) {
			t.Errorf("Emit output missing %q\n--- output ---\n%s", w, out)
		}
	}
}

// TestEmit_NoUpstreamTriggers asserts emitUpstreamTriggers is a no-op
// when UpstreamPipelines is empty — the shared data sources and per-
// producer resources must NOT appear, since their presence would
// otherwise force every pipeline (most have no cross-pipeline reads)
// to compute aws_caller_identity at plan time for nothing.
func TestEmit_NoUpstreamTriggers(t *testing.T) {
	t.Parallel()
	out, err := Emit(twoTransforms())
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	doesNotContain := []string{
		`"clavesa_upstream"`,
		`"upstream_`,
		`_upstream_pipeline`,
	}
	for _, w := range doesNotContain {
		if strings.Contains(out, w) {
			t.Errorf("Emit output unexpectedly contains %q with empty UpstreamPipelines", w)
		}
	}
}

// TestEmit_Validate asserts the new validation rules around Transforms.
func TestEmit_Validate(t *testing.T) {
	t.Parallel()

	// Empty Transforms → error.
	p := twoTransforms()
	p.Transforms = nil
	if _, err := Emit(p); err == nil {
		t.Error("expected error for empty Transforms; got nil")
	}

	// Missing NodeID.
	p = twoTransforms()
	p.Transforms[0].NodeID = ""
	if _, err := Emit(p); err == nil {
		t.Error("expected error for empty NodeID; got nil")
	}

	// Missing Language.
	p = twoTransforms()
	p.Transforms[0].Language = ""
	if _, err := Emit(p); err == nil {
		t.Error("expected error for empty Language; got nil")
	}

	// Missing LogicS3URI.
	p = twoTransforms()
	p.Transforms[0].LogicS3URI = ""
	if _, err := Emit(p); err == nil {
		t.Error("expected error for empty LogicS3URI; got nil")
	}
}
