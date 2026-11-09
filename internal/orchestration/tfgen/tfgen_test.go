package tfgen

import (
	"fmt"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/version"
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
		`memory_size   = 3008`,
		// GH #43: 10 GB ephemeral storage — Spark shuffle/spill space.
		`ephemeral_storage {`,
		`size = 10240`,

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
		fmt.Sprintf(`CLAVESA_MODULE_VERSION      = %q`, version.Module),

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
		// Live in-flight progress sink prefix (bucket root).
		`"arn:aws:s3:::${data.terraform_remote_state.workspace.outputs.pipeline_bucket}/_progress/*"`,
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

// TestEmit_PollerQueueExprs asserts the poller watches PollerQueueExprs
// when set, the SQS drain IAM keeps all TriggerQueueExprs, and an empty
// non-nil PollerQueueExprs emits no poller.
func TestEmit_PollerQueueExprs(t *testing.T) {
	t.Parallel()
	p := twoTransforms()
	p.BatchWindowExpr = "var.trigger_batch_window"
	p.TriggerQueueExprs = []string{"module.src_x.trigger_queue_arn", "module.src_y.trigger_queue_arn"}
	p.PollerQueueExprs = []string{"module.src_x.trigger_queue_arn"}
	out, err := Emit(p)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if !strings.Contains(out, "_poller_queue_arns = [module.src_x.trigger_queue_arn]\n") {
		t.Error("poller should watch only PollerQueueExprs")
	}
	if !strings.Contains(out, "module.src_y.trigger_queue_arn") {
		t.Error("SQS drain IAM lost the no-trigger queue")
	}
	p.PollerQueueExprs = []string{}
	out, err = Emit(p)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if strings.Contains(out, `resource "aws_lambda_function" "poller"`) {
		t.Error("poller emitted although PollerQueueExprs is empty")
	}
	if !strings.Contains(out, `Sid = "SQSDrain"`) {
		t.Error("SQSDrain IAM missing")
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

// TestEmit_ExternalBuckets asserts that when ExternalBuckets is non-
// empty, the pipeline Lambda IAM policy gains an `S3ReadExternal`
// Statement listing the bucket + bucket/* ARNs for each entry. Without
// this, pipelines reading from any non-workspace S3 bucket (kind=s3
// sources, cross-account inputs) 403 at runtime — v2.2.0 collapsed the
// per-transform IAM into a single per-pipeline role and dropped the
// per-transform input_buckets grant in the process.
func TestEmit_ExternalBuckets(t *testing.T) {
	t.Parallel()
	p := twoTransforms()
	p.ExternalBuckets = []string{
		"webbaa-customers-eu-north-1-cloudfront-logs",
		"another-external-bucket",
	}
	out, err := Emit(p)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	wants := []string{
		`Sid = "S3ReadExternal"`,
		`Action = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]`,
		`"arn:aws:s3:::webbaa-customers-eu-north-1-cloudfront-logs"`,
		`"arn:aws:s3:::webbaa-customers-eu-north-1-cloudfront-logs/*"`,
		`"arn:aws:s3:::another-external-bucket"`,
		`"arn:aws:s3:::another-external-bucket/*"`,
	}
	for _, w := range wants {
		if !strings.Contains(out, w) {
			t.Errorf("Emit output missing %q\n--- output ---\n%s", w, out)
		}
	}
}

// TestEmit_NoExternalBuckets asserts the S3ReadExternal Statement is
// omitted entirely when ExternalBuckets is empty — most pipelines read
// nothing outside the workspace bucket, and an empty Resource array
// would be a Terraform plan-time error anyway.
func TestEmit_NoExternalBuckets(t *testing.T) {
	t.Parallel()
	out, err := Emit(twoTransforms())
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if strings.Contains(out, `Sid = "S3ReadExternal"`) {
		t.Errorf("Emit output unexpectedly contains S3ReadExternal Sid with empty ExternalBuckets:\n%s", out)
	}
}

// TestEmit_UnityCatalog: a connected workspace gets one module call for the
// pipeline's Unity Catalog schema (ADR-027); an unconnected one gets
// nothing, so its orchestration.tf is unchanged.
func TestEmit_UnityCatalog(t *testing.T) {
	t.Parallel()
	off, err := Emit(twoTransforms())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(off, "unity_catalog") || strings.Contains(off, "databricks") {
		t.Errorf("unconnected pipeline mentions Unity Catalog:\n%s", off)
	}

	cfg := UnityCatalogConfig{
		Host: "https://dbc-1.cloud.databricks.com", ClientID: "app-id", Catalog: "analytics",
		ModuleSource:  "../.clavesa/modules/v9.9.9/unity-catalog/aws/schema",
		SecretARNExpr: `"arn:aws:secretsmanager:us-east-2:1:secret:x"`,
		PublishTables: true, JobsExpr: "var.databricks_jobs",
	}

	// Default mode: the team's schema; only the runner env, IAM and the
	// jobs variable change.
	p := twoTransforms()
	existing := cfg
	p.UnityCatalog = &existing
	on, err := Emit(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`CLAVESA_DATABRICKS_HOST      = "https://dbc-1.cloud.databricks.com"`,
		`CLAVESA_DATABRICKS_CLIENT_ID = "app-id"`,
		`CLAVESA_DATABRICKS_SECRET    = "arn:aws:secretsmanager:us-east-2:1:secret:x"`,
		`CLAVESA_UC_CATALOG           = "analytics"`,
		`CLAVESA_UC_SCHEMA            = replace(var.schema, "-", "_")`,
		`CLAVESA_UC_PUBLISH           = "true"`,
		`CLAVESA_UC_LINEAGE           = "false"`,
		`CLAVESA_DATABRICKS_JOBS      = jsonencode(var.databricks_jobs)`,
		`Sid = "DatabricksSecret"`,
		`variable "databricks_jobs" {`,
	} {
		if !strings.Contains(on, want) {
			t.Errorf("default mode: missing %q", want)
		}
	}
	if strings.Contains(on, "CLAVESA_UC_READERS") {
		t.Error("no readers: CLAVESA_UC_READERS must not be emitted")
	}
	withReaders := existing
	withReaders.Readers = []string{"analysts", "vesa@example.com"}
	p.UnityCatalog = &withReaders
	if out, err := Emit(p); err != nil || !strings.Contains(out, `CLAVESA_UC_READERS           = jsonencode(["analysts", "vesa@example.com"])`) {
		t.Errorf("readers env missing (err %v)", err)
	}
	p.UnityCatalog = &existing
	if strings.Contains(on, "unity_catalog_schema") {
		t.Error("default mode must not manage the team's schema")
	}

	// --create-catalog: the pipeline stack also creates its schema.
	created := cfg
	created.CreateSchema = true
	p.UnityCatalog = &created
	on, err = Emit(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`module "unity_catalog_schema" {`,
		`source               = "../.clavesa/modules/v9.9.9/unity-catalog/aws/schema"`,
		`databricks_client_id = "app-id"`,
		`catalog              = "analytics"`,
		`schema               = var.schema`,
		`value       = module.unity_catalog_schema.full_name`,
	} {
		if !strings.Contains(on, want) {
			t.Errorf("create-catalog mode: missing %q", want)
		}
	}
	if strings.Contains(on, "required_providers") && !strings.Contains(off, "required_providers") {
		t.Error("orchestration.tf must not add a required_providers block (main.tf holds the one a module may have)")
	}
}

// TestEmit_Architectures: both image Lambdas follow the pushed image's
// architecture; an arm64 image on an x86_64 Lambda cannot start.
func TestEmit_Architectures(t *testing.T) {
	t.Parallel()
	p := twoTransforms()
	p.RunnerArchExpr = "data.terraform_remote_state.workspace.outputs.runner_architecture"
	out, err := Emit(p)
	if err != nil {
		t.Fatal(err)
	}
	line := `architectures = [try(data.terraform_remote_state.workspace.outputs.runner_architecture, "x86_64")]`
	if n := strings.Count(out, line); n != 2 {
		t.Errorf("architectures line appears %d times, want 2 (pipeline runner + runs_writer)", n)
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

// TestEmit_WorkspaceScopedNames asserts that with AWSName set every AWS
// resource name uses the literal workspace-scoped base and the upstream
// trigger names the producer's own base. Tags and S3 prefixes keep the
// plain pipeline name.
func TestEmit_WorkspaceScopedNames(t *testing.T) {
	t.Parallel()
	p := twoTransforms()
	p.AWSName = "clavesa-ws-orders"
	p.UpstreamPipelines = []string{"bronze"}
	p.UpstreamAWSNames = map[string]string{"bronze": "clavesa-ws-bronze"}
	p.ScheduleExpr = "var.trigger_schedule"
	p.TriggerQueueExprs = []string{"module.src_a.queue_arn"}
	p.BatchWindowExpr = "var.trigger_batch_window"
	out, err := Emit(p)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	for _, w := range []string{
		`name               = "clavesa-ws-orders-orchestration"`,
		`function_name = "clavesa-ws-orders-runner"`,
		`name     = "clavesa-ws-orders"`,
		`name                = "clavesa-ws-orders-schedule"`,
		`function_name    = "clavesa-ws-orders-poller"`,
		`function_name = "clavesa-ws-orders-runs-writer"`,
		`name        = "clavesa-ws-orders-runs"`,
		`name              = "/clavesa/clavesa-ws-orders/sfn"`,
		`stateMachine:clavesa-ws-bronze"`,
		`name        = "clavesa-ws-orders-from-bronze"`,
		`"clavesa:pipeline" = var.pipeline_name`,
	} {
		if !strings.Contains(out, w) {
			t.Errorf("Emit output missing %q", w)
		}
	}
	if strings.Contains(out, "clavesa-${var.pipeline_name}") {
		t.Errorf("workspace mode still emits clavesa-${var.pipeline_name}")
	}
}

// TestEmit_LegacyNamesUnchanged pins the legacy names an already-deployed
// workspace has, so an upgrade shows no Terraform diff.
func TestEmit_LegacyNamesUnchanged(t *testing.T) {
	t.Parallel()
	p := twoTransforms()
	p.UpstreamPipelines = []string{"bronze"}
	out, err := Emit(p)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	for _, w := range []string{
		`"clavesa-${var.pipeline_name}-orchestration"`,
		`"clavesa-${var.pipeline_name}-runner"`,
		`"clavesa-${var.pipeline_name}"`,
		`"clavesa-${var.pipeline_name}-runs-writer"`,
		`"/clavesa/${var.pipeline_name}/sfn"`,
		`stateMachine:clavesa-bronze"`,
		`"clavesa-${var.pipeline_name}-from-bronze"`,
	} {
		if !strings.Contains(out, w) {
			t.Errorf("legacy Emit output missing %q", w)
		}
	}
}
