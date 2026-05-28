package tfgen

import (
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/orchestration/aslgen"
)

// minimal pipeline — a → b → c — exercising the linear path through the
// emitter end-to-end. Asserts structural properties of the rendered HCL
// rather than diffing the full text (which would be brittle against every
// formatting tweak). Real validation comes from `terraform validate`
// during the AWS deploy verification.
func TestEmit_Linear(t *testing.T) {
	t.Parallel()
	sm, err := aslgen.Build([]string{"a", "b", "c"}, []aslgen.Edge{{From: "a", To: "b"}, {From: "b", To: "c"}})
	if err != nil {
		t.Fatalf("aslgen.Build: %v", err)
	}
	out, err := Emit(Pipeline{
		PipelineNameExpr: "var.pipeline_name",
		Catalog:          "clavesa_x",
		SchemaExpr:       "var.schema",
		SystemCatalog:    "clavesa_x_system",
		BucketExpr:       "data.terraform_remote_state.workspace.outputs.pipeline_bucket",
		RunnerImageExpr:  "data.terraform_remote_state.workspace.outputs.runner_image",
		ScheduleExpr:     "var.trigger_schedule",
		BatchWindowExpr:  "var.trigger_batch_window",
		StateMachine:     sm,
		NodeMeta: map[string]NodeMeta{
			"a": {LambdaARNExpr: "module.a.lambda_function_arn", InputsExpr: "{}", OutputsExpr: "{ default = \"\" }", TimeoutSeconds: 300},
			"b": {LambdaARNExpr: "module.b.lambda_function_arn", InputsExpr: "{ a = module.a.outputs[\"default\"] }", OutputsExpr: "{ default = \"\" }", TimeoutSeconds: 300},
			"c": {LambdaARNExpr: "module.c.lambda_function_arn", InputsExpr: "{ b = module.b.outputs[\"default\"] }", OutputsExpr: "{ default = \"\" }", TimeoutSeconds: 300},
		},
	})
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}

	wants := []string{
		// Locals + Glue + log group + IAM
		`local.clavesa_tags`,
		`resource "aws_glue_catalog_database" "pipeline"`,
		`name         = "clavesa_x__${replace(var.schema, "-", "_")}"`,
		`location_uri = "s3://${data.terraform_remote_state.workspace.outputs.pipeline_bucket}/${var.pipeline_name}/_warehouse/clavesa_x__${replace(var.schema, "-", "_")}.db"`,
		`resource "aws_cloudwatch_log_group" "sfn_logs"`,
		`resource "aws_iam_role" "sfn_exec"`,
		`resource "aws_iam_role_policy" "sfn_exec_policy"`,
		// State machine: StartAt + each task + the unconditional Fail state
		`resource "aws_sfn_state_machine" "pipeline"`,
		`StartAt = "a"`,
		`a = {`,
		`b = {`,
		`c = {`,
		`PipelineFailed = {`,
		`FunctionName = module.a.lambda_function_arn`,
		`FunctionName = module.c.lambda_function_arn`,
		// Schedule wiring (gated by var.trigger_schedule)
		`resource "aws_cloudwatch_event_rule" "schedule"`,
		`count = var.trigger_schedule != null ? 1 : 0`,
		// runs_writer (always emitted) — ADR-018: image-based Lambda
		// using the runner image, no more Athena INSERT path.
		`resource "aws_lambda_function" "runs_writer"`,
		`package_type  = "Image"`,
		`command = ["runner.runs_writer_handler"]`,
		`CLAVESA_SYSTEM_CATALOG   = "clavesa_x_system"`,
		`data "aws_ecr_image" "runs_writer"`,
		// Poller is skipped because no TriggerQueueExprs
	}
	for _, w := range wants {
		if !strings.Contains(out, w) {
			t.Errorf("Emit output missing %q\n--- output ---\n%s", w, out)
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

func TestEmit_PollerEnabled(t *testing.T) {
	t.Parallel()
	sm, _ := aslgen.Build([]string{"a"}, []aslgen.Edge{})
	out, err := Emit(Pipeline{
		PipelineNameExpr:  "var.pipeline_name",
		Catalog:           "c",
		SchemaExpr:        "var.schema",
		SystemCatalog:     "c_system",
		BucketExpr:        "data.x.outputs.b",
		RunnerImageExpr:   "data.x.outputs.r",
		BatchWindowExpr:   "var.trigger_batch_window",
		TriggerQueueExprs: []string{"module.src_x.trigger_queue_arn", "module.src_y.trigger_queue_arn"},
		StateMachine:      sm,
		NodeMeta: map[string]NodeMeta{
			"a": {LambdaARNExpr: "module.a.lambda_function_arn", InputsExpr: "{}", OutputsExpr: "{}", TimeoutSeconds: 300},
		},
	})
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if !strings.Contains(out, `resource "aws_lambda_function" "poller"`) {
		t.Error("poller resource missing when TriggerQueueExprs is non-empty")
	}
	if !strings.Contains(out, `module.src_x.trigger_queue_arn, module.src_y.trigger_queue_arn`) {
		t.Error("queue ARN list not assembled correctly")
	}
}

// TestEmit_UpstreamTriggers asserts that when UpstreamPipelines is
// populated, the emitter produces one EventBridge rule + role + role
// policy + target per producer, all with consistent naming and the
// correct cross-account-style ARN literal pointing at the producer's
// state machine.
func TestEmit_UpstreamTriggers(t *testing.T) {
	t.Parallel()
	sm, _ := aslgen.Build([]string{"a"}, []aslgen.Edge{})
	out, err := Emit(Pipeline{
		PipelineNameExpr:  "var.pipeline_name",
		Catalog:           "c",
		SchemaExpr:        "var.schema",
		SystemCatalog:     "c_system",
		BucketExpr:        "data.x.outputs.b",
		RunnerImageExpr:   "data.x.outputs.r",
		UpstreamPipelines: []string{"bronze", "silver-team"},
		StateMachine:      sm,
		NodeMeta: map[string]NodeMeta{
			"a": {LambdaARNExpr: "module.a.lambda_function_arn", InputsExpr: "{}", OutputsExpr: "{}", TimeoutSeconds: 300},
		},
	})
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
	sm, _ := aslgen.Build([]string{"a"}, []aslgen.Edge{})
	out, err := Emit(Pipeline{
		PipelineNameExpr: "var.pipeline_name",
		Catalog:          "c",
		SchemaExpr:       "var.schema",
		SystemCatalog:    "c_system",
		BucketExpr:       "data.x.outputs.b",
		RunnerImageExpr:  "data.x.outputs.r",
		StateMachine:     sm,
		NodeMeta: map[string]NodeMeta{
			"a": {LambdaARNExpr: "module.a.lambda_function_arn", InputsExpr: "{}", OutputsExpr: "{}", TimeoutSeconds: 300},
		},
	})
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

// TestEmit_CatchOnlyAtTopLevel is the v1.1.5→v1.1.6 regression pin.
// AWS Step Functions rejects a Catch.Next that targets a state outside
// the same States map (MISSING_TRANSITION_TARGET). Tasks and Parallels
// nested inside a Parallel branch must NOT emit a Catch — errors
// propagate up to the enclosing Parallel's Catch, which sits at the top
// level where PipelineFailed is in scope.
func TestEmit_CatchOnlyAtTopLevel(t *testing.T) {
	t.Parallel()
	// Same nested-fanout shape that tripped the validator on
	// cloudfront-analytics: a → b → {c → d, e}, with b a fanout.
	sm, err := aslgen.Build([]string{"a", "b", "c", "d", "e"},
		[]aslgen.Edge{{From: "a", To: "b"}, {From: "b", To: "c"}, {From: "b", To: "e"}, {From: "c", To: "d"}})
	if err != nil {
		t.Fatalf("aslgen.Build: %v", err)
	}
	meta := map[string]NodeMeta{}
	for _, n := range []string{"a", "b", "c", "d", "e"} {
		meta[n] = NodeMeta{LambdaARNExpr: "module." + n + ".lambda_function_arn", InputsExpr: "{}", OutputsExpr: "{}", TimeoutSeconds: 300}
	}
	out, err := Emit(Pipeline{
		PipelineNameExpr: "var.pipeline_name",
		Catalog:          "c", SchemaExpr: "var.schema", SystemCatalog: "c_system",
		BucketExpr:      "data.x.outputs.b",
		RunnerImageExpr: "data.x.outputs.r",
		StateMachine: sm, NodeMeta: meta,
	})
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}

	// Extract just the SFN definition jsonencode({...}) block so we don't
	// match Catch occurrences inside IAM jsonencode policies (which have
	// nothing to do with ASL Catch semantics).
	defStart := strings.Index(out, "definition = jsonencode({")
	if defStart == -1 {
		t.Fatal("definition block not found")
	}
	defEnd := strings.Index(out[defStart:], "  })\n\n  logging_configuration")
	if defEnd == -1 {
		t.Fatal("definition block end not found")
	}
	asl := out[defStart : defStart+defEnd]

	catchCount := strings.Count(asl, `Catch = [{ ErrorEquals = ["States.ALL"], Next = "PipelineFailed" }]`)
	// Top-level states for this shape: a (Task), b (Task), b_Branches
	// (Parallel). Each gets one Catch. Inner Tasks (c, d, e) and the
	// implicit nested Parallel — none of those get Catches.
	wantCatch := 3
	if catchCount != wantCatch {
		t.Errorf("Catch count = %d, want %d (one per top-level state, none inside Parallel branches)\nASL:\n%s",
			catchCount, wantCatch, asl)
	}

	// Belt-and-suspenders: assert the inner Task `d` does NOT carry a
	// Catch. Use ResultPath as the anchor — `d = {` would also match
	// inside `Payloa[d = {]`, the false positive that bit the first
	// version of this test.
	anchor := `ResultPath = "$.runner_results.d"`
	dStart := strings.Index(asl, anchor)
	if dStart == -1 {
		t.Fatal("inner state d not found via ResultPath anchor")
	}
	// d's block from ResultPath onward ends at the first End=true (d is
	// always terminal in this shape — leaf of the c branch).
	dEnd := strings.Index(asl[dStart:], "End = true")
	if dEnd == -1 {
		t.Fatal("d's End=true not found")
	}
	dTail := asl[dStart : dStart+dEnd]
	if strings.Contains(dTail, "Catch =") {
		t.Errorf("inner Task d carries a Catch — would fail AWS validator (MISSING_TRANSITION_TARGET):\n--- d's tail ---\n%s\n--- end ---", dTail)
	}
}

func TestEmit_NestedFanout(t *testing.T) {
	t.Parallel()
	// a → b → {c → d, e} — the nested-fanout case that v1.1.4 mangled.
	sm, err := aslgen.Build([]string{"a", "b", "c", "d", "e"},
		[]aslgen.Edge{{From: "a", To: "b"}, {From: "b", To: "c"}, {From: "b", To: "e"}, {From: "c", To: "d"}})
	if err != nil {
		t.Fatalf("aslgen.Build: %v", err)
	}
	meta := map[string]NodeMeta{}
	for _, n := range []string{"a", "b", "c", "d", "e"} {
		meta[n] = NodeMeta{LambdaARNExpr: "module." + n + ".lambda_function_arn", InputsExpr: "{}", OutputsExpr: "{}", TimeoutSeconds: 300}
	}
	out, err := Emit(Pipeline{
		PipelineNameExpr: "var.pipeline_name",
		Catalog:          "c", SchemaExpr: "var.schema", SystemCatalog: "c_system",
		BucketExpr:      "data.x.outputs.b",
		RunnerImageExpr: "data.x.outputs.r",
		StateMachine: sm, NodeMeta: meta,
	})
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	// b_Branches MUST appear as a top-level key in the States map (with
	// proper indentation), and inside its Branches list the inner d state
	// must appear (multi-hop branch through c).
	if !strings.Contains(out, "b_Branches = {") {
		t.Error("missing b_Branches Parallel state")
	}
	if !strings.Contains(out, "d = {") {
		t.Error("missing inner d state (multi-hop branch)")
	}
}
