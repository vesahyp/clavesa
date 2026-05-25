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
		`name        = "clavesa_x__${replace(var.schema, "-", "_")}"`,
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
		// runs_writer (always emitted)
		`resource "aws_lambda_function" "runs_writer"`,
		`CLAVESA_DATABASE         = "clavesa_x_system__pipelines"`,
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
		BucketExpr:   "data.x.outputs.b",
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
