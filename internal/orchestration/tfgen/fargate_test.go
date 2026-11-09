package tfgen

import (
	"strings"
	"testing"
)

func TestEmit_Fargate(t *testing.T) {
	p := twoTransforms()
	p.Compute = ComputeFargate
	p.AWSName = "clavesa-ws-p"
	out, err := Emit(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`resource "aws_ecs_cluster" "pipeline"`,
		`resource "aws_ecs_task_definition" "pipeline_runner"`,
		`family                   = "clavesa-ws-p-runner"`,
		`cpu                      = tostring(var.fargate_cpu * 1024)`,
		`{ name = "CLAVESA_FARGATE", value = "1" }`,
		`{ name = "CLAVESA_COMPUTE", value = "fargate" }`,
		`environment = concat([for k, v in local.pipeline_runner_env`,
		`awslogs-group         = "/aws/lambda/clavesa-ws-p-runner"`,
		`resource "aws_s3_object" "pipeline_bundle"`,
		`Resource       = "arn:aws:states:::ecs:runTask.sync"`,
		`StartAt = "Bundle"`,
		`Payload = merge(local.pipeline_bundle, {`,
		`"Value.$" = "States.JsonToString($$.Execution.Input)"`,
		`identifiers = ["lambda.amazonaws.com", "ecs-tasks.amazonaws.com"]`,
		`Sid = "EcsRunTask"`,
		`Sid = "EcsPassRoles"`,
		`variable "fargate_memory_gb"`,
		// The runner Lambda stays: operations and cloud-local use it.
		`resource "aws_lambda_function" "pipeline_runner"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("fargate emit lacks %q", want)
		}
	}
	if strings.Contains(out, `Resource       = "arn:aws:states:::lambda:invoke"`) {
		t.Error("fargate state machine must not invoke the Lambda")
	}
	// The bundle carries every transform once, in local.pipeline_bundle,
	// which both the S3 object and the state machine use.
	if strings.Count(out, `node       = "`) != len(p.Transforms) {
		t.Errorf("want each transform in the bundle local once; got %d", strings.Count(out, `node       = "`))
	}

	lam, err := Emit(twoTransforms())
	if err != nil {
		t.Fatal(err)
	}
	for _, not := range []string{"aws_ecs_", "ecs-tasks.amazonaws.com", "CLAVESA_FARGATE", "pipeline_bundle"} {
		if strings.Contains(lam, not) {
			t.Errorf("lambda emit must not contain %q", not)
		}
	}
}

func TestEmit_EMRServerlessRefused(t *testing.T) {
	p := twoTransforms()
	p.Compute = "emr-serverless"
	if _, err := Emit(p); err == nil || !strings.Contains(err.Error(), "not built") {
		t.Errorf("emr-serverless: err = %v", err)
	}
}
