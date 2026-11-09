package service

import (
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/sources"
)

// TestPipelineRunnerLambdaName pins the backfill service's Lambda
// function-name derivation to the name the orchestration emitter
// produces in tfgen.emitPipelineLambda: `clavesa-<pipeline>-runner`
// (or clavesa-<workspace>-<pipeline>): the argument is the AWS base name.
//
// Regression guard for the cloud-backfill 502: the previous derivation
// was `fmt.Sprintf("%s-%s", pipelineName, node)`, which named a
// nonexistent per-node Lambda and made GetFunctionConfiguration /
// Invoke fail. v2.2.0+ is single-Lambda-per-pipeline.
func TestPipelineRunnerLambdaName(t *testing.T) {
	t.Parallel()
	cases := []struct {
		pipeline string
		want     string
	}{
		{"clavesa-demo", "clavesa-demo-runner"},
		{"clavesa-taxi_rides", "clavesa-taxi_rides-runner"},
		{"clavesa-ws-nyc-trips", "clavesa-ws-nyc-trips-runner"},
	}
	for _, c := range cases {
		if got := pipelineRunnerLambdaName(c.pipeline); got != c.want {
			t.Errorf("pipelineRunnerLambdaName(%q) = %q, want %q", c.pipeline, got, c.want)
		}
	}
}

func TestEmitS3SourceModuleAWSName(t *testing.T) {
	t.Parallel()
	var legacy, scoped strings.Builder
	spec := sources.Spec{Name: "raw", Bucket: "b", Prefix: "p", Format: "csv"}
	emitS3SourceModule(&legacy, "../src", spec, "")
	emitS3SourceModule(&scoped, "../src", spec, "clavesa-ws-orders")
	if strings.Contains(legacy.String(), "aws_name") {
		t.Errorf("legacy source block must not set aws_name:\n%s", legacy.String())
	}
	if !strings.Contains(scoped.String(), `aws_name      = "clavesa-ws-orders"`) {
		t.Errorf("scoped source block missing aws_name:\n%s", scoped.String())
	}
}
