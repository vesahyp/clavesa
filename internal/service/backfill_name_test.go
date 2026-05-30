package service

import "testing"

// TestPipelineRunnerLambdaName pins the backfill service's Lambda
// function-name derivation to the name the orchestration emitter
// produces in tfgen.emitPipelineLambda: `clavesa-<pipeline>-runner`
// (the var.pipeline_name value, which equals the pipeline dir name).
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
		{"demo", "clavesa-demo-runner"},
		{"taxi_rides", "clavesa-taxi_rides-runner"},
		{"nyc-trips", "clavesa-nyc-trips-runner"},
	}
	for _, c := range cases {
		if got := pipelineRunnerLambdaName(c.pipeline); got != c.want {
			t.Errorf("pipelineRunnerLambdaName(%q) = %q, want %q", c.pipeline, got, c.want)
		}
	}
}
