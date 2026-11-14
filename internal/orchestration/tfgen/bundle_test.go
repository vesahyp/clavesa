package tfgen

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEmitBundle(t *testing.T) {
	p := twoTransforms()
	p.Bundle = &BundleConfig{
		FilesDir: "bundle", JobFile: "bundle-job.json",
		ScheduleExpression: "cron(30 4 * * ? *)", ScheduleTimezone: "Europe/Helsinki", SchedulePaused: true,
		FileArrivalBucket: "landing", FileArrivalPrefix: "weekly/",
		Notify:           map[string][]string{"failure": {"a@example.com", "b@example.com"}},
		HealthMaxSeconds: 3600,
		GlueDatabases:    []string{"main__sales"},
		ReadBuckets:      []string{"databricks-datasets-oregon"},
		SecretsPrefix:    "clavesa/ws/dbx/",
	}
	out, err := Emit(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`resource "aws_s3_object" "bundle_files"`,
		`for_each = fileset("${path.module}/bundle", "**")`,
		`key    = "${var.pipeline_name}/_bundle/bundle-job.json"`,
		`schedule_expression = "cron(30 4 * * ? *)"`,
		`schedule_expression_timezone = "Europe/Helsinki"`,
		`state               = "DISABLED"`,
		`bucket = { name = ["landing"] }, object = { key = [{ prefix = "weekly/" }] }`,
		`resource "aws_sns_topic_subscription" "bundle_failure_1"`,
		`endpoint  = "b@example.com"`,
		`status = ["FAILED", "TIMED_OUT", "ABORTED"]`,
		`threshold           = 3.6e+06`,
		`"arn:aws:glue:*:*:database/main__sales"`,
		`"arn:aws:s3:::databricks-datasets-oregon/*"`,
		`"arn:aws:secretsmanager:*:*:secret:clavesa/ws/dbx/*"`,
		`CLAVESA_DBX_SECRET_PREFIX   = "clavesa/ws/dbx/"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("orchestration lacks %q", want)
		}
	}
	// No bundle: nothing of it.
	plain, _ := Emit(twoTransforms())
	if strings.Contains(plain, `"bundle_`) || strings.Contains(plain, "CLAVESA_DBX") {
		t.Error("a pipeline without a bundle got bundle resources")
	}
}

func TestEmitMixedSegmentsGolden(t *testing.T) {
	p := twoTransforms()
	p.Transforms[0].Language, p.Transforms[1].Language = "task", "task"
	p.Bundle = &BundleConfig{
		FilesDir: "bundle", JobFile: "bundle-job.json", DatabricksJobID: 923072819710352,
		JobURI: `"s3://${data.terraform_remote_state.workspace.outputs.pipeline_bucket}/${var.pipeline_name}/_bundle/bundle-job.json"`,
		Segments: []PipelineSegment{
			{Place: "clavesa", Transforms: p.Transforms[:1], Tasks: []string{"bronze"}},
			{Place: "databricks", Tasks: []string{"gold"}},
			{Place: "clavesa", Transforms: p.Transforms[1:], Tasks: []string{"silver"}},
		},
	}
	out, err := Emit(p)
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(out, `resource "aws_sfn_state_machine" "pipeline"`)
	end := strings.Index(out[start:], "\n}\n") + start + 3
	got := out[start:end]
	path := filepath.Join("testdata", "mixed_state_machine.golden")
	if os.Getenv("UPDATE_GOLDEN") == "1" {
		_ = os.MkdirAll("testdata", 0o755)
		_ = os.WriteFile(path, []byte(got), 0o644)
	}
	want, _ := os.ReadFile(path)
	if got != string(want) {
		t.Errorf("state machine differs from %s (UPDATE_GOLDEN=1 rewrites):\n%s", path, got)
	}
	p.Compute = ComputeFargate
	if _, err := Emit(p); err == nil {
		t.Error("a mixed job on Fargate must be refused")
	}
}
