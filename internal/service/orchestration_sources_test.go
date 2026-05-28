package service

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// pipelineWithAttachedSource stamps a workspace + pipeline + attached
// source, returning the workspace and pipeline-relative dir.
func pipelineWithAttachedSource(t *testing.T) (string, string) {
	t.Helper()
	ws, dir := pipelineForAttachTest(t)
	svc := New(ws)
	if _, err := svc.AddSource(SourceSpec{
		Name: "trips", Kind: "http",
		URL: "https://example.com/yellow_tripdata_2024-01.parquet",
	}); err != nil {
		t.Fatalf("AddSource: %v", err)
	}
	if err := svc.AttachSource(dir, "trips", "t1", "raw"); err != nil {
		t.Fatalf("AttachSource: %v", err)
	}
	return ws, dir
}

func TestSyncOrchestrationEmitsHTTPSourceDescriptor(t *testing.T) {
	t.Parallel()
	ws, dir := pipelineWithAttachedSource(t)
	svc := New(ws)
	if err := svc.SyncOrchestration(dir, ""); err != nil {
		t.Fatalf("SyncOrchestration: %v", err)
	}
	body, err := os.ReadFile(filepath.Join(ws, dir, "orchestration.tf"))
	if err != nil {
		t.Fatal(err)
	}
	got := string(body)
	// Look for the dict-form descriptor on the t1 input.
	want := `raw = { kind = "http", url = "https://example.com/yellow_tripdata_2024-01.parquet", format = "parquet" }`
	if !strings.Contains(got, want) {
		t.Errorf("orchestration.tf missing http source descriptor.\nwant substring: %s\ngot:\n%s", want, got)
	}
}

func TestSyncOrchestrationEmitsCredentialDescriptor(t *testing.T) {
	t.Parallel()
	ws, dir := pipelineForAttachTest(t)
	svc := New(ws)
	if _, err := svc.AddCredential(CredentialSpec{
		Name: "stripe", Kind: "header",
		HeaderName: "Authorization", ValuePrefix: "Bearer ",
		Secret: "arn:aws:secretsmanager:eu-north-1:111122223333:secret:stripe-AbCdEf",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.AddSource(SourceSpec{
		Name: "events", Kind: "http",
		URL: "https://api.stripe.com/v1/events", Format: "json",
		Credentials: "stripe",
	}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AttachSource(dir, "events", "t1", "raw"); err != nil {
		t.Fatal(err)
	}
	if err := svc.SyncOrchestration(dir, ""); err != nil {
		t.Fatalf("SyncOrchestration: %v", err)
	}
	body, _ := os.ReadFile(filepath.Join(ws, dir, "orchestration.tf"))
	got := string(body)
	if !strings.Contains(got, `credentials = { kind = "header"`) {
		t.Errorf("orchestration.tf missing credentials block:\n%s", got)
	}
	if !strings.Contains(got, "arn:aws:secretsmanager:eu-north-1:111122223333:secret:stripe-AbCdEf") {
		t.Errorf("orchestration.tf missing secret arn:\n%s", got)
	}
}

func TestSyncOrchestrationRejectsLocalBackendOnCloudCompute(t *testing.T) {
	t.Parallel()
	// Default compute is lambda (cloud) — env: backend should fail at
	// emit time rather than blow up at runtime in Lambda.
	ws, dir := pipelineForAttachTest(t)
	svc := New(ws)
	if _, err := svc.AddCredential(CredentialSpec{
		Name: "stripe", Kind: "header",
		HeaderName: "Authorization", Secret: "env:STRIPE_KEY",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.AddSource(SourceSpec{
		Name: "events", Kind: "http",
		URL: "https://api.stripe.com/v1/events", Format: "json",
		Credentials: "stripe",
	}); err != nil {
		t.Fatal(err)
	}
	// AttachSource itself triggers SyncOrchestration — the rejection
	// surfaces there, before the user ever touches `terraform apply`.
	err := svc.AttachSource(dir, "events", "t1", "raw")
	if err == nil || !strings.Contains(err.Error(), "local-only backend") {
		t.Errorf("AttachSource with env:-backed credential on cloud compute = %v, want emit-time rejection", err)
	}
}

func TestSyncOrchestrationEmitsS3SourceDescriptor(t *testing.T) {
	t.Parallel()
	ws, dir := pipelineForAttachTest(t)
	svc := New(ws)
	if _, err := svc.AddSource(SourceSpec{
		Name: "logs",
		URL:  "s3://my-bucket/events/2024/data.parquet",
	}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AttachSource(dir, "logs", "t1", "raw"); err != nil {
		t.Fatal(err)
	}
	if err := svc.SyncOrchestration(dir, ""); err != nil {
		t.Fatalf("SyncOrchestration: %v", err)
	}
	body, _ := os.ReadFile(filepath.Join(ws, dir, "orchestration.tf"))
	got := string(body)
	want := `raw = { kind = "s3", bucket = "my-bucket", prefix = "events/2024/data.parquet/", format = "parquet" }`
	if !strings.Contains(got, want) {
		t.Errorf("orchestration.tf missing s3 descriptor.\nwant substring: %s\ngot:\n%s", want, got)
	}
}

// TestSyncOrchestrationMaterialisesS3SourceModule covers v0.23.0: the
// emitter writes a `module "src_<name>"` block per registered kind=s3
// source attached to a transform, and the orchestration block's
// trigger_queue_arns list references it. Without this, registered s3
// sources never get an SQS queue or EventBridge rule and the
// s3-trigger cookbook recipe can't fire on new data.
func TestSyncOrchestrationMaterialisesS3SourceModule(t *testing.T) {
	t.Parallel()
	ws, dir := pipelineForAttachTest(t)
	svc := New(ws)
	if _, err := svc.AddSource(SourceSpec{
		Name:                      "events",
		Kind:                      "s3",
		Bucket:                    "raw-bucket",
		Prefix:                    "ev/",
		Format:                    "parquet",
		Partitions:                []string{"year", "month"},
		StartFrom:                 "now",
		ManageBucketNotifications: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AttachSource(dir, "events", "t1", "raw"); err != nil {
		t.Fatal(err)
	}
	body, _ := os.ReadFile(filepath.Join(ws, dir, "orchestration.tf"))
	got := string(body)

	for _, want := range []string{
		`module "src_events"`,
		`bucket        = "raw-bucket"`,
		`prefix        = "ev/"`,
		`partitions    = ["year", "month"]`,
		`start_from    = "now"`,
		`manage_bucket_notifications = true`,
		// v1.1.5+: trigger queue ARNs flow into the poller's local._poller_queue_arns
		// list rather than a module variable (orchestration is now inline TF).
		`_poller_queue_arns = [module.src_events.trigger_queue_arn]`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("orchestration.tf missing %q\ngot:\n%s", want, got)
		}
	}
}

// TestSyncOrchestrationOmitsSourceModuleForHTTP covers the negative
// case: kind=http registered sources have no S3 bucket and don't need
// SQS / EventBridge wiring, so the emitter must not materialise a
// `module "src_*"` block for them.
func TestSyncOrchestrationOmitsSourceModuleForHTTP(t *testing.T) {
	t.Parallel()
	ws, dir := pipelineForAttachTest(t)
	svc := New(ws)
	if _, err := svc.AddSource(SourceSpec{
		Name: "trips",
		URL:  "https://example.com/trips.parquet",
	}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AttachSource(dir, "trips", "t1", "raw"); err != nil {
		t.Fatal(err)
	}
	body, _ := os.ReadFile(filepath.Join(ws, dir, "orchestration.tf"))
	got := string(body)
	if strings.Contains(got, `module "src_trips"`) {
		t.Errorf("orchestration.tf materialised a source module for kind=http:\n%s", got)
	}
	// v1.1.5+: with no source-triggered queues the poller block is omitted
	// entirely (no Lambda, no event rule). Asserts no poller resource was
	// emitted — equivalent to the prior `trigger_queue_arns = []` check.
	if strings.Contains(got, `aws_lambda_function" "poller"`) {
		t.Errorf("orchestration.tf emitted a poller for an http-only pipeline; got:\n%s", got)
	}
}

// TestSyncOrchestrationEmitsMultipleOutputs covers v0.24.0: a transform
// with output_definitions = { default = {}, outliers = {} } gets both
// keys in the emitted SFN payload, so the runner doesn't have to fall
// back to auto-tables for non-default keys and can carry per-output
// mode + merge_keys descriptors. Single-default replace-mode transforms
// keep the legacy bare-string form unchanged.
func TestSyncOrchestrationEmitsMultipleOutputs(t *testing.T) {
	t.Parallel()
	ws, dir := pipelineForAttachTest(t)
	abs := filepath.Join(ws, dir)
	// Augment the seeded transform with output_definitions covering two
	// keys; AttachSource and SyncOrchestration both read it as Config.
	body, _ := os.ReadFile(filepath.Join(abs, "main.tf"))
	patched := strings.Replace(string(body),
		`sql    = "SELECT 1"`,
		"sql    = \"SELECT 1\"\n  output_definitions = { default = { mode = \"append\" }, outliers = {} }",
		1)
	if err := os.WriteFile(filepath.Join(abs, "main.tf"), []byte(patched), 0o644); err != nil {
		t.Fatal(err)
	}
	svc := New(ws)
	if err := svc.SyncOrchestration(dir, ""); err != nil {
		t.Fatalf("SyncOrchestration: %v", err)
	}
	got, _ := os.ReadFile(filepath.Join(abs, "orchestration.tf"))
	s := string(got)

	if !strings.Contains(s, `outliers = ""`) {
		t.Errorf("orchestration.tf missing bare-string `outliers = \"\"` entry\n%s", s)
	}
	if !strings.Contains(s, `default = { kind = "delta_table", table_id = "", mode = "append"`) {
		t.Errorf("orchestration.tf missing default-append dict entry\n%s", s)
	}
}

// TestSyncOrchestrationKeepsLegacyBareStringForDefaultReplace covers the
// back-compat branch: a single-default-replace-no-merge transform still
// emits the original `outputs = { default = "" }` form, so existing
// pipelines whose orchestration.tf was last emitted before v0.24.0 don't
// see a noisy diff just because they re-ran the emitter.
func TestSyncOrchestrationKeepsLegacyBareStringForDefaultReplace(t *testing.T) {
	t.Parallel()
	ws, dir := pipelineForAttachTest(t)
	svc := New(ws)
	if err := svc.SyncOrchestration(dir, ""); err != nil {
		t.Fatalf("SyncOrchestration: %v", err)
	}
	got, _ := os.ReadFile(filepath.Join(ws, dir, "orchestration.tf"))
	// v2.2.0: outputs live inside the per-pipeline Lambda transforms[]
	// payload; tfgen column-aligns the keys (`outputs    = ...`). The bare-
	// string compact form for single-default-replace is still preserved —
	// the test is that the dict shape isn't gratuitously emitted.
	if !strings.Contains(string(got), `outputs    = { default = "" }`) {
		t.Errorf("compact single-default-replace emit shape regressed:\n%s", got)
	}
}

// TestSyncOrchestrationEmitsIncrementalTransformInput covers v0.24.0
// (ported to v2.0.0 Delta CDF): when a downstream transform declares
// incremental_inputs = ["<alias>"] for a transform-upstream edge, the
// orchestration emitter writes the delta_table_cdf descriptor so the
// runner does a CDF-bounded read plus watermark advance. Other inputs
// on the same node keep the legacy bare-string Delta-table shape.
func TestSyncOrchestrationEmitsIncrementalTransformInput(t *testing.T) {
	t.Parallel()
	ws, dir := pipelineForAttachTest(t)
	abs := filepath.Join(ws, dir)
	// Replace the seeded single-transform pipeline with bronze → silver,
	// silver declaring incremental_inputs = ["bronze"]. The transform
	// module's source path doesn't matter for the parser; only the
	// nodeType-from-source heuristic.
	bronzeSilver := `terraform {
  required_providers { aws = { source = "hashicorp/aws" } }
}
variable "pipeline_name" { type = string default = "demo" }

module "bronze" {
  source = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v0.23.0"
  name   = "bronze"
  sql    = "SELECT * FROM trips"
}

module "silver" {
  source = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v0.23.0"
  name   = "silver"
  sql    = "SELECT COUNT(*) FROM bronze"
  incremental_inputs = ["bronze"]
  inputs = {
    bronze = module.bronze.outputs["default"]
  }
}
`
	if err := os.WriteFile(filepath.Join(abs, "main.tf"), []byte(bronzeSilver), 0o644); err != nil {
		t.Fatal(err)
	}
	svc := New(ws)
	if err := svc.SyncOrchestration(dir, ""); err != nil {
		t.Fatalf("SyncOrchestration: %v", err)
	}
	got, _ := os.ReadFile(filepath.Join(abs, "orchestration.tf"))
	s := string(got)

	for _, want := range []string{
		`kind = "delta_table_cdf"`,
		`alias = "silver__bronze"`,
		`module.bronze.outputs["default"].catalog_db`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("orchestration.tf missing %q\n%s", want, s)
		}
	}
	// Bronze (no incremental_inputs) should keep the legacy bare-string
	// shape against its source — but bronze here has no upstreams, so
	// just check we haven't accidentally promoted everything to dict.
	if strings.Count(s, "delta_table_cdf") != 1 {
		t.Errorf("expected exactly one delta_table_cdf block; got:\n%s", s)
	}
	// v2.0.0: Delta tables live under spark_catalog, so emitted
	// identifiers must NOT carry the "clavesa." prefix.
	if strings.Contains(s, `"clavesa.${module.`) {
		t.Errorf("orchestration.tf still emits legacy clavesa. prefix:\n%s", s)
	}
}

func TestSyncOrchestrationFailsOnMissingRegistryEntry(t *testing.T) {
	t.Parallel()
	// Set up a pipeline with an inputs reference to a source that doesn't
	// exist in the registry — the sync must fail loudly instead of
	// silently emitting a half-formed descriptor.
	ws, dir := pipelineForAttachTest(t)
	svc := New(ws)
	// Hand-write the inputs reference without registering the source —
	// simulates a hand-edited .tf or a delete-then-sync race.
	mainPath := filepath.Join(ws, dir, "main.tf")
	body, _ := os.ReadFile(mainPath)
	patched := strings.Replace(string(body),
		`sql    = "SELECT 1"`,
		`sql    = "SELECT 1"`+"\n  inputs = { raw = \"sources.ghost\" }",
		1)
	if err := os.WriteFile(mainPath, []byte(patched), 0o644); err != nil {
		t.Fatal(err)
	}
	err := svc.SyncOrchestration(dir, "")
	if err == nil || !strings.Contains(err.Error(), "ghost") {
		t.Errorf("SyncOrchestration with unregistered source = %v, want error mentioning the source name", err)
	}
}
