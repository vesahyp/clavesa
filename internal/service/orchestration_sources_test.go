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
	// queue_url is an unquoted TF reference to the source module's output —
	// the runner drains it for new keys (notification-drain ingest, #25).
	want := `raw = { kind = "s3", bucket = "my-bucket", prefix = "events/2024/data.parquet/", format = "parquet", queue_url = module.src_logs.trigger_queue_url }`
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
		// #25 notification-drain: the partitioned descriptor carries queue_url,
		// the runner role gains SQS drain perms, and the poller no longer consumes.
		`queue_url = module.src_events.trigger_queue_url`,
		`Sid = "SQSDrain"`,
		`"sqs:ReceiveMessage", "sqs:DeleteMessage"`,
		`Sid = "SQSPoll",  Effect = "Allow", Action = ["sqs:GetQueueAttributes"]`,
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

// cdfDescriptor extracts the single delta_table_cdf input descriptor from
// an emitted orchestration.tf body — everything from `kind =
// "delta_table_cdf"` up to its closing brace. Lets a test assert on the
// INPUT descriptor's merge_keys without colliding with the OUTPUT
// descriptor's merge_keys, which buildNodeOutputsExpr also emits.
func cdfDescriptor(t *testing.T, body string) string {
	t.Helper()
	i := strings.Index(body, `kind = "delta_table_cdf"`)
	if i < 0 {
		t.Fatalf("no delta_table_cdf descriptor in:\n%s", body)
	}
	j := strings.IndexByte(body[i:], '}')
	if j < 0 {
		t.Fatalf("unterminated delta_table_cdf descriptor in:\n%s", body)
	}
	return body[i : i+j]
}

// TestSyncOrchestrationEmitsIncrementalCrossPipelineInput covers v2.6.0:
// incremental_inputs now works for a CROSS-PIPELINE input too (an upstream
// table owned by another pipeline, referenced as "<schema>.<table>"). The
// emitter writes a delta_table_cdf descriptor pointing at the resolved table
// id directly (no module.X.outputs — the producer is in another pipeline) so
// the runner CDF-reads it and the keyed merge upserts. This is the path the
// medallion uses when bronze/silver/gold are separate pipelines.
//
// The descriptor's merge_keys are the PRODUCER's grain, resolved from the
// upstream pipeline's output_definitions — not the consumer's own output
// merge_keys. Here the producer (rawpipe.events) merges on "event_id" while
// the consumer merges on "row_id"; the CDF descriptor must carry "event_id".
func TestSyncOrchestrationEmitsIncrementalCrossPipelineInput(t *testing.T) {
	t.Parallel()
	ws := xpipeWorkspace(t)
	writePipelineMain(t, ws, "rawpipe", `module "events" {
  source = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v2.6.0"
  name   = "events"
  sql    = "SELECT 1 AS event_id"
  output_definitions = { default = { mode = "merge", merge_keys = ["event_id"] } }
}
`)
	writePipelineMain(t, ws, "consumer", `module "silver" {
  source = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v2.6.0"
  name   = "silver"
  sql    = "SELECT * FROM up"
  incremental_inputs = ["up"]
  inputs = {
    up = "rawpipe.events"
  }
  output_definitions = { default = { mode = "merge", merge_keys = ["row_id"] } }
}
`)
	svc := New(ws)
	if err := svc.SyncOrchestration("consumer", ""); err != nil {
		t.Fatalf("SyncOrchestration: %v", err)
	}
	s := mustRead(t, filepath.Join(ws, "consumer", "orchestration.tf"))

	for _, want := range []string{
		`kind = "delta_table_cdf"`,
		`alias = "silver__up"`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("orchestration.tf missing %q\n%s", want, s)
		}
	}
	desc := cdfDescriptor(t, s)
	if !strings.Contains(desc, `merge_keys = ["event_id"]`) {
		t.Errorf("CDF descriptor should key on the producer grain event_id, got:\n%s", desc)
	}
	if strings.Contains(desc, "row_id") {
		t.Errorf("CDF descriptor must NOT carry the consumer's output key row_id:\n%s", desc)
	}
	// The cross-pipeline CDF table is a resolved string id, NOT a
	// module.X.outputs reference (the producer is in another pipeline).
	if strings.Contains(s, `table = "${module.`) {
		t.Errorf("cross-pipeline CDF should reference a resolved table id, not module.X.outputs:\n%s", s)
	}
}

// TestSyncOrchestrationCrossPipelineCDFKeysOnProducerGrain is the regression
// lock for the dim CDF crash: a gold dimension reading silver's enriched
// table incrementally renames the upstream column
// (`SELECT cs_User_Agent AS user_agent`) and merges its OWN output on
// `user_agent`. The CDF input descriptor must key the range-dedup on the
// PRODUCER's grain `x_edge_request_id` — a column that exists on the
// enriched feed — not on `user_agent`, which doesn't exist upstream and
// makes the runner's partitionBy throw "column not found".
func TestSyncOrchestrationCrossPipelineCDFKeysOnProducerGrain(t *testing.T) {
	t.Parallel()
	ws := xpipeWorkspace(t)
	writePipelineMain(t, ws, "silver", `module "enriched" {
  source = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v2.6.0"
  name   = "enriched"
  sql    = "SELECT x_edge_request_id, cs_User_Agent FROM bronze"
  output_definitions = { default = { mode = "merge", merge_keys = ["x_edge_request_id"] } }
}
`)
	writePipelineMain(t, ws, "gold", `module "dim_device" {
  source = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v2.6.0"
  name   = "dim_device"
  sql    = "SELECT DISTINCT cs_User_Agent AS user_agent FROM enriched"
  incremental_inputs = ["enriched"]
  inputs = {
    enriched = "silver.enriched"
  }
  output_definitions = { default = { mode = "merge", merge_keys = ["user_agent"] } }
}
`)
	svc := New(ws)
	if err := svc.SyncOrchestration("gold", ""); err != nil {
		t.Fatalf("SyncOrchestration: %v", err)
	}
	s := mustRead(t, filepath.Join(ws, "gold", "orchestration.tf"))

	desc := cdfDescriptor(t, s)
	if !strings.Contains(desc, `merge_keys = ["x_edge_request_id"]`) {
		t.Errorf("CDF descriptor must key on producer grain x_edge_request_id, got:\n%s", desc)
	}
	if strings.Contains(desc, "user_agent") {
		t.Errorf("CDF descriptor must NOT key on the consumer output column user_agent (it doesn't exist upstream):\n%s", desc)
	}
}

// TestSyncOrchestrationSkipsDisabledNode covers `enabled = false`: a disabled
// transform is omitted from the emitted run (its module stays in main.tf, but
// it isn't in the bundle's transforms list), while enabled nodes still emit.
func TestSyncOrchestrationSkipsDisabledNode(t *testing.T) {
	t.Parallel()
	ws, dir := pipelineForAttachTest(t)
	abs := filepath.Join(ws, dir)
	pipeline := `terraform {
  required_providers { aws = { source = "hashicorp/aws" } }
}
variable "pipeline_name" { type = string default = "demo" }

module "keep" {
  source = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v2.6.0"
  name   = "keep"
  sql    = "SELECT 1"
}

module "skip" {
  source  = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v2.6.0"
  name    = "skip"
  sql     = "SELECT 2"
  enabled = false
}
`
	if err := os.WriteFile(filepath.Join(abs, "main.tf"), []byte(pipeline), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := New(ws).SyncOrchestration(dir, ""); err != nil {
		t.Fatalf("SyncOrchestration: %v", err)
	}
	got, _ := os.ReadFile(filepath.Join(abs, "orchestration.tf"))
	// Collapse whitespace so the `node = "..."` marker matches regardless of
	// the emitter's column alignment.
	flat := strings.Join(strings.Fields(string(got)), " ")
	if !strings.Contains(flat, `node = "keep"`) {
		t.Errorf("enabled node 'keep' missing from orchestration:\n%s", got)
	}
	if strings.Contains(flat, `node = "skip"`) {
		t.Errorf("disabled node 'skip' should be omitted from orchestration:\n%s", got)
	}
}

// TestSyncOrchestrationSkipsDisabledMiddleNode locks the cascade behaviour
// for a disabled node in the MIDDLE of a chain: t1 (enabled) → t2 (disabled)
// → t3 (enabled, reads t2). The emitter must (a) omit t2 from the transforms
// list, (b) keep t3 with its inputs still pointing at module.t2.outputs (the
// module block stays, so downstream reads t2's last-materialized table), and
// (c) drop t2 from t3's `parents` — a disabled upstream never runs, so it
// can't be in the runner's skipped-set and would otherwise block cascade-skip.
func TestSyncOrchestrationSkipsDisabledMiddleNode(t *testing.T) {
	t.Parallel()
	ws, dir := pipelineForAttachTest(t)
	abs := filepath.Join(ws, dir)
	pipeline := `terraform {
  required_providers { aws = { source = "hashicorp/aws" } }
}
variable "pipeline_name" { type = string default = "demo" }

module "t1" {
  source = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v2.6.0"
  name   = "t1"
  sql    = "SELECT 1"
}

module "t2" {
  source  = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v2.6.0"
  name    = "t2"
  sql     = "SELECT * FROM t1"
  enabled = false
  inputs  = { t1 = module.t1.outputs["default"] }
}

module "t3" {
  source = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v2.6.0"
  name   = "t3"
  sql    = "SELECT * FROM t2"
  inputs = { t2 = module.t2.outputs["default"] }
}
`
	if err := os.WriteFile(filepath.Join(abs, "main.tf"), []byte(pipeline), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := New(ws).SyncOrchestration(dir, ""); err != nil {
		t.Fatalf("SyncOrchestration: %v", err)
	}
	got, _ := os.ReadFile(filepath.Join(abs, "orchestration.tf"))
	s := string(got)
	// Collapse whitespace so column-aligned markers match regardless of the
	// emitter's spacing.
	flat := strings.Join(strings.Fields(s), " ")

	// (a) t2 is skipped; t1 and t3 are present.
	if strings.Contains(flat, `node = "t2"`) {
		t.Errorf("disabled middle node 't2' should be omitted from transforms:\n%s", s)
	}
	for _, want := range []string{`node = "t1"`, `node = "t3"`} {
		if !strings.Contains(flat, want) {
			t.Errorf("enabled node marker %q missing:\n%s", want, s)
		}
	}

	// (b) t3's inputs still reference module.t2.outputs — the module block
	// stays so downstream reads the disabled node's materialized table.
	if !strings.Contains(s, `module.t2.outputs`) {
		t.Errorf("t3 should still read module.t2.outputs (disabled node's table):\n%s", s)
	}

	// (c) t3's parents must NOT list t2. t2 is t3's only upstream and it's
	// disabled, so t3 renders an empty parents list.
	if strings.Contains(flat, `parents = ["t2"]`) {
		t.Errorf("t3 parents must exclude disabled upstream t2:\n%s", s)
	}
	if !strings.Contains(flat, `parents = []`) {
		t.Errorf("t3 should render empty parents (only upstream t2 is disabled):\n%s", s)
	}
}

// TestSyncOrchestrationEmitsS3ReadExternalForRegisteredS3 covers the
// v2.2.1 regression fix: a transform attached to a registered kind=s3
// source must populate ExternalBuckets so the per-pipeline Lambda's IAM
// policy gains an S3ReadExternal Statement covering the external
// bucket. Without it the runner 403s reading from any bucket outside
// the workspace bucket — v2.2.0 dropped the per-transform input_buckets
// grant when collapsing to a single per-pipeline role.
func TestSyncOrchestrationEmitsS3ReadExternalForRegisteredS3(t *testing.T) {
	t.Parallel()
	ws, dir := pipelineForAttachTest(t)
	svc := New(ws)
	if _, err := svc.AddSource(SourceSpec{
		Name:   "logs",
		Kind:   "s3",
		Bucket: "external-bucket",
		Prefix: "ev/",
		Format: "parquet",
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
	for _, want := range []string{
		`Sid = "S3ReadExternal"`,
		`"arn:aws:s3:::external-bucket"`,
		`"arn:aws:s3:::external-bucket/*"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("orchestration.tf missing %q\n%s", want, got)
		}
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
