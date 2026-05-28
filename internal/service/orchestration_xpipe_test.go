package service

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSyncOrchestrationEmitsCrossPipelineTrigger asserts the
// orchestration emitter detects a sibling-pipeline producer from a
// transform's `inputs = { x = "<schema>.<table>" }` reference and
// emits the EventBridge rule + IAM + target that auto-starts this
// pipeline on the producer's SUCCEEDED execution event (ADR-016 §6).
func TestSyncOrchestrationEmitsCrossPipelineTrigger(t *testing.T) {
	t.Parallel()
	ws := xpipeWorkspace(t)
	writePipelineMain(t, ws, "bronze", `module "raw" {
  source = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v0.18.0"
  name     = "raw"
  language = "sql"
  sql      = "SELECT 1"
}
`)
	writePipelineMain(t, ws, "silver", `module "enriched" {
  source = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v0.18.0"
  name     = "enriched"
  language = "sql"
  sql      = "SELECT * FROM bronze"
  inputs   = { bronze = "bronze.raw" }
}
`)

	svc := New(ws)
	if err := svc.SyncOrchestration("silver", ""); err != nil {
		t.Fatalf("SyncOrchestration: %v", err)
	}
	body := mustRead(t, filepath.Join(ws, "silver", "orchestration.tf"))

	wants := []string{
		`resource "aws_cloudwatch_event_rule" "upstream_bronze"`,
		`stateMachine:clavesa-bronze`,
		`status          = ["SUCCEEDED"]`,
		`resource "aws_iam_role" "upstream_trigger_bronze"`,
		`resource "aws_cloudwatch_event_target" "upstream_bronze"`,
		`_trigger           = "upstream"`,
		`_upstream_pipeline = "bronze"`,
	}
	for _, w := range wants {
		if !strings.Contains(body, w) {
			t.Errorf("silver/orchestration.tf missing %q", w)
		}
	}
}

// TestSyncOrchestrationSkipsUnresolvedCrossPipelineRefs asserts that
// a `<schema>.<table>` reference whose schema doesn't match any sibling
// pipeline (typo, external Glue table) emits no upstream trigger —
// there's nothing to listen to.
func TestSyncOrchestrationSkipsUnresolvedCrossPipelineRefs(t *testing.T) {
	t.Parallel()
	ws := xpipeWorkspace(t)
	writePipelineMain(t, ws, "silver", `module "enriched" {
  source = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v0.18.0"
  name     = "enriched"
  language = "sql"
  sql      = "SELECT * FROM external_db"
  inputs   = { x = "ghost.raw" }
}
`)

	svc := New(ws)
	if err := svc.SyncOrchestration("silver", ""); err != nil {
		t.Fatalf("SyncOrchestration: %v", err)
	}
	body := mustRead(t, filepath.Join(ws, "silver", "orchestration.tf"))

	doesNotContain := []string{
		`"clavesa_upstream"`,
		`"upstream_`,
		`_upstream_pipeline`,
	}
	for _, w := range doesNotContain {
		if strings.Contains(body, w) {
			t.Errorf("orchestration.tf contains %q despite unresolved ref", w)
		}
	}
}

// TestSyncOrchestrationDeduplicatesUpstreamProducers asserts a pipeline
// that reads multiple tables from the same upstream pipeline emits one
// rule per producer, not one rule per reference. EventBridge fires the
// rule once per producer execution regardless of how many tables the
// consumer reads from it.
func TestSyncOrchestrationDeduplicatesUpstreamProducers(t *testing.T) {
	t.Parallel()
	ws := xpipeWorkspace(t)
	writePipelineMain(t, ws, "bronze", `module "raw_a" {
  source = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v0.18.0"
  name     = "raw_a"
  language = "sql"
  sql      = "SELECT 1"
}

module "raw_b" {
  source = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v0.18.0"
  name     = "raw_b"
  language = "sql"
  sql      = "SELECT 2"
}
`)
	writePipelineMain(t, ws, "silver", `module "enriched" {
  source = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v0.18.0"
  name     = "enriched"
  language = "sql"
  sql      = "SELECT * FROM a, b"
  inputs   = { a = "bronze.raw_a", b = "bronze.raw_b" }
}
`)
	svc := New(ws)
	if err := svc.SyncOrchestration("silver", ""); err != nil {
		t.Fatalf("SyncOrchestration: %v", err)
	}
	body := mustRead(t, filepath.Join(ws, "silver", "orchestration.tf"))
	count := strings.Count(body, `resource "aws_cloudwatch_event_rule" "upstream_bronze"`)
	if count != 1 {
		t.Errorf("expected exactly one upstream_bronze rule, got %d", count)
	}
}

// xpipeWorkspace stamps a workspace manifest. Hand-authored .tf per
// pipeline gets dropped in by writePipelineMain.
func xpipeWorkspace(t *testing.T) string {
	t.Helper()
	ws := t.TempDir()
	manifest := `{"name":"xp-ws","cloud":"aws","version":1,"catalog":"clavesa_xp_ws"}` + "\n"
	if err := os.WriteFile(filepath.Join(ws, "clavesa.json"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	return ws
}

func writePipelineMain(t *testing.T, ws, name, modules string) {
	t.Helper()
	dir := filepath.Join(ws, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	main := `terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
  }
}

variable "pipeline_name" {
  type    = string
  default = "` + name + `"
}

` + modules
	if err := os.WriteFile(filepath.Join(dir, "main.tf"), []byte(main), 0o644); err != nil {
		t.Fatal(err)
	}
}

func mustRead(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}
