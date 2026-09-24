package service

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/workspace"
)

// backendManifest returns a manifest carrying an ADR-025 remote backend,
// namespaced by name so tests using different workspace names get
// non-colliding state keys.
func backendManifest(name string) workspace.Manifest {
	return workspace.Manifest{
		Name:          name,
		Cloud:         "aws",
		Version:       1,
		Catalog:       "clavesa_" + name,
		SystemCatalog: "clavesa_" + name + "_system",
		Backend: &workspace.Backend{
			Type:   "s3",
			Bucket: name + "-tfstate",
			Region: "eu-north-1",
		},
	}
}

func writeManifest(t *testing.T, ws string, m workspace.Manifest) {
	t.Helper()
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ws, "clavesa.json"), append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

// noLocalBackendLeftovers fails the test if any .tf file directly under
// dir carries a local backend or a local terraform_remote_state block —
// the ADR-025 "Regen keeps the backend" invariant every regen path must
// hold once a stack has a backend.tf.
func noLocalBackendLeftovers(t *testing.T, dir string) {
	t.Helper()
	tfFiles, err := filepath.Glob(filepath.Join(dir, "*.tf"))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range tfFiles {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		s := string(data)
		if strings.Contains(s, `backend "local"`) {
			t.Errorf("%s carries a local backend block after a regen on a backend-configured stack:\n%s", f, s)
		}
		if strings.Contains(s, "terraform_remote_state") && strings.Contains(s, `backend = "local"`) {
			t.Errorf("%s carries a local terraform_remote_state block after a regen on a backend-configured stack:\n%s", f, s)
		}
	}
}

// TestCreatePipelineNoBackendMainTFByteIdentical pins the ADR-025 promise
// that a no-backend workspace's newly created pipeline emits exactly the
// same main.tf as before this slice.
func TestCreatePipelineNoBackendMainTFByteIdentical(t *testing.T) {
	ws := t.TempDir()
	m := workspace.Manifest{Name: "smoke-ws", Cloud: "aws", Version: 1, Catalog: "clavesa_smoke_ws", SystemCatalog: "clavesa_smoke_ws_system"}
	writeManifest(t, ws, m)
	svc := New(ws)
	if _, err := svc.CreatePipeline("demo", ""); err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(ws, "demo", "main.tf"))
	if err != nil {
		t.Fatal(err)
	}
	want := `# clavesa pipeline
terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
  }
}

data "terraform_remote_state" "workspace" {
  backend = "local"
  config  = { path = "${path.module}/../terraform.tfstate" }
}
`
	if string(got) != want {
		t.Errorf("main.tf changed for a no-backend workspace:\ngot\n%s\nwant\n%s", got, want)
	}
	if _, err := os.Stat(filepath.Join(ws, "demo", "backend.tf")); !os.IsNotExist(err) {
		t.Errorf("backend.tf written for a no-backend workspace (stat err=%v)", err)
	}
}

// TestCreatePipelineLegacyWorkspaceMainTFByteIdentical covers the other
// no-backend branch: no clavesa.json at all (standalone pipeline).
func TestCreatePipelineLegacyWorkspaceMainTFByteIdentical(t *testing.T) {
	ws := t.TempDir()
	svc := New(ws)
	if _, err := svc.CreatePipeline("demo", ""); err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(ws, "demo", "main.tf"))
	if err != nil {
		t.Fatal(err)
	}
	want := `# clavesa pipeline
terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
  }
}

resource "aws_s3_bucket" "pipeline_bucket" {
  bucket        = "clavesa-demo"
  force_destroy = true
}
`
	if string(got) != want {
		t.Errorf("main.tf changed for a legacy standalone pipeline:\ngot\n%s\nwant\n%s", got, want)
	}
}

// TestCreatePipelineWithBackendEmitsBackendTF is CreatePipeline's ADR-025
// emit site: main.tf carries neither a backend nor a terraform_remote_state
// block, and a clavesa-owned backend.tf carries both.
func TestCreatePipelineWithBackendEmitsBackendTF(t *testing.T) {
	ws := t.TempDir()
	m := backendManifest("analytics")
	writeManifest(t, ws, m)
	svc := New(ws)
	if _, err := svc.CreatePipeline("trips", ""); err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	dir := filepath.Join(ws, "trips")

	mainTF, err := os.ReadFile(filepath.Join(dir, "main.tf"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(mainTF), "terraform_remote_state") {
		t.Errorf("main.tf still carries terraform_remote_state for a backend-configured workspace:\n%s", mainTF)
	}
	if strings.Contains(string(mainTF), `backend "local"`) {
		t.Errorf("main.tf still carries backend \"local\" for a backend-configured workspace:\n%s", mainTF)
	}

	backendTF, err := os.ReadFile(filepath.Join(dir, "backend.tf"))
	if err != nil {
		t.Fatalf("backend.tf not written: %v", err)
	}
	body := string(backendTF)
	for _, want := range []string{
		`bucket       = "analytics-tfstate"`,
		`key          = "clavesa/analytics/pipelines/trips.tfstate"`,
		`region       = "eu-north-1"`,
		`use_lockfile = true`,
		`data "terraform_remote_state" "workspace"`,
		`key    = "clavesa/analytics/workspace.tfstate"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("backend.tf missing %q:\n%s", want, body)
		}
	}
	noLocalBackendLeftovers(t, dir)
}

// TestSyncOrchestrationRefreshesBackendTF is the SyncOrchestration regen
// path (ADR-025 "Regen keeps the backend"): backend.tf tracks a manifest
// bucket edit on the next sync.
func TestSyncOrchestrationRefreshesBackendTF(t *testing.T) {
	ws := t.TempDir()
	m := backendManifest("analytics")
	writeManifest(t, ws, m)
	svc := New(ws)
	// SyncOrchestration requires at least one transform (tfgen.Emit); a
	// bare CreatePipeline (no nodes) leaves orchestration.tf unwritten, so
	// give it one via the real authoring API, same as addListablePipeline.
	if _, err := svc.CreatePipeline("trips", ""); err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	if _, err := svc.AddNode("trips", "transform", "t1"); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if _, err := svc.UpdateNode("trips", "t1", map[string]interface{}{"sql": "SELECT 1 AS x"}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	dir := filepath.Join(ws, "trips")

	m.Backend.Bucket = "analytics-tfstate-v2"
	writeManifest(t, ws, m)
	if err := svc.SyncOrchestration("trips", ""); err != nil {
		t.Fatalf("SyncOrchestration: %v", err)
	}
	after, err := os.ReadFile(filepath.Join(dir, "backend.tf"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(after), `bucket       = "analytics-tfstate-v2"`) {
		t.Errorf("backend.tf not refreshed after a manifest bucket change:\n%s", after)
	}
	if strings.Contains(string(after), `"analytics-tfstate"`) {
		t.Errorf("backend.tf still references the stale bucket:\n%s", after)
	}
	noLocalBackendLeftovers(t, dir)
}

// TestUpgradePipelineRefreshesBackendTF is the UpgradePipeline regen path.
func TestUpgradePipelineRefreshesBackendTF(t *testing.T) {
	ws := t.TempDir()
	m := backendManifest("analytics")
	writeManifest(t, ws, m)
	svc := New(ws)
	if _, err := svc.CreatePipeline("trips", ""); err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	dir := filepath.Join(ws, "trips")

	m.Backend.Bucket = "analytics-tfstate-v2"
	writeManifest(t, ws, m)

	if _, _, _, _, err := svc.UpgradePipeline("trips", ModuleVersion); err != nil {
		t.Fatalf("UpgradePipeline: %v", err)
	}
	backendTF, err := os.ReadFile(filepath.Join(dir, "backend.tf"))
	if err != nil {
		t.Fatalf("backend.tf missing after upgrade: %v", err)
	}
	if !strings.Contains(string(backendTF), `bucket       = "analytics-tfstate-v2"`) {
		t.Errorf("backend.tf not refreshed by UpgradePipeline:\n%s", backendTF)
	}
	noLocalBackendLeftovers(t, dir)
}

// initBackendUpgradeTestWorkspace lays out a workspace shell (main.tf +
// variables.tf, like initUpgradeTestWorkspace in workspace_upgrade_test.go)
// under a manifest that already carries an ADR-025 backend.
func initBackendUpgradeTestWorkspace(t *testing.T, m workspace.Manifest) string {
	t.Helper()
	ws := t.TempDir()
	writeManifest(t, ws, m)
	mainTF := `module "workspace" {
  source         = "./.clavesa/modules/v0.1.0/workspace/aws"
  workspace_name = var.workspace_name
}
`
	if err := os.WriteFile(filepath.Join(ws, "main.tf"), []byte(mainTF), 0o644); err != nil {
		t.Fatal(err)
	}
	varsTF := `variable "workspace_name" {
  default = "` + m.Name + `"
}

variable "runner_version" {
  description = "Transform runner image version tag (must be built locally before apply)."
  default     = "v0.1.0"
}
`
	if err := os.WriteFile(filepath.Join(ws, "variables.tf"), []byte(varsTF), 0o644); err != nil {
		t.Fatal(err)
	}
	// A migrated shell: regen refreshes backend.tf, it never creates one.
	if m.Backend != nil {
		if err := workspace.WriteWorkspaceBackendTF(ws, &m); err != nil {
			t.Fatal(err)
		}
	}
	return ws
}

// TestRegenDoesNotCreateBackendTFBeforeMigration pins the other half of
// "Regen keeps the backend": a workspace whose manifest names a backend
// but whose stacks were never migrated still has the local backend in
// main.tf. Regen must not write backend.tf beside it, or terraform sees
// two backends. Creating backend.tf is create's and migrate-state's job.
func TestRegenDoesNotCreateBackendTFBeforeMigration(t *testing.T) {
	local := backendManifest("analytics")
	local.Backend = nil
	ws := initBackendUpgradeTestWorkspace(t, local)
	svc := New(ws)
	addListablePipeline(t, svc, "trips")

	writeManifest(t, ws, backendManifest("analytics"))

	if err := svc.SyncOrchestration("trips", ""); err != nil {
		t.Fatalf("SyncOrchestration: %v", err)
	}
	res, err := svc.UpgradeWorkspace("", true)
	if err != nil {
		t.Fatalf("UpgradeWorkspace: %v", err)
	}
	for _, p := range res.Pipelines {
		if p.Err != "" {
			t.Fatalf("pipeline %s upgrade failed: %s", p.Name, p.Err)
		}
	}
	for _, dir := range []string{ws, filepath.Join(ws, "trips")} {
		if _, err := os.Stat(filepath.Join(dir, "backend.tf")); !os.IsNotExist(err) {
			t.Errorf("regen created %s/backend.tf in an unmigrated stack (stat err %v)", dir, err)
		}
	}
}

// TestUpgradeWorkspaceRefreshesBackendTFForShellAndPipelines is the
// UpgradeWorkspace regen path — the workspace-shell backend.tf (via the
// embedded workspace.Upgrade call) and every pipeline's backend.tf (via
// the per-pipeline UpgradePipeline loop) both track a manifest bucket
// edit on the same run.
func TestUpgradeWorkspaceRefreshesBackendTFForShellAndPipelines(t *testing.T) {
	m := backendManifest("analytics")
	ws := initBackendUpgradeTestWorkspace(t, m)
	svc := New(ws)
	addListablePipeline(t, svc, "trips")

	// Change the bucket after the pipeline's initial backend.tf was
	// written by CreatePipeline, so the upgrade has something to refresh.
	m.Backend.Bucket = "analytics-tfstate-v2"
	writeManifest(t, ws, m)

	res, err := svc.UpgradeWorkspace("", true)
	if err != nil {
		t.Fatalf("UpgradeWorkspace: %v", err)
	}
	for _, p := range res.Pipelines {
		if p.Err != "" {
			t.Fatalf("pipeline %s upgrade failed: %s", p.Name, p.Err)
		}
	}

	wsBackendTF, err := os.ReadFile(filepath.Join(ws, "backend.tf"))
	if err != nil {
		t.Fatalf("workspace backend.tf not written by UpgradeWorkspace: %v", err)
	}
	if !strings.Contains(string(wsBackendTF), `bucket       = "analytics-tfstate-v2"`) {
		t.Errorf("workspace backend.tf not refreshed:\n%s", wsBackendTF)
	}
	if !strings.Contains(string(wsBackendTF), `key          = "clavesa/analytics/workspace.tfstate"`) {
		t.Errorf("workspace backend.tf missing the workspace state key:\n%s", wsBackendTF)
	}

	pipelineBackendTF, err := os.ReadFile(filepath.Join(ws, "trips", "backend.tf"))
	if err != nil {
		t.Fatalf("pipeline backend.tf missing: %v", err)
	}
	if !strings.Contains(string(pipelineBackendTF), `bucket       = "analytics-tfstate-v2"`) {
		t.Errorf("pipeline backend.tf not refreshed by UpgradeWorkspace's per-pipeline upgrade:\n%s", pipelineBackendTF)
	}

	noLocalBackendLeftovers(t, ws)
	noLocalBackendLeftovers(t, filepath.Join(ws, "trips"))
}
