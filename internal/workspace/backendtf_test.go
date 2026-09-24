package workspace_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/workspace"
)

// analyticsBackendManifest mirrors the ADR-025 "Manifest field" worked
// example exactly, so the assertions below can be checked against the
// ADR's own rendered backend.tf.
func analyticsBackendManifest() *workspace.Manifest {
	return &workspace.Manifest{
		Name:          "analytics",
		Cloud:         "aws",
		Version:       1,
		Catalog:       "clavesa_analytics",
		SystemCatalog: "clavesa_analytics_system",
		Backend: &workspace.Backend{
			Type:   "s3",
			Bucket: "analytics-tfstate",
			Region: "eu-north-1",
		},
	}
}

func TestRenderWorkspaceBackendTF(t *testing.T) {
	got := workspace.RenderWorkspaceBackendTF(analyticsBackendManifest())
	for _, want := range []string{
		`terraform {`,
		`backend "s3" {`,
		`bucket       = "analytics-tfstate"`,
		`key          = "clavesa/analytics/workspace.tfstate"`,
		`region       = "eu-north-1"`,
		`encrypt      = true`,
		`use_lockfile = true`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("RenderWorkspaceBackendTF missing %q:\n%s", want, got)
		}
	}
	// Generated-file header, so a hand edit is a clear mistake.
	if !strings.Contains(got, "generated from clavesa.json") {
		t.Errorf("RenderWorkspaceBackendTF missing the generated-file header:\n%s", got)
	}
	// The workspace's own backend.tf never carries a remote_state read —
	// that belongs only in a pipeline's backend.tf.
	if strings.Contains(got, "terraform_remote_state") {
		t.Errorf("RenderWorkspaceBackendTF must not carry a terraform_remote_state block:\n%s", got)
	}
}

func TestRenderPipelineBackendTF(t *testing.T) {
	m := analyticsBackendManifest()
	got := workspace.RenderPipelineBackendTF(m, "/home/user/ws/trips")
	for _, want := range []string{
		`backend "s3" {`,
		`key          = "clavesa/analytics/pipelines/trips.tfstate"`,
		`bucket       = "analytics-tfstate"`,
		`region       = "eu-north-1"`,
		`use_lockfile = true`,
		`data "terraform_remote_state" "workspace" {`,
		`backend = "s3"`,
		`bucket = "analytics-tfstate"`,
		`key    = "clavesa/analytics/workspace.tfstate"`,
		`region = "eu-north-1"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("RenderPipelineBackendTF missing %q:\n%s", want, got)
		}
	}
	// A bare dir name (no path separators) must resolve the same way —
	// PipelineStateKey takes filepath.Base internally either way.
	bare := workspace.RenderPipelineBackendTF(m, "trips")
	if bare != got {
		t.Errorf("RenderPipelineBackendTF(bare name) != RenderPipelineBackendTF(full path):\nbare:\n%s\nfull:\n%s", bare, got)
	}
}

func TestWriteWorkspaceBackendTF(t *testing.T) {
	dir := t.TempDir()
	m := analyticsBackendManifest()
	if err := workspace.WriteWorkspaceBackendTF(dir, m); err != nil {
		t.Fatalf("WriteWorkspaceBackendTF: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "backend.tf"))
	if err != nil {
		t.Fatalf("read backend.tf: %v", err)
	}
	if string(got) != workspace.RenderWorkspaceBackendTF(m) {
		t.Errorf("written backend.tf does not match RenderWorkspaceBackendTF's output")
	}
}

func TestWritePipelineBackendTF(t *testing.T) {
	dir := t.TempDir()
	pipelineDir := filepath.Join(dir, "trips")
	if err := os.MkdirAll(pipelineDir, 0o755); err != nil {
		t.Fatal(err)
	}
	m := analyticsBackendManifest()
	if err := workspace.WritePipelineBackendTF(pipelineDir, m); err != nil {
		t.Fatalf("WritePipelineBackendTF: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(pipelineDir, "backend.tf"))
	if err != nil {
		t.Fatalf("read backend.tf: %v", err)
	}
	if string(got) != workspace.RenderPipelineBackendTF(m, pipelineDir) {
		t.Errorf("written backend.tf does not match RenderPipelineBackendTF's output")
	}
}
