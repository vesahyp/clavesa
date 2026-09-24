package pipelinestatus

// Same-package tests for readStateMachineARN, which is unexported — the
// external pipelinestatus_test package (handler_test.go) can't reach it
// directly. Covers the ADR-025 read-side move onto workspace.ReadStackState:
// local behavior unchanged, remote dispatch through the seam.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/vesahyp/clavesa/internal/workspace"
)

func writeManifestForTest(t *testing.T, root, name string, backend *workspace.Backend) {
	t.Helper()
	m := workspace.Manifest{
		Name:          name,
		Cloud:         "aws",
		Version:       1,
		Catalog:       workspace.DefaultCatalog(name),
		SystemCatalog: workspace.DefaultSystemCatalog(workspace.DefaultCatalog(name)),
		Backend:       backend,
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "clavesa.json"), append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

const sampleStateMachineState = `{
  "version": 4,
  "resources": [
    {
      "type": "aws_sfn_state_machine",
      "name": "pipeline",
      "instances": [
        { "attributes": { "arn": "arn:aws:states:eu-north-1:123456789012:stateMachine:demo" } }
      ]
    }
  ]
}`

func TestReadStateMachineARNLocalPresent(t *testing.T) {
	root := t.TempDir()
	pipelineDir := filepath.Join(root, "demo")
	if err := os.MkdirAll(pipelineDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeManifestForTest(t, root, "ws", nil)
	if err := os.WriteFile(filepath.Join(pipelineDir, "terraform.tfstate"), []byte(sampleStateMachineState), 0o644); err != nil {
		t.Fatal(err)
	}

	arn, err := readStateMachineARN(context.Background(), root, pipelineDir)
	if err != nil {
		t.Fatalf("readStateMachineARN: %v", err)
	}
	want := "arn:aws:states:eu-north-1:123456789012:stateMachine:demo"
	if arn != want {
		t.Fatalf("arn = %q, want %q", arn, want)
	}
}

func TestReadStateMachineARNLocalAbsent(t *testing.T) {
	root := t.TempDir()
	pipelineDir := filepath.Join(root, "demo")
	if err := os.MkdirAll(pipelineDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeManifestForTest(t, root, "ws", nil)

	arn, err := readStateMachineARN(context.Background(), root, pipelineDir)
	if err != nil {
		t.Fatalf("readStateMachineARN: %v", err)
	}
	if arn != "" {
		t.Fatalf("arn = %q, want empty", arn)
	}
}

// TestReadStateMachineARNLegacyNoManifest checks a pipeline dir under a
// workspace with no clavesa.json at all still reads the local tfstate —
// readStateMachineARN's pre-ADR-025 callers never required a manifest.
func TestReadStateMachineARNLegacyNoManifest(t *testing.T) {
	root := t.TempDir()
	pipelineDir := filepath.Join(root, "demo")
	if err := os.MkdirAll(pipelineDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pipelineDir, "terraform.tfstate"), []byte(sampleStateMachineState), 0o644); err != nil {
		t.Fatal(err)
	}

	arn, err := readStateMachineARN(context.Background(), root, pipelineDir)
	if err != nil {
		t.Fatalf("readStateMachineARN: %v", err)
	}
	want := "arn:aws:states:eu-north-1:123456789012:stateMachine:demo"
	if arn != want {
		t.Fatalf("arn = %q, want %q", arn, want)
	}
}

func TestReadStateMachineARNRemote(t *testing.T) {
	root := t.TempDir()
	pipelineDir := filepath.Join(root, "trips")
	if err := os.MkdirAll(pipelineDir, 0o755); err != nil {
		t.Fatal(err)
	}
	backend := &workspace.Backend{Type: "s3", Bucket: "tfstate-bucket", Region: "eu-north-1"}
	writeManifestForTest(t, root, "analytics", backend)

	var gotKey string
	restore := workspace.SetStateGetterForTest(func(ctx context.Context, bucket, region, key string) ([]byte, error) {
		gotKey = key
		return []byte(sampleStateMachineState), nil
	})
	defer restore()

	arn, err := readStateMachineARN(context.Background(), root, pipelineDir)
	if err != nil {
		t.Fatalf("readStateMachineARN: %v", err)
	}
	want := "arn:aws:states:eu-north-1:123456789012:stateMachine:demo"
	if arn != want {
		t.Fatalf("arn = %q, want %q", arn, want)
	}
	if wantKey := "clavesa/analytics/pipelines/trips.tfstate"; gotKey != wantKey {
		t.Fatalf("getter key = %q, want %q", gotKey, wantKey)
	}
}

func TestReadStateMachineARNRemoteNotFound(t *testing.T) {
	root := t.TempDir()
	pipelineDir := filepath.Join(root, "trips")
	if err := os.MkdirAll(pipelineDir, 0o755); err != nil {
		t.Fatal(err)
	}
	backend := &workspace.Backend{Type: "s3", Bucket: "tfstate-bucket", Region: "eu-north-1"}
	writeManifestForTest(t, root, "analytics", backend)

	restore := workspace.SetStateGetterForTest(func(ctx context.Context, bucket, region, key string) ([]byte, error) {
		return nil, nil
	})
	defer restore()

	arn, err := readStateMachineARN(context.Background(), root, pipelineDir)
	if err != nil {
		t.Fatalf("readStateMachineARN: %v", err)
	}
	if arn != "" {
		t.Fatalf("arn = %q, want empty", arn)
	}
}
