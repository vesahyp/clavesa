package workspace_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/workspace"
)

// TestManifestMarshalNoBackend pins the ADR-025 promise that a manifest
// without a backend marshals exactly as it did before this field existed
// — omitempty on a nil *Backend must drop the key entirely, not emit
// "backend": null.
func TestManifestMarshalNoBackend(t *testing.T) {
	m := workspace.Manifest{
		Name:          "demo",
		Cloud:         "aws",
		Version:       1,
		Catalog:       "clavesa_demo",
		SystemCatalog: "clavesa_demo_system",
	}
	got, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want := `{
  "name": "demo",
  "cloud": "aws",
  "version": 1,
  "catalog": "clavesa_demo",
  "system_catalog": "clavesa_demo_system"
}`
	if string(got) != want {
		t.Fatalf("Marshal (no backend) =\n%s\nwant\n%s", got, want)
	}
}

// TestManifestMarshalRoundTripWithBackend checks a manifest carrying a
// backend survives marshal → unmarshal unchanged, and that the wire shape
// matches ADR-025's "Manifest field" example.
func TestManifestMarshalRoundTripWithBackend(t *testing.T) {
	m := workspace.Manifest{
		Name:          "analytics",
		Cloud:         "aws",
		Version:       1,
		Catalog:       "clavesa_analytics",
		SystemCatalog: "clavesa_analytics_system",
		Backend: &workspace.Backend{
			Type:      "s3",
			Bucket:    "analytics-tfstate",
			Region:    "eu-north-1",
			KeyPrefix: "clavesa/",
		},
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !strings.Contains(string(data), `"backend": {`) {
		t.Fatalf("Marshal missing backend block:\n%s", data)
	}

	var back workspace.Manifest
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if back.Backend == nil {
		t.Fatal("round-trip: Backend is nil, want non-nil")
	}
	if *back.Backend != *m.Backend {
		t.Fatalf("round-trip Backend = %+v, want %+v", *back.Backend, *m.Backend)
	}
}

// TestLoadOldManifestNoBackend checks a manifest predating this field
// loads with Backend == nil and is not rewritten to disk.
func TestLoadOldManifestNoBackend(t *testing.T) {
	dir := t.TempDir()
	raw := `{
  "name": "legacy",
  "cloud": "aws",
  "version": 1,
  "catalog": "clavesa_legacy",
  "system_catalog": "clavesa_legacy_system"
}
`
	path := filepath.Join(dir, "clavesa.json")
	if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}

	m, err := workspace.Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if m.Backend != nil {
		t.Fatalf("Backend = %+v, want nil", m.Backend)
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != raw {
		t.Fatalf("clavesa.json rewritten by Load with no backend present:\ngot\n%s\nwant\n%s", after, raw)
	}
}

// TestLoadRejectsInvalidBackend checks Load surfaces Backend.Validate's
// error rather than silently accepting a broken manifest.
func TestLoadRejectsInvalidBackend(t *testing.T) {
	dir := t.TempDir()
	raw := `{
  "name": "broken",
  "cloud": "aws",
  "version": 1,
  "catalog": "clavesa_broken",
  "system_catalog": "clavesa_broken_system",
  "backend": {
    "type": "s3",
    "bucket": "",
    "region": "eu-north-1"
  }
}
`
	if err := os.WriteFile(filepath.Join(dir, "clavesa.json"), []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := workspace.Load(dir); err == nil {
		t.Fatal("Load with empty backend.bucket: got nil error, want one")
	}
}

func TestBackendValidate(t *testing.T) {
	cases := []struct {
		name    string
		backend workspace.Backend
		wantErr bool
	}{
		{
			name:    "valid, no key_prefix",
			backend: workspace.Backend{Type: "s3", Bucket: "b", Region: "eu-north-1"},
		},
		{
			name:    "valid, with key_prefix",
			backend: workspace.Backend{Type: "s3", Bucket: "b", Region: "eu-north-1", KeyPrefix: "team/"},
		},
		{
			name:    "wrong type",
			backend: workspace.Backend{Type: "gcs", Bucket: "b", Region: "eu-north-1"},
			wantErr: true,
		},
		{
			name:    "missing bucket",
			backend: workspace.Backend{Type: "s3", Region: "eu-north-1"},
			wantErr: true,
		},
		{
			name:    "missing region",
			backend: workspace.Backend{Type: "s3", Bucket: "b"},
			wantErr: true,
		},
		{
			name:    "key_prefix missing trailing slash",
			backend: workspace.Backend{Type: "s3", Bucket: "b", Region: "eu-north-1", KeyPrefix: "team"},
			wantErr: true,
		},
		{
			name:    "key_prefix starts with slash",
			backend: workspace.Backend{Type: "s3", Bucket: "b", Region: "eu-north-1", KeyPrefix: "/team/"},
			wantErr: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.backend.Validate()
			if c.wantErr && err == nil {
				t.Fatal("Validate: got nil error, want one")
			}
			if !c.wantErr && err != nil {
				t.Fatalf("Validate: %v", err)
			}
		})
	}
}

func TestBackendKeyHelpers(t *testing.T) {
	// Default prefix.
	b := workspace.Backend{Type: "s3", Bucket: "b", Region: "eu-north-1"}
	if got, want := b.KeyPrefixOrDefault(), "clavesa/"; got != want {
		t.Errorf("KeyPrefixOrDefault() = %q, want %q", got, want)
	}
	if got, want := b.WorkspaceStateKey("analytics"), "clavesa/analytics/workspace.tfstate"; got != want {
		t.Errorf("WorkspaceStateKey() = %q, want %q", got, want)
	}
	if got, want := b.PipelineStateKey("analytics", "trips"), "clavesa/analytics/pipelines/trips.tfstate"; got != want {
		t.Errorf("PipelineStateKey() = %q, want %q", got, want)
	}
	// pipelineDir as a full path: only the base name is used.
	if got, want := b.PipelineStateKey("analytics", "/home/user/ws/trips"), "clavesa/analytics/pipelines/trips.tfstate"; got != want {
		t.Errorf("PipelineStateKey() with full path = %q, want %q", got, want)
	}

	// Custom prefix.
	b.KeyPrefix = "team/"
	if got, want := b.KeyPrefixOrDefault(), "team/"; got != want {
		t.Errorf("KeyPrefixOrDefault() = %q, want %q", got, want)
	}
	if got, want := b.WorkspaceStateKey("analytics"), "team/analytics/workspace.tfstate"; got != want {
		t.Errorf("WorkspaceStateKey() = %q, want %q", got, want)
	}
	if got, want := b.PipelineStateKey("analytics", "trips"), "team/analytics/pipelines/trips.tfstate"; got != want {
		t.Errorf("PipelineStateKey() = %q, want %q", got, want)
	}
}
