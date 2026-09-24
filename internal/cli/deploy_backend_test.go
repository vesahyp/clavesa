package cli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/workspace"
)

// writeBackendManifest writes a clavesa.json carrying an ADR-025 backend
// (or none, when b is nil) into dir — the deploy guard's precondition
// input. Distinct from the package's own writeManifest (helpers_test.go),
// which always writes a no-backend manifest.
func writeBackendManifest(t *testing.T, dir string, b *workspace.Backend) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	m := workspace.Manifest{Name: "demo", Cloud: "aws", Version: 1, Catalog: "clavesa_demo", SystemCatalog: "clavesa_demo_system", Backend: b}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "clavesa.json"), append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

func demoBackend() *workspace.Backend {
	return &workspace.Backend{Type: "s3", Bucket: "demo-tfstate", Region: "eu-north-1"}
}

// TestBackendGuardNoopWithoutBackend confirms the ADR-025 guard is a
// complete no-op for a workspace whose manifest names no backend — no
// backend.tf, no tfstate, nothing to check, and no terraform-version
// call either.
func TestBackendGuardNoopWithoutBackend(t *testing.T) {
	ws := t.TempDir()
	writeBackendManifest(t, ws, nil)
	d := deployFlow{
		WorkspaceRoot: ws,
		TfDir:         ws,
		CheckTerraformVersion: func(context.Context) error {
			t.Fatal("terraform version check must not run for a no-backend workspace")
			return nil
		},
	}
	if err := d.checkBackendGuard(); err != nil {
		t.Fatalf("checkBackendGuard on a no-backend workspace: %v", err)
	}
}

func TestBackendGuardFailsOnTerraformVersion(t *testing.T) {
	ws := t.TempDir()
	writeBackendManifest(t, ws, demoBackend())
	d := deployFlow{
		WorkspaceRoot:         ws,
		TfDir:                 ws,
		CheckTerraformVersion: func(context.Context) error { return errGuardTFVersion },
	}
	if err := d.checkBackendGuard(); err != errGuardTFVersion {
		t.Fatalf("checkBackendGuard terraform-version failure: got %v, want %v", err, errGuardTFVersion)
	}
}

func TestBackendGuardRefusesMissingBackendTF(t *testing.T) {
	ws := t.TempDir()
	writeBackendManifest(t, ws, demoBackend())
	// No backend.tf in TfDir — stack hasn't been migrated.
	d := deployFlow{
		WorkspaceRoot:         ws,
		TfDir:                 ws,
		CheckTerraformVersion: func(context.Context) error { return nil },
	}
	err := d.checkBackendGuard()
	if err == nil || !strings.Contains(err.Error(), "migrate-state") {
		t.Fatalf("checkBackendGuard with no backend.tf: got %v, want an error mentioning migrate-state", err)
	}
}

func TestBackendGuardRefusesNonEmptyLocalState(t *testing.T) {
	ws := t.TempDir()
	writeBackendManifest(t, ws, demoBackend())
	if err := os.WriteFile(filepath.Join(ws, "backend.tf"), []byte("# clavesa-managed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ws, "terraform.tfstate"), []byte(`{"not":"empty"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	d := deployFlow{
		WorkspaceRoot:         ws,
		TfDir:                 ws,
		CheckTerraformVersion: func(context.Context) error { return nil },
	}
	err := d.checkBackendGuard()
	if err == nil || !strings.Contains(err.Error(), "migrate-state") {
		t.Fatalf("checkBackendGuard with a non-empty local tfstate: got %v, want an error mentioning migrate-state", err)
	}
}

// TestBackendGuardPassesMigratedStack: backend.tf present, no local
// state (or an empty/absent one) — the normal post-migration state.
func TestBackendGuardPassesMigratedStack(t *testing.T) {
	ws := t.TempDir()
	writeBackendManifest(t, ws, demoBackend())
	if err := os.WriteFile(filepath.Join(ws, "backend.tf"), []byte("# clavesa-managed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	d := deployFlow{
		WorkspaceRoot:         ws,
		TfDir:                 ws,
		CheckTerraformVersion: func(context.Context) error { return nil },
	}
	if err := d.checkBackendGuard(); err != nil {
		t.Fatalf("checkBackendGuard on a migrated stack: %v", err)
	}

	// A 0-byte tfstate (the state finalizeLocalState would have already
	// cleaned up in practice, but defensively tolerated here too) also passes.
	if err := os.WriteFile(filepath.Join(ws, "terraform.tfstate"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := d.checkBackendGuard(); err != nil {
		t.Fatalf("checkBackendGuard with a 0-byte local tfstate: %v", err)
	}
}

// TestBackendGuardScopedToTfDir checks the guard evaluates backend.tf /
// terraform.tfstate in TfDir (the pipeline dir for `pipeline deploy`),
// not WorkspaceRoot — a migrated workspace root doesn't excuse an
// unmigrated pipeline.
func TestBackendGuardScopedToTfDir(t *testing.T) {
	ws := t.TempDir()
	writeBackendManifest(t, ws, demoBackend())
	if err := os.WriteFile(filepath.Join(ws, "backend.tf"), []byte("# clavesa-managed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	pipelineDir := filepath.Join(ws, "trips")
	if err := os.MkdirAll(pipelineDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// No backend.tf in the pipeline dir.
	d := deployFlow{
		WorkspaceRoot:         ws,
		TfDir:                 pipelineDir,
		CheckTerraformVersion: func(context.Context) error { return nil },
	}
	err := d.checkBackendGuard()
	if err == nil || !strings.Contains(err.Error(), "migrate-state") {
		t.Fatalf("checkBackendGuard for an unmigrated pipeline under a migrated workspace: got %v, want a refusal", err)
	}
}

var errGuardTFVersion = errGuard("terraform too old for a remote backend")

type errGuard string

func (e errGuard) Error() string { return string(e) }
