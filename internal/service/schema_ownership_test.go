package service

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// newSchemaOwnershipWS lays down a workspace manifest so CreatePipeline can
// resolve the workspace catalog. No hand-authored .tf — every pipeline below
// is created through the real CreatePipeline path.
func newSchemaOwnershipWS(t *testing.T) (string, *Service) {
	t.Helper()
	ws := t.TempDir()
	manifest := `{"name":"so-ws","cloud":"aws","version":1,"catalog":"clavesa_so_ws"}` + "\n"
	if err := os.WriteFile(filepath.Join(ws, "clavesa.json"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	return ws, New(ws)
}

// TestCreatePipelineRefusesDuplicateSchema is the core ADR-016 §5 guard: two
// pipelines may not write into the same <catalog>.<schema>.
func TestCreatePipelineRefusesDuplicateSchema(t *testing.T) {
	ws, svc := newSchemaOwnershipWS(t)

	if _, err := svc.CreatePipeline("alpha", "shared"); err != nil {
		t.Fatalf("CreatePipeline alpha: %v", err)
	}

	_, err := svc.CreatePipeline("beta", "shared")
	if err == nil {
		t.Fatal("CreatePipeline beta: expected a schema-ownership error, got nil")
	}
	if !strings.Contains(err.Error(), "alpha") || !strings.Contains(err.Error(), "shared") {
		t.Errorf("error should name the conflicting pipeline and schema, got: %v", err)
	}
	// A rejected create must leave no directory behind.
	if _, statErr := os.Stat(filepath.Join(ws, "beta")); !os.IsNotExist(statErr) {
		t.Errorf("rejected create left a beta/ directory behind (stat err: %v)", statErr)
	}
}

// TestCreatePipelineDefaultSchemasNeverCollide confirms the common path —
// schema defaults to the sanitized pipeline name, which is unique per dir.
func TestCreatePipelineDefaultSchemasNeverCollide(t *testing.T) {
	_, svc := newSchemaOwnershipWS(t)
	if _, err := svc.CreatePipeline("alpha", ""); err != nil {
		t.Fatalf("CreatePipeline alpha: %v", err)
	}
	if _, err := svc.CreatePipeline("beta", ""); err != nil {
		t.Fatalf("CreatePipeline beta: %v", err)
	}
}

// TestCreatePipelineSchemaEqualToOwnNameSucceeds — a pipeline explicitly
// passing its own name as the schema is not a conflict with itself.
func TestCreatePipelineSchemaEqualToOwnNameSucceeds(t *testing.T) {
	_, svc := newSchemaOwnershipWS(t)
	if _, err := svc.CreatePipeline("alpha", "alpha"); err != nil {
		t.Fatalf("CreatePipeline alpha --schema alpha: %v", err)
	}
}
