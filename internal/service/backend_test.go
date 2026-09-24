package service

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/workspace"
)

// ---------------------------------------------------------------------------
// Test fixtures shared by the SetBackend / MigrateState / stripping tests.
// backendManifest, writeManifest and noLocalBackendLeftovers come from
// backend_regen_test.go (same package).
// ---------------------------------------------------------------------------

// newLocalBackendWorkspace lays out a workspace exactly as `workspace
// init` + `pipeline create` leave it before any backend is configured:
// a manifest with no `backend` field, and a root main.tf carrying the
// real `backend "local" {}` line (via workspace.WorkspaceMainTF, not a
// hand-written fixture) so the strip logic is exercised against
// production output.
func newLocalBackendWorkspace(t *testing.T, name string) (ws string, m workspace.Manifest) {
	t.Helper()
	ws = t.TempDir()
	m = workspace.Manifest{Name: name, Cloud: "aws", Version: 1, Catalog: "clavesa_" + name, SystemCatalog: "clavesa_" + name + "_system"}
	writeManifest(t, ws, m)
	mainTF := workspace.WorkspaceMainTF(&m, "v1.0.0")
	if err := os.WriteFile(filepath.Join(ws, "main.tf"), []byte(mainTF), 0o644); err != nil {
		t.Fatal(err)
	}
	return ws, m
}

// writeFakeLocalState drops a non-empty terraform.tfstate into dir,
// simulating a stack that's already been deployed with local state.
func writeFakeLocalState(t *testing.T, dir string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "terraform.tfstate"), []byte(`{"version":4,"resources":["fake"]}`), 0o644); err != nil {
		t.Fatal(err)
	}
}

// fakeS3Store is an in-memory stand-in for the state bucket, shared
// between the stateObjectStatus and runTerraform seams below so a
// simulated `terraform init -migrate-state` can "land" an object that a
// later stateObjectStatus call sees — the same before/after check
// MigrateState itself relies on.
type fakeS3Store struct {
	objects map[string][]byte
}

func newFakeS3Store() *fakeS3Store {
	return &fakeS3Store{objects: map[string][]byte{}}
}

func (f *fakeS3Store) status(_ context.Context, _, _, key string) (bool, int64, error) {
	data, ok := f.objects[key]
	return ok, int64(len(data)), nil
}

// defaultFakeInit simulates a successful `terraform init -migrate-state
// -force-copy`: it moves dir's local terraform.tfstate content into the
// fake store at the stack's key, then leaves a 0-byte terraform.tfstate
// and the original content in terraform.tfstate.backup — the exact
// on-disk shape real terraform leaves, per ADR-025 step "d".
func defaultFakeInit(store *fakeS3Store, m *workspace.Manifest, ws string) func(dir string) (int, error) {
	return func(dir string) (int, error) {
		statePath := filepath.Join(dir, "terraform.tfstate")
		data, err := os.ReadFile(statePath)
		if err != nil {
			return 1, nil // nothing to migrate; callers shouldn't reach here in that case
		}
		key := stackStateKey(m, ws, dir)
		store.objects[key] = data
		if err := os.WriteFile(filepath.Join(dir, "terraform.tfstate.backup"), data, 0o644); err != nil {
			return 1, err
		}
		if err := os.WriteFile(statePath, nil, 0o644); err != nil {
			return 1, err
		}
		return 0, nil
	}
}

// wireBackendSeams stubs every MigrateState precondition/execution seam
// so tests need neither AWS credentials nor a terraform binary.
// initHook controls what "terraform init -migrate-state" does per
// stack dir; planExit fixes what every "terraform plan
// -detailed-exitcode" call reports.
func wireBackendSeams(svc *Service, store *fakeS3Store, planExit int, initHook func(dir string) (int, error)) {
	svc.checkTerraformVersion = func(context.Context) error { return nil }
	svc.checkStateBucket = func(context.Context, string, string) error { return nil }
	svc.stateObjectStatus = store.status
	svc.runTerraform = func(_ context.Context, dir string, _, _ io.Writer, args ...string) (int, error) {
		if len(args) == 0 {
			return 0, nil
		}
		switch {
		case isMigrateInit(args):
			return initHook(dir)
		case args[0] == "init":
			return 0, nil // the plain init before plan
		case args[0] == "plan":
			return planExit, nil
		}
		return 0, nil
	}
}

// isMigrateInit reports whether args are the state-moving
// `terraform init -migrate-state ...`, as opposed to the plain
// `terraform init` MigrateState runs before each plan.
func isMigrateInit(args []string) bool {
	if len(args) == 0 || args[0] != "init" {
		return false
	}
	for _, a := range args[1:] {
		if a == "-migrate-state" {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// SetBackend
// ---------------------------------------------------------------------------

func TestSetBackendWritesValidatedBackend(t *testing.T) {
	ws, m := newLocalBackendWorkspace(t, "analytics")
	svc := New(ws)

	b := &workspace.Backend{Type: "s3", Bucket: "analytics-tfstate", Region: "eu-north-1"}
	if err := svc.SetBackend(b); err != nil {
		t.Fatalf("SetBackend: %v", err)
	}

	got, err := workspace.Load(ws)
	if err != nil {
		t.Fatal(err)
	}
	if got.Backend == nil || *got.Backend != *b {
		t.Fatalf("Backend after SetBackend = %+v, want %+v", got.Backend, b)
	}
	// Every other field survives untouched.
	if got.Name != m.Name || got.Catalog != m.Catalog || got.SystemCatalog != m.SystemCatalog {
		t.Errorf("SetBackend altered unrelated fields: got %+v, want name/catalog/system_catalog from %+v", got, m)
	}
	// No file other than clavesa.json was touched.
	if _, err := os.Stat(filepath.Join(ws, "backend.tf")); !os.IsNotExist(err) {
		t.Errorf("SetBackend must not write backend.tf (stat err=%v)", err)
	}
}

func TestSetBackendRejectsInvalid(t *testing.T) {
	ws, _ := newLocalBackendWorkspace(t, "analytics")
	svc := New(ws)
	err := svc.SetBackend(&workspace.Backend{Type: "s3", Region: "eu-north-1"}) // missing bucket
	if err == nil {
		t.Fatal("SetBackend with missing bucket: got nil error, want one")
	}
	m, loadErr := workspace.Load(ws)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if m.Backend != nil {
		t.Errorf("rejected SetBackend still wrote a backend: %+v", m.Backend)
	}
}

func TestSetBackendNilClearsWhenUnmigrated(t *testing.T) {
	ws, _ := newLocalBackendWorkspace(t, "analytics")
	svc := New(ws)
	if err := svc.SetBackend(&workspace.Backend{Type: "s3", Bucket: "b", Region: "eu-north-1"}); err != nil {
		t.Fatalf("SetBackend: %v", err)
	}
	if err := svc.SetBackend(nil); err != nil {
		t.Fatalf("SetBackend(nil) with no backend.tf anywhere: %v", err)
	}
	m, err := workspace.Load(ws)
	if err != nil {
		t.Fatal(err)
	}
	if m.Backend != nil {
		t.Errorf("Backend after clearing = %+v, want nil", m.Backend)
	}
}

func TestSetBackendNilRefusedOnceMigrated(t *testing.T) {
	ws, m := newLocalBackendWorkspace(t, "analytics")
	m.Backend = &workspace.Backend{Type: "s3", Bucket: "analytics-tfstate", Region: "eu-north-1"}
	writeManifest(t, ws, m)
	// Simulate a completed migration: a backend.tf at the root.
	if err := workspace.WriteWorkspaceBackendTF(ws, &m); err != nil {
		t.Fatal(err)
	}

	svc := New(ws)
	if err := svc.SetBackend(nil); err == nil {
		t.Fatal("SetBackend(nil) with a migrated stack: got nil error, want a refusal")
	}
	got, err := workspace.Load(ws)
	if err != nil {
		t.Fatal(err)
	}
	if got.Backend == nil {
		t.Error("refused SetBackend(nil) still cleared the manifest backend")
	}
}

// ---------------------------------------------------------------------------
// Stack discovery
// ---------------------------------------------------------------------------

func TestDiscoverStacksRootFirstThenSorted(t *testing.T) {
	ws, m := newLocalBackendWorkspace(t, "analytics")
	svc := New(ws)
	for _, name := range []string{"bravo", "alpha"} {
		if _, err := svc.CreatePipeline(name, ""); err != nil {
			t.Fatalf("CreatePipeline %s: %v", name, err)
		}
	}
	_ = m

	stacks, err := discoverStacks(ws)
	if err != nil {
		t.Fatal(err)
	}
	if len(stacks) != 3 {
		t.Fatalf("discoverStacks = %v, want 3 entries", stacks)
	}
	if stacks[0] != ws {
		t.Errorf("stacks[0] = %s, want workspace root %s", stacks[0], ws)
	}
	if filepath.Base(stacks[1]) != "alpha" || filepath.Base(stacks[2]) != "bravo" {
		t.Errorf("pipeline stacks not sorted: got %v", stacks[1:])
	}
}

func TestDiscoverStacksIncludesDeployedMaintenance(t *testing.T) {
	ws, _ := newLocalBackendWorkspace(t, "analytics")
	maintDir := filepath.Join(ws, "_maintenance")
	if err := os.MkdirAll(maintDir, 0o755); err != nil {
		t.Fatal(err)
	}
	mainTF := `data "terraform_remote_state" "workspace" {
  backend = "local"
  config  = { path = "${path.module}/../terraform.tfstate" }
}
`
	if err := os.WriteFile(filepath.Join(maintDir, "main.tf"), []byte(mainTF), 0o644); err != nil {
		t.Fatal(err)
	}
	writeFakeLocalState(t, maintDir)

	stacks, err := discoverStacks(ws)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, s := range stacks {
		if filepath.Base(s) == "_maintenance" {
			found = true
		}
	}
	if !found {
		t.Errorf("discoverStacks missed deployed _maintenance: %v", stacks)
	}
}

func TestDiscoverStacksSkipsUndeployedUnderscoreAndDotDirs(t *testing.T) {
	ws, _ := newLocalBackendWorkspace(t, "analytics")
	// _scratch: underscore dir with no state at all — scanPipelines-style
	// exclusion should hold since it carries neither a tfstate, a
	// backend.tf, nor a terraform_remote_state block.
	scratchDir := filepath.Join(ws, "_scratch")
	if err := os.MkdirAll(scratchDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(scratchDir, "main.tf"), []byte("# not a pipeline\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// .clavesa: dot-dir, always skipped regardless of content.
	dotDir := filepath.Join(ws, ".clavesa")
	if err := os.MkdirAll(dotDir, 0o755); err != nil {
		t.Fatal(err)
	}

	stacks, err := discoverStacks(ws)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range stacks {
		if filepath.Base(s) == "_scratch" || filepath.Base(s) == ".clavesa" {
			t.Errorf("discoverStacks included a dir it should have skipped: %v", stacks)
		}
	}
}

// ---------------------------------------------------------------------------
// main.tf stripping — current and historical pipeline shapes
// ---------------------------------------------------------------------------

func TestStripLocalRemoteStateBlockCurrentShape(t *testing.T) {
	src := `# clavesa pipeline
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
	out, ok := stripLocalRemoteStateBlock([]byte(src))
	if !ok {
		t.Fatal("stripLocalRemoteStateBlock: no match on the current shape")
	}
	if strings.Contains(string(out), "terraform_remote_state") {
		t.Errorf("block not removed:\n%s", out)
	}
	if !strings.Contains(string(out), `aws = { source = "hashicorp/aws" }`) {
		t.Errorf("stripping the block ate unrelated content:\n%s", out)
	}
}

// TestStripLocalRemoteStateBlockOldWorkspaceSubdirShape covers the
// pre-v1.1.x layout, when the workspace stack lived in a `_workspace/`
// subdirectory rather than the workspace root — the remote-state config
// pointed at `../_workspace/terraform.tfstate` instead of
// `../terraform.tfstate`. A regex anchored on the current path would
// miss this; the brace-counting stripper doesn't care about the path
// value at all.
func TestStripLocalRemoteStateBlockOldWorkspaceSubdirShape(t *testing.T) {
	src := `# astrophage pipeline
terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
  }
}

data "terraform_remote_state" "workspace" {
  backend = "local"
  config  = { path = "${path.module}/../_workspace/terraform.tfstate" }
}
`
	out, ok := stripLocalRemoteStateBlock([]byte(src))
	if !ok {
		t.Fatal("stripLocalRemoteStateBlock: no match on the old _workspace/ shape")
	}
	if strings.Contains(string(out), "terraform_remote_state") {
		t.Errorf("block not removed:\n%s", out)
	}
}

// TestStripLocalRemoteStateBlockLeavesS3BlockAlone is defensive: a
// terraform_remote_state block with backend = "s3" (shouldn't exist in
// main.tf pre-migration, but if it somehow does) is left untouched.
func TestStripLocalRemoteStateBlockLeavesS3BlockAlone(t *testing.T) {
	src := `data "terraform_remote_state" "workspace" {
  backend = "s3"
  config = {
    bucket = "b"
    key    = "k"
    region = "eu-north-1"
  }
}
`
	out, ok := stripLocalRemoteStateBlock([]byte(src))
	if ok {
		t.Errorf("stripLocalRemoteStateBlock removed an s3 block: %s", out)
	}
	if string(out) != src {
		t.Errorf("stripLocalRemoteStateBlock modified src it shouldn't have matched")
	}
}

func TestStripWorkspaceLocalBackend(t *testing.T) {
	m := workspace.Manifest{Name: "demo"}
	src := workspace.WorkspaceMainTF(&m, "v1.0.0")
	out := stripWorkspaceLocalBackend([]byte(src))
	if strings.Contains(string(out), `backend "local"`) {
		t.Errorf("backend \"local\" {} line not stripped:\n%s", out)
	}
	if !strings.Contains(string(out), `module "workspace"`) {
		t.Errorf("stripping the backend line ate unrelated content:\n%s", out)
	}
}

// ---------------------------------------------------------------------------
// finalizeLocalState — backup rename rules
// ---------------------------------------------------------------------------

func TestFinalizeLocalStateRenamesBackup(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "terraform.tfstate"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "terraform.tfstate.backup"), []byte(`{"real":"state"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := finalizeLocalState(dir); err != nil {
		t.Fatalf("finalizeLocalState: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "terraform.tfstate")); !os.IsNotExist(err) {
		t.Errorf("0-byte terraform.tfstate not removed (stat err=%v)", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "terraform.tfstate.pre-migrate"))
	if err != nil {
		t.Fatalf("terraform.tfstate.pre-migrate missing: %v", err)
	}
	if string(data) != `{"real":"state"}` {
		t.Errorf("terraform.tfstate.pre-migrate content = %q, want the backup's content", data)
	}
}

// TestFinalizeLocalStateKeepsFullStateFromExplicitLocalBackend: the
// workspace root declares `backend "local" {}`, and migrating out of an
// explicit local backend leaves terraform.tfstate full (seen on the smoke
// workspace). It is kept as terraform.tfstate.pre-migrate, the older
// backup is left alone, and no terraform.tfstate remains for the deploy
// guard to trip on.
func TestFinalizeLocalStateKeepsFullStateFromExplicitLocalBackend(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "terraform.tfstate"), []byte(`{"current":"state"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "terraform.tfstate.backup"), []byte(`{"older":"state"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := finalizeLocalState(dir); err != nil {
		t.Fatalf("finalizeLocalState: %v", err)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "terraform.tfstate.pre-migrate")); string(got) != `{"current":"state"}` {
		t.Errorf("pre-migrate = %q, want the current state", got)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "terraform.tfstate.backup")); string(got) != `{"older":"state"}` {
		t.Errorf("backup = %q, want it left alone", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "terraform.tfstate")); !os.IsNotExist(err) {
		t.Errorf("terraform.tfstate still present (stat err=%v)", err)
	}
}

func TestFinalizeLocalStateNoBackupIsNoop(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "terraform.tfstate"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := finalizeLocalState(dir); err != nil {
		t.Fatalf("finalizeLocalState with no backup file: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "terraform.tfstate")); !os.IsNotExist(err) {
		t.Errorf("0-byte terraform.tfstate should still be removed even with no backup (stat err=%v)", err)
	}
}

// ---------------------------------------------------------------------------
// MigrateState preconditions
// ---------------------------------------------------------------------------

func TestMigrateStateRequiresBackend(t *testing.T) {
	ws, _ := newLocalBackendWorkspace(t, "analytics")
	svc := New(ws)
	if _, err := svc.MigrateState(context.Background(), MigrateStateOptions{}); err == nil {
		t.Fatal("MigrateState with no backend configured: got nil error, want one")
	}
}

func TestMigrateStateFailsOnTerraformVersion(t *testing.T) {
	ws, m := newLocalBackendWorkspace(t, "analytics")
	m.Backend = &workspace.Backend{Type: "s3", Bucket: "b", Region: "eu-north-1"}
	writeManifest(t, ws, m)
	svc := New(ws)
	svc.checkTerraformVersion = func(context.Context) error { return errTooOld }
	if _, err := svc.MigrateState(context.Background(), MigrateStateOptions{}); err != errTooOld {
		t.Fatalf("MigrateState terraform-version precondition: got %v, want %v", err, errTooOld)
	}
}

func TestMigrateStateFailsOnBucketPrecondition(t *testing.T) {
	ws, m := newLocalBackendWorkspace(t, "analytics")
	m.Backend = &workspace.Backend{Type: "s3", Bucket: "b", Region: "eu-north-1"}
	writeManifest(t, ws, m)
	svc := New(ws)
	svc.checkTerraformVersion = func(context.Context) error { return nil }
	svc.checkStateBucket = func(context.Context, string, string) error { return errBadBucket }
	_, err := svc.MigrateState(context.Background(), MigrateStateOptions{})
	if err == nil || !strings.Contains(err.Error(), "state bucket precondition failed") {
		t.Fatalf("MigrateState bucket precondition: got %v, want it to wrap the bucket-check error", err)
	}
}

var errTooOld = &staticErr{"terraform too old"}
var errBadBucket = &staticErr{"bucket has no versioning"}

type staticErr struct{ msg string }

func (e *staticErr) Error() string { return e.msg }

// ---------------------------------------------------------------------------
// MigrateState end to end (seams only — no AWS, no real terraform)
// ---------------------------------------------------------------------------

func TestMigrateStateHappyPath(t *testing.T) {
	ws, m := newLocalBackendWorkspace(t, "analytics")
	svc := New(ws)
	if _, err := svc.CreatePipeline("trips", ""); err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	writeFakeLocalState(t, ws)
	writeFakeLocalState(t, filepath.Join(ws, "trips"))

	m.Backend = &workspace.Backend{Type: "s3", Bucket: "analytics-tfstate", Region: "eu-north-1"}
	writeManifest(t, ws, m)

	store := newFakeS3Store()
	wireBackendSeams(svc, store, 0 /* plan: no changes */, defaultFakeInit(store, &m, ws))

	result, err := svc.MigrateState(context.Background(), MigrateStateOptions{})
	if err != nil {
		t.Fatalf("MigrateState: %v", err)
	}
	if len(result.Stacks) != 2 {
		t.Fatalf("Stacks = %+v, want 2 entries", result.Stacks)
	}
	root, trips := result.Stacks[0], result.Stacks[1]
	if root.Dir != "." || root.Status != MigrateStatusMigrated || root.Plan != PlanNoChanges {
		t.Errorf("root row = %+v, want dir=. status=migrated plan=%q", root, PlanNoChanges)
	}
	if trips.Dir != "trips" || trips.Status != MigrateStatusMigrated || trips.Plan != PlanNoChanges {
		t.Errorf("trips row = %+v, want dir=trips status=migrated plan=%q", trips, PlanNoChanges)
	}

	// backend.tf written, main.tf stripped, in both stacks.
	noLocalBackendLeftovers(t, ws)
	noLocalBackendLeftovers(t, filepath.Join(ws, "trips"))
	for _, dir := range []string{ws, filepath.Join(ws, "trips")} {
		if _, err := os.Stat(filepath.Join(dir, "backend.tf")); err != nil {
			t.Errorf("%s: backend.tf missing after migration: %v", dir, err)
		}
	}

	// Local state cleanup: 0-byte tfstate gone, pre-migrate backup kept.
	for _, dir := range []string{ws, filepath.Join(ws, "trips")} {
		if _, err := os.Stat(filepath.Join(dir, "terraform.tfstate")); !os.IsNotExist(err) {
			t.Errorf("%s: terraform.tfstate should be removed (stat err=%v)", dir, err)
		}
		if _, err := os.Stat(filepath.Join(dir, "terraform.tfstate.pre-migrate")); err != nil {
			t.Errorf("%s: terraform.tfstate.pre-migrate missing: %v", dir, err)
		}
	}

	// The fake S3 store actually received both objects.
	if len(store.objects) != 2 {
		t.Errorf("fake S3 store has %d objects, want 2: %v", len(store.objects), store.objects)
	}
}

func TestMigrateStateNeverDeployedPipelineSkipsInit(t *testing.T) {
	ws, m := newLocalBackendWorkspace(t, "analytics")
	svc := New(ws)
	if _, err := svc.CreatePipeline("fresh", ""); err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	// No local tfstate anywhere — root and pipeline are both "never deployed".

	m.Backend = &workspace.Backend{Type: "s3", Bucket: "analytics-tfstate", Region: "eu-north-1"}
	writeManifest(t, ws, m)

	store := newFakeS3Store()
	initCalls := 0
	wireBackendSeams(svc, store, 0, func(dir string) (int, error) {
		initCalls++
		return 0, nil
	})

	result, err := svc.MigrateState(context.Background(), MigrateStateOptions{})
	if err != nil {
		t.Fatalf("MigrateState: %v", err)
	}
	if initCalls != 0 {
		t.Errorf("terraform init called %d times for never-deployed stacks, want 0", initCalls)
	}
	for _, row := range result.Stacks {
		if row.Status != MigrateStatusSkippedNoState {
			t.Errorf("stack %s status = %s, want %s", row.Dir, row.Status, MigrateStatusSkippedNoState)
		}
	}
	// Still gets backend.tf + a stripped main.tf, so it's remote-backed
	// for its first deploy.
	if _, err := os.Stat(filepath.Join(ws, "fresh", "backend.tf")); err != nil {
		t.Errorf("backend.tf missing for a never-deployed pipeline: %v", err)
	}
	noLocalBackendLeftovers(t, filepath.Join(ws, "fresh"))
}

func TestMigrateStateRefusesExistingKey(t *testing.T) {
	ws, m := newLocalBackendWorkspace(t, "analytics")
	svc := New(ws)
	if _, err := svc.CreatePipeline("trips", ""); err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	writeFakeLocalState(t, ws)
	writeFakeLocalState(t, filepath.Join(ws, "trips"))

	m.Backend = &workspace.Backend{Type: "s3", Bucket: "analytics-tfstate", Region: "eu-north-1"}
	writeManifest(t, ws, m)

	store := newFakeS3Store()
	// Pre-occupy the pipeline's key, simulating a stray object already there.
	store.objects[m.Backend.PipelineStateKey(m.Name, "trips")] = []byte("already here")
	wireBackendSeams(svc, store, 0, defaultFakeInit(store, &m, ws))

	result, err := svc.MigrateState(context.Background(), MigrateStateOptions{})
	if err == nil {
		t.Fatal("MigrateState with an occupied target key: got nil error, want a refusal")
	}
	if len(result.Stacks) != 2 {
		t.Fatalf("Stacks = %+v, want root (migrated) + trips (failed)", result.Stacks)
	}
	if result.Stacks[0].Status != MigrateStatusMigrated {
		t.Errorf("root status = %s, want migrated (it ran before the conflict was hit)", result.Stacks[0].Status)
	}
	trips := result.Stacks[1]
	if trips.Status != MigrateStatusFailed || !strings.Contains(trips.Err, "already exists") {
		t.Errorf("trips row = %+v, want status=failed mentioning 'already exists'", trips)
	}
	// The pipeline's main.tf must be untouched — the refusal happens
	// before any file is written.
	data, err := os.ReadFile(filepath.Join(ws, "trips", "main.tf"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "terraform_remote_state") {
		t.Errorf("main.tf was rewritten despite the refusal:\n%s", data)
	}
}

func TestMigrateStateRollsBackOnInitFailure(t *testing.T) {
	ws, m := newLocalBackendWorkspace(t, "analytics")
	svc := New(ws)
	if _, err := svc.CreatePipeline("trips", ""); err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	origMain, err := os.ReadFile(filepath.Join(ws, "trips", "main.tf"))
	if err != nil {
		t.Fatal(err)
	}
	writeFakeLocalState(t, ws)
	writeFakeLocalState(t, filepath.Join(ws, "trips"))

	m.Backend = &workspace.Backend{Type: "s3", Bucket: "analytics-tfstate", Region: "eu-north-1"}
	writeManifest(t, ws, m)

	store := newFakeS3Store()
	wireBackendSeams(svc, store, 0, func(dir string) (int, error) {
		if filepath.Base(dir) == "trips" {
			return 1, nil // terraform init "fails"
		}
		return defaultFakeInit(store, &m, ws)(dir)
	})

	result, err := svc.MigrateState(context.Background(), MigrateStateOptions{})
	if err == nil {
		t.Fatal("MigrateState with a failing init: got nil error, want one")
	}
	if len(result.Stacks) != 2 || result.Stacks[1].Status != MigrateStatusFailed {
		t.Fatalf("Stacks = %+v, want trips to be the failed row", result.Stacks)
	}

	// Rollback: main.tf restored verbatim, backend.tf removed.
	after, err := os.ReadFile(filepath.Join(ws, "trips", "main.tf"))
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(origMain) {
		t.Errorf("main.tf not restored after rollback:\ngot\n%s\nwant\n%s", after, origMain)
	}
	if _, err := os.Stat(filepath.Join(ws, "trips", "backend.tf")); !os.IsNotExist(err) {
		t.Errorf("backend.tf not removed after rollback (stat err=%v)", err)
	}
	// The local state itself is untouched — rollback only undoes the
	// file rewrite, never the pre-existing local state.
	if _, err := os.Stat(filepath.Join(ws, "trips", "terraform.tfstate")); err != nil {
		t.Errorf("terraform.tfstate should survive a rollback: %v", err)
	}
}

// TestMigrateStateResumesAfterFix runs MigrateState once with a
// conflict on the second stack, fixes the conflict, and reruns:
// the already-migrated root is recognized and skipped, and only the
// previously-failed stack is (re)processed. This is the resumability
// the "already migrated" check exists for.
func TestMigrateStateResumesAfterFix(t *testing.T) {
	ws, m := newLocalBackendWorkspace(t, "analytics")
	svc := New(ws)
	if _, err := svc.CreatePipeline("trips", ""); err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	writeFakeLocalState(t, ws)
	writeFakeLocalState(t, filepath.Join(ws, "trips"))

	m.Backend = &workspace.Backend{Type: "s3", Bucket: "analytics-tfstate", Region: "eu-north-1"}
	writeManifest(t, ws, m)

	store := newFakeS3Store()
	tripsKey := m.Backend.PipelineStateKey(m.Name, "trips")
	store.objects[tripsKey] = []byte("conflict")
	wireBackendSeams(svc, store, 0, defaultFakeInit(store, &m, ws))

	if _, err := svc.MigrateState(context.Background(), MigrateStateOptions{}); err == nil {
		t.Fatal("first MigrateState run: expected the trips conflict to fail it")
	}

	// Fix the conflict and rerun.
	delete(store.objects, tripsKey)
	initCalls := map[string]int{}
	wireBackendSeams(svc, store, 0, func(dir string) (int, error) {
		label := "trips"
		if dir == ws {
			label = "root"
		}
		initCalls[label]++
		return defaultFakeInit(store, &m, ws)(dir)
	})

	result, err := svc.MigrateState(context.Background(), MigrateStateOptions{})
	if err != nil {
		t.Fatalf("resumed MigrateState: %v", err)
	}
	if initCalls["root"] != 0 {
		t.Errorf("resumed run re-initialized the already-migrated root: %v", initCalls)
	}
	if initCalls["trips"] != 1 {
		t.Errorf("resumed run should init trips exactly once, got %d", initCalls["trips"])
	}
	root, trips := result.Stacks[0], result.Stacks[1]
	if root.Status != MigrateStatusAlreadyMigrated {
		t.Errorf("root status on resume = %s, want already-migrated", root.Status)
	}
	if trips.Status != MigrateStatusMigrated {
		t.Errorf("trips status on resume = %s, want migrated", trips.Status)
	}
}

func TestMigrateStatePlanReportsChangesAndError(t *testing.T) {
	ws, m := newLocalBackendWorkspace(t, "analytics")
	svc := New(ws)
	writeFakeLocalState(t, ws)
	m.Backend = &workspace.Backend{Type: "s3", Bucket: "analytics-tfstate", Region: "eu-north-1"}
	writeManifest(t, ws, m)

	store := newFakeS3Store()
	wireBackendSeams(svc, store, 2 /* -detailed-exitcode: changes */, defaultFakeInit(store, &m, ws))
	result, err := svc.MigrateState(context.Background(), MigrateStateOptions{})
	if err != nil {
		t.Fatalf("MigrateState: %v", err)
	}
	if result.Stacks[0].Plan != PlanChanges {
		t.Errorf("Plan = %q, want %q", result.Stacks[0].Plan, PlanChanges)
	}

	// A plan exit code that's neither 0 nor 2 is a terraform-reported error.
	ws2, m2 := newLocalBackendWorkspace(t, "other")
	svc2 := New(ws2)
	writeFakeLocalState(t, ws2)
	m2.Backend = &workspace.Backend{Type: "s3", Bucket: "other-tfstate", Region: "eu-north-1"}
	writeManifest(t, ws2, m2)
	store2 := newFakeS3Store()
	wireBackendSeams(svc2, store2, 1 /* plan error */, defaultFakeInit(store2, &m2, ws2))
	result2, err := svc2.MigrateState(context.Background(), MigrateStateOptions{})
	if err != nil {
		t.Fatalf("MigrateState: %v", err)
	}
	if result2.Stacks[0].Plan != PlanError {
		t.Errorf("Plan = %q, want %q", result2.Stacks[0].Plan, PlanError)
	}
}

// initThatEmptiesLocalState simulates a `terraform init -migrate-state`
// that got as far as terraform's local cleanup: the state is copied to
// terraform.tfstate.backup and terraform.tfstate is left 0 bytes. With
// land=false no object reaches the store; exit is the process exit code.
func initThatEmptiesLocalState(t *testing.T, store *fakeS3Store, m *workspace.Manifest, ws string, land bool, exit int) func(dir string) (int, error) {
	return func(dir string) (int, error) {
		statePath := filepath.Join(dir, "terraform.tfstate")
		data, err := os.ReadFile(statePath)
		if err != nil {
			t.Fatal(err)
		}
		if land {
			store.objects[stackStateKey(m, ws, dir)] = data
		}
		if err := os.WriteFile(filepath.Join(dir, "terraform.tfstate.backup"), data, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(statePath, nil, 0o644); err != nil {
			t.Fatal(err)
		}
		return exit, nil
	}
}

// migrateOneDeployedPipeline lays out a deployed workspace root plus a
// deployed "trips" pipeline and sets the backend, returning the service,
// the fake store and trips' original main.tf and state.
func migrateOneDeployedPipeline(t *testing.T) (ws string, m workspace.Manifest, svc *Service, store *fakeS3Store, origMain, origState []byte) {
	t.Helper()
	ws, m = newLocalBackendWorkspace(t, "analytics")
	svc = New(ws)
	if _, err := svc.CreatePipeline("trips", ""); err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	trips := filepath.Join(ws, "trips")
	writeFakeLocalState(t, ws)
	writeFakeLocalState(t, trips)
	var err error
	if origMain, err = os.ReadFile(filepath.Join(trips, "main.tf")); err != nil {
		t.Fatal(err)
	}
	if origState, err = os.ReadFile(filepath.Join(trips, "terraform.tfstate")); err != nil {
		t.Fatal(err)
	}
	m.Backend = &workspace.Backend{Type: "s3", Bucket: "analytics-tfstate", Region: "eu-north-1"}
	writeManifest(t, ws, m)
	return ws, m, svc, newFakeS3Store(), origMain, origState
}

// assertRolledBackToLocal checks a stack is back to exactly its
// pre-migration local shape: original main.tf, no backend.tf, and the
// original state in terraform.tfstate.
func assertRolledBackToLocal(t *testing.T, dir string, origMain, origState []byte) {
	t.Helper()
	if got, _ := os.ReadFile(filepath.Join(dir, "main.tf")); string(got) != string(origMain) {
		t.Errorf("main.tf not restored:\n%s", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "backend.tf")); !os.IsNotExist(err) {
		t.Errorf("backend.tf not removed (stat err %v)", err)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "terraform.tfstate")); string(got) != string(origState) {
		t.Errorf("terraform.tfstate = %q, want the original state %q", got, origState)
	}
}

// TestMigrateStateInitFailureRestoresEmptiedLocalState: an init that
// fails after terraform emptied the local state file must not leave the
// stack with no usable state.
func TestMigrateStateInitFailureRestoresEmptiedLocalState(t *testing.T) {
	ws, m, svc, store, origMain, origState := migrateOneDeployedPipeline(t)
	wireBackendSeams(svc, store, 0, func(dir string) (int, error) {
		if filepath.Base(dir) == "trips" {
			return initThatEmptiesLocalState(t, store, &m, ws, false, 1)(dir)
		}
		return defaultFakeInit(store, &m, ws)(dir)
	})

	if _, err := svc.MigrateState(context.Background(), MigrateStateOptions{}); err == nil {
		t.Fatal("MigrateState with a failing init: got nil error, want one")
	}
	assertRolledBackToLocal(t, filepath.Join(ws, "trips"), origMain, origState)
}

// TestMigrateStateMissingObjectAfterInitRollsBack: init exits 0 but no
// object reached S3. The stack must go back to local state, not stay
// half-migrated where the next run would call it done.
func TestMigrateStateMissingObjectAfterInitRollsBack(t *testing.T) {
	ws, m, svc, store, origMain, origState := migrateOneDeployedPipeline(t)
	wireBackendSeams(svc, store, 0, func(dir string) (int, error) {
		if filepath.Base(dir) == "trips" {
			return initThatEmptiesLocalState(t, store, &m, ws, false, 0)(dir)
		}
		return defaultFakeInit(store, &m, ws)(dir)
	})

	result, err := svc.MigrateState(context.Background(), MigrateStateOptions{})
	if err == nil {
		t.Fatal("MigrateState with no object after init: got nil error, want one")
	}
	if got := result.Stacks[len(result.Stacks)-1].Status; got != MigrateStatusFailed {
		t.Errorf("trips status = %s, want failed", got)
	}
	assertRolledBackToLocal(t, filepath.Join(ws, "trips"), origMain, origState)
}

// TestMigrateStateResumeRefusesBackendTFWithoutRemoteState: a stack left
// with backend.tf, an empty local state and its real state only in
// terraform.tfstate.backup, and nothing in S3, must fail with a pointer to
// the local copy rather than count as already migrated.
func TestMigrateStateResumeRefusesBackendTFWithoutRemoteState(t *testing.T) {
	ws, m, svc, store, _, origState := migrateOneDeployedPipeline(t)
	trips := filepath.Join(ws, "trips")
	if err := workspace.WritePipelineBackendTF(trips, &m); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(trips, "terraform.tfstate.backup"), origState, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(trips, "terraform.tfstate"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	wireBackendSeams(svc, store, 0, defaultFakeInit(store, &m, ws))

	result, err := svc.MigrateState(context.Background(), MigrateStateOptions{})
	if err == nil {
		t.Fatal("MigrateState: got nil error, want a refusal for trips")
	}
	row := result.Stacks[len(result.Stacks)-1]
	if row.Status != MigrateStatusFailed || !strings.Contains(row.Err, "terraform.tfstate.backup") {
		t.Errorf("trips row = %+v, want failed naming terraform.tfstate.backup", row)
	}
}

// TestMigrateStatePlansOnlyStacksWithState: a never-deployed stack has
// no .terraform, so the post-migration plan skips it.
func TestMigrateStatePlansOnlyStacksWithState(t *testing.T) {
	ws, m, svc, store, _, _ := migrateOneDeployedPipeline(t)
	if _, err := svc.CreatePipeline("fresh", ""); err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	init := defaultFakeInit(store, &m, ws)
	var planned []string
	svc.checkTerraformVersion = func(context.Context) error { return nil }
	svc.checkStateBucket = func(context.Context, string, string) error { return nil }
	svc.stateObjectStatus = store.status
	svc.runTerraform = func(_ context.Context, dir string, _, _ io.Writer, args ...string) (int, error) {
		switch {
		case isMigrateInit(args):
			return init(dir)
		case args[0] == "plan":
			planned = append(planned, relStackDir(ws, dir))
		}
		return 0, nil
	}

	if _, err := svc.MigrateState(context.Background(), MigrateStateOptions{}); err != nil {
		t.Fatalf("MigrateState: %v", err)
	}
	if strings.Join(planned, ",") != ".,trips" {
		t.Errorf("planned stacks = %v, want [. trips] (fresh has no state)", planned)
	}
}

// TestMigrateStatePreconditionFailureMarshalsEmptyStacks: a run that
// stops before any stack still returns a JSON array, not null, because
// the UI and `--json` callers read `stacks` without a null check.
func TestMigrateStatePreconditionFailureMarshalsEmptyStacks(t *testing.T) {
	ws, m := newLocalBackendWorkspace(t, "analytics")
	m.Backend = &workspace.Backend{Type: "s3", Bucket: "analytics-tfstate", Region: "eu-north-1"}
	writeManifest(t, ws, m)
	svc := New(ws)
	wireBackendSeams(svc, newFakeS3Store(), 0, nil)
	svc.checkStateBucket = func(context.Context, string, string) error { return &staticErr{msg: "no such bucket"} }

	res, err := svc.MigrateState(context.Background(), MigrateStateOptions{})
	if err == nil {
		t.Fatal("MigrateState with a failing bucket check: got nil error")
	}
	out, jerr := json.Marshal(res)
	if jerr != nil {
		t.Fatal(jerr)
	}
	if !strings.Contains(string(out), `"stacks":[]`) {
		t.Errorf("result JSON = %s, want \"stacks\":[]", out)
	}
}

// TestDiscoverStacksIncludesUndeployedMaintenanceWithLocalWiring: a
// scaffolded _maintenance that was never deployed has no state, but its
// main.tf reads the workspace state through a local
// terraform_remote_state block. It must be discovered so migrate-state
// rewires it; left alone it would read a local file that no longer
// exists, and deploy would refuse it for having no backend.tf.
func TestDiscoverStacksIncludesUndeployedMaintenanceWithLocalWiring(t *testing.T) {
	ws, _ := newLocalBackendWorkspace(t, "analytics")
	maintDir := filepath.Join(ws, "_maintenance")
	if err := os.MkdirAll(maintDir, 0o755); err != nil {
		t.Fatal(err)
	}
	mainTF := `data "terraform_remote_state" "workspace" {
  backend = "local"
  config  = { path = "${path.module}/../terraform.tfstate" }
}
`
	if err := os.WriteFile(filepath.Join(maintDir, "main.tf"), []byte(mainTF), 0o644); err != nil {
		t.Fatal(err)
	}

	stacks, err := discoverStacks(ws)
	if err != nil {
		t.Fatal(err)
	}
	if len(stacks) != 2 || filepath.Base(stacks[1]) != "_maintenance" {
		t.Errorf("discoverStacks = %v, want [root _maintenance]", stacks)
	}
}

// TestMigrateStateInitsBeforePlan: after `workspace upgrade` the module
// sources point at a version terraform has not installed, so every
// planned stack gets a plain `terraform init` (never -migrate-state)
// right before its plan. Found by the cloud smoke gate.
func TestMigrateStateInitsBeforePlan(t *testing.T) {
	ws, m, svc, store, _, _ := migrateOneDeployedPipeline(t)
	migrate := defaultFakeInit(store, &m, ws)
	var calls []string
	svc.checkTerraformVersion = func(context.Context) error { return nil }
	svc.checkStateBucket = func(context.Context, string, string) error { return nil }
	svc.stateObjectStatus = store.status
	svc.runTerraform = func(_ context.Context, dir string, _, _ io.Writer, args ...string) (int, error) {
		label := relStackDir(ws, dir) + " " + args[0]
		if isMigrateInit(args) {
			label += " -migrate-state"
			calls = append(calls, label)
			return migrate(dir)
		}
		calls = append(calls, label)
		return 0, nil
	}

	if _, err := svc.MigrateState(context.Background(), MigrateStateOptions{}); err != nil {
		t.Fatalf("MigrateState: %v", err)
	}
	want := ". init -migrate-state,trips init -migrate-state,. init,. plan,trips init,trips plan"
	if got := strings.Join(calls, ","); got != want {
		t.Errorf("terraform calls =\n  %s\nwant\n  %s", got, want)
	}
}

// TestMigrateStatePlanInitFailureIsPlanError: a failing init before plan
// marks the plan as an error and skips the plan itself.
func TestMigrateStatePlanInitFailureIsPlanError(t *testing.T) {
	ws, m, svc, store, _, _ := migrateOneDeployedPipeline(t)
	migrate := defaultFakeInit(store, &m, ws)
	planned := 0
	svc.checkTerraformVersion = func(context.Context) error { return nil }
	svc.checkStateBucket = func(context.Context, string, string) error { return nil }
	svc.stateObjectStatus = store.status
	svc.runTerraform = func(_ context.Context, dir string, _, _ io.Writer, args ...string) (int, error) {
		switch {
		case isMigrateInit(args):
			return migrate(dir)
		case args[0] == "init":
			return 1, nil
		case args[0] == "plan":
			planned++
		}
		return 0, nil
	}

	res, err := svc.MigrateState(context.Background(), MigrateStateOptions{})
	if err != nil {
		t.Fatalf("MigrateState: %v", err)
	}
	if planned != 0 {
		t.Errorf("plan ran %d times after its init failed", planned)
	}
	for _, row := range res.Stacks {
		if row.Plan != PlanError || !strings.Contains(row.Err, "init before plan") {
			t.Errorf("row %s = %+v, want plan error naming the init", row.Dir, row)
		}
	}
}
