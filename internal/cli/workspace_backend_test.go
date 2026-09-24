package cli

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/service"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// captureStdout redirects os.Stdout for the duration of fn and returns
// everything written to it. Tests using it must not run t.Parallel() —
// os.Stdout is process-global.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	defer func() { os.Stdout = orig }()

	fn()

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

// ---------------------------------------------------------------------------
// parseBackendFlags
// ---------------------------------------------------------------------------

func TestParseBackendFlags(t *testing.T) {
	t.Parallel()

	// No flags at all — local state, no error.
	b, err := parseBackendFlags("", "", "", "")
	if err != nil || b != nil {
		t.Fatalf("no flags: got (%+v, %v), want (nil, nil)", b, err)
	}

	// --backend alone, missing bucket/region.
	if _, err := parseBackendFlags("s3", "", "", ""); err == nil {
		t.Error("--backend with no --state-bucket/--state-region: expected an error")
	}
	if _, err := parseBackendFlags("s3", "my-bucket", "", ""); err == nil {
		t.Error("--backend with no --state-region: expected an error")
	}

	// --state-bucket/--state-region without --backend.
	if _, err := parseBackendFlags("", "my-bucket", "eu-north-1", ""); err == nil {
		t.Error("--state-bucket without --backend: expected an error")
	}

	// Well-formed.
	b, err = parseBackendFlags("s3", "my-bucket", "eu-north-1", "")
	if err != nil {
		t.Fatalf("well-formed backend flags: %v", err)
	}
	if b == nil || b.Type != "s3" || b.Bucket != "my-bucket" || b.Region != "eu-north-1" {
		t.Errorf("parsed backend = %+v, want type=s3 bucket=my-bucket region=eu-north-1", b)
	}

	// Invalid type is caught by Backend.Validate().
	if _, err := parseBackendFlags("dynamodb", "my-bucket", "eu-north-1", ""); err == nil {
		t.Error("--backend dynamodb: expected a validation error")
	}

	// key_prefix without a trailing slash is caught by Backend.Validate().
	if _, err := parseBackendFlags("s3", "my-bucket", "eu-north-1", "no-slash"); err == nil {
		t.Error("--state-key-prefix without trailing slash: expected a validation error")
	}
}

// ---------------------------------------------------------------------------
// workspace init — with and without --backend
// ---------------------------------------------------------------------------

// TestWorkspaceInitWithoutBackendByteIdentical pins ADR-025's promise:
// omitting the backend flags leaves `workspace init` byte-identical to
// pre-ADR-025 behavior. Compares the CLI's output against a direct
// workspace.Init(..., nil) call rather than a checked-in fixture, so the
// assertion tracks the real emitter instead of a copy of it.
func TestWorkspaceInitWithoutBackendByteIdentical(t *testing.T) {
	t.Parallel()
	name := "byte-identical-ws"

	direct := t.TempDir()
	if err := workspace.Init(direct, name, "aws", "", service.ModuleVersion, nil); err != nil {
		t.Fatalf("direct workspace.Init: %v", err)
	}

	viaCLI := t.TempDir()
	if err := Run([]string{"workspace", "init", name, "--workspace", viaCLI}); err != nil {
		t.Fatalf("workspace init (no backend flags): %v", err)
	}

	for _, f := range []string{"clavesa.json", "main.tf", "variables.tf", "outputs.tf"} {
		want, err := os.ReadFile(filepath.Join(direct, f))
		if err != nil {
			t.Fatalf("read direct %s: %v", f, err)
		}
		got, err := os.ReadFile(filepath.Join(viaCLI, f))
		if err != nil {
			t.Fatalf("read CLI %s: %v", f, err)
		}
		if !bytes.Equal(want, got) {
			t.Errorf("%s differs between direct workspace.Init and `workspace init` with no --backend flags:\ndirect:\n%s\ncli:\n%s", f, want, got)
		}
	}
	if _, err := os.Stat(filepath.Join(viaCLI, "backend.tf")); !os.IsNotExist(err) {
		t.Errorf("backend.tf should not exist without --backend (stat err=%v)", err)
	}
}

// TestWorkspaceInitWithBackend covers the ADR-025 flag path end to end:
// the manifest, main.tf, and backend.tf all agree with the flags.
func TestWorkspaceInitWithBackend(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	name := "remote-ws"
	if err := Run([]string{
		"workspace", "init", name, "--workspace", ws,
		"--backend", "s3",
		"--state-bucket", "remote-ws-tfstate",
		"--state-region", "eu-north-1",
	}); err != nil {
		t.Fatalf("workspace init --backend: %v", err)
	}

	m, err := workspace.Load(ws)
	if err != nil || m == nil {
		t.Fatalf("load manifest: %v", err)
	}
	if m.Backend == nil || m.Backend.Type != "s3" || m.Backend.Bucket != "remote-ws-tfstate" || m.Backend.Region != "eu-north-1" {
		t.Fatalf("manifest backend = %+v, want s3/remote-ws-tfstate/eu-north-1", m.Backend)
	}

	mainTF, err := os.ReadFile(filepath.Join(ws, "main.tf"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(mainTF), `backend "local"`) {
		t.Errorf("main.tf still carries a local backend block:\n%s", mainTF)
	}

	backendTF, err := os.ReadFile(filepath.Join(ws, "backend.tf"))
	if err != nil {
		t.Fatalf("backend.tf not written: %v", err)
	}
	if !strings.Contains(string(backendTF), `bucket       = "remote-ws-tfstate"`) {
		t.Errorf("backend.tf missing expected bucket:\n%s", backendTF)
	}
}

func TestWorkspaceInitBackendRequiresBucketAndRegion(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	err := Run([]string{"workspace", "init", "ws", "--workspace", ws, "--backend", "s3"})
	if err == nil || !strings.Contains(err.Error(), "--state-bucket") {
		t.Fatalf("workspace init --backend with no bucket/region: got %v, want a --state-bucket error", err)
	}
}

// ---------------------------------------------------------------------------
// workspace set-backend / --clear / backend
// ---------------------------------------------------------------------------

// Not t.Parallel(): uses captureStdout, which swaps the process-global
// os.Stdout.
func TestWorkspaceSetBackendAndClear(t *testing.T) {
	ws := t.TempDir()
	if err := Run([]string{"workspace", "init", "set-ws", "--workspace", ws}); err != nil {
		t.Fatalf("workspace init: %v", err)
	}

	if err := Run([]string{
		"workspace", "set-backend", "--workspace", ws,
		"--backend", "s3", "--state-bucket", "set-ws-tfstate", "--state-region", "eu-north-1",
	}); err != nil {
		t.Fatalf("workspace set-backend: %v", err)
	}
	m, err := workspace.Load(ws)
	if err != nil || m == nil || m.Backend == nil || m.Backend.Bucket != "set-ws-tfstate" {
		t.Fatalf("manifest after set-backend: m=%+v err=%v", m, err)
	}
	// set-backend never writes backend.tf — that's migrate-state's job.
	if _, statErr := os.Stat(filepath.Join(ws, "backend.tf")); !os.IsNotExist(statErr) {
		t.Errorf("set-backend must not write backend.tf (stat err=%v)", statErr)
	}

	out := captureStdout(t, func() {
		if err := Run([]string{"workspace", "backend", "--workspace", ws}); err != nil {
			t.Fatalf("workspace backend: %v", err)
		}
	})
	if !strings.Contains(out, "s3://set-ws-tfstate") {
		t.Errorf("workspace backend output = %q, want it to mention s3://set-ws-tfstate", out)
	}

	if err := Run([]string{"workspace", "set-backend", "--workspace", ws, "--clear"}); err != nil {
		t.Fatalf("workspace set-backend --clear: %v", err)
	}
	m, err = workspace.Load(ws)
	if err != nil || m == nil || m.Backend != nil {
		t.Fatalf("manifest after --clear: m=%+v err=%v, want Backend nil", m, err)
	}

	out = captureStdout(t, func() {
		if err := Run([]string{"workspace", "backend", "--workspace", ws}); err != nil {
			t.Fatalf("workspace backend after clear: %v", err)
		}
	})
	if strings.TrimSpace(out) != "local" {
		t.Errorf("workspace backend after clear = %q, want %q", strings.TrimSpace(out), "local")
	}
}

func TestWorkspaceSetBackendRequiresBackendOrClear(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	if err := Run([]string{"workspace", "init", "ws", "--workspace", ws}); err != nil {
		t.Fatalf("workspace init: %v", err)
	}
	if err := Run([]string{"workspace", "set-backend", "--workspace", ws}); err == nil {
		t.Fatal("set-backend with no flags: expected an error")
	}
}

func TestWorkspaceSetBackendClearRejectsOtherFlags(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	if err := Run([]string{"workspace", "init", "ws", "--workspace", ws}); err != nil {
		t.Fatalf("workspace init: %v", err)
	}
	err := Run([]string{"workspace", "set-backend", "--workspace", ws, "--clear", "--state-bucket", "b"})
	if err == nil || !strings.Contains(err.Error(), "--clear") {
		t.Fatalf("set-backend --clear --state-bucket: got %v, want a combination error", err)
	}
}

// TestWorkspaceSetBackendClearRefusesAfterMigration pins the refusal
// SetBackend documents: once a stack has a backend.tf, --clear must not
// silently revert the manifest to local while the stack stays remote.
func TestWorkspaceSetBackendClearRefusesAfterMigration(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	if err := Run([]string{
		"workspace", "init", "migrated-ws", "--workspace", ws,
		"--backend", "s3", "--state-bucket", "b", "--state-region", "eu-north-1",
	}); err != nil {
		t.Fatalf("workspace init --backend: %v", err)
	}
	// init already wrote backend.tf for a backend-configured workspace —
	// that alone is what the refusal checks for.
	if _, err := os.Stat(filepath.Join(ws, "backend.tf")); err != nil {
		t.Fatalf("expected backend.tf from init --backend: %v", err)
	}
	err := Run([]string{"workspace", "set-backend", "--workspace", ws, "--clear"})
	if err == nil || !strings.Contains(err.Error(), "backend.tf") {
		t.Fatalf("set-backend --clear with an existing backend.tf: got %v, want a refusal mentioning backend.tf", err)
	}
}

// Not t.Parallel(): uses captureStdout, which swaps the process-global
// os.Stdout.
func TestWorkspaceBackendCmdOnFreshWorkspace(t *testing.T) {
	ws := t.TempDir()
	if err := Run([]string{"workspace", "init", "fresh-ws", "--workspace", ws}); err != nil {
		t.Fatalf("workspace init: %v", err)
	}
	out := captureStdout(t, func() {
		if err := Run([]string{"workspace", "backend", "--workspace", ws}); err != nil {
			t.Fatalf("workspace backend: %v", err)
		}
	})
	if strings.TrimSpace(out) != "local" {
		t.Errorf("workspace backend on a fresh workspace = %q, want %q", strings.TrimSpace(out), "local")
	}
}

// ---------------------------------------------------------------------------
// migrate-state: error paths that don't need terraform or AWS, plus the
// pure result-formatting helpers.
// ---------------------------------------------------------------------------

// TestWorkspaceMigrateStateRequiresBackend exercises Service.MigrateState's
// very first precondition (no backend configured) through the real CLI
// path — cheap because it never reaches terraform or AWS — and pins the
// non-zero exit contract.
func TestWorkspaceMigrateStateRequiresBackend(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	if err := Run([]string{"workspace", "init", "ws", "--workspace", ws}); err != nil {
		t.Fatalf("workspace init: %v", err)
	}
	err := Run([]string{"workspace", "migrate-state", "--workspace", ws})
	if err == nil || !strings.Contains(err.Error(), "set-backend") {
		t.Fatalf("migrate-state with no backend configured: got %v, want an error pointing at set-backend", err)
	}
}

func TestMigrateResultFailed(t *testing.T) {
	t.Parallel()
	clean := service.MigrateResult{Stacks: []service.MigrateStackResult{
		{Dir: ".", Status: service.MigrateStatusMigrated, Plan: service.PlanNoChanges},
		{Dir: "p1", Status: service.MigrateStatusAlreadyMigrated, Plan: service.PlanChanges},
	}}
	if n := migrateResultFailed(clean); n != 0 {
		t.Errorf("clean result: migrateResultFailed = %d, want 0", n)
	}

	failedStack := service.MigrateResult{Stacks: []service.MigrateStackResult{
		{Dir: ".", Status: service.MigrateStatusFailed, Err: "boom"},
	}}
	if n := migrateResultFailed(failedStack); n != 1 {
		t.Errorf("failed stack: migrateResultFailed = %d, want 1", n)
	}

	planError := service.MigrateResult{Stacks: []service.MigrateStackResult{
		{Dir: ".", Status: service.MigrateStatusMigrated, Plan: service.PlanError},
	}}
	if n := migrateResultFailed(planError); n != 1 {
		t.Errorf("plan error: migrateResultFailed = %d, want 1", n)
	}
}

func TestMigrateSummaryLine(t *testing.T) {
	t.Parallel()
	res := service.MigrateResult{Stacks: []service.MigrateStackResult{
		{Status: service.MigrateStatusMigrated},
		{Status: service.MigrateStatusMigrated},
		{Status: service.MigrateStatusAlreadyMigrated},
		{Status: service.MigrateStatusSkippedNoState},
		{Status: service.MigrateStatusFailed},
	}}
	got := migrateSummaryLine(res)
	want := "2 migrated, 1 already migrated, 1 skipped (no state), 1 failed"
	if got != want {
		t.Errorf("migrateSummaryLine = %q, want %q", got, want)
	}
}

func TestPrintMigrateResultTable(t *testing.T) {
	t.Parallel()
	res := service.MigrateResult{Stacks: []service.MigrateStackResult{
		{Dir: ".", Key: "clavesa/ws/workspace.tfstate", Status: service.MigrateStatusMigrated, Plan: service.PlanNoChanges},
		{Dir: "p1", Key: "clavesa/ws/pipelines/p1.tfstate", Status: service.MigrateStatusFailed, Err: "boom"},
	}}
	var buf bytes.Buffer
	printMigrateResultTable(&buf, res)
	out := buf.String()
	for _, want := range []string{"DIR", "KEY", "STATUS", "PLAN", "migrated", "no changes", "failed", "boom"} {
		if !strings.Contains(out, want) {
			t.Errorf("printMigrateResultTable output missing %q:\n%s", want, out)
		}
	}
}
