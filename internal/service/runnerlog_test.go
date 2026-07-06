package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/observability"
)

// --- helper-level coverage ---------------------------------------------

func TestCreateRunnerLogFilePreferred(t *testing.T) {
	ws := t.TempDir()
	preferred := filepath.Join(ws, "demo", ".clavesa", "runs", "r1", "_bundle.log")

	f, path, err := createRunnerLogFile(preferred, ws, "clavesa-bundle-r1-*.log")
	if err != nil {
		t.Fatalf("createRunnerLogFile: %v", err)
	}
	defer f.Close()
	if path != preferred {
		t.Errorf("path = %q, want the preferred location %q", path, preferred)
	}
	if _, err := f.WriteString("hello"); err != nil {
		t.Errorf("returned file is not writable: %v", err)
	}
}

// When the preferred run-dir location can't be created (here: a file sits
// where the parent dir should be), the log must land under the workspace
// .clavesa dir instead of being silently dropped.
func TestCreateRunnerLogFileWorkspaceFallback(t *testing.T) {
	ws := t.TempDir()
	pipelineDir := filepath.Join(ws, "demo")
	if err := os.MkdirAll(filepath.Join(pipelineDir, ".clavesa"), 0o755); err != nil {
		t.Fatal(err)
	}
	// Block the run dir: `.clavesa/runs` is a file, so MkdirAll fails.
	if err := os.WriteFile(filepath.Join(pipelineDir, ".clavesa", "runs"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	preferred := filepath.Join(pipelineDir, ".clavesa", "runs", "r1", "_bundle.log")

	f, path, err := createRunnerLogFile(preferred, ws, "clavesa-bundle-r1-*.log")
	if err != nil {
		t.Fatalf("createRunnerLogFile should fall back, got error: %v", err)
	}
	defer f.Close()
	wantDir := filepath.Join(ws, ".clavesa")
	if filepath.Dir(path) != wantDir {
		t.Errorf("fallback path = %q, want a file under %q", path, wantDir)
	}
}

// When the workspace dir is unusable too, the system temp dir is the last
// resort — creation still succeeds.
func TestCreateRunnerLogFileTempFallback(t *testing.T) {
	blocked := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocked, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	preferred := filepath.Join(blocked, "runs", "r1", "_bundle.log")

	f, path, err := createRunnerLogFile(preferred, blocked, "clavesa-bundle-r1-*.log")
	if err != nil {
		t.Fatalf("createRunnerLogFile should fall back to the system temp dir, got error: %v", err)
	}
	defer f.Close()
	defer os.Remove(path)
	if !strings.HasPrefix(path, os.TempDir()) {
		t.Errorf("last-resort path = %q, want a file under os.TempDir() %q", path, os.TempDir())
	}
}

func TestRunnerLogRef(t *testing.T) {
	if got := runnerLogRef("/x/y.log", nil); got != "full runner log: /x/y.log" {
		t.Errorf("runnerLogRef with a path = %q", got)
	}
	got := runnerLogRef("", fmt.Errorf("disk full"))
	if !strings.Contains(got, "full runner log unavailable") || !strings.Contains(got, "disk full") {
		t.Errorf("runnerLogRef without a path should carry the reason, got %q", got)
	}
}

func TestWriteRunnerFailureLog(t *testing.T) {
	ws := t.TempDir()
	preferred := filepath.Join(ws, "demo", ".clavesa", "runs", "r1", "logs", "t1.log")

	path, err := writeRunnerFailureLog(preferred, ws, "clavesa-runner-t1-*.log",
		[]byte("STDOUT-BODY"), []byte("STDERR-BODY line1\nline2"))
	if err != nil {
		t.Fatalf("writeRunnerFailureLog: %v", err)
	}
	if path != preferred {
		t.Errorf("path = %q, want %q", path, preferred)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read failure log: %v", err)
	}
	for _, want := range []string{"STDOUT-BODY", "STDERR-BODY line1\nline2", "--- stdout ---", "--- stderr ---"} {
		if !strings.Contains(string(data), want) {
			t.Errorf("failure log missing %q:\n%s", want, data)
		}
	}
}

// transformFailureLogRef (GH #82, the backfill-replay path): the buffered
// output is written to the standard run-log location and referenced.
func TestTransformFailureLogRef(t *testing.T) {
	ws := t.TempDir()
	pipelineDir := filepath.Join(ws, "demo")

	ref := transformFailureLogRef(pipelineDir, "backfill-42", "t1", ws, []byte("OUT"), []byte("FULL STDERR"))
	wantPath := observability.RunLogPath(pipelineDir, "backfill-42", "t1")
	if ref != "full runner log: "+wantPath {
		t.Errorf("failure ref = %q, want path %q", ref, wantPath)
	}
	data, err := os.ReadFile(wantPath)
	if err != nil {
		t.Fatalf("failure log not written: %v", err)
	}
	if !strings.Contains(string(data), "FULL STDERR") || !strings.Contains(string(data), "OUT") {
		t.Errorf("failure log missing buffered output:\n%s", data)
	}
}

// --- bundle-path end-to-end (fake docker) ------------------------------

// fakeDockerOnPath installs a `docker` stub that consumes stdin, prints a
// head marker + >2KiB of stderr noise + a tail marker, and exits 1 — a
// failing runner whose full stack trace can't fit the inline bounded tail.
func fakeDockerOnPath(t *testing.T) {
	t.Helper()
	bin := t.TempDir()
	script := `#!/bin/sh
if [ "$1" = "run" ]; then
  cat >/dev/null
  echo "HEAD-MARKER only in the full log" >&2
  i=0
  while [ $i -lt 100 ]; do
    echo "spark stderr noise line $i ................................................" >&2
    i=$((i+1))
  done
  echo "TAIL-MARKER the inline tail ends with this" >&2
  exit 1
fi
exit 1
`
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	// Skip the Docker-VM heap probe — deterministic args, no extra execs.
	t.Setenv("CLAVESA_JVM_HEAP_MB", "1024")
}

// TestRunPipelineBundleFailureNamesFullLog — a failed bundle run's error
// must inline only the bounded stderr tail but name the on-disk bundle log,
// and that file must hold the full stderr (including the head the tail cut).
func TestRunPipelineBundleFailureNamesFullLog(t *testing.T) {
	fakeDockerOnPath(t)
	ws := t.TempDir()
	svc := New(ws)
	pipelineDir := filepath.Join(ws, "demo")
	if err := os.MkdirAll(pipelineDir, 0o755); err != nil {
		t.Fatal(err)
	}

	bundle := []bundleTransformConfig{{NodeID: "t1", Language: "sql", LogicPath: filepath.Join(t.TempDir(), "logic.txt")}}
	_, err := svc.runPipelineBundle(context.Background(), "img:latest", pipelineDir, t.TempDir(), bundle, "run-test", "", "", "", false, nil, "", "")
	if err == nil {
		t.Fatal("runPipelineBundle should fail against the failing docker stub")
	}

	msg := err.Error()
	wantPath := filepath.Join(pipelineDir, ".clavesa", "runs", "run-test", "_bundle.log")
	if !strings.Contains(msg, "full runner log: "+wantPath) {
		t.Errorf("error does not name the bundle log path:\n%s", msg)
	}
	if !strings.Contains(msg, "TAIL-MARKER") {
		t.Errorf("inline stderr tail missing from error:\n%s", msg)
	}
	if strings.Contains(msg, "HEAD-MARKER") {
		t.Errorf("inline stderr should be bounded (head must be cut):\n%s", msg)
	}

	data, rerr := os.ReadFile(wantPath)
	if rerr != nil {
		t.Fatalf("bundle log not written: %v", rerr)
	}
	if !strings.Contains(string(data), "HEAD-MARKER") || !strings.Contains(string(data), "TAIL-MARKER") {
		t.Errorf("bundle log does not hold the full stderr (head+tail):\n%.200s…", data)
	}
}

// TestRunPipelineBundleFailureLogFallback — when the pipeline run dir can't
// be created, the full log must still land somewhere (workspace .clavesa)
// and the error must name that location, not silently drop the disk copy.
func TestRunPipelineBundleFailureLogFallback(t *testing.T) {
	fakeDockerOnPath(t)
	ws := t.TempDir()
	svc := New(ws)
	pipelineDir := filepath.Join(ws, "demo")
	if err := os.MkdirAll(filepath.Join(pipelineDir, ".clavesa"), 0o755); err != nil {
		t.Fatal(err)
	}
	// Block the run dir: `.clavesa/runs` is a file.
	if err := os.WriteFile(filepath.Join(pipelineDir, ".clavesa", "runs"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	bundle := []bundleTransformConfig{{NodeID: "t1", Language: "sql", LogicPath: filepath.Join(t.TempDir(), "logic.txt")}}
	_, err := svc.runPipelineBundle(context.Background(), "img:latest", pipelineDir, t.TempDir(), bundle, "run-test", "", "", "", false, nil, "", "")
	if err == nil {
		t.Fatal("runPipelineBundle should fail against the failing docker stub")
	}

	msg := err.Error()
	const prefix = "full runner log: "
	idx := strings.Index(msg, prefix)
	if idx < 0 {
		t.Fatalf("error does not name a full runner log:\n%s", msg)
	}
	logPath := strings.TrimSpace(strings.SplitN(msg[idx+len(prefix):], "\n", 2)[0])
	if filepath.Dir(logPath) != filepath.Join(ws, ".clavesa") {
		t.Errorf("fallback log path = %q, want a file under %q", logPath, filepath.Join(ws, ".clavesa"))
	}
	data, rerr := os.ReadFile(logPath)
	if rerr != nil {
		t.Fatalf("fallback log not written: %v", rerr)
	}
	if !strings.Contains(string(data), "HEAD-MARKER") {
		t.Errorf("fallback log does not hold the full stderr:\n%.200s…", data)
	}
}
