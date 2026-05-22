//go:build integration

package integration

import (
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

var binPath string

func TestMain(m *testing.M) {
	binPath = filepath.Join(os.TempDir(), "clavesa-test-bin")
	cmd := exec.Command("go", "build", "-o", binPath, "./cmd/clavesa")
	cmd.Dir = repoRoot()
	if out, err := cmd.CombinedOutput(); err != nil {
		log.Fatalf("build failed: %v\n%s", err, out)
	}
	os.Exit(m.Run())
}

func repoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	// tests/cli/pipeline_test.go → tests/cli/ → tests/ → repo root
	return filepath.Dir(filepath.Dir(filepath.Dir(file)))
}

// run executes the clavesa binary with the given args and returns stdout.
func run(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.Command(binPath, args...)
	out, err := cmd.Output()
	if err != nil {
		stderr := ""
		if ee, ok := err.(*exec.ExitError); ok {
			stderr = string(ee.Stderr)
		}
		t.Fatalf("clavesa %s: %v\nstderr: %s", strings.Join(args, " "), err, stderr)
	}
	return strings.TrimSpace(string(out))
}

// runIn is like run but executes with the working directory set to dir,
// exercising the cwd-inference path where the <pipeline-dir> argument is
// omitted.
func runIn(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command(binPath, args...)
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		stderr := ""
		if ee, ok := err.(*exec.ExitError); ok {
			stderr = string(ee.Stderr)
		}
		t.Fatalf("(in %s) clavesa %s: %v\nstderr: %s", dir, strings.Join(args, " "), err, stderr)
	}
	return strings.TrimSpace(string(out))
}

// TestPipelineCommandInfersDirFromCwd drives the built binary from inside
// a pipeline directory and confirms commands resolve the pipeline from
// the current directory when the <pipeline-dir> argument is omitted.
func TestPipelineCommandInfersDirFromCwd(t *testing.T) {
	ws := t.TempDir()
	run(t, "workspace", "init", "test-ws", "--workspace", ws)
	run(t, "pipeline", "create", "demo", "--workspace", ws)
	transformID := addNode(t, ws, "demo", "transform")

	pipelineDir := filepath.Join(ws, "demo")

	// pipeline show / node list with no <pipeline-dir> argument.
	runIn(t, pipelineDir, "pipeline", "show")
	listOut := runIn(t, pipelineDir, "node", "list", "--json")
	if !strings.Contains(listOut, transformID) {
		t.Errorf("node list (inferred dir) missing %s: %s", transformID, listOut)
	}
	// node show keeps the node-id positional; the dir is inferred.
	runIn(t, pipelineDir, "node", "show", transformID)

	// From a non-pipeline directory, a bare command must fail clearly.
	cmd := exec.Command(binPath, "pipeline", "show")
	cmd.Dir = ws // workspace root — not a pipeline
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected failure running `pipeline show` from a non-pipeline dir; output: %s", out)
	}
	if !strings.Contains(string(out), "not a pipeline") {
		t.Errorf("error should explain the cwd is not a pipeline, got: %s", out)
	}
}

// addNode runs "node add" and returns the new node's ID. Per ADR-017
// slice 4, source nodes are workspace-registry entries, not pipeline-
// local modules — use addRegistrySourceFromTestdata for those.
func addNode(t *testing.T, ws, pipeline, nodeType string) string {
	t.Helper()
	if nodeType == "source" {
		t.Fatalf("addNode(source) is gone in ADR-017 slice 4 — call addRegistrySourceFromTestdata")
	}
	out := run(t, "node", "add", pipeline, "--type", nodeType, "--workspace", ws)
	parts := strings.SplitN(out, ": ", 2)
	if len(parts) != 2 {
		t.Fatalf("unexpected node add output: %q", out)
	}
	return strings.TrimSpace(parts[1])
}

// addRegistrySourceFromTestdata stands up a tiny http server over the
// repo's testdata dir, registers it as a workspace source under `name`,
// and returns the URL (not used by callers — kept for diagnostics if a
// failing test wants to print it). Call defer srv.Close() in the
// caller's scope so the server outlives the test.
//
// Uses host.docker.internal to address the host from inside the runner
// container — works on Docker Desktop (the dev/CI shape). Linux Docker
// installs need `--add-host=host.docker.internal:host-gateway` in the
// runner invocation; out of scope for slice 4.
func addRegistrySourceFromTestdata(t *testing.T, ws, name, file, format string) (*httptest.Server, string) {
	t.Helper()
	srv := httptest.NewServer(http.FileServer(http.Dir(filepath.Join(repoRoot(), "testdata"))))
	hostPort := strings.TrimPrefix(srv.URL, "http://")
	dockerURL := "http://host.docker.internal:" + strings.SplitN(hostPort, ":", 2)[1] + "/" + file
	run(t, "source", "register", name,
		"--from", dockerURL,
		"--format", format,
		"--workspace", ws,
	)
	return srv, dockerURL
}

// TestPipelineCreateAndList verifies workspace init, pipeline create, node add,
// node connect, and pipeline list.
func TestPipelineCreateAndList(t *testing.T) {
	ws := t.TempDir()

	run(t, "workspace", "init", "test-ws", "--workspace", ws)

	run(t, "pipeline", "create", "first-pipeline", "--workspace", ws)
	srv, _ := addRegistrySourceFromTestdata(t, ws, "orders", "orders.csv", "csv")
	defer srv.Close()
	transformID := addNode(t, ws, "first-pipeline", "transform")
	run(t, "source", "attach", "first-pipeline", "orders", "--to", transformID, "--as", "orders", "--workspace", ws)

	run(t, "pipeline", "create", "second-pipeline", "--workspace", ws)

	listOut := run(t, "pipeline", "list", "--json", "--workspace", ws)
	var pipelines []struct{ Name string }
	if err := json.Unmarshal([]byte(listOut), &pipelines); err != nil {
		t.Fatalf("parse pipeline list: %v\noutput: %s", err, listOut)
	}

	if len(pipelines) < 2 {
		t.Errorf("want ≥2 pipelines, got %d", len(pipelines))
	}

	var found bool
	for _, p := range pipelines {
		if p.Name == "first-pipeline" {
			found = true
			break
		}
	}
	if !found {
		t.Error("first-pipeline not found in pipeline list")
	}
}

// TestPipelineCreateRefusesDuplicateSchema exercises the ADR-016 §5
// schema-ownership guard through the built binary: two pipelines may not
// write into the same <catalog>.<schema>.
func TestPipelineCreateRefusesDuplicateSchema(t *testing.T) {
	ws := t.TempDir()
	run(t, "workspace", "init", "test-ws", "--workspace", ws)
	run(t, "pipeline", "create", "alpha", "--schema", "shared", "--workspace", ws)

	cmd := exec.Command(binPath, "pipeline", "create", "beta", "--schema", "shared", "--workspace", ws)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected non-zero exit creating beta with a taken schema; output: %s", out)
	}
	s := string(out)
	if !strings.Contains(s, "create pipeline:") || !strings.Contains(s, "alpha") || !strings.Contains(s, "shared") {
		t.Errorf("error should name the conflicting pipeline and schema, got: %s", s)
	}
	if _, statErr := os.Stat(filepath.Join(ws, "beta")); !os.IsNotExist(statErr) {
		t.Errorf("rejected create left a beta/ directory behind")
	}
}

// TestTransformPreviewCorrectness builds a pipeline with two transforms and
// asserts that preview output matches expected row counts.
func TestTransformPreviewCorrectness(t *testing.T) {
	ws := t.TempDir()
	testdataDir := filepath.Join(repoRoot(), "testdata")

	run(t, "workspace", "init", "test-ws", "--workspace", ws)
	run(t, "pipeline", "create", "orders-pipeline", "--workspace", ws)
	_ = testdataDir // kept for diagnostics; the registry path uses an httptest server below

	srv, _ := addRegistrySourceFromTestdata(t, ws, "orders", "orders.csv", "csv")
	defer srv.Close()

	// transform1: filter rows with amount > 0
	transformID1 := addNode(t, ws, "orders-pipeline", "transform")
	run(t, "node", "edit", "orders-pipeline", transformID1,
		"--set", "sql=SELECT * FROM orders WHERE amount > 0",
		"--workspace", ws,
	)

	// transform2: filter completed orders
	transformID2 := addNode(t, ws, "orders-pipeline", "transform")
	run(t, "node", "edit", "orders-pipeline", transformID2,
		"--set", "sql=SELECT * FROM orders WHERE status = 'complete'",
		"--workspace", ws,
	)

	// Attach the workspace source to both transforms under alias `orders`
	// (the SQL above references it by that name).
	run(t, "source", "attach", "orders-pipeline", "orders", "--to", transformID1, "--as", "orders", "--workspace", ws)
	run(t, "source", "attach", "orders-pipeline", "orders", "--to", transformID2, "--as", "orders", "--workspace", ws)

	// transform1: expect 4 rows (amount > 0: ord-001,002,004,006)
	out1 := run(t, "node", "preview", "--json", "--rows", "100", "--workspace", ws, "orders-pipeline", transformID1)
	rows1 := parsePreviewRows(t, out1)
	if len(rows1) != 4 {
		t.Errorf("transform1 preview: want 4 rows, got %d\noutput: %s", len(rows1), out1)
	}

	// transform2: expect 3 rows (ord-001, ord-003, ord-006 have status=complete)
	out2 := run(t, "node", "preview", "--json", "--rows", "100", "--workspace", ws, "orders-pipeline", transformID2)
	rows2 := parsePreviewRows(t, out2)
	if len(rows2) != 3 {
		t.Errorf("transform2 preview: want 3 rows, got %d\noutput: %s", len(rows2), out2)
	}
}

func parsePreviewRows(t *testing.T, jsonStr string) []map[string]interface{} {
	t.Helper()
	var result struct {
		Pairs []struct {
			Output []map[string]interface{} `json:"output"`
		} `json:"pairs"`
	}
	if err := json.Unmarshal([]byte(jsonStr), &result); err != nil {
		t.Fatalf("parse preview JSON: %v\ninput: %s", err, jsonStr)
	}
	var rows []map[string]interface{}
	for _, p := range result.Pairs {
		rows = append(rows, p.Output...)
	}
	return rows
}
