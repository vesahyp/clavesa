package pipelinestatus_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/pipelinestatus"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// writeLocalPipeline lays a minimal `compute = "local"` pipeline at <dir>/main.tf
// so the resolver classifies it correctly. Mirrors the fixture used by the
// resolver tests in observability/.
func writeLocalPipeline(t *testing.T, dir string) {
	t.Helper()
	tf := `variable "pipeline_name" { type = string default = "demo" }

module "src" {
  source        = "clavesa/source/aws"
  pipeline_name = var.pipeline_name
  bucket        = "in"
  prefix        = "raw/"
  format        = "csv"
}

module "xform" {
  source        = "clavesa/transform/aws"
  pipeline_name = var.pipeline_name
  inputs        = { primary = module.src.outputs["default"] }
  compute       = "local"
  language      = "sparksql"
  sql           = "SELECT 1"
}
`
	if err := os.WriteFile(filepath.Join(dir, "main.tf"), []byte(tf), 0o644); err != nil {
		t.Fatalf("write tf: %v", err)
	}
}

// writeRunFixture seeds one run's per-node log file under
// <pipelineDir>/.clavesa/runs/<runID>/logs/ — the surface ExecutionLogs reads.
// Per-run state moved to the warehouse `_progress/<run>/_run.json` marker
// (ADR-024); only logs remain in the per-pipeline runs dir.
func writeRunFixture(t *testing.T, pipelineDir, runID string) {
	t.Helper()
	logPath := observability.RunLogPath(pipelineDir, runID, "xform")
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		t.Fatalf("mkdir log dir: %v", err)
	}
	if err := os.WriteFile(logPath, []byte("starting xform\nselected 1 row\n"), 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}
}

func TestExecutionStatesDispatchToLocal(t *testing.T) {
	workspace := t.TempDir()
	pipelineDir := filepath.Join(workspace, "demo")
	if err := os.MkdirAll(pipelineDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeLocalPipeline(t, pipelineDir)
	writeProgressFixture(t, workspace, "run-xyz", "demo")

	resolver := observability.NewResolver(
		workspace,
		nil, // no cloud provider — proves dispatch routes to local
		observability.NewLocalProvider(workspace),
	)
	h := pipelinestatus.NewHandler(workspace).WithResolver(resolver)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	q := url.Values{}
	q.Set("dir", "demo")
	q.Set("run", "run-xyz")
	req := httptest.NewRequest(http.MethodGet, "/pipeline/execution/states?"+q.Encode(), nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}

	var res observability.ExecutionStatesResult
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if res.Status != "RUNNING" {
		t.Errorf("Status = %q, want RUNNING", res.Status)
	}
	if got := res.States["xform"].Status; got != "RUNNING" {
		t.Errorf("xform status = %q, want RUNNING", got)
	}
}

// writeProgressFixture seeds one run under the workspace warehouse
// `_progress/<run>/` tree (ADR-024): a fresh "running" xform node marker plus
// the run-level `_run.json`. This is the read path ExecutionStates / Runs /
// NodeRuns now consume for local runs.
func writeProgressFixture(t *testing.T, workspaceRoot, runID, pipeline string) {
	t.Helper()
	warehouse := filepath.Join(workspaceRoot, ".clavesa", "warehouse")
	dir := filepath.Join(warehouse, "_progress", runID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir progress dir: %v", err)
	}
	body := fmt.Sprintf(`{"status":"running","updated_ms":%d}`, time.Now().UnixMilli())
	if err := os.WriteFile(filepath.Join(dir, "xform.json"), []byte(body), 0o644); err != nil {
		t.Fatalf("write node marker: %v", err)
	}
	store := observability.NewFileProgressStore(warehouse)
	if err := observability.WriteRunMarker(context.Background(), store, runID, observability.RunMarker{
		Status: "RUNNING", Pipeline: pipeline,
	}); err != nil {
		t.Fatalf("WriteRunMarker: %v", err)
	}
}

func TestExecutionLogsDispatchToLocal(t *testing.T) {
	workspace := t.TempDir()
	pipelineDir := filepath.Join(workspace, "demo")
	if err := os.MkdirAll(pipelineDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeLocalPipeline(t, pipelineDir)
	writeRunFixture(t, pipelineDir, "run-xyz")

	resolver := observability.NewResolver(
		workspace,
		nil,
		observability.NewLocalProvider(workspace),
	)
	h := pipelinestatus.NewHandler(workspace).WithResolver(resolver)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	q := url.Values{}
	q.Set("dir", "demo")
	q.Set("run", "run-xyz")
	q.Set("step", "xform")
	req := httptest.NewRequest(http.MethodGet, "/pipeline/execution/logs?"+q.Encode(), nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}

	var res observability.ExecutionLogsResult
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(res.Events) != 2 {
		t.Errorf("len(Events) = %d, want 2", len(res.Events))
	}
	if res.FunctionName != "xform" {
		t.Errorf("FunctionName = %q, want xform", res.FunctionName)
	}
}

// TestExecutionStatesCloudLocalRoutesCloudOnCloudWarehouse proves the
// post-ADR-024 routing: on a CLOUD warehouse, a cloud-local run id (the old
// `local-` prefix) routes to the CLOUD provider, which reads the S3
// `_progress` tree the runner wrote there — not the filesystem provider. The
// old `local-`-prefix special-case that force-routed such runs to the local
// provider is gone. With a nil cloud provider the request surfaces the
// cloud-unavailable error, confirming it no longer falls back to local.
func TestExecutionStatesCloudLocalRoutesCloudOnCloudWarehouse(t *testing.T) {
	workspace := t.TempDir()
	pipelineDir := filepath.Join(workspace, "demo")
	if err := os.MkdirAll(pipelineDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeLocalPipeline(t, pipelineDir)

	// Cloud warehouse: r.For(dir) returns the cloud provider.
	if err := workspaceWriteCloud(t, workspace); err != nil {
		t.Fatalf("write cloud warehouse: %v", err)
	}

	resolver := observability.NewResolver(
		workspace,
		nil, // no cloud provider wired
		observability.NewLocalProvider(workspace),
	)
	h := pipelinestatus.NewHandler(workspace).WithResolver(resolver)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	q := url.Values{}
	q.Set("dir", "demo")
	q.Set("run", "local-abc123")
	req := httptest.NewRequest(http.MethodGet, "/pipeline/execution/states?"+q.Encode(), nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	// Routed to the (nil) cloud provider → cloud-unavailable error, NOT a
	// local-provider 200. That's the proof the run id no longer steers
	// dispatch to the local provider.
	if w.Code == http.StatusOK {
		t.Fatalf("expected cloud routing (non-200, cloud provider unavailable), got 200: %s", w.Body.String())
	}
}

// fakeCloudLocalRunner records the dir it was dispatched with and returns a
// canned run id, so the POST /pipeline/run dispatch can be asserted without
// Docker/AWS.
type fakeCloudLocalRunner struct {
	called  bool
	gotDir  string
	gotOpts pipelinestatus.RunOpts
	runID   string
}

func (f *fakeCloudLocalRunner) StartRunCloudLocal(_ context.Context, dir string, opts pipelinestatus.RunOpts) (string, error) {
	f.called = true
	f.gotDir = dir
	f.gotOpts = opts
	return f.runID, nil
}

// TestRunPipelineComputeLocalRoutesCloudLocal proves POST /pipeline/run with
// body compute="local" on a cloud warehouse dispatches StartRunCloudLocal and
// returns the local-prefixed run id (ADR-024). The local-compute resolver
// guard is off (cloud warehouse), so the request falls past the local path
// into the cloud-local branch.
func TestRunPipelineComputeLocalRoutesCloudLocal(t *testing.T) {
	workspace := t.TempDir()
	pipelineDir := filepath.Join(workspace, "demo")
	if err := os.MkdirAll(pipelineDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeLocalPipeline(t, pipelineDir)
	if err := workspaceWriteCloud(t, workspace); err != nil {
		t.Fatalf("write cloud warehouse: %v", err)
	}

	resolver := observability.NewResolver(workspace, nil, observability.NewLocalProvider(workspace))
	fake := &fakeCloudLocalRunner{runID: "local-deadbeef"}
	h := pipelinestatus.NewHandler(workspace).
		WithResolver(resolver).
		WithCloudLocalRunner(fake)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body, _ := json.Marshal(map[string]any{"dir": "demo", "compute": "local", "force": true})
	req := httptest.NewRequest(http.MethodPost, "/pipeline/run", bytes.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}
	if !fake.called {
		t.Fatal("StartRunCloudLocal was not dispatched")
	}
	if !fake.gotOpts.Force {
		t.Error("Force flag did not thread through to StartRunCloudLocal")
	}
	var res struct {
		RunID        string `json:"run_id"`
		ExecutionARN string `json:"execution_arn"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if res.RunID != "local-deadbeef" {
		t.Errorf("run_id = %q, want local-deadbeef", res.RunID)
	}
	if res.ExecutionARN != "" {
		t.Errorf("execution_arn = %q, want empty (cloud-local is not an SFN execution)", res.ExecutionARN)
	}
}

func workspaceWriteCloud(t *testing.T, root string) error {
	t.Helper()
	return workspace.WriteWarehouse(root, workspace.WarehouseCloud)
}

func TestExecutionStatesMissingArnAndDir(t *testing.T) {
	// Without resolver and without `arn`, the handler should 400 cleanly.
	h := pipelinestatus.NewHandler(t.TempDir())
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/pipeline/execution/states", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}
