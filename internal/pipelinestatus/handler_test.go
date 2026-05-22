package pipelinestatus_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/pipelinestatus"
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

// writeRunFixture seeds one run's progress channel under
// <pipelineDir>/.clavesa/runs/<runID>/, including a state.json + one log
// file. Same shape RunPipeline writes during a real local execution.
func writeRunFixture(t *testing.T, pipelineDir, runID string) {
	t.Helper()
	state := &observability.RunStateFile{
		RunID:     runID,
		Pipeline:  filepath.Base(pipelineDir),
		Status:    "RUNNING",
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano),
		States: map[string]observability.NodeRunState{
			"xform": {
				Status:    "RUNNING",
				EnteredAt: time.Now().UTC().Format(time.RFC3339Nano),
			},
		},
	}
	if err := observability.WriteRunState(pipelineDir, state); err != nil {
		t.Fatalf("WriteRunState: %v", err)
	}
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
	writeRunFixture(t, pipelineDir, "run-xyz")

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
