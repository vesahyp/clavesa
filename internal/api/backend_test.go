package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/api"
)

// backendReq issues a GET/PUT /workspace/backend request. body == "" for
// GET (no body); for PUT, "null" clears. Returns the status and, on 200,
// the decoded response.
func backendReq(t *testing.T, mux *http.ServeMux, method, body string) (int, api.BackendResponse) {
	t.Helper()
	var r *http.Request
	if body != "" {
		r = httptest.NewRequest(method, "/workspace/backend", strings.NewReader(body))
	} else {
		r = httptest.NewRequest(method, "/workspace/backend", nil)
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, r)
	if rr.Code != http.StatusOK {
		return rr.Code, api.BackendResponse{}
	}
	var resp api.BackendResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode %s response: %v", method, err)
	}
	return rr.Code, resp
}

func initWorkspaceManifest(t *testing.T, dir, name string) {
	t.Helper()
	manifest := `{"name":"` + name + `","cloud":"aws","version":1,"catalog":"clavesa_` + name + `"}` + "\n"
	if err := os.WriteFile(filepath.Join(dir, "clavesa.json"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	// A root main.tf carrying the real local-backend line, as
	// workspace.Init leaves it — SetBackend's clear-refusal check looks
	// for backend.tf, not main.tf, but a stack directory that looks
	// nothing like a real workspace is worth avoiding in these tests.
	mainTF := "terraform {\n  backend \"local\" {}\n}\n"
	if err := os.WriteFile(filepath.Join(dir, "main.tf"), []byte(mainTF), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestBackendDefaultLocal — a fresh workspace reports Configured: false.
func TestBackendDefaultLocal(t *testing.T) {
	ws := t.TempDir()
	initWorkspaceManifest(t, ws, "demo")
	mux := http.NewServeMux()
	api.NewWorkspaceHandler(ws).RegisterRoutes(mux)

	code, resp := backendReq(t, mux, http.MethodGet, "")
	if code != http.StatusOK || resp.Configured {
		t.Fatalf("GET = (%d, %+v), want (200, Configured=false)", code, resp)
	}
}

// TestBackendSetAndGet — PUT a valid backend persists it, and GET reflects it.
func TestBackendSetAndGet(t *testing.T) {
	ws := t.TempDir()
	initWorkspaceManifest(t, ws, "demo")
	mux := http.NewServeMux()
	api.NewWorkspaceHandler(ws).RegisterRoutes(mux)

	body := `{"type":"s3","bucket":"demo-tfstate","region":"eu-north-1"}`
	code, resp := backendReq(t, mux, http.MethodPut, body)
	if code != http.StatusOK || !resp.Configured || resp.Bucket != "demo-tfstate" || resp.Region != "eu-north-1" {
		t.Fatalf("PUT = (%d, %+v), want (200, configured demo-tfstate/eu-north-1)", code, resp)
	}
	if resp.KeyPrefix != "clavesa/" {
		t.Errorf("PUT response key_prefix = %q, want default %q", resp.KeyPrefix, "clavesa/")
	}

	code, resp = backendReq(t, mux, http.MethodGet, "")
	if code != http.StatusOK || !resp.Configured || resp.Bucket != "demo-tfstate" {
		t.Fatalf("GET after PUT = (%d, %+v), want configured demo-tfstate", code, resp)
	}
}

// TestBackendSetRejectsInvalid — PUT with a missing required field is a 400.
func TestBackendSetRejectsInvalid(t *testing.T) {
	ws := t.TempDir()
	initWorkspaceManifest(t, ws, "demo")
	mux := http.NewServeMux()
	api.NewWorkspaceHandler(ws).RegisterRoutes(mux)

	code, _ := backendReq(t, mux, http.MethodPut, `{"type":"s3","region":"eu-north-1"}`) // missing bucket
	if code != http.StatusBadRequest {
		t.Errorf("PUT missing bucket: status = %d, want 400", code)
	}
}

// TestBackendClear — PUT a JSON null body clears a configured backend.
func TestBackendClear(t *testing.T) {
	ws := t.TempDir()
	initWorkspaceManifest(t, ws, "demo")
	mux := http.NewServeMux()
	api.NewWorkspaceHandler(ws).RegisterRoutes(mux)

	if code, _ := backendReq(t, mux, http.MethodPut, `{"type":"s3","bucket":"b","region":"eu-north-1"}`); code != http.StatusOK {
		t.Fatalf("PUT backend: status = %d, want 200", code)
	}
	code, resp := backendReq(t, mux, http.MethodPut, "null")
	if code != http.StatusOK || resp.Configured {
		t.Fatalf("PUT null (clear) = (%d, %+v), want (200, Configured=false)", code, resp)
	}
}

// TestBackendClearRefusesAfterMigration — once a stack has a backend.tf,
// clearing is refused with 409 (ADR-025 "No split-brain").
func TestBackendClearRefusesAfterMigration(t *testing.T) {
	ws := t.TempDir()
	initWorkspaceManifest(t, ws, "demo")
	mux := http.NewServeMux()
	api.NewWorkspaceHandler(ws).RegisterRoutes(mux)

	if code, _ := backendReq(t, mux, http.MethodPut, `{"type":"s3","bucket":"b","region":"eu-north-1"}`); code != http.StatusOK {
		t.Fatalf("PUT backend: status = %d, want 200", code)
	}
	// Simulate a migrated workspace stack: a backend.tf on disk.
	if err := os.WriteFile(filepath.Join(ws, "backend.tf"), []byte("# managed\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	code, _ := backendReq(t, mux, http.MethodPut, "null")
	if code != http.StatusConflict {
		t.Errorf("PUT null after a stack migrated: status = %d, want 409", code)
	}
}

// TestMigrateBackendNoBackendConfigured — POST /workspace/backend/migrate
// with no backend set is a 400 and still returns a well-formed body.
func TestMigrateBackendNoBackendConfigured(t *testing.T) {
	ws := t.TempDir()
	initWorkspaceManifest(t, ws, "demo")
	mux := http.NewServeMux()
	api.NewWorkspaceHandler(ws).RegisterRoutes(mux)

	r := httptest.NewRequest(http.MethodPost, "/workspace/backend/migrate", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, r)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("POST migrate with no backend: status = %d, want 400", rr.Code)
	}
	var body struct {
		Error  string `json:"error"`
		Stacks []any  `json:"stacks"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode migrate response: %v", err)
	}
	if body.Error == "" || !strings.Contains(body.Error, "set-backend") {
		t.Errorf("migrate error = %q, want it to point at set-backend", body.Error)
	}
}
