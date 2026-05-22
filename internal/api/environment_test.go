package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/api"
)

func newWorkspaceMux(t *testing.T, root string) *http.ServeMux {
	t.Helper()
	mux := http.NewServeMux()
	api.NewWorkspaceHandler(root).RegisterRoutes(mux)
	return mux
}

func envMode(t *testing.T, mux *http.ServeMux, method, body string) (int, string) {
	t.Helper()
	var r *http.Request
	if body != "" {
		r = httptest.NewRequest(method, "/workspace/environment", strings.NewReader(body))
	} else {
		r = httptest.NewRequest(method, "/workspace/environment", nil)
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, r)
	if rr.Code != http.StatusOK {
		return rr.Code, ""
	}
	var resp struct {
		Mode string `json:"mode"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode %s response: %v", method, err)
	}
	return rr.Code, resp.Mode
}

// TestEnvironmentModeDefaultsLocal — a fresh workspace with no
// environment.json reports "local".
func TestEnvironmentModeDefaultsLocal(t *testing.T) {
	mux := newWorkspaceMux(t, t.TempDir())
	code, mode := envMode(t, mux, http.MethodGet, "")
	if code != http.StatusOK || mode != "local" {
		t.Fatalf("GET = (%d, %q), want (200, local)", code, mode)
	}
}

// TestEnvironmentModeSetAndGet — PUT persists the mode; a subsequent GET
// reflects it.
func TestEnvironmentModeSetAndGet(t *testing.T) {
	mux := newWorkspaceMux(t, t.TempDir())

	if code, mode := envMode(t, mux, http.MethodPut, `{"mode":"cloud"}`); code != http.StatusOK || mode != "cloud" {
		t.Fatalf("PUT cloud = (%d, %q), want (200, cloud)", code, mode)
	}
	if code, mode := envMode(t, mux, http.MethodGet, ""); code != http.StatusOK || mode != "cloud" {
		t.Fatalf("GET after PUT = (%d, %q), want (200, cloud)", code, mode)
	}
	if code, mode := envMode(t, mux, http.MethodPut, `{"mode":"local"}`); code != http.StatusOK || mode != "local" {
		t.Fatalf("PUT local = (%d, %q), want (200, local)", code, mode)
	}
}

// TestEnvironmentModeRejectsBadInput — an unknown mode and malformed
// JSON both 400.
func TestEnvironmentModeRejectsBadInput(t *testing.T) {
	mux := newWorkspaceMux(t, t.TempDir())

	if code, _ := envMode(t, mux, http.MethodPut, `{"mode":"banana"}`); code != http.StatusBadRequest {
		t.Errorf("PUT banana: status = %d, want 400", code)
	}
	if code, _ := envMode(t, mux, http.MethodPut, `not json`); code != http.StatusBadRequest {
		t.Errorf("PUT malformed: status = %d, want 400", code)
	}
}
