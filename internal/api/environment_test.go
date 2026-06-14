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

// envWarehouse drives GET/PUT /workspace/environment and returns the
// status plus both response keys: warehouse (current) and mode (the
// deprecated alias).
func envWarehouse(t *testing.T, mux *http.ServeMux, method, body string) (int, string, string) {
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
		return rr.Code, "", ""
	}
	var resp struct {
		Warehouse string `json:"warehouse"`
		Mode      string `json:"mode"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode %s response: %v", method, err)
	}
	return rr.Code, resp.Warehouse, resp.Mode
}

// TestEnvironmentWarehouseDefaultsLocal — a fresh workspace with no
// environment.json reports "local" under both keys.
func TestEnvironmentWarehouseDefaultsLocal(t *testing.T) {
	mux := newWorkspaceMux(t, t.TempDir())
	code, warehouse, mode := envWarehouse(t, mux, http.MethodGet, "")
	if code != http.StatusOK || warehouse != "local" || mode != "local" {
		t.Fatalf("GET = (%d, %q, %q), want (200, local, local)", code, warehouse, mode)
	}
}

// TestEnvironmentWarehouseSetAndGet — PUT persists the warehouse; a
// subsequent GET reflects it under both keys.
func TestEnvironmentWarehouseSetAndGet(t *testing.T) {
	mux := newWorkspaceMux(t, t.TempDir())

	if code, warehouse, mode := envWarehouse(t, mux, http.MethodPut, `{"warehouse":"cloud"}`); code != http.StatusOK || warehouse != "cloud" || mode != "cloud" {
		t.Fatalf("PUT cloud = (%d, %q, %q), want (200, cloud, cloud)", code, warehouse, mode)
	}
	if code, warehouse, mode := envWarehouse(t, mux, http.MethodGet, ""); code != http.StatusOK || warehouse != "cloud" || mode != "cloud" {
		t.Fatalf("GET after PUT = (%d, %q, %q), want (200, cloud, cloud)", code, warehouse, mode)
	}
	if code, warehouse, mode := envWarehouse(t, mux, http.MethodPut, `{"warehouse":"local"}`); code != http.StatusOK || warehouse != "local" || mode != "local" {
		t.Fatalf("PUT local = (%d, %q, %q), want (200, local, local)", code, warehouse, mode)
	}
}

// TestEnvironmentWarehouseLegacyModeKey — a PUT carrying only the
// deprecated `mode` key still works; when both keys are present,
// `warehouse` wins.
func TestEnvironmentWarehouseLegacyModeKey(t *testing.T) {
	mux := newWorkspaceMux(t, t.TempDir())

	if code, warehouse, mode := envWarehouse(t, mux, http.MethodPut, `{"mode":"cloud"}`); code != http.StatusOK || warehouse != "cloud" || mode != "cloud" {
		t.Fatalf("PUT legacy mode=cloud = (%d, %q, %q), want (200, cloud, cloud)", code, warehouse, mode)
	}
	if code, warehouse, mode := envWarehouse(t, mux, http.MethodPut, `{"warehouse":"local","mode":"cloud"}`); code != http.StatusOK || warehouse != "local" || mode != "local" {
		t.Fatalf("PUT both keys = (%d, %q, %q), want (200, local, local) — warehouse wins", code, warehouse, mode)
	}
}

// TestEnvironmentWarehouseRejectsBadInput — an unknown warehouse and
// malformed JSON both 400.
func TestEnvironmentWarehouseRejectsBadInput(t *testing.T) {
	mux := newWorkspaceMux(t, t.TempDir())

	if code, _, _ := envWarehouse(t, mux, http.MethodPut, `{"warehouse":"banana"}`); code != http.StatusBadRequest {
		t.Errorf("PUT banana: status = %d, want 400", code)
	}
	if code, _, _ := envWarehouse(t, mux, http.MethodPut, `not json`); code != http.StatusBadRequest {
		t.Errorf("PUT malformed: status = %d, want 400", code)
	}
}
