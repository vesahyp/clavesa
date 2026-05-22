package api_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vesahyp/clavesa/internal/api"
	"github.com/vesahyp/clavesa/internal/fileops"
)

// fakeLineager returns canned edges (or an error) so the handler can be
// exercised without a real Service. The Service-side derivation has its own
// unit tests; here we cover only the HTTP shape.
type fakeLineager struct {
	edges []api.LineageEdge
	err   error
}

func (f fakeLineager) Lineage(_ string) (*api.LineageResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &api.LineageResponse{Edges: f.edges}, nil
}

func newLineageMux(t *testing.T, lin api.Lineager) *http.ServeMux {
	t.Helper()
	h := api.New(fileops.New(), "")
	if lin != nil {
		h = h.WithLineage(lin)
	}
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	return mux
}

func TestGetLineageOK(t *testing.T) {
	t.Parallel()
	want := []api.LineageEdge{
		{FromNode: "src", FromType: "source", ToNode: "xform", ToType: "transform"},
		{
			FromNode: "xform", FromType: "transform",
			ToNode: "agg", ToType: "transform",
			ViaTable: "clavesa_demo.xform__default",
		},
	}
	mux := newLineageMux(t, fakeLineager{edges: want})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/pipeline/lineage?dir=/tmp/anything", nil)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", rr.Code, rr.Body.String())
	}
	var got api.LineageResponse
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Edges) != 2 {
		t.Fatalf("expected 2 edges, got %d", len(got.Edges))
	}
	if got.Edges[1].ViaTable != "clavesa_demo.xform__default" {
		t.Errorf("via_table = %q, want clavesa_demo.xform__default", got.Edges[1].ViaTable)
	}
}

func TestGetLineageMissingDir(t *testing.T) {
	t.Parallel()
	mux := newLineageMux(t, fakeLineager{})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/pipeline/lineage", nil)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestGetLineageServiceError(t *testing.T) {
	t.Parallel()
	mux := newLineageMux(t, fakeLineager{err: errors.New("parse: malformed .tf")})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/pipeline/lineage?dir=/tmp/x", nil)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rr.Code)
	}
}

// TestGetLineageNotWired guards against forgetting WithLineage at server
// construction time — the server must respond with a clear error rather
// than panicking on a nil interface deref.
func TestGetLineageNotWired(t *testing.T) {
	t.Parallel()
	mux := newLineageMux(t, nil) // no WithLineage
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/pipeline/lineage?dir=/tmp/x", nil)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rr.Code)
	}
}

// TestGetLineageEmptyEdgesNotNull verifies the JSON serializes "edges": []
// rather than "edges": null when a pipeline has no edges — UI Zod parses
// an empty array fine but treats null as a schema error.
func TestGetLineageEmptyEdgesNotNull(t *testing.T) {
	t.Parallel()
	mux := newLineageMux(t, fakeLineager{edges: nil})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/pipeline/lineage?dir=/tmp/x", nil)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !contains(body, `"edges":[]`) {
		t.Errorf("body = %q, want it to contain \"edges\":[]", body)
	}
}

func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
