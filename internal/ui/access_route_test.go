package ui

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/wsdeps"
)

// TestAccessRouteIsMacOnly: GET /workspace/access names hidden columns,
// so the Mac's own UI serves it and a read-only (hosted) mux does not.
func TestAccessRouteIsMacOnly(t *testing.T) {
	ws := t.TempDir()
	pipe := filepath.Join(ws, "taxi")
	if err := os.MkdirAll(pipe, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ws, "clavesa.json"), []byte(`{"name":"demo","cloud":"aws","version":1,"catalog":"clavesa_demo","access":{"tags":["open","restricted"]}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	tf := `module "trips" {
  source = "../.clavesa/modules/v1.0.0/transform/aws"
  name   = "trips"
  output_definitions = { default = { table_tag = "open", column_tags = { trip_distance = "restricted" } } }
  sql = "SELECT 1"
}
`
	if err := os.WriteFile(filepath.Join(pipe, "main.tf"), []byte(tf), 0o644); err != nil {
		t.Fatal(err)
	}
	get := func(readOnly bool) (int, string) {
		h := BuildWorkspaceMux(WorkspaceDeps{Deps: wsdeps.New(context.Background(), ws, wsdeps.Options{}), ReadOnly: readOnly})
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/workspace/access", nil))
		return w.Code, w.Body.String()
	}
	if code, body := get(false); code != 200 || !strings.Contains(body, `"trip_distance"`) || !strings.Contains(body, `"enabled":true`) {
		t.Errorf("Mac UI: %d %s", code, body)
	}
	if code, body := get(true); code == 200 || strings.Contains(body, "trip_distance") {
		t.Errorf("read-only mux served the tags: %d %s", code, body)
	}
	// GET /pipeline: the graph keeps column_tags on the Mac and drops
	// them in a read-only mux.
	graph := func(readOnly bool) string {
		h := BuildWorkspaceMux(WorkspaceDeps{Deps: wsdeps.New(context.Background(), ws, wsdeps.Options{}), ReadOnly: readOnly})
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/pipeline?dir=taxi", nil))
		if w.Code != 200 {
			t.Fatalf("GET /pipeline (read-only %v): %d %s", readOnly, w.Code, w.Body)
		}
		return w.Body.String()
	}
	if !strings.Contains(graph(false), "trip_distance") {
		t.Error("Mac graph lost column_tags")
	}
	if b := graph(true); strings.Contains(b, "column_tags") || strings.Contains(b, "trip_distance") {
		t.Errorf("read-only graph keeps column_tags: %s", b)
	}
}
