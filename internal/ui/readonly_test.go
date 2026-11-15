package ui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/wsdeps"
)

// recordWorkspaceRoutes builds one workspace's route table on a recording
// router, over a fresh temp workspace. Every handler constructor is lazy,
// so this spawns no container and makes no network call.
func recordWorkspaceRoutes(t *testing.T) (*routeRecorder, WorkspaceDeps) {
	t.Helper()
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	d := WorkspaceDeps{Deps: wsdeps.New(context.Background(), t.TempDir(), wsdeps.Options{})}
	rec := newRouteRecorder()
	registerWorkspaceRoutes(d, rec)
	if len(rec.patterns) == 0 {
		t.Fatal("no routes recorded")
	}
	return rec, d
}

var wildcardRE = regexp.MustCompile(`\{[^}]+\}`)

// concretePath turns a ServeMux pattern path into a request path that the
// pattern matches: every {wildcard} becomes "x".
func concretePath(p string) string {
	return wildcardRE.ReplaceAllString(p, "x")
}

func isRead(method string) bool {
	return method == http.MethodGet || method == http.MethodHead
}

// TestReadOnly_EveryWriteRouteAnswers405 walks the registered route table
// of one workspace and sends every non-GET route its own method through
// the read-only guard over the REAL mux. Each must answer 405 with
// Allow: GET, HEAD and a JSON error, so a route added later cannot open a
// write in read-only mode (ADR-032). The guard answers before the handler
// runs, so no write happens.
func TestReadOnly_EveryWriteRouteAnswers405(t *testing.T) {
	rec, _ := recordWorkspaceRoutes(t)
	guarded := ReadOnlyGuard(rec.mux)

	writes := 0
	seen := map[string]bool{}
	for _, p := range rec.patterns {
		method, path := splitPattern(p)
		if method == "" {
			t.Errorf("route %q has no method: every route must declare its method so the table can be walked", p)
			continue
		}
		seen[p] = true
		if isRead(method) {
			continue
		}
		writes++
		req := httptest.NewRequest(method, concretePath(path), strings.NewReader(`{}`))
		w := httptest.NewRecorder()
		guarded.ServeHTTP(w, req)
		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s: status %d, want 405", p, w.Code)
			continue
		}
		if got := w.Header().Get("Allow"); got != "GET, HEAD" {
			t.Errorf("%s: Allow %q, want %q", p, got, "GET, HEAD")
		}
		var body map[string]string
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body["error"] == "" {
			t.Errorf("%s: body %q is not a JSON error", p, w.Body.String())
		}
	}
	if writes < 40 {
		t.Errorf("only %d write routes found; the route table walk is missing routes", writes)
	}
	// The two read POSTs are refused too (accepted in ADR-032 slice 2).
	for _, p := range []string{"POST /data/query", "POST /dashboards/query", "POST /pipeline/run", "PUT /workspace/aws-profile"} {
		if !seen[p] {
			t.Errorf("route %q missing from the recorded table", p)
		}
	}
}

// TestReadOnly_ReadRoutesPass checks the other half on the same route
// table: every GET route answers through the guard (GET and HEAD), and
// no non-GET request reaches a handler. Handlers are stubs here, so the
// GET routes do not call AWS or Spark.
func TestReadOnly_ReadRoutesPass(t *testing.T) {
	rec, _ := recordWorkspaceRoutes(t)
	stub := http.NewServeMux()
	reached := map[string]int{}
	for _, p := range rec.patterns {
		p := p
		stub.HandleFunc(p, func(w http.ResponseWriter, r *http.Request) {
			reached[p]++
			w.WriteHeader(http.StatusOK)
		})
	}
	guarded := ReadOnlyGuard(stub)

	reads := 0
	for _, p := range rec.patterns {
		method, path := splitPattern(p)
		if isRead(method) {
			reads++
			for _, m := range []string{http.MethodGet, http.MethodHead} {
				w := httptest.NewRecorder()
				guarded.ServeHTTP(w, httptest.NewRequest(m, concretePath(path), nil))
				if w.Code != http.StatusOK {
					t.Errorf("%s %s: status %d, want 200 (read routes must pass the guard)", m, path, w.Code)
				}
			}
			continue
		}
		w := httptest.NewRecorder()
		guarded.ServeHTTP(w, httptest.NewRequest(method, concretePath(path), nil))
		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s: status %d, want 405", p, w.Code)
		}
		if reached[p] != 0 {
			t.Errorf("%s: the handler ran in read-only mode", p)
		}
	}
	if reads < 30 {
		t.Errorf("only %d read routes found", reads)
	}

	// Without the guard, a write route reaches its handler.
	w := httptest.NewRecorder()
	stub.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/pipeline/run", nil))
	if w.Code != http.StatusOK || reached["POST /pipeline/run"] != 1 {
		t.Errorf("unguarded POST /pipeline/run: status %d, reached %d", w.Code, reached["POST /pipeline/run"])
	}
}

// TestBuildWorkspaceMux_ReadOnlyFlag checks that the public constructor
// applies the guard when ReadOnly is set.
func TestBuildWorkspaceMux_ReadOnlyFlag(t *testing.T) {
	_, d := recordWorkspaceRoutes(t)
	d.ReadOnly = true
	h := BuildWorkspaceMux(d)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/pipeline/run", strings.NewReader(`{"dir":"x"}`)))
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /pipeline/run: status %d, want 405", w.Code)
	}
}
