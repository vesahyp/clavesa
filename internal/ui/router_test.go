package ui

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
)

func writeManifest(t *testing.T, dir, body string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "clavesa.json"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// fixtureWorkspaces lays out a --workspaces dir: two workspaces, one
// directory without a manifest and one whose name is not path-safe.
func fixtureWorkspaces(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeManifest(t, filepath.Join(root, "alpha"), `{"name":"Alpha WS","version":1,"backend":{"type":"s3","bucket":"b","region":"eu-north-1"}}`)
	writeManifest(t, filepath.Join(root, "beta"), `{"name":"beta","version":1}`)
	if err := os.MkdirAll(filepath.Join(root, "not-a-workspace"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeManifest(t, filepath.Join(root, "bad name"), `{"name":"bad","version":1}`)
	return root
}

var testIndex = fstest.MapFS{
	"index.html":    {Data: []byte("<!doctype html><html><head><title>x</title></head><body></body></html>")},
	"assets/app.js": {Data: []byte("console.log(1)")},
}

type stubFactory struct {
	calls map[string]int
}

func (s *stubFactory) build(e WorkspaceEntry) (http.Handler, error) {
	s.calls[e.Slug]++
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"ws": e.Slug, "path": r.URL.Path, "method": r.Method})
	}), nil
}

func newTestRouter(t *testing.T, readOnly bool) (*MultiRouter, *stubFactory) {
	t.Helper()
	entries, err := DiscoverWorkspaces(fixtureWorkspaces(t))
	if err != nil {
		t.Fatal(err)
	}
	f := &stubFactory{calls: map[string]int{}}
	return NewMultiRouter(entries, f.build, testIndex, readOnly), f
}

func get(t *testing.T, h http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(method, path, nil))
	return w
}

func TestDiscoverWorkspaces(t *testing.T) {
	entries, err := DiscoverWorkspaces(fixtureWorkspaces(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2: %+v", len(entries), entries)
	}
	a, b := entries[0], entries[1]
	if a.Slug != "alpha" || a.Name != "Alpha WS" || a.Region != "eu-north-1" || a.APIBase != "/w/alpha/api" {
		t.Errorf("alpha entry: %+v", a)
	}
	if b.Slug != "beta" || b.Kind != "local" {
		t.Errorf("beta entry: %+v (a workspace with no environment file is local)", b)
	}
}

func TestMultiRouter_ListWorkspaces(t *testing.T) {
	r, _ := newTestRouter(t, false)
	w := get(t, r, http.MethodGet, "/api/workspaces")
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "not-a-workspace") || strings.Contains(w.Body.String(), `"Path"`) {
		t.Errorf("list leaks a non-workspace or a local path: %s", w.Body.String())
	}
	var body struct {
		Workspaces []map[string]any `json:"workspaces"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Workspaces) != 2 || body.Workspaces[0]["slug"] != "alpha" || body.Workspaces[0]["kind"] == nil {
		t.Errorf("workspaces: %+v", body.Workspaces)
	}
	if _, ok := body.Workspaces[0]["path"]; ok {
		t.Errorf("list carries a local path: %+v", body.Workspaces[0])
	}
}

func TestMultiRouter_PrefixRouting(t *testing.T) {
	r, f := newTestRouter(t, false)
	for i := 0; i < 2; i++ {
		w := get(t, r, http.MethodGet, "/w/alpha/api/pipeline/status?dir=x")
		if w.Code != http.StatusOK {
			t.Fatalf("status %d", w.Code)
		}
		var got map[string]string
		_ = json.Unmarshal(w.Body.Bytes(), &got)
		if got["ws"] != "alpha" || got["path"] != "/pipeline/status" {
			t.Errorf("routed to %+v, want alpha /pipeline/status", got)
		}
	}
	w := get(t, r, http.MethodGet, "/w/beta/api/pipelines")
	var got map[string]string
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["ws"] != "beta" || got["path"] != "/pipelines" {
		t.Errorf("routed to %+v, want beta /pipelines", got)
	}
	if f.calls["alpha"] != 1 || f.calls["beta"] != 1 {
		t.Errorf("factory calls %v, want one per workspace (cached mux)", f.calls)
	}
}

func TestMultiRouter_UnknownWorkspace404(t *testing.T) {
	r, f := newTestRouter(t, false)
	for _, p := range []string{"/w/nope/api/pipelines", "/w/not-a-workspace/api/workspace/summary", "/api/pipelines"} {
		w := get(t, r, http.MethodGet, p)
		if w.Code != http.StatusNotFound {
			t.Errorf("%s: status %d, want 404", p, w.Code)
		}
		var body map[string]string
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body["error"] == "" {
			t.Errorf("%s: body %q is not a JSON error", p, w.Body.String())
		}
	}
	if len(f.calls) != 0 {
		t.Errorf("factory ran for an unknown workspace: %v", f.calls)
	}
}

func TestMultiRouter_SPAConfigInjection(t *testing.T) {
	r, _ := newTestRouter(t, true)
	cases := []struct {
		path string
		want string
	}{
		{"/", `window.__CLAVESA__={"mode":"multi","basename":"","apiBase":"/api","readOnly":true};`},
		{"/w/alpha/", `window.__CLAVESA__={"mode":"multi","basename":"/w/alpha","apiBase":"/w/alpha/api","readOnly":true,"workspace":"alpha"};`},
		{"/w/alpha/pipelines/dashboard", `"basename":"/w/alpha","apiBase":"/w/alpha/api"`},
		{"/w/nope/pipelines", `"basename":"","apiBase":"/api"`},
		{"/index.html", `"basename":"","apiBase":"/api"`},
	}
	for _, c := range cases {
		w := get(t, r, http.MethodGet, c.path)
		if w.Code != http.StatusOK {
			t.Errorf("%s: status %d", c.path, w.Code)
			continue
		}
		body := w.Body.String()
		if !strings.Contains(body, c.want) {
			t.Errorf("%s: body %q does not contain %q", c.path, body, c.want)
		}
		if !strings.Contains(body, "</script></head>") {
			t.Errorf("%s: config script not placed before </head>: %q", c.path, body)
		}
		if w.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("%s: index must not be cached", c.path)
		}
	}
	// A static file is served as is, with no config.
	w := get(t, r, http.MethodGet, "/assets/app.js")
	if w.Body.String() != "console.log(1)" {
		t.Errorf("asset body %q", w.Body.String())
	}
}

func TestMultiRouter_ReadOnlyGuard(t *testing.T) {
	r, f := newTestRouter(t, true)
	h := ReadOnlyGuard(r)
	w := get(t, h, http.MethodPost, "/w/alpha/api/pipeline/run")
	if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != "GET, HEAD" {
		t.Errorf("POST: status %d Allow %q, want 405 GET, HEAD", w.Code, w.Header().Get("Allow"))
	}
	if f.calls["alpha"] != 0 {
		t.Errorf("a refused write built the workspace mux")
	}
	if w := get(t, h, http.MethodGet, "/w/alpha/api/pipelines"); w.Code != http.StatusOK {
		t.Errorf("GET: status %d", w.Code)
	}
}

func TestInjectRuntimeConfig_EscapesScript(t *testing.T) {
	out := string(InjectRuntimeConfig([]byte("<html><head></head></html>"), RuntimeConfig{Mode: "local", Workspace: "</script><script>alert(1)"}))
	if strings.Count(out, "</script>") != 1 {
		t.Errorf("a config value closed the script tag: %s", out)
	}
	out = string(InjectRuntimeConfig([]byte("<p>no head</p>"), RuntimeConfig{Mode: "local"}))
	if !strings.HasPrefix(out, "<script>window.__CLAVESA__=") {
		t.Errorf("no </head>: config not prepended: %s", out)
	}
}
