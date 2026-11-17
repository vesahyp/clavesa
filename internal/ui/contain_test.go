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

// containWorkspace is a workspace with one pipeline directory holding a
// script, a secret file beside the workspace, and two symlinks inside the
// pipeline that lead out to it.
func containWorkspace(t *testing.T) (ws string, h http.Handler) {
	t.Helper()
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	base := t.TempDir()
	ws = filepath.Join(base, "ws")
	outside := filepath.Join(base, "outside")
	for _, d := range []string{filepath.Join(ws, "pipe"), outside} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write := func(p, body string) {
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(ws, "pipe", "script.py"), "print('ok')\n")
	write(filepath.Join(outside, "environ"), "AWS_SECRET_ACCESS_KEY=leak\n")
	if err := os.Symlink(filepath.Join(outside, "environ"), filepath.Join(ws, "pipe", "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(ws, "linkdir")); err != nil {
		t.Fatal(err)
	}
	d := WorkspaceDeps{Deps: wsdeps.New(context.Background(), ws, wsdeps.Options{}), ReadOnly: true}
	return ws, BuildWorkspaceMux(d)
}

// TestContainPathParams_ScriptTraversal: the 2026-11-17 review's request,
// GET /pipeline/script?dir=/proc/self&path=environ, gave the hosted
// Lambda's AWS keys. Every way out of the workspace answers 400 and never
// the file; a script inside the pipeline still reads.
func TestContainPathParams_ScriptTraversal(t *testing.T) {
	_, h := containWorkspace(t)
	get := func(q string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/pipeline/script?"+q, nil))
		return w
	}
	for _, q := range []string{
		"dir=/proc/self&path=environ",
		"dir=%2Fproc%2Fself&path=environ",
		"dir=pipe&path=/proc/self/environ",
		"dir=../outside&path=environ",
		"dir=pipe/../../outside&path=environ",
		"dir=pipe&path=../../outside/environ",
		"dir=pipe&path=link",
		"dir=linkdir&path=environ",
		"dir=pipe&dir=/proc/self&path=environ",
	} {
		w := get(q)
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400; body %q", q, w.Code, w.Body.String())
		}
		if strings.Contains(w.Body.String(), "AWS_") || strings.Contains(w.Body.String(), "leak") {
			t.Errorf("%s: body leaks the file: %q", q, w.Body.String())
		}
	}
	if w := get("dir=pipe&path=script.py"); w.Code != http.StatusOK || w.Body.String() != "print('ok')\n" {
		t.Errorf("script inside the pipeline: status %d body %q", w.Code, w.Body.String())
	}
}

// TestContainPathParams_EveryGetRoute walks every GET route with
// dir=/proc/self: none may reach its handler.
func TestContainPathParams_EveryGetRoute(t *testing.T) {
	rec, _ := recordWorkspaceRoutes(t)
	stub := http.NewServeMux()
	reached := 0
	for _, p := range rec.patterns {
		stub.HandleFunc(p, func(w http.ResponseWriter, r *http.Request) { reached++ })
	}
	h := ContainPathParams(t.TempDir(), stub)
	for _, p := range rec.patterns {
		method, path := splitPattern(p)
		if method != http.MethodGet {
			continue
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(method, concretePath(path)+"?dir=/proc/self&path=environ", nil))
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", p, w.Code)
		}
	}
	if reached != 0 {
		t.Errorf("%d handler(s) ran with dir=/proc/self", reached)
	}
}
