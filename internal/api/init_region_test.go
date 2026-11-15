package api_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/api"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// TestInitWorkspaceRegion: POST /workspace/init records the region, the
// twin of `workspace init --region` (GH #107).
func TestInitWorkspaceRegion(t *testing.T) {
	ws := t.TempDir()
	mux := http.NewServeMux()
	api.NewWorkspaceHandler(ws).RegisterRoutes(mux)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/workspace/init", strings.NewReader(`{"name":"remote-ws","region":"us-east-2"}`)))
	if rr.Code != http.StatusCreated {
		t.Fatalf("POST /workspace/init = %d %s", rr.Code, rr.Body.String())
	}
	if got := workspace.RecordedRegion(ws); got != "us-east-2" {
		t.Errorf("recorded region = %q, want us-east-2", got)
	}
}

func TestInitWorkspaceRejectsBadRegion(t *testing.T) {
	ws := t.TempDir()
	mux := http.NewServeMux()
	api.NewWorkspaceHandler(ws).RegisterRoutes(mux)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/workspace/init", strings.NewReader(`{"name":"east","region":"east"}`)))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("POST /workspace/init with a bad region = %d, want 400", rr.Code)
	}
	if _, err := os.Stat(filepath.Join(ws, "clavesa.json")); !os.IsNotExist(err) {
		t.Errorf("a rejected init left clavesa.json behind (stat err=%v)", err)
	}
}
