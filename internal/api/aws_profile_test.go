package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vesahyp/clavesa/internal/api"
)

// awsConfigWithProfiles writes a throwaway ~/.aws/config-shaped file and
// points AWS_CONFIG_FILE at it, so ListAWSProfiles (used by the handler
// to validate) sees a known profile set.
func awsConfigWithProfiles(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config")
	if err := os.WriteFile(cfg, []byte("[default]\n\n[profile webbaa]\n"), 0o644); err != nil {
		t.Fatalf("write aws config: %v", err)
	}
	t.Setenv("AWS_CONFIG_FILE", cfg)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "no-creds"))
}

func awsProfileReq(t *testing.T, mux *http.ServeMux, method, body string) (int, string) {
	t.Helper()
	var r *http.Request
	if body != "" {
		r = httptest.NewRequest(method, "/workspace/aws-profile", strings.NewReader(body))
	} else {
		r = httptest.NewRequest(method, "/workspace/aws-profile", nil)
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, r)
	if rr.Code != http.StatusOK {
		return rr.Code, ""
	}
	var resp struct {
		Profile string `json:"profile"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode %s response: %v", method, err)
	}
	return rr.Code, resp.Profile
}

// TestAWSProfileDefaultEmpty — a fresh workspace reports no profile
// override.
func TestAWSProfileDefaultEmpty(t *testing.T) {
	awsConfigWithProfiles(t)
	mux := http.NewServeMux()
	api.NewWorkspaceHandler(t.TempDir()).RegisterRoutes(mux)
	if code, profile := awsProfileReq(t, mux, http.MethodGet, ""); code != http.StatusOK || profile != "" {
		t.Fatalf("GET = (%d, %q), want (200, empty)", code, profile)
	}
}

// TestAWSProfileSetAndGet — PUT a valid profile persists it, fires the
// restart hook, and a subsequent GET reflects it.
func TestAWSProfileSetAndGet(t *testing.T) {
	awsConfigWithProfiles(t)
	restarted := make(chan struct{}, 1)
	mux := http.NewServeMux()
	api.NewWorkspaceHandler(t.TempDir()).
		WithRestart(func() { restarted <- struct{}{} }).
		RegisterRoutes(mux)

	if code, profile := awsProfileReq(t, mux, http.MethodPut, `{"profile":"webbaa"}`); code != http.StatusOK || profile != "webbaa" {
		t.Fatalf("PUT webbaa = (%d, %q), want (200, webbaa)", code, profile)
	}
	select {
	case <-restarted:
	case <-time.After(2 * time.Second):
		t.Fatal("restart hook not called after PUT")
	}
	if code, profile := awsProfileReq(t, mux, http.MethodGet, ""); code != http.StatusOK || profile != "webbaa" {
		t.Fatalf("GET after PUT = (%d, %q), want (200, webbaa)", code, profile)
	}
}

// TestAWSProfileRejectsUnknown — a profile not in ~/.aws is a 400, so a
// typo doesn't restart the server into local-only mode.
func TestAWSProfileRejectsUnknown(t *testing.T) {
	awsConfigWithProfiles(t)
	mux := http.NewServeMux()
	api.NewWorkspaceHandler(t.TempDir()).
		WithRestart(func() {}).
		RegisterRoutes(mux)
	if code, _ := awsProfileReq(t, mux, http.MethodPut, `{"profile":"nonesuch"}`); code != http.StatusBadRequest {
		t.Errorf("PUT unknown profile: status = %d, want 400", code)
	}
}

// TestAWSProfileClear — PUT an empty profile clears the override.
func TestAWSProfileClear(t *testing.T) {
	awsConfigWithProfiles(t)
	mux := http.NewServeMux()
	api.NewWorkspaceHandler(t.TempDir()).
		WithRestart(func() {}).
		RegisterRoutes(mux)
	if code, _ := awsProfileReq(t, mux, http.MethodPut, `{"profile":"webbaa"}`); code != http.StatusOK {
		t.Fatalf("PUT webbaa: status = %d, want 200", code)
	}
	if code, profile := awsProfileReq(t, mux, http.MethodPut, `{"profile":""}`); code != http.StatusOK || profile != "" {
		t.Errorf("PUT clear = (%d, %q), want (200, empty)", code, profile)
	}
}
