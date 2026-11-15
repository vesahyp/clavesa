//go:build integration

package integration

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestWorkspaceHosted drives `clavesa workspace hosted enable|show|disable`
// (ADR-032) against the built binary: the happy path, the error paths and
// that a refused enable writes nothing.
func TestWorkspaceHosted(t *testing.T) {
	ws := newWorkspace(t)
	run(t, "workspace", "init", "hosted-ws", "--workspace", ws)
	run(t, "pipeline", "create", "orders", "--workspace", ws)
	tfPath := filepath.Join(ws, "hosted.tf")

	// A stand-in for the hosted UI's well-known document (ADR-032 slice 6).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/.well-known/clavesa.json" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"reader_principal":"arn:aws:iam::111122223333:role/clavesa-ui-api",` +
			`"event_bus_arn":"arn:aws:events:eu-north-1:111122223333:event-bus/clavesa-ui"}`))
	}))
	defer srv.Close()
	missing := httptest.NewServer(http.NotFoundHandler())
	defer missing.Close()

	fail := func(wantErr string, args ...string) {
		t.Helper()
		cmd := exec.Command(binPath, append(args, "--workspace", ws)...)
		out, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatalf("clavesa %s: want failure, got:\n%s", strings.Join(args, " "), out)
		}
		if !strings.Contains(string(out), wantErr) {
			t.Errorf("clavesa %s: output lacks %q:\n%s", strings.Join(args, " "), wantErr, out)
		}
	}
	fail("required flag(s) \"url\" not set", "workspace", "hosted", "enable")
	fail("HTTP 404", "workspace", "hosted", "enable", "--url", missing.URL)
	fail("is not an IAM principal ARN", "workspace", "hosted", "enable", "--url", srv.URL, "--reader-principal", "vesa")
	fail("must be an https URL", "workspace", "hosted", "enable", "--url", "http://ui.clavesa.dev", "--reader-principal", "arn:aws:iam::111122223333:role/x")
	if _, err := os.Stat(tfPath); !os.IsNotExist(err) {
		t.Fatalf("a refused enable wrote hosted.tf: %v", err)
	}

	out := run(t, "workspace", "hosted", "show", "--workspace", ws)
	if !strings.Contains(out, "Not hosted") {
		t.Errorf("show before enable:\n%s", out)
	}

	// With no flags but the URL: the principal and the bus come from the
	// well-known document.
	out = run(t, "workspace", "hosted", "enable", "--url", srv.URL+"/", "--workspace", ws)
	if !strings.Contains(out, "role/clavesa-ui-api") || !strings.Contains(out, "event-bus/clavesa-ui") {
		t.Errorf("enable from the well-known document:\n%s", out)
	}
	// --reader-principal adds extras after the hosted UI's own.
	out = run(t, "workspace", "hosted", "enable", "--url", srv.URL+"/",
		"--reader-principal", "arn:aws:iam::111122223333:role/a",
		"--reader-principal", "arn:aws:iam::111122223333:role/b",
		"--workspace", ws)
	if !strings.Contains(out, "clavesa-ui-reader-hosted-ws") || !strings.Contains(out, "Next: clavesa deploy") {
		t.Errorf("enable output:\n%s", out)
	}
	tf, err := os.ReadFile(tfPath)
	if err != nil {
		t.Fatalf("hosted.tf: %v", err)
	}
	for _, want := range []string{`/hosted-reader/aws"`, `"clavesa-hosted-ws-*"`, `event-bus/clavesa-ui"`, `role/clavesa-ui-api", "arn:aws:iam::111122223333:role/a", "arn:aws:iam::111122223333:role/b"`} {
		if !strings.Contains(string(tf), want) {
			t.Errorf("hosted.tf lacks %s:\n%s", want, tf)
		}
	}

	var st struct {
		Enabled bool `json:"enabled"`
		Hosted  struct {
			URL              string   `json:"url"`
			ReaderPrincipals []string `json:"reader_principals"`
			EventBusARN      string   `json:"event_bus_arn"`
		} `json:"hosted"`
		WorkGroup string `json:"workgroup"`
	}
	if err := json.Unmarshal([]byte(run(t, "workspace", "hosted", "show", "--json", "--workspace", ws)), &st); err != nil {
		t.Fatal(err)
	}
	if !st.Enabled || st.Hosted.URL != srv.URL || len(st.Hosted.ReaderPrincipals) != 3 || st.WorkGroup != "clavesa-ui-hosted-ws" {
		t.Errorf("show --json = %+v", st)
	}
	manifest, _ := os.ReadFile(filepath.Join(ws, "clavesa.json"))
	if !strings.Contains(string(manifest), `"hosted"`) {
		t.Errorf("clavesa.json lacks the hosted block:\n%s", manifest)
	}

	out = run(t, "workspace", "hosted", "disable", "--workspace", ws)
	if !strings.Contains(out, "Removed the hosted block") {
		t.Errorf("disable output:\n%s", out)
	}
	if _, err := os.Stat(tfPath); !os.IsNotExist(err) {
		t.Errorf("disable left hosted.tf: %v", err)
	}
	manifest, _ = os.ReadFile(filepath.Join(ws, "clavesa.json"))
	if strings.Contains(string(manifest), `"hosted"`) {
		t.Errorf("disable left the block:\n%s", manifest)
	}
}
