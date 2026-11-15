//go:build integration

package integration

import (
	"encoding/json"
	"errors"
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
	// A cloud-warehouse workspace: the reader role and hosted.tf. The
	// local-warehouse kind is TestWorkspaceHostedLocal.
	run(t, "workspace", "use", "--warehouse", "cloud", "--workspace", ws)
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

// TestWorkspaceHostedLocal: `workspace hosted enable` on a local-warehouse
// workspace records the publisher role and the data bucket, writes no
// hosted.tf, and a local `pipeline run` then tries to publish its summary:
// a failed publish is one stderr line and leaves the run's exit code and
// stdout as they are with --no-publish.
func TestWorkspaceHostedLocal(t *testing.T) {
	ws := newWorkspace(t)
	run(t, "workspace", "init", "hosted-local", "--workspace", ws)
	run(t, "pipeline", "create", "orders", "--workspace", ws)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"reader_principal":"arn:aws:iam::111122223333:role/clavesa-ui-api",` +
			`"event_bus_arn":"arn:aws:events:eu-north-1:111122223333:event-bus/clavesa-ui",` +
			`"publisher_role_arn":"arn:aws:iam::111122223333:role/clavesa-ui-publisher",` +
			`"data_bucket":"clavesa-ui-data-111122223333","region":"eu-north-1"}`))
	}))
	defer srv.Close()

	out := run(t, "workspace", "hosted", "enable", "--url", srv.URL, "--workspace", ws)
	for _, want := range []string{"local warehouse", "role/clavesa-ui-publisher", "s3://clavesa-ui-data-111122223333/summaries/hosted-local.json", "kind \"local\""} {
		if !strings.Contains(out, want) {
			t.Errorf("enable output lacks %q:\n%s", want, out)
		}
	}
	if _, err := os.Stat(filepath.Join(ws, "hosted.tf")); !os.IsNotExist(err) {
		t.Errorf("a local workspace got hosted.tf: %v", err)
	}
	cmd := exec.Command(binPath, "workspace", "hosted", "enable", "--url", srv.URL, "--reader-principal", "arn:aws:iam::111122223333:role/a", "--workspace", ws)
	if b, err := cmd.CombinedOutput(); err == nil || !strings.Contains(string(b), "local-warehouse workspace") {
		t.Errorf("--reader-principal on a local workspace: %v\n%s", err, b)
	}

	// A run with no AWS credentials at all: the publish fails at once.
	// The pipeline has no nodes, so the run needs no docker.
	runPipeline := func(extra ...string) (stdout, stderr string, code int) {
		t.Helper()
		c := exec.Command(binPath, append([]string{"pipeline", "run", "orders", "--json", "--workspace", ws}, extra...)...)
		c.Env = append(os.Environ(), "AWS_PROFILE=", "AWS_ACCESS_KEY_ID=", "AWS_SECRET_ACCESS_KEY=", "AWS_SESSION_TOKEN=",
			"AWS_CONFIG_FILE="+filepath.Join(ws, "no-aws-config"), "AWS_SHARED_CREDENTIALS_FILE="+filepath.Join(ws, "no-aws-credentials"),
			"AWS_EC2_METADATA_DISABLED=true", "CLAVESA_NO_PUBLISH=")
		var so, se strings.Builder
		c.Stdout, c.Stderr = &so, &se
		err := c.Run()
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			code = ee.ExitCode()
		} else if err != nil {
			t.Fatal(err)
		}
		return so.String(), se.String(), code
	}
	offOut, offErr, offCode := runPipeline("--no-publish")
	onOut, onErr, onCode := runPipeline()
	if strings.Contains(offErr, "Hosted:") {
		t.Errorf("--no-publish still published:\n%s", offErr)
	}
	if strings.Count(onErr, "Hosted: summary not published") != 1 {
		t.Errorf("want one stderr line for the failed publish:\n%s", onErr)
	}
	if onCode != offCode {
		t.Errorf("exit code with the failed publish = %d, without the publish = %d", onCode, offCode)
	}
	if (offOut == "") != (onOut == "") || (offOut != "" && !json.Valid([]byte(onOut))) {
		t.Errorf("stdout changed by the publish:\n--no-publish: %s\npublish: %s", offOut, onOut)
	}
}
