//go:build integration

// Transpile-server integration test: boots the runner image in
// CLAVESA_TRANSPILE_SERVER=1 mode (the long-lived, non-Spark sqlglot
// server `clavesa ui` uses to transpile authored Spark serving-SQL to
// Athena/Trino), polls /healthz, then POSTs a small battery to /transpile.
//
// Mirrors the warm-worker spawn idiom in
// internal/observability/local_query_warm.go: `docker run -d` with
// `-p 127.0.0.1::PORT/tcp` for a random loopback host port, `docker port`
// to resolve it, then poll /healthz. The `image` const, TestMain build,
// and repoPath helper are shared with runner_test.go in this package.
package runner_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

const transpilePort = "8770"

// startTranspileServer runs the image in transpile-server mode with a random
// loopback host port, waits for /healthz, and returns the base URL plus a
// teardown func. Skips the test if docker isn't available.
func startTranspileServer(t *testing.T) (baseURL string, stop func()) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available")
	}

	args := []string{
		"run", "-d", "--rm",
		"-e", "CLAVESA_TRANSPILE_SERVER=1",
		// Bind only to loopback with a random host port (mirrors the warm
		// worker spawn) so concurrent runs don't collide on a fixed port.
		"-p", "127.0.0.1::" + transpilePort + "/tcp",
		image,
	}
	out, err := exec.Command("docker", args...).Output()
	if err != nil {
		t.Fatalf("docker run transpile server: %v", err)
	}
	containerID := strings.TrimSpace(string(out))
	if containerID == "" {
		t.Fatal("docker run transpile server: empty container id")
	}
	stop = func() {
		_ = exec.Command("docker", "stop", containerID).Run()
	}

	hostPort, err := resolveHostPort(containerID, transpilePort+"/tcp")
	if err != nil {
		logs, _ := exec.Command("docker", "logs", "--tail", "40", containerID).CombinedOutput()
		stop()
		t.Fatalf("resolve host port: %v\nlogs:\n%s", err, logs)
	}
	baseURL = fmt.Sprintf("http://127.0.0.1:%d", hostPort)

	// Poll /healthz — the non-Spark server comes up in milliseconds, but the
	// container + port publish can lag a beat. 30s is generous headroom.
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp, err := http.Get(baseURL + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				break
			}
		}
		if time.Now().After(deadline) {
			logs, _ := exec.Command("docker", "logs", "--tail", "40", containerID).CombinedOutput()
			stop()
			t.Fatalf("transpile server /healthz not ready within 30s\nlogs:\n%s", logs)
		}
		time.Sleep(200 * time.Millisecond)
	}
	return baseURL, stop
}

// resolveHostPort maps the container's published port to the host port via
// `docker port`, retrying briefly for the publish to register.
func resolveHostPort(containerID, containerPort string) (int, error) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		out, err := exec.Command("docker", "port", containerID, containerPort).Output()
		if err == nil {
			line := strings.TrimSpace(string(out))
			if i := strings.LastIndex(line, ":"); i >= 0 {
				if p, perr := strconv.Atoi(strings.TrimSpace(line[i+1:])); perr == nil {
					return p, nil
				}
			}
		}
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("docker port %s %s: %v", containerID, containerPort, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

type transpileEnvelope struct {
	OK    bool    `json:"ok"`
	Trino string  `json:"trino"`
	Error string  `json:"error"`
	Line  *int    `json:"line"`
	Col   *int    `json:"col"`
}

// postTranspile POSTs {"sql": sql} to /transpile and returns the HTTP status
// plus the decoded envelope.
func postTranspile(t *testing.T, baseURL, sql string) (int, transpileEnvelope) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"sql": sql})
	resp, err := http.Post(baseURL+"/transpile", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /transpile: %v", err)
	}
	defer resp.Body.Close()
	var env transpileEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode /transpile response: %v", err)
	}
	return resp.StatusCode, env
}

func TestTranspileServer_DateDiff(t *testing.T) {
	baseURL, stop := startTranspileServer(t)
	defer stop()

	status, env := postTranspile(t, baseURL, "SELECT datediff(d2, d1) AS n FROM t")
	if status != 200 {
		t.Fatalf("status: want 200, got %d (%+v)", status, env)
	}
	if !env.OK {
		t.Fatalf("ok: want true, got false (error=%q)", env.Error)
	}
	if !strings.Contains(strings.ToUpper(env.Trino), "DATE_DIFF") {
		t.Errorf("trino should contain DATE_DIFF, got %q", env.Trino)
	}
}

func TestTranspileServer_ApproxCountDistinct(t *testing.T) {
	baseURL, stop := startTranspileServer(t)
	defer stop()

	status, env := postTranspile(t, baseURL, "SELECT approx_count_distinct(uid) AS n FROM t")
	if status != 200 {
		t.Fatalf("status: want 200, got %d (%+v)", status, env)
	}
	if !env.OK {
		t.Fatalf("ok: want true, got false (error=%q)", env.Error)
	}
	if !strings.Contains(strings.ToUpper(env.Trino), "APPROX_DISTINCT") {
		t.Errorf("trino should contain APPROX_DISTINCT, got %q", env.Trino)
	}
}

func TestTranspileServer_EmptySQL(t *testing.T) {
	baseURL, stop := startTranspileServer(t)
	defer stop()

	status, env := postTranspile(t, baseURL, "")
	if status != 400 {
		t.Fatalf("status: want 400, got %d (%+v)", status, env)
	}
	if env.OK {
		t.Errorf("ok: want false on empty SQL, got true")
	}
	if env.Error == "" {
		t.Errorf("error: want non-empty on empty SQL")
	}
}
