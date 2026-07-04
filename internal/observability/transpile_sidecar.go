package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/vesahyp/clavesa/internal/workspace"
)

// transpileServerPort is the in-container port the transpile server binds
// (matches the runner's CLAVESA_TRANSPILE_SERVER_PORT default of 8770).
const transpileServerPort = "8770"

// DialectError is returned by the transpile sidecar when the sqlglot
// server rejected the SparkSQL input (unparseable, or a construct it
// cannot map to the Trino/Athena dialect). The Message is the server's
// hint — surface it directly to the user. Line/Col are 1-based when the
// server pinpointed the failure, 0 when unspecified.
//
// Transport/runner failures (server down, undecodable body) return a
// plain wrapped error, not a *DialectError — callers use
// errors.As(&DialectError{}) to distinguish "your SQL doesn't transpile"
// from "the transpile server is broken". This is the observability-layer
// twin of service.DialectError, mirroring how ParseError is duplicated
// across the two packages to avoid an import cycle.
type DialectError struct {
	Message string
	Line    int
	Col     int
}

func (e *DialectError) Error() string { return e.Message }

// transpileSidecar drives a single, lazily-spawned, non-Spark sqlglot
// HTTP server (the runner's CLAVESA_TRANSPILE_SERVER mode) and reuses it
// across calls. Transpilation is warehouse-independent, so unlike the warm
// query runner there is exactly ONE container for the whole process — no
// per-warehouse map, no metastore, no Spark JVM. The server boots in
// milliseconds.
type transpileSidecar struct {
	// workspaceRoot is kept (rather than a pre-resolved image name) so the
	// runner image is resolved lazily at spawn time — `clavesa ui` can be
	// started in an empty directory and have the workspace created later.
	workspaceRoot string
	httpC         *http.Client
	healthCtx     time.Duration // how long to wait for /healthz before giving up

	// imageOverride pins the image instead of resolving from the workspace
	// manifest. Test-only (the docker-gated sidecar test points at the
	// make-build-runner tag directly); empty in production, where spawn
	// resolves via workspace.LocalRunnerImageTag.
	imageOverride string

	mu          sync.Mutex
	once        sync.Once
	containerID string
	port        int
	spawnErr    error
	closed      bool
}

// NewTranspileSidecar constructs the lazily-spawned transpile sidecar for
// a workspace. The container isn't started until the first ToServing call.
// Callers are responsible for Close() at shutdown; otherwise the docker
// container keeps running.
func NewTranspileSidecar(workspaceRoot string) *transpileSidecar {
	return &transpileSidecar{
		workspaceRoot: workspaceRoot,
		httpC:         &http.Client{Timeout: 30 * time.Second},
		healthCtx:     30 * time.Second,
	}
}

// ToServing transpiles one SparkSQL statement to the Trino/Athena serving
// dialect. The first call spawns the container and blocks until /healthz
// responds (~ms, but a generous deadline covers container/port-publish
// lag); concurrent first-callers share the single spawn. Subsequent calls
// are a single round-trip POST /transpile.
//
// On dialect rejection ToServing returns *DialectError carrying the
// server's message (and line/col when present). Any other return is a
// transport or runner failure (server dead, docker gone, network,
// undecodable body) — callers must not surface those as dialect errors.
//
// The signature is deliberately identical to service.Transpiler so
// *transpileSidecar satisfies it directly — no warehouse-bound wrapper.
func (t *transpileSidecar) ToServing(ctx context.Context, sparkSQL string) (string, error) {
	if err := t.ensureSpawned(ctx); err != nil {
		return "", err
	}
	return t.transpileAt(ctx, t.port, sparkSQL)
}

// ensureSpawned starts the container once under sync.Once; concurrent
// first-callers block on Do and then observe the shared result. A failed
// spawn is sticky (the container image is broken or docker is down); a
// retry-from-scratch would need a fresh sidecar — kept simpler than the
// warm worker's evict/respawn machinery on purpose (Slice 2 scope).
func (t *transpileSidecar) ensureSpawned(ctx context.Context) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return fmt.Errorf("transpile sidecar closed")
	}
	t.mu.Unlock()

	t.once.Do(func() {
		id, port, err := t.spawn(ctx)
		t.mu.Lock()
		t.containerID, t.port, t.spawnErr = id, port, err
		t.mu.Unlock()
	})
	t.mu.Lock()
	err := t.spawnErr
	t.mu.Unlock()
	return err
}

// spawn runs the transpile server container, resolves its host port, and
// blocks until /healthz answers. Mirrors PersistentQueryRunner.spawn
// but far simpler: no warehouse volume, no metastore, no Spark ports.
func (t *transpileSidecar) spawn(ctx context.Context) (string, int, error) {
	image := t.imageOverride
	if image == "" {
		image = workspace.LocalRunnerImageTag(t.workspaceRoot)
	}
	args := []string{
		"run", "-d", "--rm",
		// Bind only to loopback with an ephemeral host port — the Go side
		// always dials 127.0.0.1 and the server has no business on the LAN.
		"-p", "127.0.0.1::" + transpileServerPort + "/tcp",
		"-e", "CLAVESA_TRANSPILE_SERVER=1",
		"-e", "CLAVESA_TRANSPILE_SERVER_PORT=" + transpileServerPort,
		image,
	}
	cmd := exec.CommandContext(ctx, "docker", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", 0, fmt.Errorf("docker run transpile sidecar: %w\nstderr: %s", err, stderr.String())
	}
	containerID := strings.TrimSpace(stdout.String())
	if containerID == "" {
		return "", 0, fmt.Errorf("docker run transpile sidecar: empty container id")
	}

	port, err := dockerHostPort(ctx, containerID, transpileServerPort+"/tcp")
	if err != nil {
		_ = exec.Command("docker", "stop", containerID).Run()
		return "", 0, fmt.Errorf("transpile sidecar %s: %w", containerID[:12], err)
	}

	if err := t.pollHealthz(ctx, port); err != nil {
		logs := dockerTailLogs(containerID, 20)
		_ = exec.Command("docker", "stop", containerID).Run()
		return "", 0, fmt.Errorf("transpile sidecar %s: %w\ncontainer logs (tail):\n%s", containerID[:12], err, logs)
	}
	return containerID, port, nil
}

// pollHealthz blocks until GET /healthz returns 200 or the health deadline
// elapses. Mirrors PersistentQueryRunner.pollHealthz.
func (t *transpileSidecar) pollHealthz(ctx context.Context, port int) error {
	deadline := time.Now().Add(t.healthCtx)
	url := fmt.Sprintf("http://127.0.0.1:%d/healthz", port)
	client := &http.Client{Timeout: 1 * time.Second}
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return fmt.Errorf("did not become healthy within %s", t.healthCtx)
}

// transpileAt POSTs the SparkSQL to /transpile and decodes the envelope.
// Mirrors PersistentQueryRunner.parseAt: 200 and 400 both carry an
// envelope (ok=true on success, ok=false on rejection / empty-sql 400);
// anything else, or an undecodable body, is a transport error.
func (t *transpileSidecar) transpileAt(ctx context.Context, port int, sparkSQL string) (string, error) {
	body, _ := json.Marshal(map[string]string{"sql": sparkSQL})
	url := fmt.Sprintf("http://127.0.0.1:%d/transpile", port)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.httpC.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read transpile response: %w", err)
	}
	// 200 (ok=true/false envelope) and 400 (empty/bad sql, ok=false
	// envelope) both carry a decodable envelope. Everything else is
	// transport — never a dialect rejection.
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusBadRequest {
		return "", fmt.Errorf("transpile server HTTP %d: %s", resp.StatusCode, string(raw))
	}
	var env struct {
		OK    bool   `json:"ok"`
		Trino string `json:"trino"`
		Error string `json:"error"`
		Line  *int   `json:"line"`
		Col   *int   `json:"col"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return "", fmt.Errorf("decode transpile response: %w (body: %s)", err, string(raw))
	}
	if env.OK {
		return env.Trino, nil
	}
	msg := env.Error
	if msg == "" {
		msg = "SQL transpile failed (no message)"
	}
	return "", &DialectError{Message: msg, Line: derefInt(env.Line), Col: derefInt(env.Col)}
}

// derefInt maps a JSON-nullable *int to its value, with null → 0
// (unspecified line/col).
func derefInt(p *int) int {
	if p == nil {
		return 0
	}
	return *p
}

// Close stops the sidecar container if running (best-effort). Idempotent —
// a second call is a no-op. Mirrors PersistentQueryRunner.Close.
func (t *transpileSidecar) Close() {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.closed = true
	id := t.containerID
	t.containerID = ""
	t.mu.Unlock()
	if id != "" {
		_ = exec.Command("docker", "stop", id).Run()
	}
}
