package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vesahyp/clavesa/internal/runner"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// warmWorkerLabel is the docker label key we tag every persistent runner
// container with so prior-session orphans can be swept on startup.
const warmWorkerLabel = "clavesa.warm-worker"

// persistentDockerQueryRunner keeps one warm Spark process per warehouse
// alive in a background container and reuses it across HTTP queries.
//
// The cold price of starting Spark (~18-30s) was the dominant cost on every
// Catalog/dashboard/TableDetail render in `clavesa ui`. Eight concurrent
// snapshot calls × ~18s each made the landing page unusable for the first
// 1-3 minutes of every UI session. This runner amortizes that to ~30s once
// per warehouse; subsequent queries are <100ms.
//
// Only `clavesa ui` wires this — CLI one-shots stay on the per-call
// `dockerQueryRunner` to avoid surprising users with lingering containers
// they didn't ask for.
type persistentDockerQueryRunner struct {
	// workspaceRoot is kept (rather than a pre-resolved image name) so the
	// runner image is resolved lazily at spawn time. `clavesa ui` can
	// be started in an empty directory and have the workspace created
	// later via the UI — resolving at construction would bake in the
	// empty-name `clavesa//transform-runner` fallback and every query
	// would fail with "docker: invalid reference format".
	workspaceRoot string
	labelKV       string // e.g. clavesa.warm-worker=/abs/workspace/path
	httpC         *http.Client
	healthCtx     time.Duration // how long to wait for /healthz before giving up

	mu      sync.Mutex
	workers map[string]*warmWorker
	closed  bool
}

// workerState is the observable lifecycle state of a warm worker. A
// failed worker is evicted from the map, so only these two are ever seen
// by Workers() / the runtime indicator.
type workerState string

const (
	workerSpawning workerState = "spawning"
	workerReady    workerState = "ready"
)

// warmWorker is one container's handle. Spawning is gated by sync.Once so
// concurrent first-callers for the same warehouse share a single docker run.
type warmWorker struct {
	once        sync.Once
	containerID string
	port        int
	spawnErr    error
	// state and startedAt are the observable runtime status the UI's
	// runtime indicator polls via Workers() / GET /api/runtime/workers.
	// Both are written under persistentDockerQueryRunner.mu.
	state     workerState
	startedAt time.Time
}

// WorkerStatus is the observable runtime state of one warm-Spark worker,
// surfaced by GET /api/runtime/workers so the UI can show a "Starting
// Spark…" indicator instead of an unexplained first-query freeze.
type WorkerStatus struct {
	// Warehouse is the warehouse directory the worker serves.
	Warehouse string `json:"warehouse"`
	// State is "spawning" while the container boots and /healthz is
	// polled, "ready" once it answers queries. Failed workers are
	// evicted, so they never appear.
	State string `json:"state"`
	// AgeMS is milliseconds since the worker entry was created.
	AgeMS int64 `json:"age_ms"`
}

// NewPersistentQueryRunner constructs the production warm-Spark runner for
// a workspace. The returned runner implements QueryRunner — pass it to
// LocalProvider.WithQueryRunner to take effect.
//
// Callers are responsible for Close() at shutdown; otherwise the docker
// containers keep running. SweepWarmWorkers handles orphans from prior
// sessions that died without calling Close (kill -9, panic, etc.).
func NewPersistentQueryRunner(workspaceRoot string) *persistentDockerQueryRunner {
	return &persistentDockerQueryRunner{
		workspaceRoot: workspaceRoot,
		labelKV:       warmWorkerLabel + "=" + workspaceRoot,
		httpC:         &http.Client{Timeout: 10 * time.Minute},
		healthCtx:     90 * time.Second,
		workers:       make(map[string]*warmWorker),
	}
}

// resolveImage returns the workspace-scoped runner image tag, resolved
// fresh from clavesa.json each call. Cheap (one small file read) and
// rare (once per warehouse spawn) — and it has to be lazy so a workspace
// created after `clavesa ui` started is picked up without a restart.
func (p *persistentDockerQueryRunner) resolveImage() string {
	if m, _ := workspace.Load(p.workspaceRoot); m != nil {
		return runner.LocalImageName(m.Name) + ":latest"
	}
	return runner.LocalImageName("") + ":latest"
}

// Run satisfies QueryRunner. First call per warehouse spawns the container
// and blocks until /healthz responds (~30s). Subsequent calls reuse the
// warm worker.
func (p *persistentDockerQueryRunner) Run(ctx context.Context, warehouse, sql string) (*QueryRunnerResult, error) {
	w, err := p.getOrSpawn(ctx, warehouse)
	if err != nil {
		return nil, err
	}
	res, err := p.query(ctx, w, sql)
	if err != nil && shouldRespawn(err) {
		// Worker is unrecoverable: either the container died (socket
		// refused / EOF) or the JVM inside died while the HTTP server
		// still answers (py4j gateway gone, Spark context shut down).
		// Drop it and try once more — but only once, so we don't loop on
		// a genuinely broken image.
		p.evict(warehouse, w)
		w2, err2 := p.getOrSpawn(ctx, warehouse)
		if err2 != nil {
			return nil, err2
		}
		return p.query(ctx, w2, sql)
	}
	return res, err
}

// Warmup spawns the warm worker for `warehouse` in the background, without
// running a query. Used by `clavesa ui` startup so the ~30s Spark cold boot
// runs while the user is still orienting on the landing page, instead of
// the lazy "first table click pays for it" path that left the runtime
// indicator stuck at "idle" for an unhelpful stretch.
//
// Errors are logged to stderr and swallowed — there's no caller waiting on
// the result, and the next user-triggered query will surface a real spawn
// failure in-context anyway.
func (p *persistentDockerQueryRunner) Warmup(ctx context.Context, warehouse string) {
	if _, err := p.getOrSpawn(ctx, warehouse); err != nil {
		fmt.Fprintf(os.Stderr, "clavesa: warm Spark spawn failed (will retry on first query): %v\n", err)
	}
}

// Close stops every tracked container. Idempotent — second call is a no-op.
func (p *persistentDockerQueryRunner) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	workers := p.workers
	p.workers = nil
	p.mu.Unlock()
	for _, w := range workers {
		if w.containerID != "" {
			_ = exec.Command("docker", "stop", w.containerID).Run()
		}
	}
}

// EvictWarehouse stops the warm worker container for one warehouse and
// drops it from the map so the next caller spawns fresh. Called by
// `pipeline run` on local pipelines: each transform invocation spawns
// its own 3GB runner container, and on a Docker Desktop with the
// default 7-8GB memory budget the warm worker + transform runner
// together OOM. Releasing the warm worker for the duration of a run
// keeps memory pressure off the user; the next Catalog / dashboard
// query pays the ~15s warm-up again but only once.
func (p *persistentDockerQueryRunner) EvictWarehouse(warehouse string) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	w, ok := p.workers[warehouse]
	if !ok {
		p.mu.Unlock()
		return
	}
	delete(p.workers, warehouse)
	p.mu.Unlock()
	if w.containerID != "" {
		_ = exec.Command("docker", "stop", w.containerID).Run()
	}
}

func (p *persistentDockerQueryRunner) getOrSpawn(ctx context.Context, warehouse string) (*warmWorker, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("persistent query runner closed")
	}
	w, ok := p.workers[warehouse]
	if !ok {
		w = &warmWorker{state: workerSpawning, startedAt: time.Now()}
		p.workers[warehouse] = w
	}
	p.mu.Unlock()

	w.once.Do(func() {
		w.containerID, w.port, w.spawnErr = p.spawn(ctx, warehouse)
	})
	if w.spawnErr != nil {
		// A failed sync.Once is permanent for that *warmWorker. Remove it
		// from the map so a subsequent caller can retry from scratch
		// (typical recovery for a transient docker-daemon hiccup).
		p.evict(warehouse, w)
		return nil, w.spawnErr
	}
	// Spawn succeeded — mark ready for the runtime indicator. Every
	// caller re-asserts this after once.Do; idempotent under the lock.
	p.mu.Lock()
	w.state = workerReady
	p.mu.Unlock()
	return w, nil
}

// Workers reports the warm-Spark workers currently tracked and their
// state, for the UI's runtime indicator. Safe for concurrent use;
// returns an empty slice when none are tracked or the runner is closed.
func (p *persistentDockerQueryRunner) Workers() []WorkerStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]WorkerStatus, 0, len(p.workers))
	for warehouse, w := range p.workers {
		out = append(out, WorkerStatus{
			Warehouse: warehouse,
			State:     string(w.state),
			AgeMS:     time.Since(w.startedAt).Milliseconds(),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Warehouse < out[j].Warehouse })
	return out
}

func (p *persistentDockerQueryRunner) spawn(ctx context.Context, warehouse string) (string, int, error) {
	if err := os.MkdirAll(warehouse, 0o755); err != nil {
		return "", 0, fmt.Errorf("create warehouse dir: %w", err)
	}
	args := []string{
		"run", "-d", "--rm",
		"--label", p.labelKV,
		"-p", "0:8765/tcp",
		"-e", "CLAVESA_QUERY_SERVER=1",
		"-e", "CLAVESA_QUERY_SERVER_PORT=8765",
		"-e", "CLAVESA_WAREHOUSE=" + warehouse,
		"-v", warehouse + ":" + warehouse,
		p.resolveImage(),
	}
	cmd := exec.CommandContext(ctx, "docker", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", 0, fmt.Errorf("docker run warm worker: %w\nstderr: %s", err, stderr.String())
	}
	containerID := strings.TrimSpace(stdout.String())
	if containerID == "" {
		return "", 0, fmt.Errorf("docker run warm worker: empty container id")
	}

	port, err := dockerHostPort(ctx, containerID, "8765/tcp")
	if err != nil {
		_ = exec.Command("docker", "stop", containerID).Run()
		return "", 0, err
	}

	if err := p.pollHealthz(ctx, port); err != nil {
		_ = exec.Command("docker", "stop", containerID).Run()
		return "", 0, fmt.Errorf("warm worker %s: %w", containerID[:12], err)
	}
	return containerID, port, nil
}

func (p *persistentDockerQueryRunner) pollHealthz(ctx context.Context, port int) error {
	deadline := time.Now().Add(p.healthCtx)
	url := fmt.Sprintf("http://127.0.0.1:%d/healthz", port)
	// Use a short per-request timeout — long ones serialize poorly with
	// the outer deadline.
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
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("did not become healthy within %s", p.healthCtx)
}

func (p *persistentDockerQueryRunner) query(ctx context.Context, w *warmWorker, sql string) (*QueryRunnerResult, error) {
	body, _ := json.Marshal(map[string]string{"sql": sql})
	url := fmt.Sprintf("http://127.0.0.1:%d/query", w.port)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.httpC.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read warm-worker response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("warm worker HTTP %d: %s", resp.StatusCode, string(raw))
	}

	// Mirror dockerQueryRunner.Run: the runner emits either {"error": "..."}
	// or the QueryRunnerResult shape on the same status code, and we surface
	// the error envelope as a Go error so callers don't have to inspect.
	var errEnv struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(raw, &errEnv); err == nil && errEnv.Error != "" {
		return nil, fmt.Errorf("query runner: %s", errEnv.Error)
	}
	var res QueryRunnerResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, fmt.Errorf("decode query response: %w (body: %s)", err, string(raw))
	}
	return &res, nil
}

func (p *persistentDockerQueryRunner) evict(warehouse string, w *warmWorker) {
	p.mu.Lock()
	if cur, ok := p.workers[warehouse]; ok && cur == w {
		delete(p.workers, warehouse)
	}
	p.mu.Unlock()
	if w.containerID != "" {
		_ = exec.Command("docker", "stop", w.containerID).Run()
	}
}

// SweepWarmWorkers stops any leftover warm-worker containers from a prior
// session of this workspace. Called by `clavesa ui` on startup so a
// SIGKILL'd previous run doesn't leak ~1GB-resident containers indefinitely.
// Best-effort: stderr-logs and returns; never blocks startup.
func SweepWarmWorkers(workspaceRoot string) {
	label := warmWorkerLabel + "=" + workspaceRoot
	out, err := exec.Command("docker", "ps", "-q", "--filter", "label="+label).Output()
	if err != nil {
		// Docker not running, not installed, etc. — same condition the rest
		// of the local provider already tolerates (queries will just fail
		// when called).
		return
	}
	ids := strings.Fields(string(out))
	if len(ids) == 0 {
		return
	}
	fmt.Fprintf(os.Stderr, "clavesa: stopping %d orphan warm-Spark worker(s) from prior session\n", len(ids))
	for _, id := range ids {
		_ = exec.Command("docker", "stop", id).Run()
	}
}

// dockerHostPort returns the host-side port docker bound for a container's
// exposed port (e.g. "8765/tcp"). Output line shape: "0.0.0.0:54321" plus
// often an IPv6 sibling on a second line; we take the first IPv4 line.
func dockerHostPort(ctx context.Context, containerID, containerPort string) (int, error) {
	out, err := exec.CommandContext(ctx, "docker", "port", containerID, containerPort).Output()
	if err != nil {
		return 0, fmt.Errorf("docker port %s %s: %w", containerID[:12], containerPort, err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// "0.0.0.0:54321" or "[::]:54321" — split on the LAST colon so
		// IPv6 brackets don't confuse us.
		idx := strings.LastIndex(line, ":")
		if idx < 0 {
			continue
		}
		p, err := strconv.Atoi(line[idx+1:])
		if err == nil && p > 0 {
			return p, nil
		}
	}
	return 0, fmt.Errorf("docker port %s %s: no usable host port in %q", containerID[:12], containerPort, string(out))
}

// isConnRefused recognizes the family of errors net/http returns when the
// remote socket isn't accepting connections — used to trigger one respawn
// after a tracked container died unexpectedly.
func isConnRefused(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "connection refused") || strings.Contains(s, "EOF")
}

// shouldRespawn decides whether a /query error means the worker is
// unrecoverable and should be evicted + respawned. Covers two failure
// shapes: (a) socket-level — container gone (isConnRefused); (b)
// container alive but Spark dead — the runner's /healthz now returns 503
// when SELECT 1 fails, and a /query against a dead JVM surfaces py4j
// "Java gateway process exited" / "Py4JNetworkError" / "SparkContext
// has been shut down" inside the error envelope. False positives cost
// one extra cold start; missing a real dead JVM hangs the UI on every
// subsequent query, so err on the side of respawning.
func shouldRespawn(err error) bool {
	if err == nil {
		return false
	}
	if isConnRefused(err) {
		return true
	}
	s := err.Error()
	// The /healthz 503 surfaces as "warm worker HTTP 503:" — but that
	// only fires when Go itself polls /healthz mid-query, which we
	// don't do today. The realistic signal is a /query that round-trips
	// to a still-alive HTTP server whose Spark gateway is gone.
	patterns := []string{
		"Py4JNetworkError",
		"Py4JJavaError",
		"Java gateway process exited",
		"SparkContext has been shut down",
		"SparkContext was shut down",
		"py4j.protocol",
	}
	for _, p := range patterns {
		if strings.Contains(s, p) {
			return true
		}
	}
	return false
}
