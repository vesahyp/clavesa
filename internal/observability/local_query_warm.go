package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

// ParseError is returned by Parse when the warm worker's SQL parser
// rejected the input. The Message is the parser's pointer-into-SQL
// hint — surface it directly to the user. Transport/runner failures
// return a different error type; callers use errors.As(&ParseError{})
// to distinguish "your SQL is broken" from "the runner is broken".
type ParseError struct {
	Message string
}

func (e *ParseError) Error() string { return e.Message }

// Parse satisfies SQLParser. First call per warehouse spawns the
// container and blocks until /healthz responds (~30s); subsequent
// calls reuse the warm worker, so a parse is a single round-trip
// POST /parse — milliseconds, not seconds.
//
// On parse failure (parser rejects the SQL) Parse returns *ParseError
// carrying the parser's message. Any other return is a transport or
// runner failure (worker dead, docker gone, network) — callers must
// not surface those as parse errors.
func (p *persistentDockerQueryRunner) Parse(ctx context.Context, warehouse, sql string) error {
	w, err := p.getOrSpawn(ctx, warehouse)
	if err != nil {
		return err
	}
	err = p.parseAt(ctx, w, sql)
	if err != nil && shouldRespawn(err) {
		// Mirror Run()'s one-shot respawn: container died, JVM died,
		// Connect gRPC dead. Drop and try once.
		p.evict(warehouse, w)
		w2, err2 := p.getOrSpawn(ctx, warehouse)
		if err2 != nil {
			return err2
		}
		return p.parseAt(ctx, w2, sql)
	}
	return err
}

func (p *persistentDockerQueryRunner) parseAt(ctx context.Context, w *warmWorker, sql string) error {
	body, _ := json.Marshal(map[string]string{"sql": sql})
	url := fmt.Sprintf("http://127.0.0.1:%d/parse", w.port)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.httpC.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read warm-worker /parse response: %w", err)
	}
	// /parse returns 400 on a missing/empty SQL body, 200 in every other
	// shape (ok=true on success, ok=false on parse failure). Treat both
	// 200 and 400 as "envelope present"; everything else is transport.
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusBadRequest {
		return fmt.Errorf("warm worker /parse HTTP %d: %s", resp.StatusCode, string(raw))
	}
	var env struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("decode /parse response: %w (body: %s)", err, string(raw))
	}
	if env.OK {
		return nil
	}
	msg := env.Error
	if msg == "" {
		msg = "SQL parse failed (no message)"
	}
	return &ParseError{Message: msg}
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

// SQLParserFor returns a Service-compatible SQL parser bound to the
// given workspace warehouse. The returned value satisfies
// service.SQLParser — its Parse(ctx, sql) routes to the warm worker's
// /parse endpoint. Translates *observability.ParseError into
// *service.ParseError at the seam so callers in internal/cli +
// internal/api can `errors.As(&service.ParseError{})` without
// reaching into the observability package.
func (p *persistentDockerQueryRunner) SQLParserFor(warehouse string) *boundSQLParser {
	return &boundSQLParser{runner: p, warehouse: warehouse}
}

// boundSQLParser binds a persistentDockerQueryRunner to one warehouse
// so it satisfies the service-layer SQLParser shape (no warehouse
// parameter). The translation to *service.ParseError happens at the
// caller (cli/api) by detecting the *observability.ParseError this
// returns — keeps the observability package free of an import cycle.
type boundSQLParser struct {
	runner    *persistentDockerQueryRunner
	warehouse string
}

// Parse satisfies service.SQLParser (and is the parse seam Slice 3
// wires across CLI / UI / dashboards / preview). The underlying
// runner returns *ParseError on parser rejection; we re-return the
// same value so callers can `errors.As(&observability.ParseError{})`
// to detect parser-vs-transport without depending on a separate
// service-layer type. service.ValidateSQL's wrapping turns it into a
// *service.ParseError at the api / cli boundary.
func (b *boundSQLParser) Parse(ctx context.Context, sql string) error {
	err := b.runner.Parse(ctx, b.warehouse, sql)
	if err == nil {
		return nil
	}
	// Sanity: confirm the error is one shape the caller understands.
	var pe *ParseError
	_ = errors.As(err, &pe)
	return err
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

// WarmWorkerBaseURL returns "http://127.0.0.1:<port>" for the warm worker
// serving `warehouse`, or "" if no worker is tracked for it. Used by the
// notebook session runner (Slice 1) to reach the /repl/* supervisor
// endpoints that live in the same container as /query.
//
// Returning a base URL string (not the port) keeps callers from having to
// know about IPv4 binding details; the warm worker spawn pins 127.0.0.1.
func (p *persistentDockerQueryRunner) WarmWorkerBaseURL(warehouse string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	w, ok := p.workers[warehouse]
	if !ok || w.state != workerReady {
		return ""
	}
	return fmt.Sprintf("http://127.0.0.1:%d", w.port)
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
		// Spark Connect gRPC binding. Exposed so future Slice-1 notebook
		// REPL subprocesses (which connect via gRPC) and host-side debug
		// tools (`grpcurl`) can reach the in-container Connect server.
		// Slice 0's /query path goes through a Connect client inside the
		// same container, so this port isn't strictly required today —
		// but exposing it is free and avoids a respawn churn when Slice 1
		// lands.
		"-p", "0:15002/tcp",
		"-e", "CLAVESA_QUERY_SERVER=1",
		"-e", "CLAVESA_QUERY_SERVER_PORT=8765",
		"-e", "CLAVESA_CONNECT_PORT=15002",
		"-e", "CLAVESA_WAREHOUSE=" + warehouse,
	}
	// ADR-019 Slice 4: register the V2 multi-catalog so warm reads of
	// three-level addresses ``<catalog>.<schema>.<table>`` resolve. The
	// runner falls back to spark_catalog for legacy two-segment reads
	// (pre-Slice-4 tables under the Hive metastore), so this is purely
	// additive.
	if m, _ := workspace.Load(p.workspaceRoot); m != nil {
		args = append(args, "-e", "CLAVESA_CATALOG="+m.CatalogIdentifier())
		args = append(args, "-e", "CLAVESA_SYSTEM_CATALOG="+m.SystemCatalogIdentifier())
	}
	args = append(args, "-v", warehouse+":"+warehouse, p.resolveImage())
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
//
// Retries briefly on transient failure: `docker run -d` returns when the
// container is created, not when the port mapping is wired up. The
// first one-or-two `docker port` calls race the runtime's
// network-namespace setup and exit non-zero with an empty body. We poll
// for up to ~3 seconds, which is long after every container observed in
// dev/CI but well under the eventual /healthz wait.
func dockerHostPort(ctx context.Context, containerID, containerPort string) (int, error) {
	deadline := time.Now().Add(3 * time.Second)
	var lastErr error
	var lastOut string
	for {
		out, err := exec.CommandContext(ctx, "docker", "port", containerID, containerPort).Output()
		if err == nil {
			for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}
				// "0.0.0.0:54321" or "[::]:54321" — split on the LAST
				// colon so IPv6 brackets don't confuse us.
				idx := strings.LastIndex(line, ":")
				if idx < 0 {
					continue
				}
				p, perr := strconv.Atoi(line[idx+1:])
				if perr == nil && p > 0 {
					return p, nil
				}
			}
			lastErr = fmt.Errorf("no usable host port in %q", string(out))
			lastOut = string(out)
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
	return 0, fmt.Errorf("docker port %s %s: %w (last output: %q)", containerID[:12], containerPort, lastErr, lastOut)
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
// container alive but Spark Connect dead — the runner's /healthz returns 503
// when its Connect-client SELECT 1 fails, and a /query against a dead
// Connect server surfaces gRPC "UNAVAILABLE" / SparkConnectGrpcException
// inside the error envelope. False positives cost one extra cold start;
// missing a real dead JVM hangs the UI on every subsequent query, so err
// on the side of respawning.
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
	// to a still-alive HTTP server whose Connect server is gone.
	patterns := []string{
		// Spark Connect surface — what the rewritten /query handler
		// returns when the Connect plugin's gRPC server is dead.
		"SparkConnectGrpcException",
		"StatusCode.UNAVAILABLE",
		"failed to connect to all addresses",
		"Spark Connect server is shutting down",
		"Spark Connect session has been deleted",
		// Generic Spark teardown — still possible since the Connect
		// plugin runs inside a regular SparkContext.
		"SparkContext has been shut down",
		"SparkContext was shut down",
	}
	for _, p := range patterns {
		if strings.Contains(s, p) {
			return true
		}
	}
	return false
}
