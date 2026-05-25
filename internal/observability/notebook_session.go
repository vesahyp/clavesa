package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"
)

// notebookSessionRunner manages REPL subprocesses inside the warm-Spark
// worker container (Slice 1). Each open notebook gets one REPL subprocess
// inside the warm container — they all share the Spark Connect server JVM
// from Slice 0, with per-session SparkSession isolation keyed by Connect
// session_id (a UUID5 derived from notebook id on the runner side).
//
// Lifecycle:
//   - GetOrSpawnREPL(notebookID, warehouse) lazily POSTs /repl/spawn on the
//     warm container's HTTP supervisor; caches the returned repl_id.
//   - RunCell forwards to /repl/<id>/cell, blocking until the cell completes
//     (Python REPL runs the cell in a daemon thread; HTTP request returns
//     when the cell finishes).
//   - CancelCell forwards to /repl/<id>/cancel, non-blocking (HTTP returns
//     immediately with ack; the cell in flight gets interrupted on the
//     runner side via spark.interruptTag + a Python-side cancel flag).
//   - StopSession DELETEs /repl/<id>, killing the subprocess. Reused by
//     idle-reaper goroutine (15 min default) and explicit user "Stop session".
//   - EvictWarehouse stops every REPL on the given warehouse but does NOT
//     touch the warm container itself — catalog queries keep working. Hooked
//     into the existing persistentDockerQueryRunner.EvictWarehouse chain
//     via NotebookEvictionHook in ui.go.
//
// Note: this type does NOT spawn its own docker container. It depends on
// persistentDockerQueryRunner (already alive when `clavesa ui` is up) for
// the underlying warm worker; if the warm worker is down or spawning, every
// op fails fast with a clear error.
type notebookSessionRunner struct {
	warm   *persistentDockerQueryRunner
	httpC  *http.Client
	idleTo time.Duration

	mu       sync.Mutex
	sessions map[string]*notebookSession // keyed by notebookID
	closed   bool
}

// notebookSession tracks one REPL the Python supervisor handed us. lastUsed
// is monotonic time updated on every RunCell/CancelCell so the idle reaper
// has a fresh signal.
type notebookSession struct {
	notebookID string
	warehouse  string
	replID     string
	sessionID  string
	startedAt  time.Time
	lastUsed   time.Time
}

// NotebookSessionStatus is the per-REPL observable state for the runtime
// indicator on the UI's header. Mirrors WorkerStatus's shape.
type NotebookSessionStatus struct {
	NotebookID string `json:"notebook_id"`
	Warehouse  string `json:"warehouse"`
	ReplID     string `json:"repl_id"`
	SessionID  string `json:"session_id"`
	AgeMS      int64  `json:"age_ms"`
	IdleMS     int64  `json:"idle_ms"`
}

// CellResult is the structured per-cell response the runner emits. Mirrors
// the JSON shape returned by notebook_repl.py — see that file's module
// docstring for the wire-level contract. We keep this in observability so
// the service layer can convert it to nbformat outputs without depending
// on the runner Python package.
type CellResult struct {
	Status     string        `json:"status"` // "ok" | "error" | "cancelled"
	DurationMS int64         `json:"duration_ms"`
	Stdout     string        `json:"stdout"`
	Stderr     string        `json:"stderr"`
	Display    *CellDisplay  `json:"display,omitempty"` // status=ok
	Error      *CellErrorMsg `json:"error,omitempty"`   // status=error
}

// CellDisplay is the rendered last-expression result.
type CellDisplay struct {
	Type        string   `json:"type"` // "table" | "text" | "none"
	Columns     []string `json:"columns,omitempty"`
	ColumnTypes []string `json:"column_types,omitempty"`
	Rows        [][]any  `json:"rows,omitempty"`
	Truncated   bool     `json:"truncated,omitempty"`
	TextRepr    string   `json:"text_repr"`
}

// CellErrorMsg is the Python-style exception breakdown.
type CellErrorMsg struct {
	EName     string   `json:"ename"`
	EValue    string   `json:"evalue"`
	Traceback []string `json:"traceback"`
}

// NewNotebookSessionRunner constructs a fresh runner sharing the given warm
// query runner's containers. Caller is responsible for Close() at shutdown
// (otherwise REPL subprocesses linger inside the warm container until it
// dies; not the end of the world but wastes memory).
func NewNotebookSessionRunner(warm *persistentDockerQueryRunner) *notebookSessionRunner {
	r := &notebookSessionRunner{
		warm:     warm,
		httpC:    &http.Client{Timeout: 10 * time.Minute},
		idleTo:   15 * time.Minute,
		sessions: make(map[string]*notebookSession),
	}
	go r.reapLoop()
	return r
}

// reapLoop kills idle REPLs every minute. Idle threshold defaults to 15 min.
func (r *notebookSessionRunner) reapLoop() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for range t.C {
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return
		}
		cutoff := time.Now().Add(-r.idleTo)
		var stale []*notebookSession
		for _, s := range r.sessions {
			if s.lastUsed.Before(cutoff) {
				stale = append(stale, s)
			}
		}
		r.mu.Unlock()
		for _, s := range stale {
			_ = r.StopSession(context.Background(), s.notebookID)
		}
	}
}

// Close stops every tracked REPL and the reaper. Idempotent.
func (r *notebookSessionRunner) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	sessions := make([]*notebookSession, 0, len(r.sessions))
	for _, s := range r.sessions {
		sessions = append(sessions, s)
	}
	r.sessions = nil
	r.mu.Unlock()
	for _, s := range sessions {
		_ = r.stopOnWarm(context.Background(), s)
	}
}

// EvictWarehouse stops every REPL whose warehouse matches. Called from the
// persistentDockerQueryRunner's eviction chain on `pipeline run` so the
// warm container's memory budget isn't pressured by lingering notebooks
// during a transform run.
func (r *notebookSessionRunner) EvictWarehouse(warehouse string) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	var stop []*notebookSession
	for id, s := range r.sessions {
		if s.warehouse == warehouse {
			stop = append(stop, s)
			delete(r.sessions, id)
		}
	}
	r.mu.Unlock()
	for _, s := range stop {
		_ = r.stopOnWarm(context.Background(), s)
	}
}

// Sessions returns the observable state of every tracked REPL. For the UI
// runtime indicator. Safe for concurrent use.
func (r *notebookSessionRunner) Sessions() []NotebookSessionStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	out := make([]NotebookSessionStatus, 0, len(r.sessions))
	for _, s := range r.sessions {
		out = append(out, NotebookSessionStatus{
			NotebookID: s.notebookID,
			Warehouse:  s.warehouse,
			ReplID:     s.replID,
			SessionID:  s.sessionID,
			AgeMS:      now.Sub(s.startedAt).Milliseconds(),
			IdleMS:     now.Sub(s.lastUsed).Milliseconds(),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NotebookID < out[j].NotebookID })
	return out
}

// getOrSpawn returns the cached session for notebookID or POSTs /repl/spawn
// to the warm container to create one. Idempotent under concurrent calls
// for the same notebook (re-checks the map under the lock after spawn).
func (r *notebookSessionRunner) getOrSpawn(ctx context.Context, notebookID, warehouse string) (*notebookSession, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, fmt.Errorf("notebook session runner closed")
	}
	if s, ok := r.sessions[notebookID]; ok && s.warehouse == warehouse {
		s.lastUsed = time.Now()
		r.mu.Unlock()
		return s, nil
	}
	r.mu.Unlock()

	base := r.warm.WarmWorkerBaseURL(warehouse)
	if base == "" {
		return nil, fmt.Errorf("warm worker for warehouse %q not ready (notebooks need the warm Spark Connect container alive)", warehouse)
	}

	reqBody, _ := json.Marshal(map[string]string{"notebook_id": notebookID})
	resp, err := r.post(ctx, base+"/repl/spawn", reqBody)
	if err != nil {
		return nil, fmt.Errorf("spawn repl: %w", err)
	}
	var sp struct {
		ReplID     string `json:"repl_id"`
		SessionID  string `json:"session_id"`
		NotebookID string `json:"notebook_id"`
		Error      string `json:"error"`
	}
	if err := json.Unmarshal(resp, &sp); err != nil {
		return nil, fmt.Errorf("decode spawn response: %w (body: %s)", err, string(resp))
	}
	if sp.Error != "" {
		return nil, fmt.Errorf("spawn repl: %s", sp.Error)
	}

	now := time.Now()
	sess := &notebookSession{
		notebookID: notebookID,
		warehouse:  warehouse,
		replID:     sp.ReplID,
		sessionID:  sp.SessionID,
		startedAt:  now,
		lastUsed:   now,
	}
	r.mu.Lock()
	// Re-check: a concurrent spawn may have raced us and won.
	if existing, ok := r.sessions[notebookID]; ok && existing.warehouse == warehouse {
		r.mu.Unlock()
		_ = r.stopOnWarmByID(context.Background(), warehouse, sess.replID)
		return existing, nil
	}
	r.sessions[notebookID] = sess
	r.mu.Unlock()
	return sess, nil
}

// RunCell forwards one cell to its notebook's REPL and returns the CellResult.
// Blocks the caller until the cell completes (or fails / times out via the
// runner-side _CELL_TIMEOUT_S of 600 seconds).
func (r *notebookSessionRunner) RunCell(ctx context.Context, notebookID, warehouse, cellRunID, language, source string) (*CellResult, error) {
	sess, err := r.getOrSpawn(ctx, notebookID, warehouse)
	if err != nil {
		return nil, err
	}
	base := r.warm.WarmWorkerBaseURL(warehouse)
	if base == "" {
		return nil, fmt.Errorf("warm worker for warehouse %q disappeared mid-request", warehouse)
	}
	body, _ := json.Marshal(map[string]string{
		"cell_run_id": cellRunID,
		"language":    language,
		"source":      source,
	})
	resp, err := r.post(ctx, fmt.Sprintf("%s/repl/%s/cell", base, sess.replID), body)
	if err != nil {
		return nil, fmt.Errorf("run cell: %w", err)
	}
	var cr CellResult
	if err := json.Unmarshal(resp, &cr); err != nil {
		// 4xx/5xx error envelope shape: {"error": "..."}.
		var env struct{ Error string }
		if jerr := json.Unmarshal(resp, &env); jerr == nil && env.Error != "" {
			return nil, fmt.Errorf("run cell: %s", env.Error)
		}
		return nil, fmt.Errorf("decode cell response: %w (body: %s)", err, string(resp))
	}
	r.markUsed(notebookID)
	return &cr, nil
}

// CancelCell sends a cancel request to the REPL. Non-blocking from Spark's
// perspective: the cancel ack returns immediately while the in-flight cell
// either errors out (if Spark-bound: spark.interruptTag aborts the job) or
// runs to completion with status=cancelled (if pure-Python tight loop).
func (r *notebookSessionRunner) CancelCell(ctx context.Context, notebookID, warehouse, cellRunID string) error {
	r.mu.Lock()
	sess, ok := r.sessions[notebookID]
	r.mu.Unlock()
	if !ok {
		// No active session = nothing to cancel; treat as no-op rather than
		// 404 so the UI button doesn't have to know whether a REPL exists.
		return nil
	}
	base := r.warm.WarmWorkerBaseURL(warehouse)
	if base == "" {
		return fmt.Errorf("warm worker disappeared during cancel")
	}
	body, _ := json.Marshal(map[string]string{"cell_run_id": cellRunID})
	_, err := r.post(ctx, fmt.Sprintf("%s/repl/%s/cancel", base, sess.replID), body)
	if err != nil {
		return fmt.Errorf("cancel cell: %w", err)
	}
	r.markUsed(notebookID)
	return nil
}

// StopSession kills the REPL subprocess for notebookID. Used by the user's
// "Stop session" UI button and by the idle reaper.
func (r *notebookSessionRunner) StopSession(ctx context.Context, notebookID string) error {
	r.mu.Lock()
	sess, ok := r.sessions[notebookID]
	if ok {
		delete(r.sessions, notebookID)
	}
	r.mu.Unlock()
	if !ok {
		return nil
	}
	return r.stopOnWarm(ctx, sess)
}

func (r *notebookSessionRunner) markUsed(notebookID string) {
	r.mu.Lock()
	if s, ok := r.sessions[notebookID]; ok {
		s.lastUsed = time.Now()
	}
	r.mu.Unlock()
}

func (r *notebookSessionRunner) stopOnWarm(ctx context.Context, sess *notebookSession) error {
	return r.stopOnWarmByID(ctx, sess.warehouse, sess.replID)
}

func (r *notebookSessionRunner) stopOnWarmByID(ctx context.Context, warehouse, replID string) error {
	base := r.warm.WarmWorkerBaseURL(warehouse)
	if base == "" {
		// Warm gone — the REPL died with it; nothing to do.
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, fmt.Sprintf("%s/repl/%s", base, replID), nil)
	if err != nil {
		return err
	}
	resp, err := r.httpC.Do(req)
	if err != nil {
		return fmt.Errorf("stop repl: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (r *notebookSessionRunner) post(ctx context.Context, url string, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.httpC.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	// Don't gate on status code — the supervisor uses 5xx for protocol
	// failures and the caller already does shape-based parsing. Surface the
	// body either way.
	_ = resp.StatusCode
	return raw, nil
}
