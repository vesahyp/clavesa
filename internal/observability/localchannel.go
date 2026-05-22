package observability

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// LocalChannelDirName is the per-pipeline subdirectory the local progress
// channel writes under. Exposed so both writer (service.RunPipeline) and
// reader (LocalProvider) agree on the layout.
const LocalChannelDirName = ".clavesa"

// RunStateFile is the JSON file that captures one pipeline run's state.
// LocalProvider reads from these for ExecutionStates / Runs / NodeRuns;
// RunPipeline writes to them per state transition.
type RunStateFile struct {
	RunID      string                  `json:"run_id"`
	Pipeline   string                  `json:"pipeline"`
	Status     string                  `json:"status"` // RUNNING | SUCCEEDED | FAILED
	StartedAt  string                  `json:"started_at"`
	EndedAt    string                  `json:"ended_at,omitempty"`
	DurationMs *int64                  `json:"duration_ms,omitempty"`
	FailedStep string                  `json:"failed_step,omitempty"`
	ErrorClass string                  `json:"error_class,omitempty"`
	ErrorMsg   string                  `json:"error_msg,omitempty"`
	Trigger    string                  `json:"trigger,omitempty"`
	States     map[string]NodeRunState `json:"states"`
}

// NodeRunState is the per-node entry within a RunStateFile.
type NodeRunState struct {
	Status     string `json:"status"` // RUNNING | SUCCEEDED | FAILED | SKIPPED
	EnteredAt  string `json:"entered_at,omitempty"`
	ExitedAt   string `json:"exited_at,omitempty"`
	DurationMs *int64 `json:"duration_ms,omitempty"`
	ErrorClass string `json:"error_class,omitempty"`
	ErrorMsg   string `json:"error_msg,omitempty"`
}

// runsRoot returns <pipelineDir>/.clavesa/runs (the parent of every run's
// per-run directory). Created on first write.
func runsRoot(pipelineDir string) string {
	return filepath.Join(pipelineDir, LocalChannelDirName, "runs")
}

// RunDir returns <pipelineDir>/.clavesa/runs/<runID>. Both writer and
// reader resolve a run's directory through this single function.
func RunDir(pipelineDir, runID string) string {
	return filepath.Join(runsRoot(pipelineDir), runID)
}

// RunStatePath returns the absolute path to one run's state.json.
func RunStatePath(pipelineDir, runID string) string {
	return filepath.Join(RunDir(pipelineDir, runID), "state.json")
}

// RunLogDir returns <pipelineDir>/.clavesa/runs/<runID>/logs. One file per
// node ID lives inside, capturing that step's stdout+stderr.
func RunLogDir(pipelineDir, runID string) string {
	return filepath.Join(RunDir(pipelineDir, runID), "logs")
}

// RunLogPath returns the absolute path to one node's captured log file.
func RunLogPath(pipelineDir, runID, nodeID string) string {
	// nodeID is a Terraform module label (validated by the parser); it's safe
	// against path traversal but we strip slashes defensively anyway.
	safe := strings.ReplaceAll(nodeID, "/", "_")
	return filepath.Join(RunLogDir(pipelineDir, runID), safe+".log")
}

// logLineSeparator separates the per-line timestamp from the message in
// captured runner log files. Tab is used because no normal log line starts
// with a tab character; it round-trips cleanly through the line reader and
// is easy to spot when tail-ing the file.
const logLineSeparator = "\t"

// NewTimestampedLogWriter wraps w so each line written gains an ISO-8601
// timestamp prefix at write time. Used by the local orchestrator when
// teeing runner stdout/stderr to the per-node log file — gives the
// LocalProvider's ExecutionLogs surface real per-line timestamps that
// match what cloud's CloudWatch payload carries (ADR-014 parity).
//
// The format is `<RFC3339Nano>\t<original line>\n`. Partial writes (no
// trailing newline) are buffered until the next newline arrives so the
// timestamp aligns with when each completed line was emitted, not when
// the byte stream was first flushed.
//
// Concurrency-safe — the underlying log file is shared between the
// docker stdout and stderr pipes, both of which may race.
func NewTimestampedLogWriter(w io.Writer) io.WriteCloser {
	return &timestampedLogWriter{w: w}
}

type timestampedLogWriter struct {
	w   io.Writer
	mu  sync.Mutex
	buf []byte // accumulates the current partial line
}

func (t *timestampedLogWriter) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	consumed := len(p)
	for {
		idx := indexNewline(p)
		if idx < 0 {
			t.buf = append(t.buf, p...)
			return consumed, nil
		}
		t.buf = append(t.buf, p[:idx+1]...)
		// Strip the trailing \n; we'll append our own after the timestamped
		// line to keep one record per file line.
		line := t.buf[:len(t.buf)-1]
		ts := time.Now().UTC().Format(time.RFC3339Nano)
		if _, err := t.w.Write([]byte(ts + logLineSeparator)); err != nil {
			t.buf = nil
			return consumed - len(p) + idx + 1, err
		}
		if _, err := t.w.Write(line); err != nil {
			t.buf = nil
			return consumed - len(p) + idx + 1, err
		}
		if _, err := t.w.Write([]byte{'\n'}); err != nil {
			t.buf = nil
			return consumed - len(p) + idx + 1, err
		}
		t.buf = t.buf[:0]
		p = p[idx+1:]
	}
}

// Close flushes any remaining partial-line buffer with the current
// timestamp. Safe to call multiple times.
func (t *timestampedLogWriter) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.buf) == 0 {
		return nil
	}
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := t.w.Write([]byte(ts + logLineSeparator + string(t.buf) + "\n"))
	t.buf = nil
	return err
}

// indexNewline returns the index of the first '\n' in p, or -1 if absent.
// Inline rather than calling bytes.IndexByte to keep the hot path small.
func indexNewline(p []byte) int {
	for i, b := range p {
		if b == '\n' {
			return i
		}
	}
	return -1
}

// ParseLogLine splits a captured log line into its (timestamp, message)
// pair. Lines written via NewTimestampedLogWriter are formatted as
// `<RFC3339Nano>\t<message>`; legacy un-timestamped lines fall through
// with timestamp = "" so older log files still render in the UI.
func ParseLogLine(line string) (timestamp, message string) {
	idx := strings.Index(line, logLineSeparator)
	if idx <= 0 || idx > 40 {
		return "", line
	}
	candidate := line[:idx]
	if _, err := time.Parse(time.RFC3339Nano, candidate); err != nil {
		return "", line
	}
	return candidate, line[idx+1:]
}

// WriteRunState writes (or replaces) the state.json for one run. Called by
// the local orchestrator on every state transition; cheap (a few hundred
// bytes), so we don't try to be incremental.
func WriteRunState(pipelineDir string, state *RunStateFile) error {
	if err := os.MkdirAll(RunDir(pipelineDir, state.RunID), 0o755); err != nil {
		return fmt.Errorf("create run dir: %w", err)
	}
	tmp := RunStatePath(pipelineDir, state.RunID) + ".tmp"
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	// Atomic publish so concurrent readers never observe a half-written file.
	return os.Rename(tmp, RunStatePath(pipelineDir, state.RunID))
}

// ReadRunState reads one run's state.json. Returns os.ErrNotExist when the
// run directory is absent so callers can render "no such run" cleanly.
func ReadRunState(pipelineDir, runID string) (*RunStateFile, error) {
	path := RunStatePath(pipelineDir, runID)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s RunStateFile
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &s, nil
}

// ListRunIDs returns run IDs ordered newest-first by mtime of state.json.
// Missing channel directory returns an empty slice without error — fresh
// pipelines that haven't been run yet are a normal case.
func ListRunIDs(pipelineDir string) ([]string, error) {
	root := runsRoot(pipelineDir)
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	type entry struct {
		id    string
		mtime int64
	}
	rows := make([]entry, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		stPath := RunStatePath(pipelineDir, e.Name())
		st, err := os.Stat(stPath)
		if err != nil {
			// In-flight run that hasn't published state.json yet — skip.
			continue
		}
		rows = append(rows, entry{id: e.Name(), mtime: st.ModTime().UnixNano()})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].mtime > rows[j].mtime })
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.id
	}
	return out, nil
}
