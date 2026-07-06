package observability

import (
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// LocalChannelDirName is the per-pipeline subdirectory the local run
// artifacts (bundle logs, failure logs) live under. Exposed so both writer
// (service.RunPipeline) and reader (LocalProvider) agree on the layout.
// Runtime STATE no longer lives here — per-node status moved to the
// warehouse `_progress/<run>/` marker tree (3be08e3 / ADR-024); this dir
// keeps only the captured runner output.
const LocalChannelDirName = ".clavesa"

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

// RunBundleLogPath returns the absolute path of one run's `_bundle.log` —
// the full runner stdout/stderr the bundle path tees per run (one container,
// one Spark session, every node). Written by service.runPipelineBundle;
// read by LocalProvider.ExecutionLogs (GH #64).
func RunBundleLogPath(pipelineDir, runID string) string {
	return filepath.Join(RunDir(pipelineDir, runID), "_bundle.log")
}

// RunLogPath returns the absolute path of one node's failure-log file under
// <pipelineDir>/.clavesa/runs/<runID>/logs/. This is a durable-log LOCATION
// only (GH #82): single-node paths (backfill replays) persist a failed
// invocation's buffered stdout+stderr here and point the error message at
// it. Nothing serves these files over HTTP — the run-level Logs surface
// reads the `_bundle.log` above.
func RunLogPath(pipelineDir, runID, nodeID string) string {
	// nodeID is a Terraform module label (validated by the parser); it's safe
	// against path traversal but we strip slashes defensively anyway.
	safe := strings.ReplaceAll(nodeID, "/", "_")
	return filepath.Join(RunDir(pipelineDir, runID), "logs", safe+".log")
}

// logLineSeparator separates the per-line timestamp from the message in
// captured runner log files. Tab is used because no normal log line starts
// with a tab character; it round-trips cleanly through the line reader and
// is easy to spot when tail-ing the file.
const logLineSeparator = "\t"

// NewTimestampedLogWriter wraps w so each line written gains an ISO-8601
// timestamp prefix at write time. Used by the local bundle runner when
// teeing runner stdout/stderr to the per-run `_bundle.log` — gives the
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

// ListProgressRunIDs returns run IDs ordered newest-first by mtime of each
// run's `_run.json`, read from the warehouse `_progress/<run>/` tree — the
// uniform progress channel the runner + dispatch layer write under the
// WAREHOUSE (ADR-024). A run directory without a `_run.json` (in flight, or
// whose marker hasn't landed yet) is skipped. A missing `_progress`
// directory returns an empty slice without error — a fresh workspace that
// hasn't run anything is a normal case.
func ListProgressRunIDs(warehouseDir string) ([]string, error) {
	root := filepath.Join(warehouseDir, "_progress")
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
		markerPath := filepath.Join(root, e.Name(), "_run.json")
		st, err := os.Stat(markerPath)
		if err != nil {
			// Run dir without a _run.json yet — in flight, or pre-marker.
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
