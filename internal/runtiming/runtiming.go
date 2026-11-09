// Package runtiming holds the per-run timing record a local `pipeline run`
// writes: wall-clock time per run-level phase (measured on the Go side) plus
// the per-node timing the runner reports. One JSON file per run lands next to
// the run's progress markers (`_progress/<run>/_timing.json`) and an
// identical copy lands in a cross-workspace history directory, so timings
// from every workspace on the machine can be read back in one place.
package runtiming

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// SchemaVersion is the record's `schema` field. Bump it on a breaking shape
// change so readers can tell old records from new ones.
const SchemaVersion = 1

// FileName is the record's name inside `_progress/<run>/`.
const FileName = "_timing.json"

// Record is one run's timing. Times are integer milliseconds.
type Record struct {
	Schema       int    `json:"schema"`
	RunID        string `json:"run_id"`
	Workspace    string `json:"workspace"`
	WorkspaceDir string `json:"workspace_dir"`
	Pipeline     string `json:"pipeline"`
	Status       string `json:"status"` // SUCCEEDED | FAILED
	Trigger      string `json:"trigger"`
	Compute      string `json:"compute"`
	// StartedAt / EndedAt are RFC 3339 with the local offset. StartedAt is
	// the start of TotalMs (process start for the CLI, the run call for the
	// UI), so StartedAt + TotalMs == EndedAt.
	StartedAt string `json:"started_at"`
	EndedAt   string `json:"ended_at"`
	TotalMs   int64  `json:"total_ms"`
	Phases    Phases `json:"phases"`
	Nodes     []Node `json:"nodes"`
}

// Phases is the run-level breakdown.
//
//   - Startup:   process start to the run service call (0 for UI runs)
//   - Prepare:   parse, topo-sort and the rest of run setup not listed below
//   - Lock:      acquiring the per-pipeline run lock
//   - Metastore: bringing up / reusing the shared Derby metastore
//   - Image:     ensuring the workspace runner image
//   - Inputs:    resolving sources and building every transform's inputs
//     (includes the S3 source mirror)
//   - Container: docker start to the first byte the container writes
//   - Boot:      runner-reported boot (runner.boot_ms)
//   - Bundle:    docker start to docker exit
//   - Nodes:     sum of the per-node spans (ended_ms - started_ms)
//   - Record:    the runs-row rollup container
type Phases struct {
	Startup   int64 `json:"startup"`
	Prepare   int64 `json:"prepare"`
	Lock      int64 `json:"lock"`
	Metastore int64 `json:"metastore"`
	Image     int64 `json:"image"`
	Inputs    int64 `json:"inputs"`
	Container int64 `json:"container"`
	Boot      int64 `json:"boot"`
	Bundle    int64 `json:"bundle"`
	Nodes     int64 `json:"nodes"`
	Record    int64 `json:"record"`
}

// Node is one transform's entry, in topological order.
type Node struct {
	Node       string `json:"node"`
	Status     string `json:"status"`
	StartedMs  int64  `json:"started_ms"`
	EndedMs    int64  `json:"ended_ms"`
	DurationMs int64  `json:"duration_ms"`
	OutputRows *int64 `json:"output_rows"`
	// Timing is the runner's per-node timing object, kept verbatim so a
	// field the runner adds survives the round trip. null when the runner
	// image predates timing. ParsedTiming decodes it.
	Timing json.RawMessage `json:"timing"`
}

// NodeTiming is the typed view of Node.Timing.
type NodeTiming struct {
	Phases  map[string]int64        `json:"phases"`
	Inputs  map[string]int64        `json:"inputs"`
	Outputs map[string]int64        `json:"outputs"`
	Spark   map[string]SparkSummary `json:"spark"`
}

// SparkSummary is the Spark listener summary for one runner phase.
type SparkSummary struct {
	NumStages          int64 `json:"num_stages"`
	NumTasks           int64 `json:"num_tasks"`
	InputBytes         int64 `json:"input_bytes"`
	InputRecords       int64 `json:"input_records"`
	ShuffleReadBytes   int64 `json:"shuffle_read_bytes"`
	ShuffleWriteBytes  int64 `json:"shuffle_write_bytes"`
	MemorySpilledBytes int64 `json:"memory_spilled_bytes"`
	DiskSpilledBytes   int64 `json:"disk_spilled_bytes"`
	ExecutorRunTimeMs  int64 `json:"executor_run_time_ms"`
}

// ParsedTiming decodes Timing. Returns nil when it is absent, null, or not
// an object.
func (n Node) ParsedTiming() *NodeTiming {
	if !HasTiming(n.Timing) {
		return nil
	}
	var t NodeTiming
	if err := json.Unmarshal(n.Timing, &t); err != nil {
		return nil
	}
	return &t
}

// HasTiming reports whether raw is a JSON object (not empty, not null).
func HasTiming(raw json.RawMessage) bool {
	s := strings.TrimSpace(string(raw))
	return strings.HasPrefix(s, "{")
}

// HistoryDir is the cross-workspace history directory:
// $CLAVESA_RUN_HISTORY_DIR, else $XDG_STATE_HOME/clavesa/runs, else
// ~/.local/state/clavesa/runs. Returns "" when none can be resolved.
func HistoryDir() string {
	if d := os.Getenv("CLAVESA_RUN_HISTORY_DIR"); d != "" {
		return d
	}
	if d := os.Getenv("XDG_STATE_HOME"); d != "" {
		return filepath.Join(d, "clavesa", "runs")
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ""
	}
	return filepath.Join(home, ".local", "state", "clavesa", "runs")
}

// HistoryFileName is the record's name in the history directory:
// `<started_at as 20060102T150405>_<workspace>_<pipeline>_<run id[:8]>.json`.
// The started_at wall clock is kept in the record's own offset.
func HistoryFileName(rec *Record) string {
	stamp := "00000000T000000"
	if t, err := time.Parse(time.RFC3339, rec.StartedAt); err == nil {
		stamp = t.Format("20060102T150405")
	}
	id := rec.RunID
	if len(id) > 8 {
		id = id[:8]
	}
	return fmt.Sprintf("%s_%s_%s_%s.json", stamp, sanitize(rec.Workspace), sanitize(rec.Pipeline), sanitize(id))
}

// sanitize keeps [A-Za-z0-9.-] and maps everything else (including '_',
// the field separator) to '-'.
func sanitize(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '-':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	if b.Len() == 0 {
		return "unknown"
	}
	return b.String()
}

// Write writes rec to path (the run's `_progress/<run>/_timing.json`) and
// mirrors the identical bytes into HistoryDir(). Both writes are attempted;
// the returned error joins whichever failed.
func Write(rec *Record, path string) error {
	body, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal timing record: %w", err)
	}
	body = append(body, '\n')
	var errs []error
	if err := writeAtomic(path, body); err != nil {
		errs = append(errs, fmt.Errorf("write %s: %w", path, err))
	}
	if dir := HistoryDir(); dir != "" {
		hp := filepath.Join(dir, HistoryFileName(rec))
		if err := writeAtomic(hp, body); err != nil {
			errs = append(errs, fmt.Errorf("write %s: %w", hp, err))
		}
	} else {
		errs = append(errs, errors.New("no run history dir: set CLAVESA_RUN_HISTORY_DIR"))
	}
	return errors.Join(errs...)
}

func writeAtomic(path string, body []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// ReadDir reads every `*.json` record in dir, sorted by started_at (oldest
// first). A missing dir returns no records and no error. Files that do not
// parse as a record are skipped.
func ReadDir(dir string) ([]Record, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	paths := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		paths = append(paths, filepath.Join(dir, e.Name()))
	}
	return ReadFiles(paths), nil
}

// ReadFiles reads the record at each path, sorted by started_at (oldest
// first). Paths that are missing or do not parse as a record are skipped.
func ReadFiles(paths []string) []Record {
	type row struct {
		rec Record
		at  time.Time
	}
	rows := make([]row, 0, len(paths))
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		var rec Record
		if err := json.Unmarshal(data, &rec); err != nil || rec.RunID == "" {
			continue
		}
		at, _ := time.Parse(time.RFC3339, rec.StartedAt)
		rows = append(rows, row{rec: rec, at: at})
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if !rows[i].at.Equal(rows[j].at) {
			return rows[i].at.Before(rows[j].at)
		}
		return rows[i].rec.RunID < rows[j].rec.RunID
	})
	out := make([]Record, len(rows))
	for i, r := range rows {
		out[i] = r.rec
	}
	return out
}
