package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/vesahyp/clavesa/internal/delta/s3fs"
)

// ProgressStore is the backend-blind seam over the per-run progress tree
// the runner writes under the WAREHOUSE (ADR-024 cloud-local). Both the S3
// warehouse (s3://<pipeline-bucket>/_progress/...) and the local Hadoop
// warehouse (<LocalWarehouseDir>/_progress/...) expose the same key layout,
// so one read path (progressStates / readRunMarker) serves both: the cloud
// CloudProvider builds an S3 store, the LocalProvider a filesystem store.
//
// Keys are warehouse-relative POSIX paths (forward slashes), e.g.
// "_progress/<run>/<node>.json" — identical across both impls so the shared
// helpers never branch on backend.
type ProgressStore interface {
	// ListKeys returns every key under prefix (a warehouse-relative path
	// such as "_progress/<run>/"). Missing prefix yields an empty slice and
	// nil error — a fresh run whose tree hasn't been written is a normal
	// case, not a failure.
	ListKeys(ctx context.Context, prefix string) ([]string, error)
	// ReadKey reads one object's bytes by warehouse-relative key.
	ReadKey(ctx context.Context, key string) ([]byte, error)
	// WriteKey writes one object's bytes by warehouse-relative key. The fs
	// impl writes atomically (temp + rename); the s3 impl does a PutObject.
	WriteKey(ctx context.Context, key string, body []byte) error
}

// s3Writer is the write-half of the S3 surface the progress store needs.
// s3fs.S3API only covers reads (List/Get); PutObject lives here so the
// store can persist run markers. The concrete *s3.Client satisfies both.
type s3Writer interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// s3ProgressStore is a ProgressStore over an S3 bucket. Read goes through
// the s3fs.S3API the CloudProvider already holds; write needs PutObject,
// which the concrete SDK client supplies but the read-only S3API does not —
// so WriteKey is a no-op (error) when no writer was wired.
type s3ProgressStore struct {
	client s3fs.S3API
	writer s3Writer
	bucket string
}

// NewS3ProgressStore wraps an s3fs.S3API + bucket as a ProgressStore. When
// the client also satisfies s3Writer (the concrete SDK client does, the
// snapshot test stub does too once it grows PutObject), WriteKey persists;
// otherwise WriteKey returns an error so callers can log a best-effort miss.
func NewS3ProgressStore(client s3fs.S3API, bucket string) ProgressStore {
	st := &s3ProgressStore{client: client, bucket: bucket}
	if w, ok := client.(s3Writer); ok {
		st.writer = w
	}
	return st
}

func (s *s3ProgressStore) ListKeys(ctx context.Context, prefix string) ([]string, error) {
	if s.client == nil || s.bucket == "" {
		return nil, nil
	}
	var keys []string
	var token *string
	for {
		resp, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return keys, err
		}
		for _, obj := range resp.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
		if resp.IsTruncated == nil || !*resp.IsTruncated || resp.NextContinuationToken == nil {
			break
		}
		token = resp.NextContinuationToken
	}
	return keys, nil
}

func (s *s3ProgressStore) ReadKey(ctx context.Context, key string) ([]byte, error) {
	if s.client == nil || s.bucket == "" {
		return nil, fmt.Errorf("s3 progress store: no client/bucket")
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

func (s *s3ProgressStore) WriteKey(ctx context.Context, key string, body []byte) error {
	if s.writer == nil || s.bucket == "" {
		return fmt.Errorf("s3 progress store: writes not supported (no PutObject client)")
	}
	_, err := s.writer.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(string(body)),
	})
	return err
}

// fileProgressStore is a ProgressStore rooted at a local warehouse dir.
// Keys are warehouse-relative POSIX paths; the impl joins them onto root
// with filepath.FromSlash so Windows separators don't leak into keys.
type fileProgressStore struct {
	root string
}

// NewFileProgressStore roots a ProgressStore at a warehouse directory (the
// LocalWarehouseDir of a workspace). A missing directory is not an error —
// ListKeys returns empty.
func NewFileProgressStore(warehouseDir string) ProgressStore {
	return &fileProgressStore{root: warehouseDir}
}

func (f *fileProgressStore) ListKeys(ctx context.Context, prefix string) ([]string, error) {
	base := filepath.Join(f.root, filepath.FromSlash(prefix))
	var keys []string
	err := filepath.WalkDir(base, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			// A missing prefix dir is a normal empty case — swallow it and
			// stop the walk cleanly.
			if os.IsNotExist(err) {
				return filepath.SkipDir
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, relErr := filepath.Rel(f.root, path)
		if relErr != nil {
			return relErr
		}
		// Return keys in the same warehouse-relative POSIX form the S3 impl
		// uses so progressStates/readRunMarker never branch on backend.
		keys = append(keys, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return keys, err
	}
	return keys, nil
}

func (f *fileProgressStore) ReadKey(ctx context.Context, key string) ([]byte, error) {
	return os.ReadFile(filepath.Join(f.root, filepath.FromSlash(key)))
}

func (f *fileProgressStore) WriteKey(ctx context.Context, key string, body []byte) error {
	dst := filepath.Join(f.root, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("create progress dir: %w", err)
	}
	tmp := dst + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return err
	}
	// Atomic publish so concurrent readers never observe a half-written file.
	return os.Rename(tmp, dst)
}

// ---------------------------------------------------------------------------
// Run marker (_run.json) — run-level overall status + failure context.
// ---------------------------------------------------------------------------

// RunMarker is the run-level `_progress/<run>/_run.json` the dispatch layer
// writes alongside the per-node markers (ADR-024 cloud-local). It carries
// the overall run status that, for non-SFN runs (local compute against
// either warehouse), replaces SFN DescribeExecution as the source of the
// execution's overall RUNNING/SUCCEEDED/FAILED. `Pipeline` lets the
// per-pipeline run listing filter the shared `_progress` tree to one
// pipeline.
type RunMarker struct {
	Status     string `json:"status"` // RUNNING | SUCCEEDED | FAILED
	Trigger    string `json:"trigger,omitempty"`
	StartedMs  *int64 `json:"started_ms,omitempty"`
	EndedMs    *int64 `json:"ended_ms,omitempty"`
	FailedStep string `json:"failed_step,omitempty"`
	ErrorClass string `json:"error_class,omitempty"`
	ErrorMsg   string `json:"error_msg,omitempty"`
	Pipeline   string `json:"pipeline,omitempty"`
}

// runMarkerKey is the warehouse-relative key for one run's _run.json.
func runMarkerKey(run string) string {
	return fmt.Sprintf("_progress/%s/_run.json", run)
}

// progressPrefix is the warehouse-relative LIST prefix for one run's
// per-node + run markers.
func progressPrefix(run string) string {
	return fmt.Sprintf("_progress/%s/", run)
}

// readRunMarker reads one run's `_run.json` from store. The bool reports
// presence: (nil, false, nil) when the marker is absent (a freshly
// dispatched run whose dispatch hasn't written it yet, or a pre-marker
// run). A read or parse error returns (nil, false, err) so callers can log.
func readRunMarker(ctx context.Context, store ProgressStore, run string) (*RunMarker, bool, error) {
	data, err := store.ReadKey(ctx, runMarkerKey(run))
	if err != nil {
		if isMissingKeyErr(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	var m RunMarker
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, false, fmt.Errorf("parse run marker %s: %w", run, err)
	}
	return &m, true, nil
}

// WriteRunMarker persists one run's `_run.json` via store. Exported so the
// service/dispatch layer (which owns writing run markers) shares the exact
// key + encoding the readers expect. Best-effort-friendly — returns the
// error so callers can log a miss without it masking the run outcome.
func WriteRunMarker(ctx context.Context, store ProgressStore, run string, m RunMarker) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return store.WriteKey(ctx, runMarkerKey(run), data)
}

// isMissingKeyErr reports whether err is an object-not-found from either
// backend: os.ErrNotExist for the filesystem store, or an S3 "NoSuchKey"
// for the S3 store (the SDK surfaces a *types.NoSuchKey, but the string
// match also covers the snapshot test stub which returns a bare error).
func isMissingKeyErr(err error) bool {
	if err == nil {
		return false
	}
	if os.IsNotExist(err) {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "NoSuchKey") || strings.Contains(s, "NotFound") || strings.Contains(s, "status code: 404")
}

// progressRuns projects the `_progress/<run>/_run.json` run markers under
// store into Run rows for one pipeline, newest-first by the marker's
// started_ms (markers without one sort last). Backend-blind — the cloud
// provider's ProgressRuns drives it against the S3 store so the status
// listing can include cloud-local runs (GH #65). A nil store returns empty
// with nil error. Per-marker read/parse failures skip that run rather than
// failing the listing.
func progressRuns(ctx context.Context, store ProgressStore, pipeline string, limit int) ([]Run, error) {
	if store == nil {
		return nil, nil
	}
	keys, err := store.ListKeys(ctx, "_progress/")
	if err != nil {
		return nil, err
	}
	type row struct {
		run       Run
		startedMs int64
	}
	rows := make([]row, 0, 8)
	for _, key := range keys {
		rest, ok := strings.CutPrefix(key, "_progress/")
		if !ok {
			continue
		}
		rid, ok := strings.CutSuffix(rest, "/_run.json")
		if !ok || rid == "" || strings.Contains(rid, "/") {
			continue
		}
		m, ok, _ := readRunMarker(ctx, store, rid)
		if !ok || m == nil {
			continue
		}
		if pipeline != "" && m.Pipeline != pipeline {
			continue
		}
		started := int64(0)
		if m.StartedMs != nil {
			started = *m.StartedMs
		}
		rows = append(rows, row{
			run: Run{
				RunID:          rid,
				Pipeline:       m.Pipeline,
				SfExecutionARN: rid,
				Status:         m.Status,
				Trigger:        m.Trigger,
				StartedAt:      millisToISO(m.StartedMs),
				EndedAt:        millisToISO(m.EndedMs),
				DurationMs:     durationMs(m.StartedMs, m.EndedMs),
				FailedStep:     m.FailedStep,
				ErrorClass:     m.ErrorClass,
				ErrorMsg:       m.ErrorMsg,
			},
			startedMs: started,
		})
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].startedMs > rows[j].startedMs })
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	out := make([]Run, len(rows))
	for i, r := range rows {
		out[i] = r.run
	}
	return out, nil
}

// progressStates reads the per-node `_progress/<run>/<node>.json` markers
// from store and returns a StateStatus per node. Backend-blind: the same
// freshness + terminal-marker logic the cloud path used in
// liveProgressStates, now driven off a ProgressStore so the local provider
// reuses it verbatim.
//
// `_run.json` is skipped — it's the run-level marker, not a node. A
// still-"running" marker older than freshnessWindowMs (relative to nowMs)
// is dropped as a crashed/abandoned node; terminal markers
// (succeeded/failed/skipped) are authoritative and never expire. An empty
// status is treated as "running" for back-compat with pre-status runners.
//
// Best-effort: a LIST error returns whatever was gathered; a per-key
// read/parse failure skips that node. Always returns a non-nil map.
func progressStates(ctx context.Context, store ProgressStore, run string, nowMs int64) map[string]StateStatus {
	states := map[string]StateStatus{}
	if store == nil {
		return states
	}
	prefix := progressPrefix(run)
	keys, err := store.ListKeys(ctx, prefix)
	if err != nil {
		// LIST failed; return whatever we have (empty at worst).
		return states
	}
	for _, key := range keys {
		if !strings.HasPrefix(key, prefix) || !strings.HasSuffix(key, ".json") {
			continue
		}
		name := strings.TrimSuffix(key[len(prefix):], ".json")
		if name == "" || name == "_run" {
			// "_run" is the run-level marker, not a node.
			continue
		}
		snap := readProgressSnapshot(ctx, store, key)
		if snap == nil {
			continue
		}
		rawStatus := snap.Status
		if rawStatus == "" {
			rawStatus = "running"
		}
		if rawStatus == "running" {
			if nowMs-snap.UpdatedMs >= freshnessWindowMs {
				// Stale "running" marker — a crashed node, or an old runner
				// whose node finished without writing a terminal marker.
				continue
			}
		}
		var status string
		switch rawStatus {
		case "running":
			status = "RUNNING"
		case "succeeded":
			status = "SUCCEEDED"
		case "failed":
			status = "FAILED"
		case "skipped":
			status = "SKIPPED"
		default:
			status = "RUNNING"
		}
		states[name] = StateStatus{
			Status:          status,
			StagesTotal:     snap.StagesTotal,
			StagesCompleted: snap.StagesCompleted,
			TasksTotal:      snap.TasksTotal,
			TasksCompleted:  snap.TasksCompleted,
			TasksFailed:     snap.TasksFailed,
		}
	}
	return states
}

// readProgressSnapshot reads + parses one per-node `<node>.json` marker.
// Returns nil on any read/parse failure — the node simply doesn't render.
func readProgressSnapshot(ctx context.Context, store ProgressStore, key string) *progressSnapshot {
	data, err := store.ReadKey(ctx, key)
	if err != nil {
		return nil
	}
	var snap progressSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil
	}
	return &snap
}
