package observability

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TestFileProgressStoreRoundTrip exercises the filesystem ProgressStore:
// WriteKey publishes atomically, ListKeys returns warehouse-relative POSIX
// keys, ReadKey reads them back, and a missing prefix lists empty.
func TestFileProgressStoreRoundTrip(t *testing.T) {
	root := t.TempDir()
	store := NewFileProgressStore(root)
	ctx := context.Background()

	// Empty warehouse: no _progress tree yet.
	keys, err := store.ListKeys(ctx, "_progress/run-1/")
	if err != nil {
		t.Fatalf("ListKeys(empty) error: %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("ListKeys(empty) = %v, want empty", keys)
	}

	if err := store.WriteKey(ctx, "_progress/run-1/a.json", []byte(`{"x":1}`)); err != nil {
		t.Fatalf("WriteKey: %v", err)
	}
	if err := store.WriteKey(ctx, "_progress/run-1/_run.json", []byte(`{"status":"RUNNING"}`)); err != nil {
		t.Fatalf("WriteKey: %v", err)
	}

	keys, err = store.ListKeys(ctx, "_progress/run-1/")
	if err != nil {
		t.Fatalf("ListKeys: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("ListKeys = %v, want 2 keys", keys)
	}
	for _, k := range keys {
		// Keys must be warehouse-relative POSIX, not absolute / OS-separated.
		if k[0] == '/' || k != "_progress/run-1/a.json" && k != "_progress/run-1/_run.json" {
			t.Errorf("unexpected key shape: %q", k)
		}
	}

	body, err := store.ReadKey(ctx, "_progress/run-1/a.json")
	if err != nil {
		t.Fatalf("ReadKey: %v", err)
	}
	if string(body) != `{"x":1}` {
		t.Errorf("ReadKey = %q, want %q", body, `{"x":1}`)
	}
}

// TestRunMarkerRoundTrip proves WriteRunMarker + readRunMarker round-trip
// through a store, and that a missing marker reads as (nil, false, nil).
func TestRunMarkerRoundTrip(t *testing.T) {
	store := NewFileProgressStore(t.TempDir())
	ctx := context.Background()

	if _, ok, err := readRunMarker(ctx, store, "missing"); ok || err != nil {
		t.Fatalf("readRunMarker(missing) = (_, %v, %v), want (false, nil)", ok, err)
	}

	started := int64(100)
	ended := int64(250)
	in := RunMarker{
		Status: "FAILED", Trigger: "manual", Pipeline: "demo",
		StartedMs: &started, EndedMs: &ended,
		FailedStep: "xform", ErrorClass: "Boom", ErrorMsg: "kaboom",
	}
	if err := WriteRunMarker(ctx, store, "run-9", in); err != nil {
		t.Fatalf("WriteRunMarker: %v", err)
	}
	got, ok, err := readRunMarker(ctx, store, "run-9")
	if err != nil || !ok {
		t.Fatalf("readRunMarker = (_, %v, %v), want present", ok, err)
	}
	if got.Status != "FAILED" || got.Pipeline != "demo" || got.FailedStep != "xform" || got.ErrorMsg != "kaboom" {
		t.Errorf("round-trip mismatch: %+v", got)
	}
	if got.StartedMs == nil || *got.StartedMs != 100 || got.EndedMs == nil || *got.EndedMs != 250 {
		t.Errorf("timing round-trip mismatch: started=%v ended=%v", got.StartedMs, got.EndedMs)
	}
}

// TestProgressStatesSkipsRunMarkerAndStale proves the shared helper skips the
// `_run.json` run-level marker, drops a stale "running" node, keeps a fresh
// one, and surfaces a terminal marker regardless of freshness.
func TestProgressStatesSkipsRunMarkerAndStale(t *testing.T) {
	store := NewFileProgressStore(t.TempDir())
	ctx := context.Background()
	now := int64(1_000_000_000)

	write := func(node, body string) {
		if err := store.WriteKey(ctx, "_progress/run-1/"+node+".json", []byte(body)); err != nil {
			t.Fatalf("WriteKey %s: %v", node, err)
		}
	}
	// Run-level marker — must be skipped, not treated as a node.
	write("_run", `{"status":"RUNNING"}`)
	// Fresh running node.
	write("fresh", `{"status":"running","tasks_total":10,"updated_ms":`+itoa(now)+`}`)
	// Stale running node — older than the freshness window → dropped.
	write("stale", `{"status":"running","updated_ms":`+itoa(now-freshnessWindowMs-1)+`}`)
	// Terminal node, stale timestamp — must still surface.
	write("done", `{"status":"succeeded","updated_ms":`+itoa(now-freshnessWindowMs-1)+`}`)

	states := progressStates(ctx, store, "run-1", now)

	if _, ok := states["_run"]; ok {
		t.Error("_run marker must not appear as a node")
	}
	if got := states["fresh"].Status; got != "RUNNING" {
		t.Errorf("fresh status = %q, want RUNNING", got)
	}
	if states["fresh"].TasksTotal == nil || *states["fresh"].TasksTotal != 10 {
		t.Errorf("fresh TasksTotal = %v, want 10", states["fresh"].TasksTotal)
	}
	if _, ok := states["stale"]; ok {
		t.Error("stale running node must be dropped")
	}
	if got := states["done"].Status; got != "SUCCEEDED" {
		t.Errorf("done status = %q, want SUCCEEDED (terminal, authoritative)", got)
	}
}

// TestProgressStatesNilStore is a safety check: a nil store yields an empty,
// non-nil map without panicking (the cloud provider passes nil when no S3 /
// bucket is wired).
func TestProgressStatesNilStore(t *testing.T) {
	states := progressStates(context.Background(), nil, "run", 0)
	if states == nil {
		t.Fatal("progressStates(nil store) = nil map, want empty non-nil")
	}
	if len(states) != 0 {
		t.Errorf("progressStates(nil store) = %v, want empty", states)
	}
}

// stubS3RW is an S3API + s3Writer stub backed by an in-memory map keyed by
// "<bucket>/<key>" — the same shape as cloud_test.go's stubS3Snap but with
// PutObject so the S3 progress store's write path is exercised.
type stubS3RW struct {
	objects map[string][]byte
}

func (s *stubS3RW) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	bucket := aws.ToString(in.Bucket)
	prefix := aws.ToString(in.Prefix)
	var contents []s3types.Object
	for path := range s.objects {
		full := bucket + "/"
		if !strings.HasPrefix(path, full) {
			continue
		}
		key := path[len(full):]
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		k := key
		contents = append(contents, s3types.Object{Key: &k})
	}
	trunc := false
	return &s3.ListObjectsV2Output{Contents: contents, IsTruncated: &trunc}, nil
}

func (s *stubS3RW) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	path := aws.ToString(in.Bucket) + "/" + aws.ToString(in.Key)
	body, ok := s.objects[path]
	if !ok {
		return nil, errors.New("NoSuchKey")
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(body))}, nil
}

func (s *stubS3RW) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	body, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	s.objects[aws.ToString(in.Bucket)+"/"+aws.ToString(in.Key)] = body
	return &s3.PutObjectOutput{}, nil
}

// TestS3ProgressStoreRoundTrip exercises the S3 ProgressStore: WriteKey
// (PutObject), ListKeys (full keys), ReadKey, and a missing-key read mapping
// to the not-found classifier so readRunMarker reports absence.
func TestS3ProgressStoreRoundTrip(t *testing.T) {
	stub := &stubS3RW{objects: map[string][]byte{}}
	store := NewS3ProgressStore(stub, "bk")
	ctx := context.Background()

	if err := WriteRunMarker(ctx, store, "run-1", RunMarker{Status: "SUCCEEDED", Pipeline: "demo"}); err != nil {
		t.Fatalf("WriteRunMarker via s3: %v", err)
	}
	if err := store.WriteKey(ctx, "_progress/run-1/a.json", []byte(`{"status":"succeeded","updated_ms":1}`)); err != nil {
		t.Fatalf("WriteKey: %v", err)
	}

	keys, err := store.ListKeys(ctx, "_progress/run-1/")
	if err != nil {
		t.Fatalf("ListKeys: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("ListKeys = %v, want 2", keys)
	}

	m, ok, err := readRunMarker(ctx, store, "run-1")
	if err != nil || !ok {
		t.Fatalf("readRunMarker = (_, %v, %v), want present", ok, err)
	}
	if m.Status != "SUCCEEDED" || m.Pipeline != "demo" {
		t.Errorf("run marker mismatch: %+v", m)
	}

	// A NoSuchKey read maps to absence, not an error.
	if _, ok, err := readRunMarker(ctx, store, "missing"); ok || err != nil {
		t.Errorf("readRunMarker(missing) = (_, %v, %v), want (false, nil)", ok, err)
	}
}

// itoa is a tiny base-10 int64 formatter to keep the JSON fixtures inline
// without pulling strconv into the test's import set noise.
func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// TestProgressRunsListsPipelineMarkers proves the run-marker listing behind
// CloudProvider.ProgressRuns (GH #65): only `_run.json`-bearing runs appear,
// filtered to the asked pipeline, newest-first by started_ms, capped at
// limit; a run dir with only node markers (an SFN run, which never writes
// the run-level marker) is invisible.
func TestProgressRunsListsPipelineMarkers(t *testing.T) {
	store := NewFileProgressStore(t.TempDir())
	ctx := context.Background()

	write := func(run, pipeline, status string, startedMs, endedMs int64) {
		m := RunMarker{Status: status, Pipeline: pipeline, Trigger: "manual"}
		if startedMs > 0 {
			m.StartedMs = &startedMs
		}
		if endedMs > 0 {
			m.EndedMs = &endedMs
		}
		if err := WriteRunMarker(ctx, store, run, m); err != nil {
			t.Fatalf("WriteRunMarker(%s): %v", run, err)
		}
	}
	write("local-old", "demo", "SUCCEEDED", 1000, 2000)
	write("local-new", "demo", "FAILED", 5000, 6000)
	write("local-other", "otherpipe", "SUCCEEDED", 9000, 9500)
	// An SFN-style run: node markers only, no _run.json — must not appear.
	if err := store.WriteKey(ctx, "_progress/arn-run/a.json", []byte(`{"status":"succeeded","updated_ms":1}`)); err != nil {
		t.Fatalf("write node marker: %v", err)
	}

	runs, err := progressRuns(ctx, store, "demo", 10)
	if err != nil {
		t.Fatalf("progressRuns: %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("len(runs) = %d, want 2 (pipeline filter + marker requirement)", len(runs))
	}
	if runs[0].RunID != "local-new" || runs[1].RunID != "local-old" {
		t.Errorf("order = [%s, %s], want newest-first [local-new, local-old]", runs[0].RunID, runs[1].RunID)
	}
	if runs[0].Status != "FAILED" || runs[0].StartedAt == "" || runs[0].DurationMs == nil {
		t.Errorf("row projection incomplete: %+v", runs[0])
	}

	// Limit caps the newest rows.
	capped, err := progressRuns(ctx, store, "demo", 1)
	if err != nil {
		t.Fatalf("progressRuns(limit=1): %v", err)
	}
	if len(capped) != 1 || capped[0].RunID != "local-new" {
		t.Errorf("capped = %+v, want just local-new", capped)
	}
}

// TestProgressRunsNilStore — an unwired store (no S3 client / undeployed
// workspace) yields an empty listing with nil error, so GetStatus degrades
// to SFN-only instead of failing.
func TestProgressRunsNilStore(t *testing.T) {
	runs, err := progressRuns(context.Background(), nil, "demo", 10)
	if err != nil || len(runs) != 0 {
		t.Errorf("progressRuns(nil store) = (%v, %v), want empty + nil", runs, err)
	}
}

// TestCloudProviderProgressRunsViaS3Stub drives the exported seam GetStatus
// calls: the S3-backed store lists `_progress/<run>/_run.json` markers from
// the warehouse bucket.
func TestCloudProviderProgressRunsViaS3Stub(t *testing.T) {
	const bucket = "demo-bucket"
	s3stub := &stubS3Snap{
		objects: map[string][]byte{
			bucket + "/_progress/local-abc/_run.json": []byte(`{"status":"SUCCEEDED","pipeline":"taxi","started_ms":1700000000000,"ended_ms":1700000030000}`),
			bucket + "/_progress/local-def/_run.json": []byte(`{"status":"RUNNING","pipeline":"weblogs","started_ms":1700000050000}`),
		},
	}
	c := NewCloudProvider(nil, bucket, nil, nil).WithS3(s3stub)

	runs, err := c.ProgressRuns(context.Background(), "taxi", 20)
	if err != nil {
		t.Fatalf("ProgressRuns: %v", err)
	}
	if len(runs) != 1 || runs[0].RunID != "local-abc" || runs[0].Status != "SUCCEEDED" {
		t.Errorf("ProgressRuns = %+v, want the single taxi run local-abc", runs)
	}

	// Unwired provider (no S3): empty, nil error.
	bare := NewCloudProvider(nil, "", nil, nil)
	if runs, err := bare.ProgressRuns(context.Background(), "taxi", 20); err != nil || len(runs) != 0 {
		t.Errorf("ProgressRuns(unwired) = (%v, %v), want empty + nil", runs, err)
	}
}
