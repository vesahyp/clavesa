package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/vesahyp/clavesa/internal/credentials"
	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/sources"
)

// fakeMirrorObject is one remote object in the fake S3 bucket.
type fakeMirrorObject struct {
	data         []byte
	lastModified time.Time
}

// fakeMirrorS3 is an in-memory mirrorS3Client. Listing paginates at pageSize
// keys per page (everything in one page when zero) so the continuation-token
// walk is exercised too. gets counts GetObject calls — the idempotence
// assertions hinge on it.
type fakeMirrorS3 struct {
	objects  map[string]fakeMirrorObject // full key -> object
	pageSize int
	gets     atomic.Int64
	listErr  error
	getErr   error
}

func (f *fakeMirrorS3) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	var keys []string
	for k := range f.objects {
		if strings.HasPrefix(k, aws.ToString(in.Prefix)) {
			keys = append(keys, k)
		}
	}
	// Deterministic listing order, matching S3's lexicographic contract.
	sort.Strings(keys)
	start := 0
	if in.ContinuationToken != nil {
		for i, k := range keys {
			if k > *in.ContinuationToken {
				start = i
				break
			}
		}
	}
	end := len(keys)
	if f.pageSize > 0 && start+f.pageSize < end {
		end = start + f.pageSize
	}
	var contents []s3types.Object
	for _, k := range keys[start:end] {
		obj := f.objects[k]
		lm := obj.lastModified
		contents = append(contents, s3types.Object{
			Key:          aws.String(k),
			Size:         aws.Int64(int64(len(obj.data))),
			LastModified: &lm,
		})
	}
	truncated := end < len(keys)
	out := &s3.ListObjectsV2Output{Contents: contents, IsTruncated: aws.Bool(truncated)}
	if truncated {
		out.NextContinuationToken = aws.String(keys[end-1])
	}
	return out, nil
}

func (f *fakeMirrorS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.gets.Add(1)
	if f.getErr != nil {
		return nil, f.getErr
	}
	obj, ok := f.objects[aws.ToString(in.Key)]
	if !ok {
		return nil, fmt.Errorf("NoSuchKey: %s", aws.ToString(in.Key))
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(obj.data))}, nil
}

func mirrorTime(day int) time.Time {
	return time.Date(2026, 8, day, 3, 0, 0, 0, time.UTC)
}

func TestSyncSourceMirrorDownloadsNewAndNestedKeys(t *testing.T) {
	t.Parallel()
	fake := &fakeMirrorS3{
		pageSize: 2, // force a multi-page listing walk
		objects: map[string]fakeMirrorObject{
			"raw/a.jsonl":            {data: []byte("aaa"), lastModified: mirrorTime(1)},
			"raw/2026/08/b.jsonl":    {data: []byte("bbbb"), lastModified: mirrorTime(2)},
			"raw/2026/08/c.jsonl":    {data: []byte("cc"), lastModified: mirrorTime(3)},
			"raw/":                   {data: nil, lastModified: mirrorTime(1)}, // dir marker
			"elsewhere/ignored.json": {data: []byte("x"), lastModified: mirrorTime(1)},
		},
	}
	dest := t.TempDir()
	sum, err := syncSourceMirror(context.Background(), fake, "bkt", "raw/", dest)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if sum.Downloaded != 3 || sum.Deleted != 0 || sum.Files != 3 {
		t.Errorf("summary = %+v, want 3 downloaded / 0 deleted / 3 files", sum)
	}
	if sum.Bytes != int64(len("aaa")+len("bbbb")+len("cc")) {
		t.Errorf("summary bytes = %d, want 9", sum.Bytes)
	}
	got, err := os.ReadFile(filepath.Join(dest, "2026", "08", "b.jsonl"))
	if err != nil || string(got) != "bbbb" {
		t.Errorf("nested key content = %q, err %v; want %q", got, err, "bbbb")
	}
	// mtime carries the S3 LastModified — the change-detection contract.
	st, err := os.Stat(filepath.Join(dest, "a.jsonl"))
	if err != nil {
		t.Fatalf("stat a.jsonl: %v", err)
	}
	if !st.ModTime().Equal(mirrorTime(1)) {
		t.Errorf("a.jsonl mtime = %v, want %v", st.ModTime(), mirrorTime(1))
	}
	// The out-of-prefix key must not appear in the mirror.
	if _, err := os.Stat(filepath.Join(dest, "..", "elsewhere")); err == nil {
		t.Errorf("out-of-prefix key leaked into the mirror tree")
	}
}

func TestSyncSourceMirrorIdempotentSecondPass(t *testing.T) {
	t.Parallel()
	fake := &fakeMirrorS3{objects: map[string]fakeMirrorObject{
		"raw/a.jsonl": {data: []byte("aaa"), lastModified: mirrorTime(1)},
	}}
	dest := t.TempDir()
	if _, err := syncSourceMirror(context.Background(), fake, "bkt", "raw/", dest); err != nil {
		t.Fatalf("first sync: %v", err)
	}
	before := fake.gets.Load()
	sum, err := syncSourceMirror(context.Background(), fake, "bkt", "raw/", dest)
	if err != nil {
		t.Fatalf("second sync: %v", err)
	}
	if fake.gets.Load() != before {
		t.Errorf("second pass issued %d GetObject calls, want 0", fake.gets.Load()-before)
	}
	if sum.Downloaded != 0 || sum.Files != 1 {
		t.Errorf("second-pass summary = %+v, want 0 downloaded / 1 file", sum)
	}
}

func TestSyncSourceMirrorRedownloadsChangedSize(t *testing.T) {
	t.Parallel()
	fake := &fakeMirrorS3{objects: map[string]fakeMirrorObject{
		"raw/a.jsonl": {data: []byte("aaa"), lastModified: mirrorTime(1)},
	}}
	dest := t.TempDir()
	if _, err := syncSourceMirror(context.Background(), fake, "bkt", "raw/", dest); err != nil {
		t.Fatalf("first sync: %v", err)
	}
	// Same mtime, different size (an overwritten object).
	fake.objects["raw/a.jsonl"] = fakeMirrorObject{data: []byte("aaaaaa"), lastModified: mirrorTime(1)}
	sum, err := syncSourceMirror(context.Background(), fake, "bkt", "raw/", dest)
	if err != nil {
		t.Fatalf("second sync: %v", err)
	}
	if sum.Downloaded != 1 {
		t.Errorf("downloaded = %d, want 1 (size changed)", sum.Downloaded)
	}
	got, _ := os.ReadFile(filepath.Join(dest, "a.jsonl"))
	if string(got) != "aaaaaa" {
		t.Errorf("content = %q, want the re-uploaded bytes", got)
	}
}

func TestSyncSourceMirrorRedownloadsChangedMtime(t *testing.T) {
	t.Parallel()
	fake := &fakeMirrorS3{objects: map[string]fakeMirrorObject{
		"raw/a.jsonl": {data: []byte("aaa"), lastModified: mirrorTime(1)},
	}}
	dest := t.TempDir()
	if _, err := syncSourceMirror(context.Background(), fake, "bkt", "raw/", dest); err != nil {
		t.Fatalf("first sync: %v", err)
	}
	// Same size, newer LastModified (same-length overwrite).
	fake.objects["raw/a.jsonl"] = fakeMirrorObject{data: []byte("zzz"), lastModified: mirrorTime(5)}
	sum, err := syncSourceMirror(context.Background(), fake, "bkt", "raw/", dest)
	if err != nil {
		t.Fatalf("second sync: %v", err)
	}
	if sum.Downloaded != 1 {
		t.Errorf("downloaded = %d, want 1 (mtime changed)", sum.Downloaded)
	}
	got, _ := os.ReadFile(filepath.Join(dest, "a.jsonl"))
	if string(got) != "zzz" {
		t.Errorf("content = %q, want %q", got, "zzz")
	}
}

func TestSyncSourceMirrorDeletesVanishedKeys(t *testing.T) {
	t.Parallel()
	fake := &fakeMirrorS3{objects: map[string]fakeMirrorObject{
		"raw/keep.jsonl":         {data: []byte("k"), lastModified: mirrorTime(1)},
		"raw/2026/expired.jsonl": {data: []byte("e"), lastModified: mirrorTime(1)},
	}}
	dest := t.TempDir()
	if _, err := syncSourceMirror(context.Background(), fake, "bkt", "raw/", dest); err != nil {
		t.Fatalf("first sync: %v", err)
	}
	// Lifecycle expiry: the nested key vanishes remotely.
	delete(fake.objects, "raw/2026/expired.jsonl")
	sum, err := syncSourceMirror(context.Background(), fake, "bkt", "raw/", dest)
	if err != nil {
		t.Fatalf("second sync: %v", err)
	}
	if sum.Deleted != 1 || sum.Files != 1 {
		t.Errorf("summary = %+v, want 1 deleted / 1 file", sum)
	}
	if _, err := os.Stat(filepath.Join(dest, "2026", "expired.jsonl")); !os.IsNotExist(err) {
		t.Errorf("expired key still present locally (err=%v)", err)
	}
	if _, err := os.Stat(filepath.Join(dest, "keep.jsonl")); err != nil {
		t.Errorf("surviving key was pruned: %v", err)
	}
}

func TestSyncSourceMirrorEmptyPrefix(t *testing.T) {
	t.Parallel()
	fake := &fakeMirrorS3{objects: map[string]fakeMirrorObject{
		"a.jsonl":     {data: []byte("a"), lastModified: mirrorTime(1)},
		"sub/b.jsonl": {data: []byte("b"), lastModified: mirrorTime(1)},
	}}
	dest := t.TempDir()
	sum, err := syncSourceMirror(context.Background(), fake, "bkt", "", dest)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if sum.Downloaded != 2 || sum.Files != 2 {
		t.Errorf("summary = %+v, want 2 downloaded / 2 files (whole-bucket scan)", sum)
	}
	if _, err := os.Stat(filepath.Join(dest, "sub", "b.jsonl")); err != nil {
		t.Errorf("nested key missing under empty prefix: %v", err)
	}
}

func TestSyncSourceMirrorRefusesEscapingKeys(t *testing.T) {
	t.Parallel()
	fake := &fakeMirrorS3{objects: map[string]fakeMirrorObject{
		"raw/../../evil.jsonl": {data: []byte("x"), lastModified: mirrorTime(1)},
	}}
	dest := t.TempDir()
	_, err := syncSourceMirror(context.Background(), fake, "bkt", "raw/", dest)
	if err == nil || !strings.Contains(err.Error(), "escapes") {
		t.Fatalf("err = %v, want an escapes-the-mirror rejection", err)
	}
}

// --- descriptor-swap wiring (buildInputs) ---------------------------------

// mirrorTestGraph is a one-transform graph consuming the named registry
// source via source_inputs — the shape hclparser produces for
// `inputs = { x = "sources.<name>" }`.
func mirrorTestGraph(sourceName string) *graph.PipelineGraph {
	return &graph.PipelineGraph{
		Nodes: []graph.Node{
			{ID: "t", Type: "transform", Config: map[string]interface{}{
				"source_inputs": map[string]interface{}{"x": "sources." + sourceName},
			}},
		},
	}
}

func registerMirrorSource(t *testing.T, ws string, spec sources.Spec) {
	t.Helper()
	if err := sources.New(ws).Add(spec); err != nil {
		t.Fatalf("register source: %v", err)
	}
}

func TestBuildInputsMirrorsS3Source(t *testing.T) {
	ws := t.TempDir()
	registerMirrorSource(t, ws, sources.Spec{
		Name: "logs", Kind: "s3", Bucket: "bkt", Prefix: "raw/", Format: "json",
		ReadOptions: map[string]string{"multiline": "true"},
	})
	svc := New(ws)
	fake := &fakeMirrorS3{objects: map[string]fakeMirrorObject{
		"raw/day1.jsonl": {data: []byte(`{"a":1}`), lastModified: mirrorTime(1)},
	}}
	svc.mirrorS3 = fake

	inputs, err := svc.buildInputs(context.Background(), mirrorTestGraph("logs"), "t", map[string]string{}, map[string]string{}, "cat")
	if err != nil {
		t.Fatalf("buildInputs: %v", err)
	}
	desc, ok := inputs["x"].(map[string]any)
	if !ok {
		t.Fatalf("inputs[x] = %T, want map descriptor", inputs["x"])
	}
	if desc["kind"] != "path" {
		t.Errorf("kind = %v, want path", desc["kind"])
	}
	wantDir := filepath.Join(ws, ".clavesa", "cache", "sources", "logs")
	if desc["path"] != wantDir {
		t.Errorf("path = %v, want %s", desc["path"], wantDir)
	}
	if desc["format"] != "json" {
		t.Errorf("format = %v, want json", desc["format"])
	}
	ro, ok := desc["read_options"].(map[string]string)
	if !ok || ro["multiline"] != "true" {
		t.Errorf("read_options = %v, want the spec's map", desc["read_options"])
	}
	// The mirror was actually primed, not just pointed at.
	got, err := os.ReadFile(filepath.Join(wantDir, "day1.jsonl"))
	if err != nil || string(got) != `{"a":1}` {
		t.Errorf("mirrored file = %q, err %v", got, err)
	}
}

func TestBuildInputsMirrorOptOutEnv(t *testing.T) {
	t.Setenv("CLAVESA_SOURCE_MIRROR", "off")
	ws := t.TempDir()
	registerMirrorSource(t, ws, sources.Spec{
		Name: "logs", Kind: "s3", Bucket: "bkt", Prefix: "raw/", Format: "json",
	})
	svc := New(ws)
	fake := &fakeMirrorS3{}
	svc.mirrorS3 = fake

	inputs, err := svc.buildInputs(context.Background(), mirrorTestGraph("logs"), "t", map[string]string{}, map[string]string{}, "cat")
	if err != nil {
		t.Fatalf("buildInputs: %v", err)
	}
	desc := inputs["x"].(map[string]any)
	if desc["kind"] != "s3" || desc["bucket"] != "bkt" || desc["prefix"] != "raw/" {
		t.Errorf("descriptor = %v, want the direct-S3 shape", desc)
	}
	if fake.gets.Load() != 0 {
		t.Errorf("opt-out still downloaded %d objects", fake.gets.Load())
	}
}

func TestBuildInputsMirrorSkipsCredentialedSource(t *testing.T) {
	t.Setenv("MIRROR_TEST_TOKEN", "sekret") // referenced by the credential
	ws := t.TempDir()
	if err := credentials.New(ws).Add(credentials.Spec{
		Name: "tok", Kind: "header", HeaderName: "Authorization", Secret: "env:MIRROR_TEST_TOKEN",
	}); err != nil {
		t.Fatalf("register credential: %v", err)
	}
	registerMirrorSource(t, ws, sources.Spec{
		Name: "logs", Kind: "s3", Bucket: "bkt", Prefix: "raw/", Format: "json",
		Credentials: "tok",
	})
	svc := New(ws)
	fake := &fakeMirrorS3{}
	svc.mirrorS3 = fake

	inputs, err := svc.buildInputs(context.Background(), mirrorTestGraph("logs"), "t", map[string]string{}, map[string]string{}, "cat")
	if err != nil {
		t.Fatalf("buildInputs: %v", err)
	}
	desc := inputs["x"].(map[string]any)
	if desc["kind"] != "s3" {
		t.Errorf("kind = %v, want s3 (credentialed sources stay direct)", desc["kind"])
	}
	if _, ok := desc["credentials"]; !ok {
		t.Errorf("credentials descriptor missing from %v", desc)
	}
	if fake.gets.Load() != 0 {
		t.Errorf("credentialed source was mirrored (%d GetObject calls)", fake.gets.Load())
	}
}

func TestBuildInputsMirrorLeavesPartitionedSourceDirect(t *testing.T) {
	ws := t.TempDir()
	registerMirrorSource(t, ws, sources.Spec{
		Name: "part", Kind: "s3", Bucket: "bkt", Prefix: "seed/", Format: "parquet",
		Partitions: []string{"y", "m"},
	})
	svc := New(ws)
	fake := &fakeMirrorS3{listErr: fmt.Errorf("must not be called")}
	svc.mirrorS3 = fake

	inputs, err := svc.buildInputs(context.Background(), mirrorTestGraph("part"), "t", map[string]string{}, map[string]string{}, "cat")
	if err != nil {
		t.Fatalf("buildInputs: %v", err)
	}
	desc := inputs["x"].(map[string]any)
	if desc["kind"] != "partitioned_path" {
		t.Errorf("kind = %v, want partitioned_path (incremental cursor stays direct-S3)", desc["kind"])
	}
}

func TestBuildInputsMirrorSyncFailureFailsRun(t *testing.T) {
	ws := t.TempDir()
	registerMirrorSource(t, ws, sources.Spec{
		Name: "logs", Kind: "s3", Bucket: "bkt", Prefix: "raw/", Format: "json",
	})
	svc := New(ws)
	svc.mirrorS3 = &fakeMirrorS3{listErr: fmt.Errorf("bucket gone")}

	_, err := svc.buildInputs(context.Background(), mirrorTestGraph("logs"), "t", map[string]string{}, map[string]string{}, "cat")
	if err == nil {
		t.Fatalf("want a loud failure, got nil (no stale-mirror fallback)")
	}
	if !strings.Contains(err.Error(), `"logs"`) || !strings.Contains(err.Error(), "bucket gone") {
		t.Errorf("error %q should name the source and carry the cause", err)
	}
}
