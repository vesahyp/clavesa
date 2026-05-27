package s3fs_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/vesahyp/clavesa/internal/delta"
	"github.com/vesahyp/clavesa/internal/delta/s3fs"
)

// stubS3 implements s3fs.S3API with an in-memory map keyed by
// "<bucket>/<key>" → bytes. Mirrors the shape moto would expose so the
// test reads like the production path; we don't depend on moto because
// the test surface is tiny.
type stubS3 struct {
	objects map[string][]byte
	// objectMtime keyed by full "<bucket>/<key>" path; defaults to the
	// stub's globalMtime when unset.
	objectMtime map[string]time.Time
	globalMtime time.Time
}

func (s *stubS3) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	bucket := aws.ToString(in.Bucket)
	prefix := aws.ToString(in.Prefix)
	var contents []s3types.Object
	for path, body := range s.objects {
		fullPrefix := bucket + "/"
		if !strings.HasPrefix(path, fullPrefix) {
			continue
		}
		key := path[len(fullPrefix):]
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		mtime := s.globalMtime
		if t, ok := s.objectMtime[path]; ok {
			mtime = t
		}
		size := int64(len(body))
		k := key
		contents = append(contents, s3types.Object{
			Key:          &k,
			Size:         &size,
			LastModified: &mtime,
		})
	}
	isTruncated := false
	return &s3.ListObjectsV2Output{
		Contents:    contents,
		IsTruncated: &isTruncated,
	}, nil
}

func (s *stubS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	path := aws.ToString(in.Bucket) + "/" + aws.ToString(in.Key)
	body, ok := s.objects[path]
	if !ok {
		return nil, &s3types.NoSuchKey{}
	}
	mtime := s.globalMtime
	if t, mok := s.objectMtime[path]; mok {
		mtime = t
	}
	size := int64(len(body))
	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: &size,
		LastModified:  &mtime,
	}, nil
}

// TestS3FSEndToEndWithDeltaReader runs delta.ReadCurrent against an
// S3-backed FS. This is the canonical production path for cloud
// observability snapshot reads — if it works here it works against
// real S3 too (modulo IAM / network).
func TestS3FSEndToEndWithDeltaReader(t *testing.T) {
	schema := `{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}`
	// Two commits: create + merge.
	c0 := `{"metaData":{"id":"abc","format":{"provider":"parquet"},"schemaString":` + jsonString(schema) + `,"partitionColumns":[],"configuration":{}}}
{"commitInfo":{"timestamp":1700000000000,"operation":"WRITE","userMetadata":"{\"clavesa.trigger\":\"manual\"}","operationMetrics":{"numOutputRows":"42"}}}
`
	c1 := `{"commitInfo":{"timestamp":1700000060000,"operation":"MERGE","userMetadata":"{\"clavesa.run-id\":\"r-2\"}","operationMetrics":{"numTargetRowsInserted":"5","numTargetRowsUpdated":"2","numTargetRowsDeleted":"3"}}}
`
	stub := &stubS3{
		objects: map[string][]byte{
			"mybucket/tables/orders/_delta_log/00000000000000000000.json": []byte(c0),
			"mybucket/tables/orders/_delta_log/00000000000000000001.json": []byte(c1),
			// Sidecar / unrelated keys — the FS / delta reader must ignore.
			"mybucket/tables/orders/_delta_log/00000000000000000000.json.crc": []byte("crc"),
			"mybucket/tables/orders/data/part-00000.snappy.parquet":           []byte("parquet"),
		},
	}

	fsys := s3fs.New(context.Background(), stub, "mybucket", "tables/orders/_delta_log/")
	sch, commits, err := delta.ReadCurrent(fsys)
	if err != nil {
		t.Fatalf("ReadCurrent over s3fs: %v", err)
	}
	if len(sch.Columns) != 1 || sch.Columns[0].Name != "id" {
		t.Errorf("schema = %+v, want one `id` column", sch.Columns)
	}
	if len(commits) != 2 {
		t.Fatalf("commits = %d, want 2", len(commits))
	}
	if commits[0].Operation != "MERGE" {
		t.Errorf("commits[0].Operation = %q, want MERGE", commits[0].Operation)
	}
	if commits[0].AddedRecords == nil || *commits[0].AddedRecords != 7 { // 5+2
		t.Errorf("merge added = %v, want 7", commits[0].AddedRecords)
	}
	if commits[0].DeletedRecords == nil || *commits[0].DeletedRecords != 3 {
		t.Errorf("merge deleted = %v, want 3", commits[0].DeletedRecords)
	}
	if commits[1].Operation != "WRITE" {
		t.Errorf("commits[1].Operation = %q, want WRITE", commits[1].Operation)
	}
	if commits[1].AddedRecords == nil || *commits[1].AddedRecords != 42 {
		t.Errorf("write added = %v, want 42", commits[1].AddedRecords)
	}
	if !strings.Contains(commits[0].UserMetadata, "r-2") {
		t.Errorf("merge userMetadata = %q, want substring r-2", commits[0].UserMetadata)
	}
}

// TestS3FSEmptyPrefixReturnsErrNotDelta — bucket+prefix that lists empty
// should surface as ErrNotDelta to the catalog walker.
func TestS3FSEmptyPrefixReturnsErrNotDelta(t *testing.T) {
	stub := &stubS3{objects: map[string][]byte{}}
	fsys := s3fs.New(context.Background(), stub, "mybucket", "absent/_delta_log/")
	_, _, err := delta.ReadCurrent(fsys)
	if !errors.Is(err, delta.ErrNotDelta) {
		t.Errorf("err = %v, want ErrNotDelta", err)
	}
}

// TestS3FSStatFallsBackToObjectMtime — when a commit has no
// commitInfo.timestamp, delta.ReadCurrent uses fs.Stat to fall back to
// the file's mtime. Confirm s3fs surfaces LastModified through Stat.
func TestS3FSStatFallsBackToObjectMtime(t *testing.T) {
	// metaData but no commitInfo: TimestampMs should come from mtime.
	schema := `{"type":"struct","fields":[{"name":"x","type":"long","nullable":true,"metadata":{}}]}`
	c0 := `{"metaData":{"id":"x","format":{"provider":"parquet"},"schemaString":` + jsonString(schema) + `,"partitionColumns":[],"configuration":{}}}
`
	mtime := time.UnixMilli(1700000123456)
	stub := &stubS3{
		objects: map[string][]byte{
			"b/p/_delta_log/00000000000000000000.json": []byte(c0),
		},
		objectMtime: map[string]time.Time{
			"b/p/_delta_log/00000000000000000000.json": mtime,
		},
	}
	fsys := s3fs.New(context.Background(), stub, "b", "p/_delta_log/")
	_, commits, err := delta.ReadCurrent(fsys)
	if err != nil {
		t.Fatalf("ReadCurrent: %v", err)
	}
	if len(commits) != 1 {
		t.Fatalf("commits = %d, want 1", len(commits))
	}
	if commits[0].TimestampMs != mtime.UnixMilli() {
		t.Errorf("TimestampMs = %d, want %d (from mtime)", commits[0].TimestampMs, mtime.UnixMilli())
	}
}

// TestS3FSReadDirFiltersNested — Delta's `_delta_log/` doesn't have
// children other than commit files (modulo `_commits/` checkpoint
// dirs); our ReadDir must elide nested keys so the delta reader's
// commit-file regex pass doesn't get confused.
func TestS3FSReadDirFiltersNested(t *testing.T) {
	stub := &stubS3{
		objects: map[string][]byte{
			"b/p/_delta_log/00000000000000000000.json":          []byte("{}"),
			"b/p/_delta_log/_commits/subfile.json":              []byte("{}"),
			"b/p/_delta_log/_changelog/00000000000000000000.json": []byte("{}"),
		},
	}
	fsys := s3fs.New(context.Background(), stub, "b", "p/_delta_log/")
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "00000000000000000000.json" {
		t.Errorf("ReadDir = %v, want single 00…000.json", entries)
	}
}

// jsonString is the minimal JSON-string encoder used to nest a JSON
// document inside another JSON document's string field. Same shape
// as the test helper in internal/delta.
func jsonString(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '\\', '"':
			b.WriteByte('\\')
			b.WriteRune(r)
		case '\n':
			b.WriteString(`\n`)
		case '\t':
			b.WriteString(`\t`)
		case '\r':
			b.WriteString(`\r`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}
