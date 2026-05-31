package api

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// stubS3 implements api.S3API from an in-memory map keyed by
// "<bucket>/<key>" → bytes, the same shape the s3fs package's own test
// stub uses. Kept local to this package because that stub lives in an
// external _test package and isn't importable.
type stubS3 struct {
	objects map[string][]byte
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
		k := key
		size := int64(len(body))
		contents = append(contents, s3types.Object{Key: &k, Size: &size})
	}
	isTruncated := false
	return &s3.ListObjectsV2Output{Contents: contents, IsTruncated: &isTruncated}, nil
}

func (s *stubS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	path := aws.ToString(in.Bucket) + "/" + aws.ToString(in.Key)
	body, ok := s.objects[path]
	if !ok {
		return nil, &s3types.NoSuchKey{}
	}
	size := int64(len(body))
	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: &size,
	}, nil
}

// countingS3 wraps an S3API and counts GetObject calls so the cache tests
// can assert that a hit serves the columns without re-reading the log.
// Atomic because the enrich worker pool may call GetObject concurrently.
type countingS3 struct {
	inner    S3API
	getCalls int64
}

func (c *countingS3) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return c.inner.ListObjectsV2(ctx, in, optFns...)
}

func (c *countingS3) GetObject(ctx context.Context, in *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	atomic.AddInt64(&c.getCalls, 1)
	return c.inner.GetObject(ctx, in, optFns...)
}

func (c *countingS3) gets() int64 { return atomic.LoadInt64(&c.getCalls) }

// deltaLogObjects lays out a minimal single-commit `_delta_log/` under
// s3://<bucket>/<key>/ whose metaData declares the given Spark struct
// schemaString. Returns the object map a stubS3 serves.
func deltaLogObjects(bucket, key, schemaString string) map[string][]byte {
	commit := `{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"abc","format":{"provider":"parquet"},"schemaString":` + jsonQuote(schemaString) + `,"partitionColumns":[],"configuration":{}}}
{"commitInfo":{"timestamp":1700000000000,"operation":"WRITE"}}
`
	logKey := bucket + "/" + strings.TrimSuffix(key, "/") + "/_delta_log/00000000000000000000.json"
	return map[string][]byte{logKey: []byte(commit)}
}

// jsonQuote wraps a string in a JSON string literal (escaping quotes /
// backslashes) so the schemaString can be embedded inline in the commit's
// JSON without a full encoding/json round-trip.
func jsonQuote(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

// stubColumns is the Glue stub a Delta table carries — a single
// `col array<string>` placeholder, which is exactly the symptom this fix
// replaces with the real schema read from the Delta log.
func stubColumns() []CatalogColumn {
	return []CatalogColumn{{Name: "col", Type: "array<string>"}}
}

// TestEnrichDeltaColumnsReplacesStub is the core of the cloud-catalog fix:
// a DELTA table whose Glue StorageDescriptor carries the `col array<string>`
// stub gets its columns replaced with the real schema read from the
// `_delta_log/` on S3 (ADR-014 parity with the local path).
func TestEnrichDeltaColumnsReplacesStub(t *testing.T) {
	const bucket = "mybucket"
	const key = "warehouse/clavesa_demo_ws__taxis/trips__default"
	schema := `{"type":"struct","fields":[` +
		`{"name":"trip_id","type":"long","nullable":true,"metadata":{}},` +
		`{"name":"fare","type":"double","nullable":true,"metadata":{}},` +
		`{"name":"vendor","type":"string","nullable":true,"metadata":{}}]}`
	stub := &stubS3{objects: deltaLogObjects(bucket, key, schema)}

	h := NewCatalogHandler(nil).WithS3(stub)
	tables := []CatalogTable{{
		Database: "clavesa_demo_ws__taxis",
		Name:     "trips__default",
		Location: "s3://" + bucket + "/" + key,
		// Empty on purpose: Spark's Delta saveAsTable leaves Glue's
		// table_type blank, so enrichment must stamp DELTA itself once it
		// reads a real log.
		TableType: "",
		Columns:   stubColumns(),
	}}

	h.enrichDeltaColumns(context.Background(), tables)

	if tables[0].TableType != "DELTA" {
		t.Errorf("TableType = %q, want DELTA stamped after successful log read", tables[0].TableType)
	}
	got := tables[0].Columns
	want := []CatalogColumn{
		{Name: "trip_id", Type: "long"},
		{Name: "fare", Type: "double"},
		{Name: "vendor", Type: "string"},
	}
	if len(got) != len(want) {
		t.Fatalf("columns = %d (%+v), want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("col[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// deltaTrips returns the (bucket, key, stub-served object map, expected
// columns) for a single Delta table — the shared fixture the cache tests
// build a handler around.
func deltaTrips() (bucket, key string, objects map[string][]byte, want []CatalogColumn) {
	bucket = "mybucket"
	key = "warehouse/clavesa_demo_ws__taxis/trips__default"
	schema := `{"type":"struct","fields":[` +
		`{"name":"trip_id","type":"long","nullable":true,"metadata":{}},` +
		`{"name":"fare","type":"double","nullable":true,"metadata":{}}]}`
	objects = deltaLogObjects(bucket, key, schema)
	want = []CatalogColumn{
		{Name: "trip_id", Type: "long"},
		{Name: "fare", Type: "double"},
	}
	return
}

func assertCols(t *testing.T, got, want []CatalogColumn) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("columns = %d (%+v), want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("col[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestEnrichDeltaColumnsCacheHit proves the cache short-circuits S3: the
// first enrich reads the `_delta_log/` (GetObject count rises), and a second
// enrich on the SAME handler + SAME table serves the cached columns with
// ZERO additional GetObject calls. Columns and TableType = "DELTA" hold on
// both passes.
func TestEnrichDeltaColumnsCacheHit(t *testing.T) {
	bucket, key, objects, want := deltaTrips()
	stub := &countingS3{inner: &stubS3{objects: objects}}
	h := NewCatalogHandler(nil).WithS3(stub)

	newTable := func() []CatalogTable {
		return []CatalogTable{{
			Database:  "clavesa_demo_ws__taxis",
			Name:      "trips__default",
			Location:  "s3://" + bucket + "/" + key,
			TableType: "",
			Columns:   stubColumns(),
		}}
	}

	first := newTable()
	h.enrichDeltaColumns(context.Background(), first)
	afterFirst := stub.gets()
	if afterFirst == 0 {
		t.Fatalf("first enrich made 0 GetObject calls, want > 0 (the cold read)")
	}
	if first[0].TableType != "DELTA" {
		t.Errorf("first pass: TableType = %q, want DELTA", first[0].TableType)
	}
	assertCols(t, first[0].Columns, want)

	second := newTable()
	h.enrichDeltaColumns(context.Background(), second)
	if got := stub.gets(); got != afterFirst {
		t.Errorf("second enrich made %d additional GetObject calls, want 0 (cache hit)", got-afterFirst)
	}
	if second[0].TableType != "DELTA" {
		t.Errorf("second pass: TableType = %q, want DELTA (stamped from cache)", second[0].TableType)
	}
	assertCols(t, second[0].Columns, want)
}

// TestEnrichDeltaColumnsCacheTTLExpiry proves a stale entry is re-read: with
// the handler clock pinned, seeding an entry older than the TTL forces the
// next enrich back to S3 (GetObject count rises) and refreshes the stamp.
func TestEnrichDeltaColumnsCacheTTLExpiry(t *testing.T) {
	bucket, key, objects, want := deltaTrips()
	stub := &countingS3{inner: &stubS3{objects: objects}}
	h := NewCatalogHandler(nil).WithS3(stub)

	// Pin the clock so the TTL boundary is deterministic.
	base := time.Unix(1_700_000_000, 0)
	var mu sync.Mutex
	clk := base
	h.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return clk
	}

	location := "s3://" + bucket + "/" + key
	// Seed a stale entry: fetched just past the TTL ago, so cachedSchema
	// treats it as expired and the enrich falls through to a real read.
	h.storeSchema(location, []CatalogColumn{{Name: "old", Type: "string"}})
	h.schemaMu.Lock()
	h.schemaCache[location] = catalogSchemaEntry{
		cols:      []CatalogColumn{{Name: "old", Type: "string"}},
		fetchedAt: base.Add(-catalogSchemaCacheTTL - time.Second),
	}
	h.schemaMu.Unlock()

	tables := []CatalogTable{{
		Database:  "clavesa_demo_ws__taxis",
		Name:      "trips__default",
		Location:  location,
		TableType: "",
		Columns:   stubColumns(),
	}}
	h.enrichDeltaColumns(context.Background(), tables)

	if stub.gets() == 0 {
		t.Fatalf("stale entry was served from cache, want a re-read (GetObject > 0)")
	}
	// The refreshed columns are the real log schema, not the stale seed.
	assertCols(t, tables[0].Columns, want)
	if tables[0].TableType != "DELTA" {
		t.Errorf("TableType = %q, want DELTA after refresh", tables[0].TableType)
	}

	// And the refreshed entry is now fresh: a follow-up enrich is a hit.
	before := stub.gets()
	follow := []CatalogTable{{
		Database: "clavesa_demo_ws__taxis",
		Name:     "trips__default",
		Location: location,
		Columns:  stubColumns(),
	}}
	h.enrichDeltaColumns(context.Background(), follow)
	if got := stub.gets(); got != before {
		t.Errorf("follow-up enrich made %d extra GetObject calls, want 0 (refreshed entry is fresh)", got-before)
	}
	assertCols(t, follow[0].Columns, want)
}

// TestEnrichDeltaColumnsFallbacks covers the best-effort paths: a nil S3
// client, a non-DELTA table, and an unreadable S3 location all leave the
// Glue stub columns untouched rather than failing the catalog.
func TestEnrichDeltaColumnsFallbacks(t *testing.T) {
	t.Run("nil S3 client leaves stub", func(t *testing.T) {
		h := NewCatalogHandler(nil) // no WithS3
		tables := []CatalogTable{{
			Name:      "trips__default",
			Location:  "s3://b/k",
			TableType: "DELTA",
			Columns:   stubColumns(),
		}}
		h.enrichDeltaColumns(context.Background(), tables)
		assertStubUntouched(t, tables[0].Columns)
	})

	t.Run("located table with no Delta log keeps Glue columns", func(t *testing.T) {
		// A non-Delta table (plain-Parquet destination override) has no
		// `_delta_log/` under its location. We attempt the read regardless
		// of declared type — Spark doesn't tag Delta tables with
		// table_type=DELTA, so location presence is the only gate — and the
		// missing log makes the read error, leaving the Glue columns and
		// the original TableType in place.
		stub := &stubS3{objects: map[string][]byte{}} // no _delta_log/ anywhere
		h := NewCatalogHandler(nil).WithS3(stub)
		tables := []CatalogTable{{
			Name:      "some_parquet",
			Location:  "s3://b/k",
			TableType: "",
			Columns:   stubColumns(),
		}}
		h.enrichDeltaColumns(context.Background(), tables)
		assertStubUntouched(t, tables[0].Columns)
		if tables[0].TableType != "" {
			t.Errorf("TableType = %q, want unchanged (empty)", tables[0].TableType)
		}
	})

	t.Run("unreadable location leaves stub", func(t *testing.T) {
		// S3 client present but no `_delta_log/` at the location — the
		// read errors and the stub survives.
		stub := &stubS3{objects: map[string][]byte{}}
		h := NewCatalogHandler(nil).WithS3(stub)
		tables := []CatalogTable{{
			Name:      "trips__default",
			Location:  "s3://b/missing",
			TableType: "DELTA",
			Columns:   stubColumns(),
		}}
		h.enrichDeltaColumns(context.Background(), tables)
		assertStubUntouched(t, tables[0].Columns)
	})

	t.Run("empty location leaves stub", func(t *testing.T) {
		stub := &stubS3{objects: map[string][]byte{}}
		h := NewCatalogHandler(nil).WithS3(stub)
		tables := []CatalogTable{{
			Name:      "trips__default",
			Location:  "",
			TableType: "DELTA",
			Columns:   stubColumns(),
		}}
		h.enrichDeltaColumns(context.Background(), tables)
		assertStubUntouched(t, tables[0].Columns)
	})
}

func assertStubUntouched(t *testing.T, cols []CatalogColumn) {
	t.Helper()
	want := stubColumns()
	if len(cols) != len(want) || cols[0] != want[0] {
		t.Fatalf("columns = %+v, want stub %+v left untouched", cols, want)
	}
}

// TestParseCatalogS3URI covers the local URI splitter the enrichment uses.
func TestParseCatalogS3URI(t *testing.T) {
	cases := []struct {
		uri, bucket, key string
	}{
		{"s3://b/k/t", "b", "k/t"},
		{"s3://b/k/t/", "b", "k/t/"},
		{"s3://justbucket", "justbucket", ""},
		{"file:///local/path", "", ""},
		{"", "", ""},
	}
	for _, c := range cases {
		b, k := parseCatalogS3URI(c.uri)
		if b != c.bucket || k != c.key {
			t.Errorf("parseCatalogS3URI(%q) = (%q, %q), want (%q, %q)", c.uri, b, k, c.bucket, c.key)
		}
	}
}
