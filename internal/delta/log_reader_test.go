package delta_test

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/vesahyp/clavesa/internal/delta"
)

// readLog wraps delta.ReadCurrentFromPath so existing on-disk fixtures
// keep their original "give me a table dir" shape. Newer tests that
// exercise the fs.FS path call delta.ReadCurrent directly.
func readLog(tablePath string) (*delta.Schema, []delta.Commit, error) {
	return delta.ReadCurrentFromPath(tablePath)
}

// writeLog lays out a `_delta_log/` directory with one file per commit
// at tablePath. Each entry maps the 0-padded version number to the
// newline-delimited JSON actions that commit carries. Helper trims the
// boilerplate from every test.
func writeLog(t *testing.T, tablePath string, files map[string]string) {
	t.Helper()
	logDir := filepath.Join(tablePath, "_delta_log")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatalf("mkdir _delta_log: %v", err)
	}
	for name, body := range files {
		path := filepath.Join(logDir, name)
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
}

// schemaJSON renders a one-line Spark struct schemaString that the
// metaData action carries. Inline so tests stay readable.
const trivialSchemaString = `{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},{"name":"amount","type":"double","nullable":true,"metadata":{}}]}`

// commitCreate is the first commit Delta writes — a protocol + metaData +
// initial adds. Trimmed to the lines this reader actually consumes.
const commitCreate = `{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"abc","format":{"provider":"parquet"},"schemaString":` + "`schema`" + `,"partitionColumns":[],"configuration":{}}}
{"commitInfo":{"timestamp":1700000000000,"operation":"CREATE TABLE","userMetadata":"{\"trigger\":\"manual\",\"run-id\":\"run-1\"}"}}
{"add":{"path":"part-00000-uuid.snappy.parquet","partitionValues":{},"size":42,"modificationTime":1700000000000,"dataChange":true}}
`

// TestReadCurrentSimpleCreate covers the most common shape: an initial
// commit with protocol, metaData, commitInfo, and one add. The reader
// should return the schema (two columns) plus a single Commit at v0.
func TestReadCurrentSimpleCreate(t *testing.T) {
	dir := t.TempDir()
	body := strings.Replace(commitCreate, "`schema`", marshalString(trivialSchemaString), 1)
	writeLog(t, dir, map[string]string{
		"00000000000000000000.json": body,
	})

	schema, commits, err := readLog(dir)
	if err != nil {
		t.Fatalf("ReadCurrent: %v", err)
	}
	if len(schema.Columns) != 2 {
		t.Fatalf("schema columns = %d, want 2", len(schema.Columns))
	}
	if schema.Columns[0].Name != "id" || schema.Columns[0].Type != "long" {
		t.Errorf("col[0] = %+v, want {id long}", schema.Columns[0])
	}
	if schema.Columns[1].Name != "amount" || schema.Columns[1].Type != "double" {
		t.Errorf("col[1] = %+v, want {amount double}", schema.Columns[1])
	}
	if len(commits) != 1 {
		t.Fatalf("commits = %d, want 1", len(commits))
	}
	c := commits[0]
	if c.Version != 0 {
		t.Errorf("version = %d, want 0", c.Version)
	}
	if c.TimestampMs != 1700000000000 {
		t.Errorf("timestamp = %d, want 1700000000000", c.TimestampMs)
	}
	if c.Operation != "CREATE TABLE" {
		t.Errorf("operation = %q, want CREATE TABLE", c.Operation)
	}
	if !strings.Contains(c.UserMetadata, "run-1") {
		t.Errorf("userMetadata = %q, want substring run-1", c.UserMetadata)
	}
}

// TestReadCurrentMultiCommitHistory exercises the newest-first ordering
// guarantee — three commits, the returned slice must be [2, 1, 0].
func TestReadCurrentMultiCommitHistory(t *testing.T) {
	dir := t.TempDir()
	schema := marshalString(trivialSchemaString)
	files := map[string]string{
		"00000000000000000000.json": `{"metaData":{"id":"x","format":{"provider":"parquet"},"schemaString":` + schema + `,"partitionColumns":[],"configuration":{}}}
{"commitInfo":{"timestamp":1000,"operation":"CREATE TABLE"}}
`,
		"00000000000000000001.json": `{"commitInfo":{"timestamp":2000,"operation":"WRITE"}}
{"add":{"path":"p.parquet","partitionValues":{},"size":1,"modificationTime":2000,"dataChange":true}}
`,
		"00000000000000000002.json": `{"commitInfo":{"timestamp":3000,"operation":"MERGE"}}
`,
	}
	writeLog(t, dir, files)

	_, commits, err := readLog(dir)
	if err != nil {
		t.Fatalf("ReadCurrent: %v", err)
	}
	if len(commits) != 3 {
		t.Fatalf("commits = %d, want 3", len(commits))
	}
	if commits[0].Version != 2 || commits[0].Operation != "MERGE" {
		t.Errorf("commits[0] = %+v, want version 2 + MERGE", commits[0])
	}
	if commits[1].Version != 1 || commits[1].Operation != "WRITE" {
		t.Errorf("commits[1] = %+v, want version 1 + WRITE", commits[1])
	}
	if commits[2].Version != 0 || commits[2].Operation != "CREATE TABLE" {
		t.Errorf("commits[2] = %+v, want version 0 + CREATE TABLE", commits[2])
	}
}

// TestReadCurrentSchemaEvolution covers the schema-evolution path: the
// metaData on a later commit should win over the initial one.
func TestReadCurrentSchemaEvolution(t *testing.T) {
	dir := t.TempDir()
	initial := `{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}`
	evolved := `{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},{"name":"created_at","type":"timestamp","nullable":true,"metadata":{}}]}`
	files := map[string]string{
		"00000000000000000000.json": `{"metaData":{"id":"x","format":{"provider":"parquet"},"schemaString":` + marshalString(initial) + `,"partitionColumns":[],"configuration":{}}}
{"commitInfo":{"timestamp":1000,"operation":"CREATE TABLE"}}
`,
		"00000000000000000001.json": `{"metaData":{"id":"x","format":{"provider":"parquet"},"schemaString":` + marshalString(evolved) + `,"partitionColumns":[],"configuration":{}}}
{"commitInfo":{"timestamp":2000,"operation":"ADD COLUMN"}}
`,
	}
	writeLog(t, dir, files)

	schema, _, err := readLog(dir)
	if err != nil {
		t.Fatalf("ReadCurrent: %v", err)
	}
	if len(schema.Columns) != 2 {
		t.Fatalf("schema columns = %d, want 2 (evolved)", len(schema.Columns))
	}
	if schema.Columns[1].Name != "created_at" || schema.Columns[1].Type != "timestamp" {
		t.Errorf("col[1] = %+v, want {created_at timestamp}", schema.Columns[1])
	}
}

// TestReadCurrentCompoundTypes confirms the type renderer's parity with
// Spark's canonical SQL string form for the four shapes that show up in
// clavesa output tables — decimal, array, map, struct.
func TestReadCurrentCompoundTypes(t *testing.T) {
	dir := t.TempDir()
	schema := `{"type":"struct","fields":[
	  {"name":"price","type":{"type":"decimal","precision":10,"scale":2},"nullable":true,"metadata":{}},
	  {"name":"tags","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{}},
	  {"name":"attrs","type":{"type":"map","keyType":"string","valueType":"long","valueContainsNull":true},"nullable":true,"metadata":{}},
	  {"name":"shipping","type":{"type":"struct","fields":[{"name":"city","type":"string","nullable":true,"metadata":{}},{"name":"weight_kg","type":"double","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}}
	]}`
	files := map[string]string{
		"00000000000000000000.json": `{"metaData":{"id":"x","format":{"provider":"parquet"},"schemaString":` + marshalString(schema) + `,"partitionColumns":[],"configuration":{}}}
{"commitInfo":{"timestamp":1000,"operation":"CREATE TABLE"}}
`,
	}
	writeLog(t, dir, files)

	parsed, _, err := readLog(dir)
	if err != nil {
		t.Fatalf("ReadCurrent: %v", err)
	}
	want := map[string]string{
		"price":    "decimal(10,2)",
		"tags":     "array<string>",
		"attrs":    "map<string,long>",
		"shipping": "struct<city:string,weight_kg:double>",
	}
	if len(parsed.Columns) != 4 {
		t.Fatalf("columns = %d, want 4", len(parsed.Columns))
	}
	for _, c := range parsed.Columns {
		if got := want[c.Name]; got != c.Type {
			t.Errorf("%s: rendered %q, want %q", c.Name, c.Type, got)
		}
	}
}

// TestReadCurrentMissingLog returns ErrNotDelta — the catalog walker
// uses this to filter directories that aren't tables.
func TestReadCurrentMissingLog(t *testing.T) {
	dir := t.TempDir() // no _delta_log
	_, _, err := readLog(dir)
	if !errors.Is(err, delta.ErrNotDelta) {
		t.Fatalf("err = %v, want ErrNotDelta", err)
	}
}

// TestReadCurrentEmptyLog returns ErrNotDelta — a `_delta_log/` with
// nothing in it (or only sidecar files like `.crc`) is indistinguishable
// from a non-Delta directory.
func TestReadCurrentEmptyLog(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "_delta_log"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Drop in some non-commit junk that should be skipped.
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(os.WriteFile(filepath.Join(dir, "_delta_log", "00000000000000000000.json.crc"), []byte("crc"), 0o644))
	must(os.MkdirAll(filepath.Join(dir, "_delta_log", "_commits"), 0o755))

	_, _, err := readLog(dir)
	if !errors.Is(err, delta.ErrNotDelta) {
		t.Fatalf("err = %v, want ErrNotDelta", err)
	}
}

// TestReadCurrentMalformedCommit surfaces parse errors rather than
// swallowing them — silent skips would hide schema-evolution bugs.
func TestReadCurrentMalformedCommit(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"00000000000000000000.json": `{not valid json`,
	}
	writeLog(t, dir, files)

	_, _, err := readLog(dir)
	if err == nil {
		t.Fatal("expected error from malformed commit, got nil")
	}
	if errors.Is(err, delta.ErrNotDelta) {
		t.Errorf("err = %v, want a parse error (not ErrNotDelta)", err)
	}
}

// TestReadCurrentMissingCommitInfo covers tools that don't stamp
// commitInfo — the reader should still surface the Commit (version +
// best-effort mtime timestamp) so the snapshot timeline has a row.
func TestReadCurrentMissingCommitInfo(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"00000000000000000000.json": `{"metaData":{"id":"x","format":{"provider":"parquet"},"schemaString":` + marshalString(trivialSchemaString) + `,"partitionColumns":[],"configuration":{}}}
{"add":{"path":"p.parquet","partitionValues":{},"size":1,"modificationTime":1,"dataChange":true}}
`,
	}
	writeLog(t, dir, files)

	_, commits, err := readLog(dir)
	if err != nil {
		t.Fatalf("ReadCurrent: %v", err)
	}
	if len(commits) != 1 {
		t.Fatalf("commits = %d, want 1", len(commits))
	}
	if commits[0].Version != 0 {
		t.Errorf("version = %d, want 0", commits[0].Version)
	}
	if commits[0].Operation != "" {
		t.Errorf("operation = %q, want empty", commits[0].Operation)
	}
	if commits[0].TimestampMs == 0 {
		t.Error("TimestampMs should fall back to file mtime when commitInfo is absent")
	}
}

// TestReadCurrentFSBackend round-trips the same Delta log shape through
// an in-memory fs.FS — the path the cloud (S3) observability layer takes
// (see internal/delta/s3fs). Uses testing/fstest.MapFS to avoid touching
// disk; if anything diverges from the on-disk path the divergence shows
// up here first.
func TestReadCurrentFSBackend(t *testing.T) {
	schema := marshalString(trivialSchemaString)
	body0 := `{"metaData":{"id":"x","format":{"provider":"parquet"},"schemaString":` + schema + `,"partitionColumns":[],"configuration":{}}}
{"commitInfo":{"timestamp":1000,"operation":"CREATE TABLE","userMetadata":"{\"clavesa.trigger\":\"manual\"}","operationMetrics":{"numOutputRows":"100"}}}
`
	body1 := `{"commitInfo":{"timestamp":2000,"operation":"MERGE","operationMetrics":{"numTargetRowsInserted":"5","numTargetRowsUpdated":"3","numTargetRowsDeleted":"1"}}}
`
	mfs := fstest.MapFS{
		"00000000000000000000.json": &fstest.MapFile{Data: []byte(body0), ModTime: time.UnixMilli(1000)},
		"00000000000000000001.json": &fstest.MapFile{Data: []byte(body1), ModTime: time.UnixMilli(2000)},
	}

	sch, commits, err := delta.ReadCurrent(mfs)
	if err != nil {
		t.Fatalf("ReadCurrent(fs): %v", err)
	}
	if len(sch.Columns) != 2 {
		t.Fatalf("columns = %d, want 2", len(sch.Columns))
	}
	if len(commits) != 2 {
		t.Fatalf("commits = %d, want 2", len(commits))
	}
	// Newest-first: MERGE then CREATE TABLE.
	if commits[0].Operation != "MERGE" || commits[1].Operation != "CREATE TABLE" {
		t.Errorf("ordering = [%q,%q], want [MERGE,CREATE TABLE]", commits[0].Operation, commits[1].Operation)
	}
	// MERGE: added = inserted + updated = 8, deleted = 1.
	if commits[0].AddedRecords == nil || *commits[0].AddedRecords != 8 {
		t.Errorf("merge added = %v, want 8", commits[0].AddedRecords)
	}
	if commits[0].DeletedRecords == nil || *commits[0].DeletedRecords != 1 {
		t.Errorf("merge deleted = %v, want 1", commits[0].DeletedRecords)
	}
	// Non-MERGE: added = numOutputRows = 100, deleted nil.
	if commits[1].AddedRecords == nil || *commits[1].AddedRecords != 100 {
		t.Errorf("write added = %v, want 100", commits[1].AddedRecords)
	}
	if commits[1].DeletedRecords != nil {
		t.Errorf("write deleted = %v, want nil", commits[1].DeletedRecords)
	}
}

// TestReadCurrentEmptyFSReturnsErrNotDelta covers the cloud-side
// equivalent of "this bucket prefix isn't a table" — an fs.FS that
// successfully lists but contains no commit files.
func TestReadCurrentEmptyFSReturnsErrNotDelta(t *testing.T) {
	mfs := fstest.MapFS{} // empty directory listing
	_, _, err := delta.ReadCurrent(mfs)
	if !errors.Is(err, delta.ErrNotDelta) {
		t.Fatalf("err = %v, want ErrNotDelta", err)
	}
}

// errFS implements fs.FS but fails on ReadDir with a non-ErrNotExist
// error — simulates an S3 network failure or auth error so we verify
// the genuine-IO path doesn't silently degrade to ErrNotDelta.
type errFS struct{ err error }

func (e errFS) Open(name string) (fs.File, error) { return nil, e.err }

func TestReadCurrentSurfacesGenuineIOError(t *testing.T) {
	want := errors.New("simulated S3 access denied")
	_, _, err := delta.ReadCurrent(errFS{err: want})
	if err == nil {
		t.Fatal("expected non-nil error from failing FS")
	}
	if errors.Is(err, delta.ErrNotDelta) {
		t.Errorf("err = %v, want a wrapped IO error (not ErrNotDelta)", err)
	}
	if !errors.Is(err, want) {
		t.Errorf("err = %v, want chain containing %v", err, want)
	}
}

// marshalString returns a JSON-encoded string literal (with the
// surrounding double quotes and any escaping JSON requires). Used to
// nest a JSON document inside another JSON document's string field —
// exactly what `metaData.schemaString` is.
func marshalString(s string) string {
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
