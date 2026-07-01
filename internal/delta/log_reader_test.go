package delta_test

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	parquetgo "github.com/parquet-go/parquet-go"
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

// checkpointRow mirrors the projection delta.schemaFromCheckpoint reads.
// We write real checkpoint parquet bytes in-test with this shape so the
// parser exercises the same metaData.schemaString leaf the production
// reader projects to. MetaData is a pointer so add-only rows serialize
// with a null metaData group.
type checkpointRow struct {
	MetaData *struct {
		SchemaString string `parquet:"schemaString"`
	} `parquet:"metaData"`
}

// writeCheckpointParquet returns the bytes of a single checkpoint parquet
// part. Each schemaString in schemas yields one row with a non-nil
// metaData carrying it; addRows nil-metaData rows are appended to simulate
// the `add` action rows that dominate a real checkpoint. Passing schemas
// with more than one entry is only used by the multi-part split helper.
func writeCheckpointParquet(t *testing.T, schemas []string, addRows int) []byte {
	t.Helper()
	rows := make([]checkpointRow, 0, len(schemas)+addRows)
	for i := 0; i < addRows; i++ {
		rows = append(rows, checkpointRow{}) // nil MetaData → an add-style row
	}
	for _, s := range schemas {
		r := checkpointRow{MetaData: &struct {
			SchemaString string `parquet:"schemaString"`
		}{SchemaString: s}}
		rows = append(rows, r)
	}
	var buf bytes.Buffer
	pw := parquetgo.NewGenericWriter[checkpointRow](&buf)
	if _, err := pw.Write(rows); err != nil {
		t.Fatalf("write checkpoint parquet: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("close checkpoint parquet: %v", err)
	}
	return buf.Bytes()
}

// appendCommit is a commitInfo-only commit body (no metaData) — the shape
// an append-mode write to node_runs produces after table creation.
func appendCommit(ts int64) string {
	return `{"commitInfo":{"timestamp":` + intStr(ts) + `,"operation":"WRITE","operationMetrics":{"numOutputRows":"1"}}}` + "\n" +
		`{"add":{"path":"p.parquet","partitionValues":{},"size":1,"modificationTime":` + intStr(ts) + `,"dataChange":true}}` + "\n"
}

// metaDataCommit is a metaData+commitInfo commit body carrying schema.
func metaDataCommit(schema string, ts int64, op string) string {
	return `{"metaData":{"id":"x","format":{"provider":"parquet"},"schemaString":` + marshalString(schema) + `,"partitionColumns":[],"configuration":{}}}` + "\n" +
		`{"commitInfo":{"timestamp":` + intStr(ts) + `,"operation":"` + op + `"}}` + "\n"
}

func intStr(n int64) string { return strconv.FormatInt(n, 10) }

// TestReadSchemaCheckpointNoVersionZero proves the checkpoint short-cut:
// the `_delta_log` carries only commits 11–15 (append commits, NO
// metaData) plus a checkpoint at version 10 holding the schema. There is
// no version 0 to walk to — the reader MUST read the schema out of the
// checkpoint or it finds nothing. This is the node_runs case.
func TestReadSchemaCheckpointNoVersionZero(t *testing.T) {
	cpBytes := writeCheckpointParquet(t, []string{trivialSchemaString}, 8)
	mfs := fstest.MapFS{
		"00000000000000000010.checkpoint.parquet": &fstest.MapFile{Data: cpBytes},
	}
	for v := int64(11); v <= 15; v++ {
		name := padVersion(v) + ".json"
		mfs[name] = &fstest.MapFile{Data: []byte(appendCommit(v * 1000))}
	}

	schema, err := delta.ReadSchema(mfs)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	if len(schema.Columns) != 2 {
		t.Fatalf("columns = %d, want 2 (from checkpoint)", len(schema.Columns))
	}
	if schema.Columns[0].Name != "id" || schema.Columns[1].Name != "amount" {
		t.Errorf("cols = %+v, want id/amount from checkpoint schema", schema.Columns)
	}
}

// TestReadSchemaEvolutionAfterCheckpoint: a metaData fired on commit 13
// (post-checkpoint schema evolution) must win over the schema snapshotted
// in the version-10 checkpoint.
func TestReadSchemaEvolutionAfterCheckpoint(t *testing.T) {
	schemaA := `{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}`
	schemaB := `{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},{"name":"added","type":"string","nullable":true,"metadata":{}}]}`
	cpBytes := writeCheckpointParquet(t, []string{schemaA}, 4)
	mfs := fstest.MapFS{
		"00000000000000000010.checkpoint.parquet": &fstest.MapFile{Data: cpBytes},
		"00000000000000000011.json":               &fstest.MapFile{Data: []byte(appendCommit(11000))},
		"00000000000000000012.json":               &fstest.MapFile{Data: []byte(appendCommit(12000))},
		"00000000000000000013.json":               &fstest.MapFile{Data: []byte(metaDataCommit(schemaB, 13000, "ADD COLUMN"))},
		"00000000000000000014.json":               &fstest.MapFile{Data: []byte(appendCommit(14000))},
	}

	schema, err := delta.ReadSchema(mfs)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	if len(schema.Columns) != 2 {
		t.Fatalf("columns = %d, want 2 (post-checkpoint evolution)", len(schema.Columns))
	}
	if schema.Columns[1].Name != "added" {
		t.Errorf("col[1] = %+v, want {added string} from commit 13", schema.Columns[1])
	}
}

// TestReadSchemaMultiPartCheckpoint: the metaData lives in one part and
// another part has only add rows. The reader must scan parts until it
// finds the metaData.
func TestReadSchemaMultiPartCheckpoint(t *testing.T) {
	// Part 1 of 2: only add rows, no metaData. Part 2 of 2: the metaData.
	part1 := writeCheckpointParquet(t, nil, 6)
	part2 := writeCheckpointParquet(t, []string{trivialSchemaString}, 6)
	mfs := fstest.MapFS{
		"00000000000000000020.checkpoint.0000000001.0000000002.parquet": &fstest.MapFile{Data: part1},
		"00000000000000000020.checkpoint.0000000002.0000000002.parquet": &fstest.MapFile{Data: part2},
		"00000000000000000021.json":                                     &fstest.MapFile{Data: []byte(appendCommit(21000))},
	}

	schema, err := delta.ReadSchema(mfs)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	if len(schema.Columns) != 2 {
		t.Fatalf("columns = %d, want 2 (from multi-part checkpoint)", len(schema.Columns))
	}
}

// TestReadSchemaNoCheckpoint: a small table, commits 0–3, metaData only at
// 0, no checkpoint. ReadSchema must still find the schema via the backward
// walk fallback.
func TestReadSchemaNoCheckpoint(t *testing.T) {
	mfs := fstest.MapFS{
		"00000000000000000000.json": &fstest.MapFile{Data: []byte(metaDataCommit(trivialSchemaString, 1000, "CREATE TABLE"))},
		"00000000000000000001.json": &fstest.MapFile{Data: []byte(appendCommit(2000))},
		"00000000000000000002.json": &fstest.MapFile{Data: []byte(appendCommit(3000))},
		"00000000000000000003.json": &fstest.MapFile{Data: []byte(appendCommit(4000))},
	}

	schema, err := delta.ReadSchema(mfs)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	if len(schema.Columns) != 2 {
		t.Fatalf("columns = %d, want 2 (backward walk)", len(schema.Columns))
	}
}

// TestReadSchemaEmptyReturnsErrNotDelta: no commits and no checkpoint is a
// non-Delta directory.
func TestReadSchemaEmptyReturnsErrNotDelta(t *testing.T) {
	_, err := delta.ReadSchema(fstest.MapFS{})
	if !errors.Is(err, delta.ErrNotDelta) {
		t.Fatalf("err = %v, want ErrNotDelta", err)
	}
}

// TestReadCurrentCheckpointAware: ReadCurrent on a table with a checkpoint
// returns BOTH the checkpoint-resolved schema and the recent commit
// history. The schema comes from the checkpoint (no version 0 present);
// the history covers the post-checkpoint JSON commits.
func TestReadCurrentCheckpointAware(t *testing.T) {
	cpBytes := writeCheckpointParquet(t, []string{trivialSchemaString}, 8)
	mfs := fstest.MapFS{
		"00000000000000000010.checkpoint.parquet": &fstest.MapFile{Data: cpBytes},
		"00000000000000000011.json":               &fstest.MapFile{Data: []byte(appendCommit(11000))},
		"00000000000000000012.json":               &fstest.MapFile{Data: []byte(appendCommit(12000))},
	}

	schema, commits, err := delta.ReadCurrent(mfs)
	if err != nil {
		t.Fatalf("ReadCurrent: %v", err)
	}
	if len(schema.Columns) != 2 {
		t.Fatalf("columns = %d, want 2 (from checkpoint)", len(schema.Columns))
	}
	if len(commits) != 2 {
		t.Fatalf("commits = %d, want 2 (11,12)", len(commits))
	}
	if commits[0].Version != 12 || commits[1].Version != 11 {
		t.Errorf("commit versions = [%d,%d], want [12,11]", commits[0].Version, commits[1].Version)
	}
}

// addAction renders a single `add` JSON action line with the given path
// and size — the shape a write commit carries per live data file.
func addAction(path string, size int64) string {
	return `{"add":{"path":"` + path + `","partitionValues":{},"size":` + intStr(size) + `,"modificationTime":1,"dataChange":true}}`
}

// removeAction renders a single `remove` (tombstone) JSON action line.
func removeAction(path string) string {
	return `{"remove":{"path":"` + path + `","dataChange":true}}`
}

// fileCheckpointRow mirrors delta.checkpointFileRow — the add/remove file
// projection ReadFileStats reads. Tests write real checkpoint parquet
// bytes with this shape so applyCheckpointFiles exercises the same leaf
// columns the production reader projects.
type fileCheckpointRow struct {
	Add *struct {
		Path string `parquet:"path"`
		Size int64  `parquet:"size"`
	} `parquet:"add"`
	Remove *struct {
		Path string `parquet:"path"`
	} `parquet:"remove"`
}

// writeFileCheckpointParquet returns the bytes of a checkpoint part whose
// `add` rows are the given path→size live files (and no removes — a real
// checkpoint snapshots only live files). Padding non-file rows aren't
// needed here; the file-stats reader ignores rows with an empty add path.
func writeFileCheckpointParquet(t *testing.T, adds map[string]int64) []byte {
	t.Helper()
	rows := make([]fileCheckpointRow, 0, len(adds))
	for path, size := range adds {
		r := fileCheckpointRow{Add: &struct {
			Path string `parquet:"path"`
			Size int64  `parquet:"size"`
		}{Path: path, Size: size}}
		rows = append(rows, r)
	}
	var buf bytes.Buffer
	pw := parquetgo.NewGenericWriter[fileCheckpointRow](&buf)
	if _, err := pw.Write(rows); err != nil {
		t.Fatalf("write file checkpoint parquet: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("close file checkpoint parquet: %v", err)
	}
	return buf.Bytes()
}

// TestReadFileStatsAddsOnly: a create + two write commits, adds only, no
// removes. Live set is all three files; bytes sum straight.
func TestReadFileStatsAddsOnly(t *testing.T) {
	mfs := fstest.MapFS{
		"00000000000000000000.json": &fstest.MapFile{Data: []byte(
			`{"metaData":{"id":"x","format":{"provider":"parquet"},"schemaString":` + marshalString(trivialSchemaString) + `,"partitionColumns":[],"configuration":{}}}` + "\n" +
				`{"commitInfo":{"timestamp":1000,"operation":"CREATE TABLE"}}` + "\n" +
				addAction("a.parquet", 100) + "\n")},
		"00000000000000000001.json": &fstest.MapFile{Data: []byte(
			`{"commitInfo":{"timestamp":2000,"operation":"WRITE"}}` + "\n" +
				addAction("b.parquet", 250) + "\n")},
		"00000000000000000002.json": &fstest.MapFile{Data: []byte(
			`{"commitInfo":{"timestamp":3000,"operation":"WRITE"}}` + "\n" +
				addAction("c.parquet", 50) + "\n")},
	}

	stats, err := delta.ReadFileStats(mfs)
	if err != nil {
		t.Fatalf("ReadFileStats: %v", err)
	}
	if stats.FileCount != 3 {
		t.Errorf("FileCount = %d, want 3", stats.FileCount)
	}
	if stats.TotalBytes != 400 {
		t.Errorf("TotalBytes = %d, want 400", stats.TotalBytes)
	}
}

// TestReadFileStatsAddThenRemove: a file added at v0 is tombstoned at v1;
// it must not be counted and its bytes must be excluded.
func TestReadFileStatsAddThenRemove(t *testing.T) {
	mfs := fstest.MapFS{
		"00000000000000000000.json": &fstest.MapFile{Data: []byte(
			`{"metaData":{"id":"x","format":{"provider":"parquet"},"schemaString":` + marshalString(trivialSchemaString) + `,"partitionColumns":[],"configuration":{}}}` + "\n" +
				`{"commitInfo":{"timestamp":1000,"operation":"CREATE TABLE"}}` + "\n" +
				addAction("old.parquet", 100) + "\n" +
				addAction("keep.parquet", 30) + "\n")},
		"00000000000000000001.json": &fstest.MapFile{Data: []byte(
			`{"commitInfo":{"timestamp":2000,"operation":"DELETE"}}` + "\n" +
				removeAction("old.parquet") + "\n")},
	}

	stats, err := delta.ReadFileStats(mfs)
	if err != nil {
		t.Fatalf("ReadFileStats: %v", err)
	}
	if stats.FileCount != 1 {
		t.Errorf("FileCount = %d, want 1 (old.parquet removed)", stats.FileCount)
	}
	if stats.TotalBytes != 30 {
		t.Errorf("TotalBytes = %d, want 30 (only keep.parquet)", stats.TotalBytes)
	}
}

// TestReadFileStatsOverwrite: an overwrite commit removes both original
// files and adds a fresh one. Only the new file survives.
func TestReadFileStatsOverwrite(t *testing.T) {
	mfs := fstest.MapFS{
		"00000000000000000000.json": &fstest.MapFile{Data: []byte(
			`{"metaData":{"id":"x","format":{"provider":"parquet"},"schemaString":` + marshalString(trivialSchemaString) + `,"partitionColumns":[],"configuration":{}}}` + "\n" +
				`{"commitInfo":{"timestamp":1000,"operation":"CREATE TABLE"}}` + "\n" +
				addAction("v0-a.parquet", 100) + "\n" +
				addAction("v0-b.parquet", 100) + "\n")},
		"00000000000000000001.json": &fstest.MapFile{Data: []byte(
			`{"commitInfo":{"timestamp":2000,"operation":"WRITE","operationParameters":{"mode":"Overwrite"}}}` + "\n" +
				removeAction("v0-a.parquet") + "\n" +
				removeAction("v0-b.parquet") + "\n" +
				addAction("v1-a.parquet", 75) + "\n")},
	}

	stats, err := delta.ReadFileStats(mfs)
	if err != nil {
		t.Fatalf("ReadFileStats: %v", err)
	}
	if stats.FileCount != 1 {
		t.Errorf("FileCount = %d, want 1 (overwrite)", stats.FileCount)
	}
	if stats.TotalBytes != 75 {
		t.Errorf("TotalBytes = %d, want 75 (overwrite)", stats.TotalBytes)
	}
}

// TestReadFileStatsCheckpointReplay: a checkpoint at v10 snapshots two live
// files; post-checkpoint commits add one and remove one of the originals.
// The reader must seed from the checkpoint parquet and replay v11/v12.
func TestReadFileStatsCheckpointReplay(t *testing.T) {
	cpBytes := writeFileCheckpointParquet(t, map[string]int64{
		"base-1.parquet": 200,
		"base-2.parquet": 300,
	})
	mfs := fstest.MapFS{
		"00000000000000000010.checkpoint.parquet": &fstest.MapFile{Data: cpBytes},
		"00000000000000000011.json": &fstest.MapFile{Data: []byte(
			`{"commitInfo":{"timestamp":11000,"operation":"WRITE"}}` + "\n" +
				addAction("new-1.parquet", 40) + "\n")},
		"00000000000000000012.json": &fstest.MapFile{Data: []byte(
			`{"commitInfo":{"timestamp":12000,"operation":"DELETE"}}` + "\n" +
				removeAction("base-1.parquet") + "\n")},
	}

	stats, err := delta.ReadFileStats(mfs)
	if err != nil {
		t.Fatalf("ReadFileStats: %v", err)
	}
	// Live at v12: base-2 (300) + new-1 (40); base-1 removed.
	if stats.FileCount != 2 {
		t.Errorf("FileCount = %d, want 2 (checkpoint base + replay)", stats.FileCount)
	}
	if stats.TotalBytes != 340 {
		t.Errorf("TotalBytes = %d, want 340 (300 + 40)", stats.TotalBytes)
	}
}

// TestReadFileStatsEmptyReturnsErrNotDelta: no commits, no checkpoint.
func TestReadFileStatsEmptyReturnsErrNotDelta(t *testing.T) {
	_, err := delta.ReadFileStats(fstest.MapFS{})
	if !errors.Is(err, delta.ErrNotDelta) {
		t.Fatalf("err = %v, want ErrNotDelta", err)
	}
}

// TestReadFileStatsFromPath round-trips the on-disk table-root wrapper —
// the shape readDeltaCurrentSnapshot calls.
func TestReadFileStatsFromPath(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, map[string]string{
		"00000000000000000000.json": `{"metaData":{"id":"x","format":{"provider":"parquet"},"schemaString":` + marshalString(trivialSchemaString) + `,"partitionColumns":[],"configuration":{}}}` + "\n" +
			`{"commitInfo":{"timestamp":1000,"operation":"CREATE TABLE"}}` + "\n" +
			addAction("only.parquet", 512) + "\n",
	})

	stats, err := delta.ReadFileStatsFromPath(dir)
	if err != nil {
		t.Fatalf("ReadFileStatsFromPath: %v", err)
	}
	if stats.FileCount != 1 || stats.TotalBytes != 512 {
		t.Errorf("stats = %+v, want {1 512}", stats)
	}
}

// padVersion renders an int64 as Delta's 20-digit zero-padded version
// prefix.
func padVersion(v int64) string {
	s := strconv.FormatInt(v, 10)
	return strings.Repeat("0", 20-len(s)) + s
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
