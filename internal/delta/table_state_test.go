package delta_test

// Tests for ReadTableState's RowCount derivation (GH #66): the exact
// current row count computed from Delta snapshot state — checkpoint
// `add.stats.numRecords` seeded, post-checkpoint commits replayed — rather
// than folded from per-commit operation metrics over a truncated window.

import (
	"bytes"
	"fmt"
	"testing"
	"testing/fstest"

	parquetgo "github.com/parquet-go/parquet-go"
	"github.com/vesahyp/clavesa/internal/delta"
)

// --- fixture helpers -------------------------------------------------------

// commitName renders a version into Delta's 20-digit commit file name.
func commitName(v int64) string {
	return fmt.Sprintf("%020d.json", v)
}

// checkpointName renders a version into a single-part checkpoint name.
func checkpointName(v int64) string {
	return fmt.Sprintf("%020d.checkpoint.parquet", v)
}

// addLine renders an `add` action carrying numRecords stats. dvCardinality
// < 0 means "no deletion vector".
func addLine(path string, numRecords int64, dvCardinality int64) string {
	stats := fmt.Sprintf(`{"numRecords":%d}`, numRecords)
	if dvCardinality >= 0 {
		return fmt.Sprintf(`{"add":{"path":%q,"size":100,"dataChange":true,"stats":%q,"deletionVector":{"storageType":"u","cardinality":%d}}}`,
			path, stats, dvCardinality)
	}
	return fmt.Sprintf(`{"add":{"path":%q,"size":100,"dataChange":true,"stats":%q}}`, path, stats)
}

// addLineNoStats renders an `add` action with no stats JSON — the shape a
// stats-collection-disabled writer produces.
func addLineNoStats(path string) string {
	return fmt.Sprintf(`{"add":{"path":%q,"size":100,"dataChange":true}}`, path)
}

// removeLine renders a `remove` action retiring path.
func removeLine(path string) string {
	return fmt.Sprintf(`{"remove":{"path":%q,"dataChange":true}}`, path)
}

// infoLine renders a minimal commitInfo. op drives the history projection
// only — RowCount must never consult these metrics.
func infoLine(ts int64, op string) string {
	return fmt.Sprintf(`{"commitInfo":{"timestamp":%d,"operation":%q,"operationMetrics":{"numOutputRows":"1"}}}`, ts, op)
}

// metaLine renders a metaData action with the trivial two-column schema
// shared with log_reader_test.go.
func metaLine() string {
	return `{"metaData":{"id":"abc","format":{"provider":"parquet"},"schemaString":` +
		fmt.Sprintf("%q", trivialSchemaString) + `,"partitionColumns":[],"configuration":{}}}`
}

func joinLines(lines ...string) []byte {
	out := ""
	for _, l := range lines {
		out += l + "\n"
	}
	return []byte(out)
}

// statsCP* mirror the add/remove/metaData column groups rowCountFromLog and
// schemaFromCheckpoint project out of a checkpoint parquet. Tests write
// real checkpoint bytes with these shapes.
type statsCPDV struct {
	Cardinality int64 `parquet:"cardinality"`
}

type statsCPAdd struct {
	Path           string     `parquet:"path"`
	Stats          string     `parquet:"stats"`
	DeletionVector *statsCPDV `parquet:"deletionVector"`
}

type statsCPRemove struct {
	Path string `parquet:"path"`
}

type statsCPMeta struct {
	SchemaString string `parquet:"schemaString"`
}

type statsCPRow struct {
	Add      *statsCPAdd    `parquet:"add"`
	Remove   *statsCPRemove `parquet:"remove"`
	MetaData *statsCPMeta   `parquet:"metaData"`
}

// writeStatsCheckpoint returns single-part checkpoint bytes carrying the
// given add/remove rows plus one metaData row with the trivial schema.
func writeStatsCheckpoint(t *testing.T, rows []statsCPRow) []byte {
	t.Helper()
	rows = append(rows, statsCPRow{MetaData: &statsCPMeta{SchemaString: trivialSchemaString}})
	var buf bytes.Buffer
	pw := parquetgo.NewGenericWriter[statsCPRow](&buf)
	if _, err := pw.Write(rows); err != nil {
		t.Fatalf("write stats checkpoint parquet: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("close stats checkpoint parquet: %v", err)
	}
	return buf.Bytes()
}

// --- tests -----------------------------------------------------------------

// TestReadTableStateRowCountFromCheckpoint is the GH #66 headline case: a
// MERGE-heavy table whose pre-window history is gone (log retention), with
// a checkpoint snapshotting the live set. The count must come from
// checkpoint stats + replay, not from the surviving commits' operation
// metrics (which are deliberately misleading here).
func TestReadTableStateRowCountFromCheckpoint(t *testing.T) {
	cp := writeStatsCheckpoint(t, []statsCPRow{
		{Add: &statsCPAdd{Path: "a.parquet", Stats: `{"numRecords":100}`}},
		{Add: &statsCPAdd{Path: "b.parquet", Stats: `{"numRecords":50}`}},
		// Tombstone of a long-gone file — must not affect the sum.
		{Remove: &statsCPRemove{Path: "z.parquet"}},
	})
	fsys := fstest.MapFS{
		checkpointName(10): &fstest.MapFile{Data: cp},
		// The checkpoint's own commit survives; its actions are ≤ v10 and
		// must not be replayed on top of the checkpoint.
		commitName(10): &fstest.MapFile{Data: joinLines(infoLine(1700000010000, "MERGE"))},
		// v11: MERGE rewrites a.parquet into c.parquet. Its operationMetrics
		// say numOutputRows=1 — irrelevant to the snapshot-state count.
		commitName(11): &fstest.MapFile{Data: joinLines(
			infoLine(1700000011000, "MERGE"),
			removeLine("a.parquet"),
			addLine("c.parquet", 120, -1),
		)},
	}
	st, err := delta.ReadTableState(fsys)
	if err != nil {
		t.Fatalf("ReadTableState: %v", err)
	}
	if st.RowCount == nil || *st.RowCount != 170 {
		t.Fatalf("RowCount = %v, want 170 (50 from checkpoint + 120 replayed)", st.RowCount)
	}
	if len(st.Commits) != 2 || st.Commits[0].Version != 11 {
		t.Errorf("Commits = %+v, want v11 newest of 2", st.Commits)
	}
	if st.Schema == nil || len(st.Schema.Columns) != 2 {
		t.Errorf("Schema = %+v, want the 2-column checkpoint schema", st.Schema)
	}
}

// TestReadTableStateRowCountNoCheckpointFullHistory: a young table whose
// whole history survives inside the window needs no checkpoint — the replay
// from version 0 is exact.
func TestReadTableStateRowCountNoCheckpointFullHistory(t *testing.T) {
	fsys := fstest.MapFS{
		commitName(0): &fstest.MapFile{Data: joinLines(
			metaLine(),
			infoLine(1700000000000, "CREATE TABLE AS SELECT"),
			addLine("a.parquet", 10, -1),
		)},
		commitName(1): &fstest.MapFile{Data: joinLines(
			infoLine(1700000001000, "WRITE"),
			addLine("b.parquet", 5, -1),
		)},
		commitName(2): &fstest.MapFile{Data: joinLines(
			infoLine(1700000002000, "MERGE"),
			removeLine("a.parquet"),
			addLine("c.parquet", 7, -1),
		)},
	}
	st, err := delta.ReadTableState(fsys)
	if err != nil {
		t.Fatalf("ReadTableState: %v", err)
	}
	if st.RowCount == nil || *st.RowCount != 12 {
		t.Fatalf("RowCount = %v, want 12 (5 + 7 live)", st.RowCount)
	}
}

// TestReadTableStateRowCountDeletionVector: a deletion vector attached to a
// live file nets its cardinality out of the file's numRecords.
func TestReadTableStateRowCountDeletionVector(t *testing.T) {
	fsys := fstest.MapFS{
		commitName(0): &fstest.MapFile{Data: joinLines(
			metaLine(),
			infoLine(1700000000000, "CREATE TABLE AS SELECT"),
			addLine("a.parquet", 100, -1),
		)},
		// DELETE via DV: remove the bare add, re-add the same file with a
		// vector marking 30 rows deleted.
		commitName(1): &fstest.MapFile{Data: joinLines(
			infoLine(1700000001000, "DELETE"),
			removeLine("a.parquet"),
			addLine("a.parquet", 100, 30),
		)},
	}
	st, err := delta.ReadTableState(fsys)
	if err != nil {
		t.Fatalf("ReadTableState: %v", err)
	}
	if st.RowCount == nil || *st.RowCount != 70 {
		t.Fatalf("RowCount = %v, want 70 (100 - 30 DV-deleted)", st.RowCount)
	}
}

// TestReadTableStateCheckpointDeletionVector: DV cardinality recorded in
// the checkpoint's own add rows nets out too.
func TestReadTableStateCheckpointDeletionVector(t *testing.T) {
	cp := writeStatsCheckpoint(t, []statsCPRow{
		{Add: &statsCPAdd{Path: "a.parquet", Stats: `{"numRecords":100}`, DeletionVector: &statsCPDV{Cardinality: 25}}},
	})
	fsys := fstest.MapFS{
		checkpointName(10): &fstest.MapFile{Data: cp},
		commitName(10):     &fstest.MapFile{Data: joinLines(infoLine(1700000010000, "DELETE"))},
	}
	st, err := delta.ReadTableState(fsys)
	if err != nil {
		t.Fatalf("ReadTableState: %v", err)
	}
	if st.RowCount == nil || *st.RowCount != 75 {
		t.Fatalf("RowCount = %v, want 75 (100 - 25 DV-deleted)", st.RowCount)
	}
}

// TestReadTableStateRowCountMissingStatsNil: a live file with no stats JSON
// makes the count underivable — RowCount must be nil, never a wrong number.
// Schema and history still resolve.
func TestReadTableStateRowCountMissingStatsNil(t *testing.T) {
	fsys := fstest.MapFS{
		commitName(0): &fstest.MapFile{Data: joinLines(
			metaLine(),
			infoLine(1700000000000, "CREATE TABLE AS SELECT"),
			addLine("a.parquet", 10, -1),
			addLineNoStats("b.parquet"),
		)},
	}
	st, err := delta.ReadTableState(fsys)
	if err != nil {
		t.Fatalf("ReadTableState: %v", err)
	}
	if st.RowCount != nil {
		t.Fatalf("RowCount = %d, want nil (live file without stats)", *st.RowCount)
	}
	if st.Schema == nil || len(st.Commits) != 1 {
		t.Errorf("schema/commits should still resolve: schema=%v commits=%d", st.Schema, len(st.Commits))
	}
}

// TestReadTableStateRowCountRetiredStatlessFileOK: a stats-less file that
// was later removed doesn't poison the count — only live files matter.
func TestReadTableStateRowCountRetiredStatlessFileOK(t *testing.T) {
	fsys := fstest.MapFS{
		commitName(0): &fstest.MapFile{Data: joinLines(
			metaLine(),
			infoLine(1700000000000, "CREATE TABLE AS SELECT"),
			addLineNoStats("a.parquet"),
		)},
		commitName(1): &fstest.MapFile{Data: joinLines(
			infoLine(1700000001000, "WRITE"),
			removeLine("a.parquet"),
			addLine("b.parquet", 33, -1),
		)},
	}
	st, err := delta.ReadTableState(fsys)
	if err != nil {
		t.Fatalf("ReadTableState: %v", err)
	}
	if st.RowCount == nil || *st.RowCount != 33 {
		t.Fatalf("RowCount = %v, want 33 (stats-less file was retired)", st.RowCount)
	}
}

// TestReadTableStateRowCountStatlessCheckpointNil: a checkpoint written
// without the JSON stats column (stats-as-struct or stats disabled)
// zero-fills on projection; live files from it are unknown → nil.
func TestReadTableStateRowCountStatlessCheckpointNil(t *testing.T) {
	cp := writeStatsCheckpoint(t, []statsCPRow{
		{Add: &statsCPAdd{Path: "a.parquet"}}, // Stats deliberately empty
	})
	fsys := fstest.MapFS{
		checkpointName(10): &fstest.MapFile{Data: cp},
		commitName(10):     &fstest.MapFile{Data: joinLines(infoLine(1700000010000, "WRITE"))},
	}
	st, err := delta.ReadTableState(fsys)
	if err != nil {
		t.Fatalf("ReadTableState: %v", err)
	}
	if st.RowCount != nil {
		t.Fatalf("RowCount = %d, want nil (checkpoint carries no stats)", *st.RowCount)
	}
}

// TestReadTableStateRowCountTruncatedWindowNoCheckpointNil: an append table
// with more commits than the read window and no checkpoint cannot
// reconstruct its live set — RowCount must be nil (the pre-fix code folded
// a wrong number out of exactly this shape; GH #66). History still caps at
// the window.
func TestReadTableStateRowCountTruncatedWindowNoCheckpointNil(t *testing.T) {
	const n = 205 // > maxCommitsScanned (200)
	fsys := fstest.MapFS{}
	fsys[commitName(0)] = &fstest.MapFile{Data: joinLines(
		metaLine(),
		infoLine(1700000000000, "CREATE TABLE AS SELECT"),
		addLine("f0.parquet", 1, -1),
	)}
	for v := int64(1); v < n; v++ {
		fsys[commitName(v)] = &fstest.MapFile{Data: joinLines(
			infoLine(1700000000000+v, "WRITE"),
			addLine(fmt.Sprintf("f%d.parquet", v), 1, -1),
		)}
	}
	st, err := delta.ReadTableState(fsys)
	if err != nil {
		t.Fatalf("ReadTableState: %v", err)
	}
	if st.RowCount != nil {
		t.Fatalf("RowCount = %d, want nil (window truncated, no checkpoint)", *st.RowCount)
	}
	if len(st.Commits) != 200 {
		t.Errorf("Commits = %d, want 200 (window cap)", len(st.Commits))
	}
	if st.Commits[0].Version != n-1 {
		t.Errorf("newest commit = v%d, want v%d", st.Commits[0].Version, n-1)
	}
}

// TestReadTableStateRowCountPostCheckpointBeyondWindowNil: commits between
// the checkpoint and the window's start would be silently skipped by the
// replay — the reader must refuse to produce a count instead.
func TestReadTableStateRowCountPostCheckpointBeyondWindowNil(t *testing.T) {
	const n = 205 // commits v0..v204, checkpoint at v0 → v1..v4 fall below the window
	cp := writeStatsCheckpoint(t, []statsCPRow{
		{Add: &statsCPAdd{Path: "f0.parquet", Stats: `{"numRecords":1}`}},
	})
	fsys := fstest.MapFS{checkpointName(0): &fstest.MapFile{Data: cp}}
	fsys[commitName(0)] = &fstest.MapFile{Data: joinLines(
		metaLine(),
		infoLine(1700000000000, "CREATE TABLE AS SELECT"),
		addLine("f0.parquet", 1, -1),
	)}
	for v := int64(1); v < n; v++ {
		fsys[commitName(v)] = &fstest.MapFile{Data: joinLines(
			infoLine(1700000000000+v, "WRITE"),
			addLine(fmt.Sprintf("f%d.parquet", v), 1, -1),
		)}
	}
	st, err := delta.ReadTableState(fsys)
	if err != nil {
		t.Fatalf("ReadTableState: %v", err)
	}
	if st.RowCount != nil {
		t.Fatalf("RowCount = %d, want nil (post-checkpoint commits fall below the window)", *st.RowCount)
	}
}

// TestReadTableStateRowCountVersionGapNil: a hole in the replayed version
// sequence (retention ate a commit we needed) makes the live set
// unreliable — nil, not a guess.
func TestReadTableStateRowCountVersionGapNil(t *testing.T) {
	cp := writeStatsCheckpoint(t, []statsCPRow{
		{Add: &statsCPAdd{Path: "a.parquet", Stats: `{"numRecords":100}`}},
	})
	fsys := fstest.MapFS{
		checkpointName(10): &fstest.MapFile{Data: cp},
		commitName(10):     &fstest.MapFile{Data: joinLines(infoLine(1700000010000, "WRITE"))},
		// v11 missing; v12 survives.
		commitName(12): &fstest.MapFile{Data: joinLines(
			infoLine(1700000012000, "WRITE"),
			addLine("b.parquet", 5, -1),
		)},
	}
	st, err := delta.ReadTableState(fsys)
	if err != nil {
		t.Fatalf("ReadTableState: %v", err)
	}
	if st.RowCount != nil {
		t.Fatalf("RowCount = %d, want nil (gap at v11)", *st.RowCount)
	}
}

// TestReadTableStateRowCountMidHistoryWindowNoCheckpointNil: retention left
// a window that starts mid-history (no version 0) and there is no
// checkpoint — the append/MERGE shape GH #66 is about.
func TestReadTableStateRowCountMidHistoryWindowNoCheckpointNil(t *testing.T) {
	fsys := fstest.MapFS{
		commitName(40): &fstest.MapFile{Data: joinLines(
			metaLine(), // schema resolvable so ReadTableState succeeds
			infoLine(1700000040000, "WRITE"),
			addLine("a.parquet", 10, -1),
		)},
		commitName(41): &fstest.MapFile{Data: joinLines(
			infoLine(1700000041000, "WRITE"),
			addLine("b.parquet", 5, -1),
		)},
	}
	st, err := delta.ReadTableState(fsys)
	if err != nil {
		t.Fatalf("ReadTableState: %v", err)
	}
	if st.RowCount != nil {
		t.Fatalf("RowCount = %d, want nil (window starts at v40, no checkpoint)", *st.RowCount)
	}
}

// TestReadTableStateEmptyTableZeroRows: a freshly created table with no
// data files has a derivable count of exactly 0 — distinct from nil.
func TestReadTableStateEmptyTableZeroRows(t *testing.T) {
	fsys := fstest.MapFS{
		commitName(0): &fstest.MapFile{Data: joinLines(
			metaLine(),
			infoLine(1700000000000, "CREATE TABLE"),
		)},
	}
	st, err := delta.ReadTableState(fsys)
	if err != nil {
		t.Fatalf("ReadTableState: %v", err)
	}
	if st.RowCount == nil || *st.RowCount != 0 {
		t.Fatalf("RowCount = %v, want 0 (empty table, exactly known)", st.RowCount)
	}
}
