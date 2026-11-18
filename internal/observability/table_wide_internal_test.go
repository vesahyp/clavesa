package observability

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestReadDeltaCurrentSnapshotIsTableWide: the catalog row of a local table
// carries the whole table's files, bytes and rows, like the cloud `tables`
// row the runner writes (_table_wide_counts), never the last commit's. A
// table appended three times shows 3 files and 180 rows, not 1 file and 30.
func TestReadDeltaCurrentSnapshotIsTableWide(t *testing.T) {
	metaLine := `{"metaData":{"id":"00000000-0000-0000-0000-000000000000","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1715000000000}}`
	add := func(path string, numRecords int64) string {
		return fmt.Sprintf(`{"add":{"path":%q,"size":100,"dataChange":true,"stats":"{\"numRecords\":%d}"}}`, path, numRecords)
	}
	write := func(t *testing.T, commits []string) string {
		t.Helper()
		dir := t.TempDir()
		logDir := filepath.Join(dir, "_delta_log")
		if err := os.MkdirAll(logDir, 0o755); err != nil {
			t.Fatal(err)
		}
		for v, body := range commits {
			if err := os.WriteFile(filepath.Join(logDir, fmt.Sprintf("%020d.json", v)), []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		return dir
	}
	commit := func(ts int, mode string, rows int, actions string) string {
		return fmt.Sprintf(`{"commitInfo":{"timestamp":%d,"operation":"WRITE","operationParameters":{"mode":%q},"operationMetrics":{"numOutputRows":"%d","numFiles":"1","numOutputBytes":"100"}}}`, ts, mode, rows) + "\n" + actions
	}

	dir := write(t, []string{
		metaLine + "\n" + commit(1715000000000, "Overwrite", 100, add("f0", 100)),
		commit(1715000001000, "Append", 50, add("f1", 50)),
		commit(1715000002000, "Append", 30, add("f2", 30)),
	})
	info, ok := readDeltaCurrentSnapshot(dir)
	if !ok {
		t.Fatal("not read")
	}
	if info.RowCount == nil || *info.RowCount != 180 {
		t.Errorf("RowCount = %v, want 180 (the table, not the last commit's 30)", info.RowCount)
	}
	if info.FileCount == nil || *info.FileCount != 3 {
		t.Errorf("FileCount = %v, want 3", info.FileCount)
	}
	if info.TotalBytes == nil || *info.TotalBytes != 300 {
		t.Errorf("TotalBytes = %v, want 300", info.TotalBytes)
	}
	if info.SnapshotID != "2" {
		t.Errorf("SnapshotID = %q, want 2", info.SnapshotID)
	}

	// A live file without numRecords: the row count is unknown, not a guess.
	dir = write(t, []string{
		metaLine + "\n" + commit(1715000000000, "Overwrite", 100, add("f0", 100)),
		commit(1715000001000, "Append", 50, `{"add":{"path":"f1","size":100,"dataChange":true}}`),
	})
	info, ok = readDeltaCurrentSnapshot(dir)
	if !ok {
		t.Fatal("not read")
	}
	if info.RowCount != nil {
		t.Errorf("RowCount = %d, want nil when a file has no stats", *info.RowCount)
	}
	if info.FileCount == nil || *info.FileCount != 2 {
		t.Errorf("FileCount = %v, want 2", info.FileCount)
	}
}
