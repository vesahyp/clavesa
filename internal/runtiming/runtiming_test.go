package runtiming

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHistoryDirRules(t *testing.T) {
	t.Setenv("CLAVESA_RUN_HISTORY_DIR", "/x/hist")
	t.Setenv("XDG_STATE_HOME", "/x/state")
	if got := HistoryDir(); got != "/x/hist" {
		t.Errorf("override: %q", got)
	}
	t.Setenv("CLAVESA_RUN_HISTORY_DIR", "")
	if got := HistoryDir(); got != "/x/state/clavesa/runs" {
		t.Errorf("xdg: %q", got)
	}
	t.Setenv("XDG_STATE_HOME", "")
	t.Setenv("HOME", "/x/home")
	if got := HistoryDir(); got != "/x/home/.local/state/clavesa/runs" {
		t.Errorf("home: %q", got)
	}
}

func TestHistoryFileName(t *testing.T) {
	rec := &Record{RunID: "abcdef0123456789", Workspace: "my ws", Pipeline: "traffic_v2", StartedAt: "2026-10-06T06:00:01+03:00"}
	if got, want := HistoryFileName(rec), "20261006T060001_my-ws_traffic-v2_abcdef01.json"; got != want {
		t.Errorf("HistoryFileName = %q, want %q", got, want)
	}
}

// TestReadDirOrder — records come back oldest first by started_at,
// regardless of file name order or offset; junk files are skipped and a
// missing dir is empty, not an error.
func TestReadDirOrder(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CLAVESA_RUN_HISTORY_DIR", dir)
	recs := []*Record{
		{Schema: 1, RunID: "late", Workspace: "a", Pipeline: "p", StartedAt: "2026-10-06T09:00:00+03:00"},
		{Schema: 1, RunID: "early", Workspace: "z", Pipeline: "p", StartedAt: "2026-10-06T05:00:00Z"}, // 08:00+03:00
		{Schema: 1, RunID: "mid", Workspace: "m", Pipeline: "p", StartedAt: "2026-10-06T08:30:00+03:00"},
	}
	for _, r := range recs {
		if err := Write(r, filepath.Join(t.TempDir(), "_progress", r.RunID, FileName)); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "junk.json"), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var ids []string
	for _, r := range got {
		ids = append(ids, r.RunID)
	}
	if len(ids) != 3 || ids[0] != "early" || ids[1] != "mid" || ids[2] != "late" {
		t.Errorf("order = %v, want [early mid late]", ids)
	}
	if none, err := ReadDir(filepath.Join(dir, "missing")); err != nil || len(none) != 0 {
		t.Errorf("missing dir: %v, %v", none, err)
	}
}

func TestParsedTimingNull(t *testing.T) {
	if (Node{Timing: []byte("null")}).ParsedTiming() != nil {
		t.Error("null timing should parse to nil")
	}
	if (Node{}).ParsedTiming() != nil {
		t.Error("absent timing should parse to nil")
	}
}
