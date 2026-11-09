package cli

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vesahyp/clavesa/internal/runtiming"
)

// TestPipelineTimingsAllTable renders `pipeline timings --all` from the
// history dir: no workspace, one row per run oldest first, the
// WORKSPACE/PIPELINE columns present, and "-" node cells for a record
// without node timing.
func TestPipelineTimingsAllTable(t *testing.T) {
	hist := t.TempDir()
	t.Setenv("CLAVESA_RUN_HISTORY_DIR", hist)
	now := time.Now()
	for _, r := range []runtiming.Record{
		{Schema: 1, RunID: "aaaa1111", Workspace: "shop", Pipeline: "orders", Status: "SUCCEEDED",
			StartedAt: now.Add(-2 * time.Hour).Format(time.RFC3339), TotalMs: 12_000,
			Phases: runtiming.Phases{Startup: 200, Metastore: 1500, Boot: 800},
			Nodes:  []runtiming.Node{{Node: "clean", Timing: json.RawMessage(`{"phases":{"spark_session":900,"inputs":1000,"transform":300,"write":4000,"stats":500}}`)}}},
		{Schema: 1, RunID: "bbbb2222", Workspace: "web", Pipeline: "traffic", Status: "FAILED",
			StartedAt: now.Add(-1 * time.Hour).Format(time.RFC3339), TotalMs: 3_000,
			Phases: runtiming.Phases{Startup: 100, Metastore: 400},
			Nodes:  []runtiming.Node{{Node: "old"}}},
	} {
		rec := r
		if err := runtiming.Write(&rec, filepath.Join(t.TempDir(), "_progress", rec.RunID, runtiming.FileName)); err != nil {
			t.Fatal(err)
		}
	}

	cmd := newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"pipeline", "timings", "--all", "--workspace", t.TempDir()})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("pipeline timings --all: %v\n%s", err, out.String())
	}
	lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("want header + 2 rows, got:\n%s", out.String())
	}
	header := strings.Fields(lines[0])
	wantHeader := []string{"STARTED", "WORKSPACE", "PIPELINE", "STATUS", "TOTAL", "STARTUP", "METASTORE", "SPARK", "READS", "TRANSFORM", "WRITES", "STATS", "OTHER"}
	if strings.Join(header, " ") != strings.Join(wantHeader, " ") {
		t.Errorf("header = %v", header)
	}
	// STARTED is "date time" (two fields), so the row has one more field.
	first := strings.Fields(lines[1])
	if first[2] != "shop" || first[3] != "orders" || first[4] != "SUCCEEDED" || first[5] != "12.0s" ||
		first[6] != "1.0s" || first[7] != "1.5s" || first[8] != "0.9s" || first[9] != "1.0s" ||
		first[10] != "0.3s" || first[11] != "4.0s" || first[12] != "0.5s" || first[13] != "2.8s" {
		t.Errorf("first row = %v", first)
	}
	second := strings.Fields(lines[2])
	if second[2] != "web" || second[4] != "FAILED" || second[8] != "-" || second[12] != "-" || second[13] != "2.5s" {
		t.Errorf("second row = %v", second)
	}

	// --json prints the History object.
	cmd = newRootCmd()
	out.Reset()
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"pipeline", "timings", "--all", "--json", "--runs", "1"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("--json: %v", err)
	}
	var h runtiming.History
	if err := json.Unmarshal(out.Bytes(), &h); err != nil {
		t.Fatalf("decode: %v\n%s", err, out.String())
	}
	if h.HistoryDir != hist || len(h.Runs) != 1 || h.Runs[0].RunID != "bbbb2222" {
		t.Errorf("history = %+v", h)
	}

	cmd = newRootCmd()
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"pipeline", "timings", "--all", "--since", "soon"})
	if err := cmd.Execute(); err == nil {
		t.Error("bad --since should fail")
	}
}
