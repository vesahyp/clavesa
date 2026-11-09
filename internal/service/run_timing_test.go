package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vesahyp/clavesa/internal/procstart"
	"github.com/vesahyp/clavesa/internal/runtiming"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// TestMain points the run history mirror at a temp dir for the whole
// package, so no test that drives a local run writes into the developer's
// real ~/.local/state/clavesa/runs.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "clavesa-run-history-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Setenv("CLAVESA_RUN_HISTORY_DIR", dir)
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// initTimingTestWorkspace scaffolds a manifest-less workspace (prepareRun
// stays off EnsureLocalRunnerImage) with one SQL transform, and stubs the
// rollup container. Returns the service, the pipeline dir and the node id.
func initTimingTestWorkspace(t *testing.T) (*Service, string, string) {
	t.Helper()
	svc := New(t.TempDir())
	if _, err := svc.CreatePipeline("demo", ""); err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	g, err := svc.AddNode("demo", "transform", "events")
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := g.Nodes[0].ID
	if _, err := svc.UpdateNode("demo", id, map[string]interface{}{"sql": "SELECT 1"}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	svc.recordRun = func(context.Context, string, string, *runOutcome, []string, func(context.Context, string, string) (string, string)) {
	}
	return svc, "demo", id
}

const fakeNodeTiming = `{"phases": {"spark_session": 0, "catalog": 12, "inputs": 840, "transform": 95, "write": 20450, "glue_sync": 3, "stats": 0, "table_state": 310, "system": 1200},
 "inputs": {"src_trips": 840}, "outputs": {"default": 20450},
 "spark": {"write": {"num_stages": 4, "num_tasks": 40, "input_bytes": 1234, "input_records": 3000000, "shuffle_read_bytes": 0, "shuffle_write_bytes": 0, "memory_spilled_bytes": 0, "disk_spilled_bytes": 0, "executor_run_time_ms": 18000}}}`

// fakeBundle returns a runBundle stub that writes the terminal node marker
// the runner would write and answers with a runner response. withTiming
// false mimics an old runner image (no per-node timing, no runner block).
func fakeBundle(t *testing.T, node string, withTiming bool, gotTrigger *string) func(context.Context, *runPrep, []bundleTransformConfig) (*bundleRun, error) {
	return func(ctx context.Context, prep *runPrep, bundle []bundleTransformConfig) (*bundleRun, error) {
		*gotTrigger = prep.outcome.trigger
		marker := (`{"status":"succeeded","started_ms":1000,"ended_ms":206000,"output_rows":40301,"updated_ms":206000}`)
		if err := prep.store.WriteKey(ctx, fmt.Sprintf("_progress/%s/%s.json", prep.runID, node), []byte(marker)); err != nil {
			t.Fatalf("write node marker: %v", err)
		}
		entry := map[string]any{"node": node, "status": "ok", "output_rows": float64(40301)}
		resp := map[string]any{"status": "ok", "failed_node": nil}
		if withTiming {
			var timing map[string]any
			if err := json.Unmarshal([]byte(fakeNodeTiming), &timing); err != nil {
				t.Fatal(err)
			}
			entry["timing"] = timing
			resp["runner"] = map[string]any{"boot_ms": float64(3000), "total_ms": float64(210000)}
		}
		resp["transforms"] = []any{entry}
		return &bundleRun{
			Statuses:    []NodeRunStatus{{NodeID: node, Type: "transform", Status: "ok"}},
			Response:    resp,
			ContainerMs: 4200,
			BundleMs:    215000,
		}, nil
	}
}

func timingPath(svc *Service, runID string) string {
	return filepath.Join(workspace.LocalWarehouseDir(svc.workspace), "_progress", runID, runtiming.FileName)
}

// TestRunTimingRecord — phase assembly from a fake bundle response: the
// Go-side spans, runner boot, node span, per-node timing, and the record
// written byte-identical to both the progress tree and the history dir.
func TestRunTimingRecord(t *testing.T) {
	history := t.TempDir()
	t.Setenv("CLAVESA_RUN_HISTORY_DIR", history)
	svc, dir, node := initTimingTestWorkspace(t)
	var gotTrigger string
	svc.runBundle = fakeBundle(t, node, true, &gotTrigger)

	res, err := svc.RunPipelineWithOpts(context.Background(), dir, RunOpts{Trigger: "cron"})
	if err != nil {
		t.Fatalf("RunPipelineWithOpts: %v", err)
	}
	rec := res.Timing
	if rec == nil {
		t.Fatal("RunResult.Timing is nil")
	}
	if gotTrigger != "cron" {
		t.Errorf("bundle saw trigger %q, want cron", gotTrigger)
	}
	if rec.Schema != 1 || rec.RunID != res.RunID || rec.Pipeline != "demo" || rec.Status != "SUCCEEDED" || rec.Trigger != "cron" || rec.Compute != "local" {
		t.Errorf("record header = %+v", rec)
	}
	if rec.Workspace != filepath.Base(svc.workspace) {
		t.Errorf("workspace = %q, want the dir name for a manifest-less workspace", rec.Workspace)
	}
	p := rec.Phases
	if p.Container != 4200 || p.Bundle != 215000 || p.Boot != 3000 || p.Nodes != 205000 {
		t.Errorf("phases = %+v, want container 4200, bundle 215000, boot 3000, nodes 205000", p)
	}
	if want := procstart.At.Format(time.RFC3339); rec.StartedAt != want {
		t.Errorf("started_at = %q, want process start %q on the sync path", rec.StartedAt, want)
	}
	if rec.TotalMs < p.Startup {
		t.Errorf("total %d < startup %d", rec.TotalMs, p.Startup)
	}
	if len(rec.Nodes) != 1 {
		t.Fatalf("nodes = %+v, want 1", rec.Nodes)
	}
	n := rec.Nodes[0]
	if n.Node != node || n.Status != "ok" || n.StartedMs != 1000 || n.EndedMs != 206000 || n.DurationMs != 205000 {
		t.Errorf("node = %+v", n)
	}
	if n.OutputRows == nil || *n.OutputRows != 40301 {
		t.Errorf("output_rows = %v, want 40301", n.OutputRows)
	}
	pt := n.ParsedTiming()
	if pt == nil || pt.Phases["write"] != 20450 || pt.Spark["write"].NumTasks != 40 {
		t.Errorf("parsed timing = %+v", pt)
	}

	// Run marker carries the trigger.
	raw, err := os.ReadFile(filepath.Join(workspace.LocalWarehouseDir(svc.workspace), "_progress", res.RunID, "_run.json"))
	if err != nil {
		t.Fatalf("read _run.json: %v", err)
	}
	var marker struct{ Trigger string }
	_ = json.Unmarshal(raw, &marker)
	if marker.Trigger != "cron" {
		t.Errorf("_run.json trigger = %q, want cron", marker.Trigger)
	}

	// Both copies exist and are identical.
	local, err := os.ReadFile(timingPath(svc, res.RunID))
	if err != nil {
		t.Fatalf("read progress-tree record: %v", err)
	}
	mirror, err := os.ReadFile(filepath.Join(history, runtiming.HistoryFileName(rec)))
	if err != nil {
		t.Fatalf("read history record: %v", err)
	}
	if !bytes.Equal(local, mirror) {
		t.Error("history copy differs from the progress-tree record")
	}
	recs, err := runtiming.ReadDir(history)
	if err != nil || len(recs) != 1 || recs[0].RunID != res.RunID {
		t.Errorf("ReadDir(history) = %v, %v", recs, err)
	}
}

// TestRunTimingOldRunner — a runner image without timing still yields a
// record: node timing null, boot 0, the node span from the marker.
func TestRunTimingOldRunner(t *testing.T) {
	t.Setenv("CLAVESA_RUN_HISTORY_DIR", t.TempDir())
	svc, dir, node := initTimingTestWorkspace(t)
	var gotTrigger string
	svc.runBundle = fakeBundle(t, node, false, &gotTrigger)

	res, err := svc.RunPipelineWithOpts(context.Background(), dir, RunOpts{})
	if err != nil {
		t.Fatalf("RunPipelineWithOpts: %v", err)
	}
	if gotTrigger != "manual" {
		t.Errorf("default trigger = %q, want manual", gotTrigger)
	}
	data, err := os.ReadFile(timingPath(svc, res.RunID))
	if err != nil {
		t.Fatalf("read record: %v", err)
	}
	var rec runtiming.Record
	if err := json.Unmarshal(data, &rec); err != nil {
		t.Fatalf("parse record: %v", err)
	}
	if rec.Phases.Boot != 0 || rec.Phases.Nodes != 205000 {
		t.Errorf("phases = %+v", rec.Phases)
	}
	if len(rec.Nodes) != 1 || rec.Nodes[0].ParsedTiming() != nil || string(rec.Nodes[0].Timing) != "null" {
		t.Errorf("nodes = %+v, want one node with timing null", rec.Nodes)
	}
}

// TestStartRunWritesTiming — the UI's async path writes the same record,
// with startup 0 (a long-lived server's process start says nothing).
func TestStartRunWritesTiming(t *testing.T) {
	t.Setenv("CLAVESA_RUN_HISTORY_DIR", t.TempDir())
	svc, dir, node := initTimingTestWorkspace(t)
	var gotTrigger string
	svc.runBundle = fakeBundle(t, node, true, &gotTrigger)

	runID, err := svc.StartRunWithOpts(dir, RunOpts{})
	if err != nil {
		t.Fatalf("StartRunWithOpts: %v", err)
	}
	path := timingPath(svc, runID)
	deadline := time.Now().Add(10 * time.Second)
	var data []byte
	for {
		if data, err = os.ReadFile(path); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timing record never written at %s", path)
		}
		time.Sleep(20 * time.Millisecond)
	}
	var rec runtiming.Record
	if err := json.Unmarshal(data, &rec); err != nil {
		t.Fatalf("parse record: %v", err)
	}
	if rec.Phases.Startup != 0 || rec.Phases.Boot != 3000 || rec.Status != "SUCCEEDED" {
		t.Errorf("record = %+v", rec)
	}
}

// TestTriggerFromEnv — CLAVESA_TRIGGER labels the run; unset or blank
// falls back to "manual".
func TestTriggerFromEnv(t *testing.T) {
	t.Setenv("CLAVESA_TRIGGER", "")
	if got := TriggerFromEnv(); got != "manual" {
		t.Errorf("unset: %q, want manual", got)
	}
	t.Setenv("CLAVESA_TRIGGER", "cron")
	if got := TriggerFromEnv(); got != "cron" {
		t.Errorf("set: %q, want cron", got)
	}
}
