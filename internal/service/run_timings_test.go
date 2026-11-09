package service

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/runtiming"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// writeTimingRecord writes rec where a local run would: the workspace's
// `_progress/<run>/_timing.json` (plus the history mirror).
func writeTimingRecord(t *testing.T, ws string, rec runtiming.Record) {
	t.Helper()
	path := filepath.Join(workspace.LocalWarehouseDir(ws), "_progress", rec.RunID, runtiming.FileName)
	if err := runtiming.Write(&rec, path); err != nil {
		t.Fatalf("write timing record: %v", err)
	}
}

// TestRunTimingsLocalFiltersByPipeline — two pipelines' records in one
// warehouse; RunTimings keeps the named pipeline's, oldest first, and
// limit keeps the most recent.
func TestRunTimingsLocalFiltersByPipeline(t *testing.T) {
	t.Setenv("CLAVESA_RUN_HISTORY_DIR", t.TempDir())
	ws := t.TempDir()
	now := time.Now()
	at := func(h int) string { return now.Add(-time.Duration(h) * time.Hour).Format(time.RFC3339) }
	for _, r := range []runtiming.Record{
		{Schema: 1, RunID: "run-a1", Pipeline: "alpha", Status: "SUCCEEDED", StartedAt: at(50)},
		{Schema: 1, RunID: "run-a2", Pipeline: "alpha", Status: "FAILED", StartedAt: at(3)},
		{Schema: 1, RunID: "run-a3", Pipeline: "alpha", Status: "SUCCEEDED", StartedAt: at(1)},
		{Schema: 1, RunID: "run-b1", Pipeline: "beta", Status: "SUCCEEDED", StartedAt: at(2)},
	} {
		writeTimingRecord(t, ws, r)
	}
	svc := New(ws)
	ctx := context.Background()

	got, err := svc.RunTimings(ctx, "alpha", RunTimingsQuery{})
	if err != nil {
		t.Fatalf("RunTimings: %v", err)
	}
	if ids := recordIDs(got); ids != "run-a1,run-a2,run-a3" {
		t.Errorf("alpha = %s", ids)
	}
	got, _ = svc.RunTimings(ctx, filepath.Join(ws, "beta"), RunTimingsQuery{})
	if ids := recordIDs(got); ids != "run-b1" {
		t.Errorf("beta (absolute dir) = %s", ids)
	}
	got, _ = svc.RunTimings(ctx, "alpha", RunTimingsQuery{Limit: 2})
	if ids := recordIDs(got); ids != "run-a2,run-a3" {
		t.Errorf("alpha limit 2 = %s", ids)
	}
	got, _ = svc.RunTimings(ctx, "alpha", RunTimingsQuery{Since: 24 * time.Hour})
	if ids := recordIDs(got); ids != "run-a2,run-a3" {
		t.Errorf("alpha since 24h = %s", ids)
	}
	got, _ = svc.RunTimings(ctx, "gamma", RunTimingsQuery{})
	if got == nil || len(got) != 0 {
		t.Errorf("unknown pipeline should be an empty, non-nil slice: %#v", got)
	}
}

// TestAllRunTimingsFromHistoryDir reads the cross-workspace history dir
// named by CLAVESA_RUN_HISTORY_DIR, no workspace needed.
func TestAllRunTimingsFromHistoryDir(t *testing.T) {
	hist := t.TempDir()
	t.Setenv("CLAVESA_RUN_HISTORY_DIR", hist)
	now := time.Now()
	for i, r := range []runtiming.Record{
		{Schema: 1, RunID: "r-old", Workspace: "w1", Pipeline: "p", StartedAt: now.Add(-100 * 24 * time.Hour).Format(time.RFC3339)},
		{Schema: 1, RunID: "r-mid", Workspace: "w2", Pipeline: "q", StartedAt: now.Add(-2 * time.Hour).Format(time.RFC3339)},
		{Schema: 1, RunID: "r-new", Workspace: "w1", Pipeline: "p", StartedAt: now.Add(-1 * time.Hour).Format(time.RFC3339)},
	} {
		rec := r
		if err := runtiming.Write(&rec, filepath.Join(t.TempDir(), "_progress", rec.RunID, runtiming.FileName)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	h, err := AllRunTimings(RunTimingsQuery{})
	if err != nil {
		t.Fatalf("AllRunTimings: %v", err)
	}
	if h.HistoryDir != hist || h.GeneratedAt == "" {
		t.Errorf("history_dir = %q, generated_at = %q", h.HistoryDir, h.GeneratedAt)
	}
	if ids := recordIDs(h.Runs); ids != "r-old,r-mid,r-new" {
		t.Errorf("all = %s", ids)
	}
	h, _ = AllRunTimings(RunTimingsQuery{Since: 90 * 24 * time.Hour})
	if ids := recordIDs(h.Runs); ids != "r-mid,r-new" {
		t.Errorf("since 90d = %s", ids)
	}
	h, _ = AllRunTimings(RunTimingsQuery{Limit: 1})
	if ids := recordIDs(h.Runs); ids != "r-new" {
		t.Errorf("limit 1 = %s", ids)
	}
}

// TestRecordsFromNodeRuns — the cloud assembly: one record per run_id,
// nodes summed into phases.nodes, the timing column passed through raw and
// null when empty, status FAILED when any node failed.
func TestRecordsFromNodeRuns(t *testing.T) {
	ms := func(v int64) *int64 { return &v }
	rows := []observability.NodeRun{
		{RunID: "r2", Node: "b", Status: "failed", StartedAt: "2026-10-06T10:00:05.000Z", EndedAt: "2026-10-06T10:00:09.000Z", DurationMs: ms(4000)},
		{RunID: "r2", Node: "a", Status: "ok", StartedAt: "2026-10-06T10:00:00.000Z", EndedAt: "2026-10-06T10:00:05.000Z", DurationMs: ms(5000), Timing: `{"phases":{"write":3000}}`},
		{RunID: "r1", Node: "a", Status: "ok", StartedAt: "2026-10-05T10:00:00Z", EndedAt: "2026-10-05T10:00:02Z", DurationMs: ms(2000)},
	}
	recs := recordsFromNodeRuns(rows, "ws", "/ws", "pipe")
	if ids := recordIDs(recs); ids != "r1,r2" {
		t.Fatalf("runs = %s, want oldest first", ids)
	}
	r2 := recs[1]
	if r2.Status != "FAILED" || r2.Compute != "cloud" || r2.Phases.Nodes != 9000 || r2.TotalMs != 9000 {
		t.Errorf("r2 = status %s compute %s nodes %d total %d", r2.Status, r2.Compute, r2.Phases.Nodes, r2.TotalMs)
	}
	if len(r2.Nodes) != 2 || r2.Nodes[0].Node != "a" {
		t.Fatalf("r2 nodes not in start order: %+v", r2.Nodes)
	}
	if string(r2.Nodes[0].Timing) != `{"phases":{"write":3000}}` {
		t.Errorf("timing passthrough = %s", r2.Nodes[0].Timing)
	}
	body, _ := json.Marshal(r2.Nodes[1])
	var m map[string]any
	_ = json.Unmarshal(body, &m)
	if v, ok := m["timing"]; !ok || v != nil {
		t.Errorf("empty timing should marshal as null: %s", body)
	}
	if recs[0].Status != "SUCCEEDED" || recs[0].StartedAt == "" {
		t.Errorf("r1 = %+v", recs[0])
	}
}

func recordIDs(recs []runtiming.Record) string {
	s := ""
	for i, r := range recs {
		if i > 0 {
			s += ","
		}
		s += r.RunID
	}
	return s
}
