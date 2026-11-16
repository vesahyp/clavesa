package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// TestSummaryFailedRunRule: a failed run in the last 14 days makes a
// pipeline and its workspace "warn" with a plain reason, even when the
// newest run succeeded. The CLI and the hosted UI read the same summary,
// so they give the same verdict and the same words.
func TestSummaryFailedRunRule(t *testing.T) {
	ws := summaryWorkspace(t, "rate(1 day)")
	store := observability.NewFileProgressStore(workspace.LocalWarehouseDir(ws))
	ctx := context.Background()
	for _, r := range []struct {
		id     string
		start  time.Time
		status string
	}{
		{"old-fail", summaryNow.Add(-20 * 24 * time.Hour), "FAILED"}, // outside 14 days
		{"fail-a", summaryNow.Add(-5 * 24 * time.Hour), "FAILED"},
		{"fail-b", summaryNow.Add(-3*24*time.Hour - 50*time.Minute), "FAILED"}, // 12.11 11:10 UTC, 13:10 Helsinki
		{"ok", summaryNow.Add(-2 * time.Hour), "SUCCEEDED"},
	} {
		end := r.start.Add(time.Minute)
		if err := observability.WriteRunMarker(ctx, store, r.id, observability.RunMarker{
			Status: r.status, Trigger: "manual", StartedMs: ms(r.start), EndedMs: ms(end), Pipeline: "orders",
		}); err != nil {
			t.Fatal(err)
		}
		node := fmt.Sprintf(`{"status":"succeeded","started_ms":%d,"ended_ms":%d,"output_rows":1}`, r.start.UnixMilli(), end.UnixMilli())
		if err := os.WriteFile(filepath.Join(workspace.LocalWarehouseDir(ws), "_progress", r.id, "xform.json"), []byte(node), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	svc := New(ws).WithResolver(observability.NewResolver(ws, nil, observability.NewLocalProvider(ws)))
	sum, err := svc.WorkspaceSummary(ctx, WorkspaceSummaryQuery{Now: summaryNow})
	if err != nil {
		t.Fatalf("WorkspaceSummary: %v", err)
	}
	want := "2 failed runs in 14 days, last 12.11 13:10"
	p := sum.Pipelines[0]
	if p.Status != SummaryWarn || !hasReason(p.StatusReasons, want) {
		t.Errorf("pipeline = %q %v, want warn with %q", p.Status, p.StatusReasons, want)
	}
	if sum.Status != SummaryWarn || !hasReason(sum.StatusReasons, "orders: "+want) {
		t.Errorf("workspace = %q %v, want warn with %q", sum.Status, sum.StatusReasons, "orders: "+want)
	}
}

func TestRecentFailuresAndOverdueReason(t *testing.T) {
	iso := func(d time.Duration) string { return summaryNow.Add(d).Format(time.RFC3339) }
	runs := []SummaryRun{
		{Result: "failed", StartedAt: iso(-time.Hour)},
		{Result: "succeeded", StartedAt: iso(-2 * time.Hour)},
	}
	// The only failure is the last run: "last run failed" says it already.
	if got := recentFailures(runs, summaryNow, true); got != "" {
		t.Errorf("single last failure = %q, want none", got)
	}
	if got := recentFailures(runs, summaryNow, false); got != "1 failed run in 14 days, last 15.11 13:00" {
		t.Errorf("one failure = %q", got)
	}
	if got := recentFailures([]SummaryRun{{Result: "succeeded", StartedAt: iso(-time.Hour)}}, summaryNow, false); got != "" {
		t.Errorf("no failure = %q", got)
	}

	last := &SummaryRun{StartedAt: "2026-11-09T05:16:00Z"}
	if got := overdueReason(last, 86400); got != "no run since 9.11 07:16, expected daily" {
		t.Errorf("overdue = %q", got)
	}
	if got := overdueReason(nil, 3600); got != "never run, expected hourly" {
		t.Errorf("never run = %q", got)
	}
	for sec, want := range map[int64]string{900: "every 15 min", 21600: "every 6 h", 172800: "every 2 days"} {
		if got := everyText(sec); got != want {
			t.Errorf("everyText(%d) = %q, want %q", sec, got, want)
		}
	}
}

func hasReason(xs []string, want string) bool {
	for _, x := range xs {
		if strings.Contains(x, want) {
			return true
		}
	}
	return false
}
