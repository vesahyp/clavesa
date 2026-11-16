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

// summaryRunsWorkspace is a local workspace whose one pipeline, orders,
// has the given runs (start, status), each with one node row.
func summaryRunsWorkspace(t *testing.T, runs []struct {
	id     string
	start  time.Time
	status string
}) *WorkspaceSummary {
	t.Helper()
	ws := summaryWorkspace(t, "rate(1 day)")
	dropSLA(t, ws)
	store := observability.NewFileProgressStore(workspace.LocalWarehouseDir(ws))
	ctx := context.Background()
	for _, r := range runs {
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
	return sum
}

// TestSummaryRecoveredFailures: failed runs that a later run of the same
// pipeline followed with a success leave the status "ok" and show as a
// note (v2.27.4; before, they kept the card amber for 14 days). The CLI
// and the hosted UI read the same summary, so they say the same.
func TestSummaryRecoveredFailures(t *testing.T) {
	sum := summaryRunsWorkspace(t, []struct {
		id     string
		start  time.Time
		status string
	}{
		{"old-fail", summaryNow.Add(-20 * 24 * time.Hour), "FAILED"}, // outside 14 days
		{"fail-a", summaryNow.Add(-5 * 24 * time.Hour), "FAILED"},
		{"fail-b", summaryNow.Add(-3*24*time.Hour - 50*time.Minute), "FAILED"}, // 12.11 11:10 UTC, 13:10 Helsinki
		{"ok", summaryNow.Add(-2 * time.Hour), "SUCCEEDED"},
	})
	want := "2 failed runs in 14 days, last 12.11 13:10, later runs succeeded"
	p := sum.Pipelines[0]
	if p.Status != SummaryOK || len(p.StatusReasons) != 0 || !hasReason(p.Notes, want) {
		t.Errorf("pipeline = %q %v notes %v, want ok with note %q", p.Status, p.StatusReasons, p.Notes, want)
	}
	if sum.Status != SummaryOK || len(sum.StatusReasons) != 0 || !hasReason(sum.Notes, "orders: "+want) {
		t.Errorf("workspace = %q %v notes %v, want ok with note %q", sum.Status, sum.StatusReasons, sum.Notes, "orders: "+want)
	}
}

// TestSummaryUnrecoveredFailures: when no success follows the failures,
// the pipeline fails as before and the count is a reason, not a note.
func TestSummaryUnrecoveredFailures(t *testing.T) {
	sum := summaryRunsWorkspace(t, []struct {
		id     string
		start  time.Time
		status string
	}{
		{"ok", summaryNow.Add(-5 * 24 * time.Hour), "SUCCEEDED"},
		{"fail-a", summaryNow.Add(-4 * time.Hour), "FAILED"},
		{"fail-b", summaryNow.Add(-2 * time.Hour), "FAILED"},
	})
	p := sum.Pipelines[0]
	if p.Status != SummaryFail || !hasReason(p.StatusReasons, "last run failed") || !hasReason(p.StatusReasons, "2 failed runs in 14 days") {
		t.Errorf("pipeline = %q %v, want fail with the failure count", p.Status, p.StatusReasons)
	}
	if len(p.Notes) != 0 || sum.Status != SummaryFail {
		t.Errorf("notes %v, workspace %q, want no note and fail", p.Notes, sum.Status)
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
