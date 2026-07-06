package observability

import (
	"testing"

	"github.com/vesahyp/clavesa/internal/delta"
)

// i64 is defined in rightsize_test.go (same package).

// Commits below are newest-first, matching delta.ReadCurrent /
// ReadCurrentFromPath output — the order snapshotsResultFromCommits expects.

func TestSnapshotsResultLatestRecordCountFold(t *testing.T) {
	commits := []delta.Commit{
		// v2: MERGE — 5 inserted+updated folded into Added, 3 of those are
		// updates (no row-count change), 1 deleted → net +1.
		{Version: 2, Operation: "MERGE", AddedRecords: i64(5), UpdatedRecords: i64(3), DeletedRecords: i64(1)},
		// v1: plain append of 10.
		{Version: 1, Operation: "WRITE", AddedRecords: i64(10)},
		// v0: overwrite establishing 100 rows.
		{Version: 0, Operation: "CREATE OR REPLACE TABLE AS SELECT", AddedRecords: i64(100), Replaces: true},
	}
	res := snapshotsResultFromCommits(commits, 0, nil)
	if res.LatestRecordCount == nil || *res.LatestRecordCount != 111 {
		t.Fatalf("LatestRecordCount = %v, want 111", res.LatestRecordCount)
	}
	if res.LatestRecordCountApproximate {
		t.Error("Approximate = true, want false (window anchors at version 0)")
	}
	if res.Total != 3 {
		t.Errorf("Total = %d, want 3", res.Total)
	}
	if res.Truncated {
		t.Error("Truncated = true, want false with limit 0 (no truncation)")
	}
}

// TestSnapshotsResultAuthoritativeRowCountWins: the row count delta derives
// from snapshot state (checkpoint stats + replay) beats both the fold and
// the writer-stamped TotalRecords — it is the GH #66 fix and stays exact
// regardless of window truncation.
func TestSnapshotsResultAuthoritativeRowCountWins(t *testing.T) {
	commits := []delta.Commit{
		// Fold would say 43; writer stamp says 42; snapshot state says 5000.
		{Version: 1, Operation: "WRITE", AddedRecords: i64(1), TotalRecords: i64(42)},
		{Version: 0, Operation: "WRITE", AddedRecords: i64(42), Replaces: true},
	}
	res := snapshotsResultFromCommits(commits, 0, i64(5000))
	if res.LatestRecordCount == nil || *res.LatestRecordCount != 5000 {
		t.Fatalf("LatestRecordCount = %v, want 5000 (authoritative row count wins)", res.LatestRecordCount)
	}
	if res.LatestRecordCountApproximate {
		t.Error("Approximate = true, want false for the authoritative count")
	}
}

// TestSnapshotsResultFoldApproximateWhenUnanchored: a MERGE/append table
// whose surviving window starts mid-history (no version 0, no Replaces)
// folds from an arbitrary zero — the value must be flagged approximate,
// and Total must still report the true lifetime commit count derived from
// the newest version (GH #66).
func TestSnapshotsResultFoldApproximateWhenUnanchored(t *testing.T) {
	commits := []delta.Commit{
		{Version: 250, Operation: "MERGE", AddedRecords: i64(5), UpdatedRecords: i64(3)},
		{Version: 249, Operation: "WRITE", AddedRecords: i64(10)},
	}
	res := snapshotsResultFromCommits(commits, 0, nil)
	if res.LatestRecordCount == nil || *res.LatestRecordCount != 12 {
		t.Fatalf("LatestRecordCount = %v, want 12 (fold over the visible window)", res.LatestRecordCount)
	}
	if !res.LatestRecordCountApproximate {
		t.Error("Approximate = false, want true (window starts mid-history)")
	}
	if res.Total != 251 {
		t.Errorf("Total = %d, want 251 (newest version 250 + 1)", res.Total)
	}
}

// TestSnapshotsResultFoldExactWhenReplacesInWindow: an overwrite inside the
// window resets the fold to a known-complete state, so the fold is exact
// even though the window doesn't reach version 0.
func TestSnapshotsResultFoldExactWhenReplacesInWindow(t *testing.T) {
	commits := []delta.Commit{
		{Version: 301, Operation: "WRITE", AddedRecords: i64(2)},
		{Version: 300, Operation: "WRITE", AddedRecords: i64(7), Replaces: true},
	}
	res := snapshotsResultFromCommits(commits, 0, nil)
	if res.LatestRecordCount == nil || *res.LatestRecordCount != 9 {
		t.Fatalf("LatestRecordCount = %v, want 9", res.LatestRecordCount)
	}
	if res.LatestRecordCountApproximate {
		t.Error("Approximate = true, want false (Replaces anchors the fold)")
	}
	if res.Total != 302 {
		t.Errorf("Total = %d, want 302", res.Total)
	}
}

func TestSnapshotsResultReplacesResetsRunningSum(t *testing.T) {
	commits := []delta.Commit{
		{Version: 1, Operation: "WRITE", AddedRecords: i64(7), Replaces: true},
		{Version: 0, Operation: "WRITE", AddedRecords: i64(1000)},
	}
	res := snapshotsResultFromCommits(commits, 0, nil)
	if res.LatestRecordCount == nil || *res.LatestRecordCount != 7 {
		t.Fatalf("LatestRecordCount = %v, want 7 (Replaces resets the sum)", res.LatestRecordCount)
	}
}

func TestSnapshotsResultNegativeSumClampsToZero(t *testing.T) {
	// Deletes exceeding the visible adds (the GH #66 truncated-window shape:
	// the fold starts mid-history) clamp at zero rather than going negative.
	commits := []delta.Commit{
		{Version: 1, Operation: "DELETE", DeletedRecords: i64(50)},
		{Version: 0, Operation: "WRITE", AddedRecords: i64(10)},
	}
	res := snapshotsResultFromCommits(commits, 0, nil)
	if res.LatestRecordCount == nil || *res.LatestRecordCount != 0 {
		t.Fatalf("LatestRecordCount = %v, want 0 (clamped)", res.LatestRecordCount)
	}
}

func TestSnapshotsResultPrefersNewestTotalRecords(t *testing.T) {
	commits := []delta.Commit{
		{Version: 1, Operation: "WRITE", AddedRecords: i64(1), TotalRecords: i64(42)},
		{Version: 0, Operation: "WRITE", AddedRecords: i64(1)},
	}
	res := snapshotsResultFromCommits(commits, 0, nil)
	if res.LatestRecordCount == nil || *res.LatestRecordCount != 42 {
		t.Fatalf("LatestRecordCount = %v, want 42 (newest commit's TotalRecords wins over the fold)", res.LatestRecordCount)
	}
}

func TestSnapshotsResultEmptyCommits(t *testing.T) {
	res := snapshotsResultFromCommits(nil, 0, nil)
	if res.LatestRecordCount != nil {
		t.Errorf("LatestRecordCount = %v, want nil for empty history", res.LatestRecordCount)
	}
	if len(res.Snapshots) != 0 || res.Total != 0 || res.Truncated {
		t.Errorf("empty history: got %+v, want zero-value result", res)
	}
}

func TestSnapshotsResultLimitTruncationAndProjection(t *testing.T) {
	commits := []delta.Commit{
		{Version: 2, TimestampMs: 1700000002000, Operation: "MERGE", AddedRecords: i64(1),
			UserMetadata: `{"clavesa.trigger":"schedule","clavesa.run-id":"run-2"}`},
		{Version: 1, TimestampMs: 1700000001000, Operation: "WRITE", AddedRecords: i64(2)},
		{Version: 0, TimestampMs: 1700000000000, Operation: "WRITE", AddedRecords: i64(3), Replaces: true},
	}
	res := snapshotsResultFromCommits(commits, 2, nil)
	if len(res.Snapshots) != 2 {
		t.Fatalf("len(Snapshots) = %d, want 2", len(res.Snapshots))
	}
	if !res.Truncated {
		t.Error("Truncated = false, want true")
	}
	if res.Total != 3 {
		t.Errorf("Total = %d, want 3 (pre-truncation count)", res.Total)
	}
	// LatestRecordCount still folds the FULL list, not the truncated one.
	if res.LatestRecordCount == nil || *res.LatestRecordCount != 6 {
		t.Errorf("LatestRecordCount = %v, want 6", res.LatestRecordCount)
	}
	top := res.Snapshots[0]
	if top.SnapshotID != "2" || top.ParentID != "1" {
		t.Errorf("SnapshotID/ParentID = %q/%q, want 2/1", top.SnapshotID, top.ParentID)
	}
	if top.Trigger != "schedule" || top.WriterRunID != "run-2" {
		t.Errorf("provenance = %q/%q, want schedule/run-2", top.Trigger, top.WriterRunID)
	}
	if top.CommittedAt == "" {
		t.Error("CommittedAt should render from TimestampMs")
	}
	// v0 row (if present) would have no ParentID; check via the second row.
	if res.Snapshots[1].SnapshotID != "1" || res.Snapshots[1].ParentID != "0" {
		t.Errorf("second row = %q/%q, want 1/0", res.Snapshots[1].SnapshotID, res.Snapshots[1].ParentID)
	}
}

func TestLegacyDBFallback(t *testing.T) {
	cases := []struct{ db, pipeline, want string }{
		{"", "demo", "clavesa_demo"},
		{"", "my-pipe", "clavesa_my-pipe"},
		{"cat__pipelines", "demo", "cat__pipelines"},
	}
	for _, c := range cases {
		if got := legacyDBFallback(c.db, c.pipeline); got != c.want {
			t.Errorf("legacyDBFallback(%q, %q) = %q, want %q", c.db, c.pipeline, got, c.want)
		}
	}
}
