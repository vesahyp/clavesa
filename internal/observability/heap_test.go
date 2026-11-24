package observability

import (
	"strings"
	"testing"
)

// TestRightsizeFromHeap: rows with heap columns are advised from the heap
// in use after GC, not from peak RSS. The web-traffic gold shape: a 3008 MB
// Lambda, a 1536 MB heap, about 400 MB in use after GC.
func TestRightsizeFromHeap(t *testing.T) {
	var rows []NodeRun
	for i := 0; i < 10; i++ {
		rows = append(rows, NodeRun{
			Node: "fact_events", MemoryMB: i64(3008), PeakRSSMB: i64(2900),
			HeapOldAfterGCMB: i64(300 + int64(i)*10), HeapCommittedMB: i64(1468), HeapMaxMB: i64(1536),
		})
	}
	got := Rightsize(rows)
	if len(got) != 1 {
		t.Fatalf("got %d rows", len(got))
	}
	nr := got[0]
	// p95 of 300..390 is 390; heap 780 rounds up to 832; + 1472 = 2304.
	if nr.P95HeapAfterGCMB == nil || *nr.P95HeapAfterGCMB != 390 {
		t.Errorf("P95HeapAfterGCMB = %v, want 390", nr.P95HeapAfterGCMB)
	}
	if nr.RecommendedHeapMB == nil || *nr.RecommendedHeapMB != 832 {
		t.Errorf("RecommendedHeapMB = %v, want 832", nr.RecommendedHeapMB)
	}
	if nr.RecommendedMB == nil || *nr.RecommendedMB != 2304 {
		t.Errorf("RecommendedMB = %v, want 2304 (peak RSS 2900 must not drive it)", nr.RecommendedMB)
	}
	if nr.HeapMaxMB == nil || *nr.HeapMaxMB != 1536 {
		t.Errorf("HeapMaxMB = %v, want 1536", nr.HeapMaxMB)
	}
	if nr.Samples != 10 || nr.Confidence != "high" {
		t.Errorf("Samples/Confidence = %d/%s, want 10/high", nr.Samples, nr.Confidence)
	}
	if !strings.Contains(nr.Reason, "p95 heap in use after GC 390 MB of a 1536 MB heap") || !strings.Contains(nr.Reason, "lower to 2304 MB") {
		t.Errorf("Reason = %q", nr.Reason)
	}
}

// TestRightsizeHeapFloorAndFallback: a tiny heap need still gets the 512 MB
// floor; a node with no heap columns keeps the peak-RSS advice.
func TestRightsizeHeapFloorAndFallback(t *testing.T) {
	got := Rightsize([]NodeRun{
		{Node: "a", MemoryMB: i64(3008), HeapOldAfterGCMB: i64(40), HeapMaxMB: i64(1024)},
		{Node: "b", MemoryMB: i64(1024), PeakRSSMB: i64(600)},
	})
	if *got[0].RecommendedHeapMB != 512 || *got[0].RecommendedMB != 512+1472 {
		t.Errorf("a: heap %v mem %v, want 512 / 1984", *got[0].RecommendedHeapMB, *got[0].RecommendedMB)
	}
	if got[1].RecommendedHeapMB != nil || got[1].P95HeapAfterGCMB != nil {
		t.Errorf("b: no heap rows, but heap advice %v / %v", got[1].RecommendedHeapMB, got[1].P95HeapAfterGCMB)
	}
	if got[1].RecommendedMB == nil || *got[1].RecommendedMB != recommendMemoryMB(600, 0) {
		t.Errorf("b: RecommendedMB = %v, want the peak-RSS advice", got[1].RecommendedMB)
	}
}

func TestWithoutHeapColumns(t *testing.T) {
	sql := "SELECT\n  max_task_duration_ms,\n  heap_old_after_gc_mb,\n  heap_committed_mb,\n  heap_max_mb\nFROM x.node_runs"
	got := withoutHeapColumns(sql)
	for _, want := range []string{
		"CAST(NULL AS BIGINT) AS heap_old_after_gc_mb,",
		"CAST(NULL AS BIGINT) AS heap_committed_mb,",
		"CAST(NULL AS BIGINT) AS heap_max_mb\nFROM",
		"\n  max_task_duration_ms,",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
	spark := errString("[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `heap_max_mb` cannot be resolved.")
	if !isMissingHeapColumnErr(spark) {
		t.Error("Spark's unresolved heap column must count as missing")
	}
	if isMissingHeapColumnErr(errString("[UNRESOLVED_COLUMN] `status` cannot be resolved")) {
		t.Error("another unresolved column must not count")
	}
}

type errString string

func (e errString) Error() string { return string(e) }
