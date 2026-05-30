package service

import (
	"context"
	"testing"

	"github.com/vesahyp/clavesa/internal/observability"
)

func ri64(v int64) *int64 { return &v }

// TestServiceRightsize exercises the service wiring: it must force the
// metrics-bearing scan (IncludeMetrics) and hand the rows to the pure
// aggregator. The arithmetic itself is pinned in the observability-package
// tests; this only verifies the plumbing.
func TestServiceRightsize(t *testing.T) {
	f := &fakeProvider{
		nodeRuns: []observability.NodeRun{
			{Node: "trips", MemoryMB: ri64(2048), PeakRSSMB: ri64(800)},
			{Node: "trips", MemoryMB: ri64(2048), PeakRSSMB: ri64(810)},
		},
	}
	svc := dashService(t, f)

	out, err := svc.Rightsize(context.Background(), "demo", 50)
	if err != nil {
		t.Fatalf("Rightsize: %v", err)
	}
	if !f.nodeRunsQ.IncludeMetrics {
		t.Error("expected IncludeMetrics=true to be forced on the NodeRuns query")
	}
	if f.nodeRunsQ.PipelineName != "demo" || f.nodeRunsQ.Limit != 50 {
		t.Errorf("query mis-wired: %+v", f.nodeRunsQ)
	}
	if len(out) != 1 || out[0].Node != "trips" {
		t.Fatalf("expected one node 'trips', got %+v", out)
	}
	if out[0].RecommendedMB == nil {
		t.Fatal("expected a recommendation")
	}
}
