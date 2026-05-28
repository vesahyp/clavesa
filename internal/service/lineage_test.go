package service

import (
	"testing"

	"github.com/vesahyp/clavesa/internal/graph"
)

// TestBuildLineageBasic exercises a three-node pipeline:
// src (source) → xform (transform) → dest (destination). via_table is
// empty for the source edge and populated for the transform edge.
func TestBuildLineageBasic(t *testing.T) {
	g := graph.PipelineGraph{
		Nodes: []graph.Node{
			{ID: "src", Type: "source"},
			{ID: "xform", Type: "transform"},
			{ID: "dest", Type: "destination"},
		},
		Edges: []graph.Edge{
			{FromNode: "src", ToNode: "xform"},
			{FromNode: "xform", ToNode: "dest"},
		},
	}
	got := buildLineage(g, "clavesa_demo")
	if len(got) != 2 {
		t.Fatalf("expected 2 edges, got %d", len(got))
	}
	// Sort: src→xform comes before xform→dest alphabetically.
	if got[0].FromNode != "src" || got[0].ToNode != "xform" {
		t.Errorf("edge[0] = %+v, want src→xform", got[0])
	}
	if got[0].ViaTable != "" {
		t.Errorf("source edge should have empty via_table, got %q", got[0].ViaTable)
	}
	if got[1].FromNode != "xform" || got[1].ToNode != "dest" {
		t.Errorf("edge[1] = %+v, want xform→dest", got[1])
	}
	if got[1].ViaTable != "clavesa_demo.xform" {
		t.Errorf("transform→destination via_table = %q, want clavesa_demo.xform", got[1].ViaTable)
	}
	if got[1].FromType != "transform" || got[1].ToType != "destination" {
		t.Errorf("types on xform→dest = %s/%s, want transform/destination", got[1].FromType, got[1].ToType)
	}
}

// TestBuildLineageDashSanitization verifies pipeline + node ids carrying
// dashes get translated into the SQL-safe form the runner uses for table
// names — without this the via_table the UI matches against won't equal
// the catalog table name and the lineage panel would render empty.
func TestBuildLineageDashSanitization(t *testing.T) {
	g := graph.PipelineGraph{
		Nodes: []graph.Node{
			{ID: "raw-orders", Type: "transform"},
			{ID: "agg-revenue", Type: "transform"},
		},
		Edges: []graph.Edge{
			{FromNode: "raw-orders", ToNode: "agg-revenue"},
		},
	}
	got := buildLineage(g, "clavesa_my_pipeline")
	if len(got) != 1 {
		t.Fatalf("expected 1 edge, got %d", len(got))
	}
	if got[0].ViaTable != "clavesa_my_pipeline.raw_orders" {
		t.Errorf("via_table = %q, want clavesa_my_pipeline.raw_orders", got[0].ViaTable)
	}
	// Node ids preserve their original form — the UI displays both.
	if got[0].FromNode != "raw-orders" || got[0].ToNode != "agg-revenue" {
		t.Errorf("node ids should preserve dashes, got %s → %s", got[0].FromNode, got[0].ToNode)
	}
}

// TestBuildLineageFanOutFanIn exercises the trickier topologies the UI has
// to render — one upstream feeding multiple downstreams (shared table) and
// multiple upstreams feeding one downstream (a join).
func TestBuildLineageFanOutFanIn(t *testing.T) {
	g := graph.PipelineGraph{
		Nodes: []graph.Node{
			{ID: "facts", Type: "transform"},
			{ID: "dims", Type: "transform"},
			{ID: "report_a", Type: "transform"},
			{ID: "report_b", Type: "transform"},
		},
		Edges: []graph.Edge{
			// fan-out from facts
			{FromNode: "facts", ToNode: "report_a"},
			{FromNode: "facts", ToNode: "report_b"},
			// fan-in into report_a
			{FromNode: "dims", ToNode: "report_a"},
		},
	}
	got := buildLineage(g, "clavesa_p")
	if len(got) != 3 {
		t.Fatalf("expected 3 edges, got %d", len(got))
	}
	// Stable order (by FromNode, then ToNode):
	//   dims→report_a, facts→report_a, facts→report_b
	want := [][2]string{
		{"dims", "report_a"},
		{"facts", "report_a"},
		{"facts", "report_b"},
	}
	for i, w := range want {
		if got[i].FromNode != w[0] || got[i].ToNode != w[1] {
			t.Errorf("edge[%d] = %s→%s, want %s→%s", i, got[i].FromNode, got[i].ToNode, w[0], w[1])
		}
	}
	// All three edges share a transform-typed upstream → all carry via_table.
	for i, e := range got {
		if e.ViaTable == "" {
			t.Errorf("edge[%d] should have non-empty via_table (transform upstream)", i)
		}
	}
}

// TestBuildLineageDanglingEdge silently drops edges whose endpoints don't
// exist in Nodes — defensive, in case the parser ever returns a malformed
// graph. The UI shouldn't crash on partial data.
// TestBuildLineageSourceRegistryUpstream verifies that a registered-source
// reference (`source_inputs[alias] = "sources.<name>"`) surfaces as a
// synthetic upstream edge with from_type=source-registry. Without this,
// TableDetail's Lineage panel renders "No upstream" for any transform
// whose only upstream is a registered source — which contradicts the
// multi-stage-pipeline cookbook's claim that bronze's lineage shows the
// source as upstream.
func TestBuildLineageSourceRegistryUpstream(t *testing.T) {
	g := graph.PipelineGraph{
		Nodes: []graph.Node{
			{
				ID:   "trips_bronze",
				Type: "transform",
				Config: map[string]interface{}{
					"source_inputs": map[string]interface{}{
						"trips": "sources.trips",
					},
				},
			},
		},
	}
	got := buildLineage(g, "clavesa_demo")
	if len(got) != 1 {
		t.Fatalf("expected 1 synthetic edge, got %d", len(got))
	}
	if got[0].FromNode != "sources.trips" {
		t.Errorf("FromNode = %q, want sources.trips", got[0].FromNode)
	}
	if got[0].FromType != "source-registry" {
		t.Errorf("FromType = %q, want source-registry", got[0].FromType)
	}
	if got[0].ToNode != "trips_bronze" || got[0].ToType != "transform" {
		t.Errorf("To = %s/%s, want trips_bronze/transform", got[0].ToNode, got[0].ToType)
	}
	if got[0].ViaTable != "" {
		t.Errorf("ViaTable = %q, want empty (registered source has no catalog table)", got[0].ViaTable)
	}
}

// TestBuildLineageSourceRegistryTypedForm covers the v0.22.0+ typed shape
// (`source_inputs[alias] = { spec_name = "<name>" }`) emitted for kind=s3
// attachments. Same synthetic edge as the legacy string form.
func TestBuildLineageSourceRegistryTypedForm(t *testing.T) {
	g := graph.PipelineGraph{
		Nodes: []graph.Node{
			{
				ID:   "events",
				Type: "transform",
				Config: map[string]interface{}{
					"source_inputs": map[string]interface{}{
						"events": map[string]interface{}{
							"spec_name": "cf_logs",
						},
					},
				},
			},
		},
	}
	got := buildLineage(g, "clavesa_demo")
	if len(got) != 1 || got[0].FromNode != "sources.cf_logs" {
		t.Fatalf("expected one sources.cf_logs edge, got %+v", got)
	}
}

func TestBuildLineageDanglingEdge(t *testing.T) {
	g := graph.PipelineGraph{
		Nodes: []graph.Node{
			{ID: "src", Type: "source"},
		},
		Edges: []graph.Edge{
			{FromNode: "src", ToNode: "ghost"},
			{FromNode: "missing", ToNode: "src"},
		},
	}
	got := buildLineage(g, "clavesa_p")
	if len(got) != 0 {
		t.Errorf("expected 0 edges (both have dangling endpoints), got %d", len(got))
	}
}
