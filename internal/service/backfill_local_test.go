package service

import (
	"sort"
	"testing"

	"github.com/vesahyp/clavesa/internal/graph"
)

// TestSeedUpstreamPathsForBackfillMultiStage covers the bug from TODO.md:
// `pipeline backfill stage --node <downstream>` errored "upstream node X
// has not produced output yet" because backfill_local.go was calling
// buildInputs with empty outputPath / outputFormat maps. The fix walks
// the transitive intra-pipeline transform upstreams of the target node
// and seeds those maps from autoDeltaTableID. This test asserts the
// reverse-BFS produces an entry for every transitive transform upstream
// (a -> b -> c, target c, so {a, b} get seeded), and that the table ids
// match autoDeltaTableID's encoding.
func TestSeedUpstreamPathsForBackfillMultiStage(t *testing.T) {
	t.Parallel()
	g := &graph.PipelineGraph{
		Nodes: []graph.Node{
			{ID: "a", Type: "transform"},
			{ID: "b", Type: "transform"},
			{ID: "c", Type: "transform"},
		},
		Edges: []graph.Edge{
			{FromNode: "a", ToNode: "b", ToInput: "default"},
			{FromNode: "b", ToNode: "c", ToInput: "default"},
		},
	}
	outputPath, outputFormat := seedUpstreamPathsForBackfill(g, "c", "clavesa_test", "demo")

	wantIDs := map[string]string{
		"a": autoDeltaTableID("clavesa_test", "demo", "a"),
		"b": autoDeltaTableID("clavesa_test", "demo", "b"),
	}
	if len(outputPath) != len(wantIDs) {
		t.Fatalf("outputPath size = %d, want %d (entries: %+v)", len(outputPath), len(wantIDs), outputPath)
	}
	for id, want := range wantIDs {
		if got := outputPath[id]; got != want {
			t.Errorf("outputPath[%q] = %q, want %q", id, got, want)
		}
		if got := outputFormat[id]; got != "iceberg" {
			t.Errorf("outputFormat[%q] = %q, want %q", id, got, "iceberg")
		}
	}
	// target itself must not be in the seed maps — it's what we're about
	// to run, not an already-produced upstream.
	if _, ok := outputPath["c"]; ok {
		t.Errorf("outputPath should not contain target node %q", "c")
	}
}

// TestSeedUpstreamPathsForBackfillIgnoresSources asserts source-typed
// upstreams do NOT get an outputPath entry. Sources resolve through
// node.Config["source_inputs"] in buildInputs, not through outputPath,
// and seeding them would steer buildInputs toward a non-existent
// `spark.table()` read instead of the source descriptor branch.
func TestSeedUpstreamPathsForBackfillIgnoresSources(t *testing.T) {
	t.Parallel()
	g := &graph.PipelineGraph{
		Nodes: []graph.Node{
			{ID: "src", Type: "source"},
			{ID: "trans_up", Type: "transform"},
			{ID: "target", Type: "transform"},
		},
		Edges: []graph.Edge{
			{FromNode: "src", ToNode: "trans_up", ToInput: "raw"},
			{FromNode: "trans_up", ToNode: "target", ToInput: "default"},
		},
	}
	outputPath, outputFormat := seedUpstreamPathsForBackfill(g, "target", "clavesa_test", "demo")

	if _, ok := outputPath["src"]; ok {
		t.Errorf("source node should not be seeded into outputPath, got %+v", outputPath)
	}
	if got := outputPath["trans_up"]; got != autoDeltaTableID("clavesa_test", "demo", "trans_up") {
		t.Errorf("outputPath[trans_up] = %q, want autoDeltaTableID encoding", got)
	}
	if got := outputFormat["trans_up"]; got != "iceberg" {
		t.Errorf("outputFormat[trans_up] = %q, want iceberg", got)
	}
}

// TestSeedUpstreamPathsForBackfillDiamond covers a transform that has
// two distinct transitive transform upstreams via two separate paths.
// The reverse-BFS must visit each upstream exactly once regardless of
// how many edges lead into a visited node.
func TestSeedUpstreamPathsForBackfillDiamond(t *testing.T) {
	t.Parallel()
	// a -> b -> d ; a -> c -> d ; target d
	g := &graph.PipelineGraph{
		Nodes: []graph.Node{
			{ID: "a", Type: "transform"},
			{ID: "b", Type: "transform"},
			{ID: "c", Type: "transform"},
			{ID: "d", Type: "transform"},
		},
		Edges: []graph.Edge{
			{FromNode: "a", ToNode: "b", ToInput: "default"},
			{FromNode: "a", ToNode: "c", ToInput: "default"},
			{FromNode: "b", ToNode: "d", ToInput: "left"},
			{FromNode: "c", ToNode: "d", ToInput: "right"},
		},
	}
	outputPath, _ := seedUpstreamPathsForBackfill(g, "d", "clavesa_test", "demo")
	got := make([]string, 0, len(outputPath))
	for k := range outputPath {
		got = append(got, k)
	}
	sort.Strings(got)
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("seeded nodes = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("seeded[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
