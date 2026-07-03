package hclutil_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/fileops"
	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/hclparser"
	"github.com/vesahyp/clavesa/internal/hclutil"
)

// joinTF is a three-node fan-in pipeline: two sources feeding one join
// transform. The b edge deliberately uses a non-default output key so the
// rebuild path's key preservation is exercised alongside sibling survival.
const joinTF = `module "src_a" {
  source = "clavesa/source/aws"
  name   = "src_a"
  bucket = "a"
}

module "src_b" {
  source = "clavesa/source/aws"
  name   = "src_b"
  bucket = "b"
}

module "join" {
  source = "clavesa/transform/aws"
  name   = "join"
  inputs = {
    a = module.src_a.outputs["default"]
    b = module.src_b.outputs["dims"]
  }
  sql = "SELECT * FROM a JOIN b USING (id)"
}
`

func writePipeline(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "main.tf"), []byte(content), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	return dir
}

func readMainTF(t *testing.T, dir string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "main.tf"))
	if err != nil {
		t.Fatalf("read main.tf: %v", err)
	}
	return string(b)
}

func findEdge(g graph.PipelineGraph, from, to string) (graph.Edge, bool) {
	for _, e := range g.Edges {
		if e.FromNode == from && e.ToNode == to {
			return e, true
		}
	}
	return graph.Edge{}, false
}

// TestRemoveEdgesReferencingKeepsSiblingInputs is the regression test for the
// P1 data-loss bug (GH #69): deleting one upstream of a multi-input join
// transform must sever only that upstream's entry, not wipe the whole
// `inputs` attribute (and with it the sibling producers' edges).
func TestRemoveEdgesReferencingKeepsSiblingInputs(t *testing.T) {
	t.Parallel()
	dir := writePipeline(t, joinTF)
	fo := fileops.New()

	// Mirror the production delete sequence: block first, then edge cleanup.
	if _, err := fo.RemoveBlock(filepath.Join(dir, "main.tf"), "module.src_a"); err != nil {
		t.Fatalf("RemoveBlock: %v", err)
	}
	if err := hclutil.RemoveEdgesReferencing(fo, dir, "src_a"); err != nil {
		t.Fatalf("RemoveEdgesReferencing: %v", err)
	}

	content := readMainTF(t, dir)
	if strings.Contains(content, "module.src_a") {
		t.Errorf("dangling module.src_a reference left behind:\n%s", content)
	}
	if !strings.Contains(content, `module.src_b.outputs["dims"]`) {
		t.Errorf("surviving sibling edge b = module.src_b.outputs[\"dims\"] was dropped:\n%s", content)
	}

	g, err := hclparser.Parse(dir)
	if err != nil {
		t.Fatalf("Parse after delete: %v", err)
	}
	e, ok := findEdge(g, "src_b", "join")
	if !ok {
		t.Fatalf("edge src_b→join missing after deleting src_a; edges: %v", g.Edges)
	}
	if e.ToInput != "b" || e.FromOutput != "dims" {
		t.Errorf("surviving edge = %+v, want ToInput=b FromOutput=dims", e)
	}
	if _, ok := findEdge(g, "src_a", "join"); ok {
		t.Errorf("edge src_a→join still present after delete; edges: %v", g.Edges)
	}
}

// TestRemoveEdgesReferencingClearsOnlyInput locks the single-input behavior:
// when the deleted node was the transform's only input, the `inputs`
// attribute is removed entirely (not left as an empty map), matching
// RemoveEdge's existing shape.
func TestRemoveEdgesReferencingClearsOnlyInput(t *testing.T) {
	t.Parallel()
	const tf = `module "src" {
  source = "clavesa/source/aws"
  name   = "src"
  bucket = "a"
}

module "validate" {
  source = "clavesa/transform/aws"
  name   = "validate"
  inputs = {
    raw = module.src.outputs["default"]
  }
  sql = "SELECT * FROM raw"
}
`
	dir := writePipeline(t, tf)
	fo := fileops.New()
	if _, err := fo.RemoveBlock(filepath.Join(dir, "main.tf"), "module.src"); err != nil {
		t.Fatalf("RemoveBlock: %v", err)
	}
	if err := hclutil.RemoveEdgesReferencing(fo, dir, "src"); err != nil {
		t.Fatalf("RemoveEdgesReferencing: %v", err)
	}
	content := readMainTF(t, dir)
	if strings.Contains(content, "inputs") {
		t.Errorf("inputs attribute should be removed when the deleted node was the only input:\n%s", content)
	}
	g, err := hclparser.Parse(dir)
	if err != nil {
		t.Fatalf("Parse after delete: %v", err)
	}
	if len(g.Edges) != 0 {
		t.Errorf("want 0 edges, got %v", g.Edges)
	}
}

// TestRemoveEdgesReferencingKeepsRegistryEntry: a legacy string-form
// `"sources.<name>"` entry shares the inputs map with a module-ref edge;
// deleting the module upstream must not drop the registry reference.
func TestRemoveEdgesReferencingKeepsRegistryEntry(t *testing.T) {
	t.Parallel()
	const tf = `module "src_a" {
  source = "clavesa/source/aws"
  name   = "src_a"
  bucket = "a"
}

module "join" {
  source = "clavesa/transform/aws"
  name   = "join"
  inputs = {
    a     = module.src_a.outputs["default"]
    trips = "sources.trips"
  }
  sql = "SELECT * FROM a JOIN trips USING (id)"
}
`
	dir := writePipeline(t, tf)
	fo := fileops.New()
	if _, err := fo.RemoveBlock(filepath.Join(dir, "main.tf"), "module.src_a"); err != nil {
		t.Fatalf("RemoveBlock: %v", err)
	}
	if err := hclutil.RemoveEdgesReferencing(fo, dir, "src_a"); err != nil {
		t.Fatalf("RemoveEdgesReferencing: %v", err)
	}
	content := readMainTF(t, dir)
	if !strings.Contains(content, `"sources.trips"`) {
		t.Errorf("registry entry trips = \"sources.trips\" was dropped:\n%s", content)
	}
	if strings.Contains(content, "module.src_a") {
		t.Errorf("dangling module.src_a reference left behind:\n%s", content)
	}
}

// TestRemoveEdgePreservesSiblingOutputKey: removing one edge of a join must
// re-emit the surviving sibling with its authored output key, not rewrite it
// to outputs["default"] (the P2 half of GH #69).
func TestRemoveEdgePreservesSiblingOutputKey(t *testing.T) {
	t.Parallel()
	dir := writePipeline(t, joinTF)
	fo := fileops.New()
	if err := hclutil.RemoveEdge(fo, dir, "src_a", "join"); err != nil {
		t.Fatalf("RemoveEdge: %v", err)
	}
	content := readMainTF(t, dir)
	if !strings.Contains(content, `module.src_b.outputs["dims"]`) {
		t.Errorf("surviving edge's output key was rewritten (want outputs[\"dims\"]):\n%s", content)
	}
	if strings.Contains(content, `module.src_a.outputs`) {
		t.Errorf("removed edge still referenced:\n%s", content)
	}
}
