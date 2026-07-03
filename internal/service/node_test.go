package service

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/hclparser"
)

// joinPipeline stamps a workspace with a fan-in pipeline: two sources
// feeding one join transform, plus a third source left unconnected (fodder
// for AddEdge). The b edge uses a non-default output key so edge-rewrite
// tests can assert authored keys survive verbatim.
func joinPipeline(t *testing.T) (workspace, dir string) {
	t.Helper()
	workspace = t.TempDir()
	dir = "demo"
	pipelineDir := filepath.Join(workspace, dir)
	if err := os.MkdirAll(pipelineDir, 0o755); err != nil {
		t.Fatal(err)
	}
	mainTF := `module "src_a" {
  source = "clavesa/source/aws"
  name   = "src_a"
  bucket = "a"
}

module "src_b" {
  source = "clavesa/source/aws"
  name   = "src_b"
  bucket = "b"
}

module "src_c" {
  source = "clavesa/source/aws"
  name   = "src_c"
  bucket = "c"
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
	if err := os.WriteFile(filepath.Join(pipelineDir, "main.tf"), []byte(mainTF), 0o644); err != nil {
		t.Fatal(err)
	}
	return workspace, dir
}

// TestDeleteNodePreservesSiblingJoinInputs is the service-level regression
// test for GH #69 (P1): deleting one upstream of a multi-input join must
// sever only that upstream's entry in the consumer's `inputs` map — the
// sibling producer's edge stays in the user's .tf, output key intact.
func TestDeleteNodePreservesSiblingJoinInputs(t *testing.T) {
	t.Parallel()
	ws, dir := joinPipeline(t)
	svc := New(ws)

	g, err := svc.DeleteNode(dir, "src_a")
	if err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	var survived *graph.Edge
	for i, e := range g.Edges {
		if e.FromNode == "src_a" {
			t.Errorf("edge from deleted node still present: %+v", e)
		}
		if e.FromNode == "src_b" && e.ToNode == "join" {
			survived = &g.Edges[i]
		}
	}
	if survived == nil {
		t.Fatalf("sibling edge src_b→join was severed by deleting src_a; edges: %v", g.Edges)
	}
	if survived.ToInput != "b" || survived.FromOutput != "dims" {
		t.Errorf("surviving edge = %+v, want ToInput=b FromOutput=dims", *survived)
	}

	b, err := os.ReadFile(filepath.Join(ws, dir, "main.tf"))
	if err != nil {
		t.Fatalf("read main.tf: %v", err)
	}
	if !strings.Contains(string(b), `module.src_b.outputs["dims"]`) {
		t.Errorf("main.tf lost the surviving edge b = module.src_b.outputs[\"dims\"]:\n%s", b)
	}
	if strings.Contains(string(b), "module.src_a") {
		t.Errorf("main.tf still references deleted node src_a:\n%s", b)
	}
}

// TestAddEdgePreservesNonDefaultOutputRef is the service-level regression
// test for GH #69 (P2): adding an edge into a consumer that already holds a
// non-default output reference on another input must re-emit that entry
// verbatim, not rewrite it to outputs["default"].
func TestAddEdgePreservesNonDefaultOutputRef(t *testing.T) {
	t.Parallel()
	ws, dir := joinPipeline(t)
	svc := New(ws)

	// Wire the fixture's unconnected third producer into the join.
	g, err := svc.AddEdge(dir, "src_c", "", "join", "c")
	if err != nil {
		t.Fatalf("AddEdge: %v", err)
	}
	if len(g.Edges) != 3 {
		t.Fatalf("want 3 edges into/out of the graph after connect, got %v", g.Edges)
	}

	b, err := os.ReadFile(filepath.Join(ws, dir, "main.tf"))
	if err != nil {
		t.Fatalf("read main.tf: %v", err)
	}
	content := string(b)
	for _, want := range []string{
		`module.src_a.outputs["default"]`,
		`module.src_b.outputs["dims"]`,
		`module.src_c.outputs["default"]`,
	} {
		if !strings.Contains(content, want) {
			t.Errorf("main.tf missing %s after AddEdge:\n%s", want, content)
		}
	}
	if strings.Contains(content, `module.src_b.outputs["default"]`) {
		t.Errorf("authored non-default output ref was rewritten to outputs[\"default\"]:\n%s", content)
	}
}

// loadNode re-parses the pipeline and returns the named node, or fails.
func loadNode(t *testing.T, abs, id string) graph.Node {
	t.Helper()
	g, err := hclparser.Parse(abs)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	for _, n := range g.Nodes {
		if n.ID == id {
			return n
		}
	}
	t.Fatalf("node %q not found in parsed graph", id)
	return graph.Node{}
}

// TestUpdateNodeRoundTripsEnabledBool locks the `enabled` attribute round-trip:
// UpdateNode writes a Go bool as an unquoted HCL `true`/`false` (NOT a quoted
// string), the generic parser reads it back as a real bool into
// Config["enabled"], and nodeEnabled reflects it. Toggling true → false → true
// keeps the attribute present as an unquoted bool both ways.
func TestUpdateNodeRoundTripsEnabledBool(t *testing.T) {
	t.Parallel()
	ws, dir := pipelineForAttachTest(t)
	abs := filepath.Join(ws, dir)
	svc := New(ws)

	// Disable: expect `enabled = false` (unquoted), nodeEnabled == false.
	if _, err := svc.UpdateNode(dir, "t1", map[string]interface{}{"enabled": false}); err != nil {
		t.Fatalf("UpdateNode enabled=false: %v", err)
	}
	// Collapse whitespace: hclwrite column-aligns the `=` within a block, so
	// a single-space substring won't match the padded form. The alignment is
	// deterministic now (#17); this only normalizes the padding.
	flat := func() string {
		b, _ := os.ReadFile(filepath.Join(abs, "main.tf"))
		return strings.Join(strings.Fields(string(b)), " ")
	}
	if f := flat(); !strings.Contains(f, "enabled = false") {
		t.Errorf("main.tf should contain unquoted `enabled = false`:\n%s", f)
	}
	if f := flat(); strings.Contains(f, `enabled = "false"`) {
		t.Errorf("main.tf must NOT quote the bool (got `enabled = \"false\"`):\n%s", f)
	}
	n := loadNode(t, abs, "t1")
	if v, ok := n.Config["enabled"].(bool); !ok || v {
		t.Errorf("Config[\"enabled\"] = %#v, want bool false", n.Config["enabled"])
	}
	if nodeEnabled(n) {
		t.Errorf("nodeEnabled should be false after disable")
	}

	// Re-enable: production writes `enabled = true` (it does NOT remove the
	// attribute — UpdateNode with a non-nil value sets it). nodeEnabled true.
	if _, err := svc.UpdateNode(dir, "t1", map[string]interface{}{"enabled": true}); err != nil {
		t.Fatalf("UpdateNode enabled=true: %v", err)
	}
	if f := flat(); !strings.Contains(f, "enabled = true") {
		t.Errorf("main.tf should contain unquoted `enabled = true` after re-enable:\n%s", f)
	}
	if f := flat(); strings.Contains(f, `enabled = "true"`) {
		t.Errorf("main.tf must NOT quote the bool after re-enable:\n%s", f)
	}
	n = loadNode(t, abs, "t1")
	if v, ok := n.Config["enabled"].(bool); !ok || !v {
		t.Errorf("Config[\"enabled\"] = %#v, want bool true", n.Config["enabled"])
	}
	if !nodeEnabled(n) {
		t.Errorf("nodeEnabled should be true after re-enable")
	}
}
