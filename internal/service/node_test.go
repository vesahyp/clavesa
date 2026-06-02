package service

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/hclparser"
)

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
	// Collapse whitespace: the HCL writer column-aligns attributes and the
	// alignment width is non-deterministic across runs, so an exact
	// single-space substring flakes.
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
