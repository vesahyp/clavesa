package cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/hclparser"
)

// TestPipelineGolden builds a complete source→transform→destination
// pipeline via the CLI and parses main.tf to assert the resulting graph
// matches the expected structure.
//
// ADR-017 slice 4: source is now a workspace-level registry entry, not
// a pipeline-local module. The golden walks the same flow a user would:
// register a source, then attach it to the transform.
//
// Catches regressions in:
//   - Node creation (transform, destination block names, attributes)
//   - Edge wiring (transform → destination)
//   - source_inputs round-trip (the parser surfaces the
//     `sources.<name>` reference under Config["source_inputs"] for the
//     orchestration emitter)
func TestPipelineGolden(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()

	steps := [][]string{
		{"pipeline", "create", "golden-test", "--workspace", ws},
		{"source", "register", "raw_events", "--from", "s3://raw-data/events/data.csv", "--format", "csv", "--workspace", ws},
		{"node", "add", "--type", "transform", "--workspace", ws, "golden-test"},
		{"node", "edit", "--set", "sql=SELECT * FROM raw_events WHERE amount > 0", "--workspace", ws, "golden-test", "transform1"},
		{"node", "add", "--type", "destination", "--workspace", ws, "golden-test"},
		{"node", "edit", "--set", "bucket=clean-data", "--set", "prefix=events/clean/", "--workspace", ws, "golden-test", "destination1"},
		{"source", "attach", "golden-test", "raw_events", "--to", "transform1", "--as", "raw_events", "--workspace", ws},
		{"node", "connect", "--from", "transform1", "--to", "destination1", "--workspace", ws, "golden-test"},
	}

	for _, args := range steps {
		if err := Run(args); err != nil {
			t.Fatalf("Run(%v): %v", args, err)
		}
	}

	mainTF := filepath.Join(ws, "golden-test", "main.tf")
	if _, err := os.Stat(mainTF); err != nil {
		t.Fatalf("main.tf not found: %v", err)
	}

	g, err := hclparser.Parse(filepath.Join(ws, "golden-test"))
	if err != nil {
		t.Fatalf("hclparser.Parse: %v", err)
	}

	if len(g.Nodes) != 2 {
		t.Fatalf("expected 2 nodes (transform + destination), got %d: %v", len(g.Nodes), nodeIDList(g.Nodes))
	}

	byID := make(map[string]graph.Node, len(g.Nodes))
	for _, n := range g.Nodes {
		byID[n.ID] = n
	}

	assertNodeType(t, byID, "transform1", "transform")
	assertNodeType(t, byID, "destination1", "destination")

	assertConfig(t, byID["transform1"], "sql", "SELECT * FROM raw_events WHERE amount > 0")
	assertConfig(t, byID["destination1"], "bucket", "clean-data")
	assertConfig(t, byID["destination1"], "prefix", "events/clean/")

	// The source attach surfaces under Config["source_inputs"] — the
	// orchestration emitter resolves these against the workspace
	// registry at sync time. v0.22.0: kind=s3 attachments land as a
	// resolved typed object (spec_name + bucket + prefix + format +
	// optional partitions/start_from); kind=http stays a "sources.X"
	// sentinel string. raw_events is kind=s3 here.
	si, ok := byID["transform1"].Config["source_inputs"].(map[string]interface{})
	if !ok {
		t.Fatalf("transform1 missing source_inputs config: %#v", byID["transform1"].Config)
	}
	got, ok := si["raw_events"].(map[string]interface{})
	if !ok {
		t.Fatalf("source_inputs[raw_events] not a typed object: %#v", si["raw_events"])
	}
	if got["spec_name"] != "raw_events" {
		t.Errorf("spec_name = %v, want raw_events", got["spec_name"])
	}
	if got["bucket"] != "raw-data" {
		t.Errorf("bucket = %v, want raw-data", got["bucket"])
	}

	if len(g.Edges) != 1 {
		t.Fatalf("expected 1 edge (transform→destination), got %d: %v", len(g.Edges), g.Edges)
	}
	assertEdge(t, g.Edges, "transform1", "destination1")
}

// --- helpers ---

func nodeIDList(nodes []graph.Node) []string {
	ids := make([]string, len(nodes))
	for i, n := range nodes {
		ids[i] = n.ID
	}
	return ids
}

func assertNodeType(t *testing.T, byID map[string]graph.Node, id, wantType string) {
	t.Helper()
	n, ok := byID[id]
	if !ok {
		t.Errorf("node %q not found", id)
		return
	}
	if n.Type != wantType {
		t.Errorf("node %q: type = %q, want %q", id, n.Type, wantType)
	}
}

func assertConfig(t *testing.T, n graph.Node, key, wantVal string) {
	t.Helper()
	got, ok := n.Config[key]
	if !ok {
		t.Errorf("node %q: config key %q missing", n.ID, key)
		return
	}
	if got != wantVal {
		t.Errorf("node %q: config[%q] = %q, want %q", n.ID, key, got, wantVal)
	}
}

func assertEdge(t *testing.T, edges []graph.Edge, fromNode, toNode string) {
	t.Helper()
	for _, e := range edges {
		if e.FromNode == fromNode && e.ToNode == toNode {
			return
		}
	}
	t.Errorf("edge %s→%s not found in %v", fromNode, toNode, edges)
}
