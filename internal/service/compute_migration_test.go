package service

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// initComputeTestWorkspace scaffolds a workspace manifest + one pipeline
// with a single transform, and returns the service, pipeline dir, and
// the transform's node id.
func initComputeTestWorkspace(t *testing.T) (svc *Service, dir, transformID string) {
	t.Helper()
	ws := t.TempDir()
	manifest := `{"name":"smoke-ws","cloud":"aws","version":1,"catalog":"clavesa_smoke_ws"}` + "\n"
	if err := os.WriteFile(filepath.Join(ws, "clavesa.json"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	svc = New(ws)
	if _, err := svc.CreatePipeline("demo", ""); err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	g, err := svc.AddNode("demo", "transform", "xform")
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if len(g.Nodes) != 1 {
		t.Fatalf("expected 1 node after AddNode, got %d", len(g.Nodes))
	}
	return svc, "demo", g.Nodes[0].ID
}

// TestAddNodeOmitsComputeAttr — a freshly added transform carries no
// `compute` attribute; it defaults to the module's "lambda" (TODO bucket
// 16: compute is the cloud deploy target, not a run-mode switch).
func TestAddNodeOmitsComputeAttr(t *testing.T) {
	svc, dir, id := initComputeTestWorkspace(t)
	g, err := svc.GetPipeline(dir)
	if err != nil {
		t.Fatalf("GetPipeline: %v", err)
	}
	var found bool
	for _, n := range g.Nodes {
		if n.ID != id {
			continue
		}
		found = true
		if c, ok := n.Config["compute"]; ok {
			t.Errorf("AddNode wrote compute=%v; expected the attribute to be omitted", c)
		}
	}
	if !found {
		t.Fatalf("transform %q not found", id)
	}
}

// TestUpdateNodeRejectsLocalCompute — compute = "local" is no longer a
// value; compute must be a cloud deploy target.
func TestUpdateNodeRejectsLocalCompute(t *testing.T) {
	svc, dir, id := initComputeTestWorkspace(t)

	if _, err := svc.UpdateNode(dir, id, map[string]interface{}{"compute": "local"}); err == nil {
		t.Error("UpdateNode accepted compute=local; want rejection")
	}
	if _, err := svc.UpdateNode(dir, id, map[string]interface{}{"compute": "banana"}); err == nil {
		t.Error("UpdateNode accepted an unknown compute value; want rejection")
	}
	for _, target := range []string{"lambda", "fargate", "emr-serverless"} {
		if _, err := svc.UpdateNode(dir, id, map[string]interface{}{"compute": target}); err != nil {
			t.Errorf("UpdateNode rejected valid compute=%q: %v", target, err)
		}
	}
}

// TestStripLocalCompute exercises the pure migration helper.
func TestStripLocalCompute(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    int
	}{
		{"no compute line", "module \"x\" {\n  language = \"sql\"\n}\n", 0},
		{"one local line", "module \"x\" {\n  compute       = \"local\"\n  language = \"sql\"\n}\n", 1},
		{"lambda is left alone", "module \"x\" {\n  compute = \"lambda\"\n}\n", 0},
		{"two local lines", "module \"a\" {\n  compute = \"local\"\n}\nmodule \"b\" {\n  compute = \"local\"\n}\n", 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := filepath.Join(t.TempDir(), "main.tf")
			if err := os.WriteFile(f, []byte(tt.content), 0o644); err != nil {
				t.Fatal(err)
			}
			got, err := stripLocalCompute(f)
			if err != nil {
				t.Fatalf("stripLocalCompute: %v", err)
			}
			if got != tt.want {
				t.Errorf("stripped %d, want %d", got, tt.want)
			}
			after, _ := os.ReadFile(f)
			if strings.Contains(string(after), `compute = "local"`) || strings.Contains(string(after), `compute       = "local"`) {
				t.Errorf("compute = \"local\" still present after strip:\n%s", after)
			}
		})
	}
}

// TestUpgradePipelineStripsLocalCompute — `pipeline upgrade` migrates a
// legacy pipeline by removing the dead compute = "local" attribute, even
// when the module ref does not change.
func TestUpgradePipelineStripsLocalCompute(t *testing.T) {
	svc, dir, id := initComputeTestWorkspace(t)

	// Simulate a legacy pipeline: inject compute = "local" as the first
	// attribute of the transform module block. AddNode no longer writes
	// it, so the migration target has to be reconstructed.
	abs := svc.resolveDir(dir)
	tfFiles, _ := filepath.Glob(filepath.Join(abs, "*.tf"))
	injected := false
	for _, f := range tfFiles {
		data, _ := os.ReadFile(f)
		marker := "module \"" + id + "\" {"
		if !strings.Contains(string(data), marker) {
			continue
		}
		patched := strings.Replace(string(data), marker, marker+"\n  compute = \"local\"", 1)
		if err := os.WriteFile(f, []byte(patched), 0o644); err != nil {
			t.Fatal(err)
		}
		injected = true
		break
	}
	if !injected {
		t.Fatalf("could not find the transform module block to inject into")
	}

	// Upgrade to the ref the pipeline already references — no network,
	// no ref rewrite; the migration still runs.
	_, _, updated, migrated, err := svc.UpgradePipeline(dir, ModuleVersion)
	if err != nil {
		t.Fatalf("UpgradePipeline: %v", err)
	}
	if updated != 0 {
		t.Errorf("updated = %d, want 0 (ref unchanged)", updated)
	}
	if migrated != 1 {
		t.Errorf("migrated = %d, want 1", migrated)
	}
	for _, f := range tfFiles {
		data, _ := os.ReadFile(f)
		if strings.Contains(string(data), `compute = "local"`) {
			t.Errorf("%s still contains compute = \"local\" after upgrade", filepath.Base(f))
		}
	}
}
