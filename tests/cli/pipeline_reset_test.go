//go:build integration

package integration

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// resetResult mirrors service.PipelineResetResult — the `pipeline reset
// --json` contract for tooling consumers.
type resetResult struct {
	Pipeline      string `json:"pipeline"`
	Mode          string `json:"mode"`
	TablesDropped []struct {
		Node      string `json:"node"`
		OutputKey string `json:"output_key"`
		Table     string `json:"table"`
		GlueDB    string `json:"glue_db"`
		Location  string `json:"location"`
	} `json:"tables_dropped"`
	WatermarksCleared []struct {
		Consumer string `json:"consumer"`
		Alias    string `json:"alias"`
		Path     string `json:"path"`
	} `json:"watermarks_cleared"`
}

// TestPipelineResetEndToEnd drives the full rebuild loop through the
// binary: run → reset (tables + watermark gone) → run again (rebuilt) →
// reset --node (only that node's table dropped). The pipeline is a
// two-transform chain with an incremental (CDF) edge so the watermark
// half of reset is exercised, not just the table drops. Needs Docker
// (runner container), same as TestPipelineRunEndToEnd.
func TestPipelineResetEndToEnd(t *testing.T) {
	ws := t.TempDir()

	run(t, "workspace", "init", "test-ws", "--workspace", ws)
	run(t, "pipeline", "create", "resetdemo", "--workspace", ws)

	producer := addNode(t, ws, "resetdemo", "transform")
	run(t, "node", "edit", "resetdemo", producer,
		"--set", "sql=SELECT 1 AS id",
		"--workspace", ws,
	)
	consumer := addNode(t, ws, "resetdemo", "transform")
	run(t, "node", "edit", "resetdemo", consumer,
		"--set", "sql=SELECT * FROM up",
		"--workspace", ws,
	)
	run(t, "node", "connect", "resetdemo",
		"--from", producer, "--to", consumer, "--input", "up",
		"--workspace", ws,
	)
	run(t, "node", "edit", "resetdemo", consumer,
		"--incremental-input", "up",
		"--workspace", ws,
	)

	run(t, "pipeline", "run", "resetdemo", "--workspace", ws, "--json")

	// The incremental edge leaves a consumer-side watermark behind.
	wmPath := filepath.Join(ws, "resetdemo", ".clavesa", "watermarks", consumer+"__up.json")
	if _, err := os.Stat(wmPath); err != nil {
		t.Fatalf("watermark not written after run: %v", err)
	}

	out := run(t, "pipeline", "reset", "resetdemo", "--workspace", ws, "--yes", "--json")
	var receipt resetResult
	if err := json.Unmarshal([]byte(out), &receipt); err != nil {
		t.Fatalf("parse reset JSON: %v\noutput: %s", err, out)
	}
	if receipt.Pipeline != "resetdemo" || receipt.Mode != "local" {
		t.Errorf("pipeline/mode = %q/%q, want resetdemo/local", receipt.Pipeline, receipt.Mode)
	}
	// Both transforms materialized, so the receipt lists both drops.
	if len(receipt.TablesDropped) != 2 {
		t.Fatalf("tables_dropped = %+v, want 2 entries", receipt.TablesDropped)
	}
	droppedNodes := map[string]string{} // node -> location
	for _, tb := range receipt.TablesDropped {
		droppedNodes[tb.Node] = tb.Location
		if _, err := os.Stat(tb.Location); !os.IsNotExist(err) {
			t.Errorf("table dir %s should be gone after reset (stat err: %v)", tb.Location, err)
		}
	}
	if _, ok := droppedNodes[producer]; !ok {
		t.Errorf("receipt missing producer %q: %+v", producer, receipt.TablesDropped)
	}
	consumerLoc, ok := droppedNodes[consumer]
	if !ok {
		t.Errorf("receipt missing consumer %q: %+v", consumer, receipt.TablesDropped)
	}
	if len(receipt.WatermarksCleared) != 1 ||
		receipt.WatermarksCleared[0].Consumer != consumer ||
		receipt.WatermarksCleared[0].Alias != "up" {
		t.Errorf("watermarks_cleared = %+v, want one entry %s/up", receipt.WatermarksCleared, consumer)
	}
	if _, err := os.Stat(wmPath); !os.IsNotExist(err) {
		t.Errorf("watermark %s should be gone after reset (stat err: %v)", wmPath, err)
	}

	// Second run rebuilds everything from scratch — the CDF consumer
	// replays upstream from version 0 because the watermark was cleared.
	run(t, "pipeline", "run", "resetdemo", "--workspace", ws, "--json")
	for node, loc := range droppedNodes {
		if _, err := os.Stat(loc); err != nil {
			t.Errorf("table dir for %s not rebuilt by second run: %v", node, err)
		}
	}

	// --node scopes the drop to one transform's output.
	out = run(t, "pipeline", "reset", "resetdemo", "--node", producer, "--workspace", ws, "--yes", "--json")
	var scoped resetResult
	if err := json.Unmarshal([]byte(out), &scoped); err != nil {
		t.Fatalf("parse scoped reset JSON: %v\noutput: %s", err, out)
	}
	if len(scoped.TablesDropped) != 1 || scoped.TablesDropped[0].Node != producer {
		t.Fatalf("scoped tables_dropped = %+v, want exactly producer %q", scoped.TablesDropped, producer)
	}
	if _, err := os.Stat(scoped.TablesDropped[0].Location); !os.IsNotExist(err) {
		t.Errorf("producer table dir should be gone after scoped reset (stat err: %v)", err)
	}
	if _, err := os.Stat(consumerLoc); err != nil {
		t.Errorf("consumer table dir must survive a --node %s reset: %v", producer, err)
	}
	// The producer has no incremental inputs — no watermarks in scope.
	if len(scoped.WatermarksCleared) != 0 {
		t.Errorf("scoped watermarks_cleared = %+v, want none", scoped.WatermarksCleared)
	}
}

// TestPipelineResetUnknownNode: --node naming a nonexistent transform is a
// clear non-zero-exit error, not a silent no-op.
func TestPipelineResetUnknownNode(t *testing.T) {
	ws := t.TempDir()
	run(t, "workspace", "init", "test-ws", "--workspace", ws)
	run(t, "pipeline", "create", "demo", "--workspace", ws)
	addNode(t, ws, "demo", "transform")

	cmd := exec.Command(binPath, "pipeline", "reset", "demo", "--node", "bogus", "--yes", "--workspace", ws)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected non-zero exit for unknown --node; output: %s", out)
	}
	if !strings.Contains(string(out), "bogus") || !strings.Contains(string(out), "not found") {
		t.Errorf("error should name the missing node, got: %s", out)
	}
}

// TestPipelineResetJSONRequiresYes: --json is non-interactive by contract;
// without --yes there is nowhere to confirm, so it must refuse up front.
func TestPipelineResetJSONRequiresYes(t *testing.T) {
	ws := t.TempDir()
	run(t, "workspace", "init", "test-ws", "--workspace", ws)
	run(t, "pipeline", "create", "demo", "--workspace", ws)
	addNode(t, ws, "demo", "transform")

	cmd := exec.Command(binPath, "pipeline", "reset", "demo", "--json", "--workspace", ws)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected non-zero exit for --json without --yes; output: %s", out)
	}
	if !strings.Contains(string(out), "--yes") {
		t.Errorf("error should point at --yes, got: %s", out)
	}
}

// TestPipelineResetNothingToReset: a pipeline with no transform nodes has
// nothing to drop — plain mode says so and exits 0.
func TestPipelineResetNothingToReset(t *testing.T) {
	ws := t.TempDir()
	run(t, "workspace", "init", "test-ws", "--workspace", ws)
	run(t, "pipeline", "create", "empty", "--workspace", ws)

	out := run(t, "pipeline", "reset", "empty", "--yes", "--workspace", ws)
	if !strings.Contains(out, "(nothing to reset)") {
		t.Errorf("expected '(nothing to reset)', got: %s", out)
	}
}
