package observability

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// fillTimingFromProgress must find the node marker under the pipeline run
// (sf_execution_arn on a node_runs row): the row's run_id is the node
// invocation's own id and names no directory in the _progress tree.
func TestFillTimingFromProgressKeysOnPipelineRun(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, ".clavesa", "warehouse", "_progress", "pipeline-run-1")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	marker := `{"status":"succeeded","started_ms":1,"ended_ms":5,"metrics":{"phases":{"write":4}}}`
	if err := os.WriteFile(filepath.Join(dir, "trips.json"), []byte(marker), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "skipped.json"), []byte(`{"status":"skipped","metrics":{}}`), 0o644); err != nil {
		t.Fatal(err)
	}

	p := NewLocalProvider(root)
	rows := []NodeRun{
		{RunID: "node-invocation-id", SfExecutionARN: "pipeline-run-1", Node: "trips"},
		{RunID: "node-invocation-id-2", SfExecutionARN: "pipeline-run-1", Node: "skipped"},
		{RunID: "node-invocation-id-3", SfExecutionARN: "pipeline-run-1", Node: "missing"},
	}
	p.fillTimingFromProgress(context.Background(), rows)

	if rows[0].Timing != `{"phases":{"write":4}}` {
		t.Errorf("trips: timing = %q, want the marker's metrics object", rows[0].Timing)
	}
	if rows[1].Timing != "" {
		t.Errorf("skipped: timing = %q, want empty for an empty metrics object", rows[1].Timing)
	}
	if rows[2].Timing != "" {
		t.Errorf("missing: timing = %q, want empty when the marker is gone", rows[2].Timing)
	}
}
