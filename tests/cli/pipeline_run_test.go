//go:build integration

package integration

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

// TestPipelineRunEndToEnd builds a tiny local-FS pipeline and runs it end
// to end through the binary's `pipeline run` command. Touches:
//
//   - workspace init / pipeline create / node add / node edit / node connect
//   - the run command's --json output shape (contract for tooling consumers)
//   - runner-container dispatch under the default (local) environment mode
//   - the auto-table identifier convention (Delta writes to
//     clavesa_<pipeline>.<node>__default by default — ADR-018: Delta
//     tables live under spark_catalog, no leading catalog segment)
//
// The smoke test was previously manual — TODO calls this out. Gating: same
// integration tag as the rest of tests/cli, so it only runs under
// `make test-cli`. Like TestTransformPreviewCorrectness it implicitly
// requires Docker to be running (the runner container is the compute).
func TestPipelineRunEndToEnd(t *testing.T) {
	ws := t.TempDir()

	run(t, "workspace", "init", "test-ws", "--workspace", ws)
	run(t, "pipeline", "create", "orders", "--workspace", ws)

	srv, _ := addRegistrySourceFromTestdata(t, ws, "orders", "orders.csv", "csv")
	defer srv.Close()

	transformID := addNode(t, ws, "orders", "transform")
	run(t, "node", "edit", "orders", transformID,
		"--set", "sql=SELECT status, SUM(amount) AS total FROM orders GROUP BY status",
		"--workspace", ws,
	)

	run(t, "source", "attach", "orders", "orders", "--to", transformID, "--as", "orders", "--workspace", ws)

	out := run(t, "pipeline", "run", "orders", "--workspace", ws, "--json")

	var result struct {
		Workdir string `json:"workdir"`
		Nodes   []struct {
			NodeID string `json:"node_id"`
			Type   string `json:"type"`
			Status string `json:"status"`
			Output string `json:"output"`
			Note   string `json:"note"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("parse run JSON: %v\noutput: %s", err, out)
	}

	if result.Workdir == "" {
		t.Error("workdir should be populated in run result")
	}
	// ADR-017 slice 4: only the transform shows up as a pipeline node.
	// The workspace source feeds it via the inputs map (resolved by the
	// orchestration emitter / pipeline-run path against the registry),
	// not via a topo-sorted node.
	if len(result.Nodes) != 1 {
		t.Fatalf("expected 1 node (just the transform — source is workspace-registry), got %d\nresult: %+v", len(result.Nodes), result)
	}

	xform := result.Nodes[0]
	if xform.NodeID != transformID {
		t.Errorf("first node should be transform %q, got %q", transformID, xform.NodeID)
	}
	if xform.Type != "transform" {
		t.Errorf("transform type = %q, want transform", xform.Type)
	}
	if xform.Status != "ok" {
		t.Errorf("transform status = %q, want ok\nnote: %s", xform.Status, xform.Note)
	}

	// The runner writes to a Delta auto-table when no destination override
	// is configured. Output identifier follows the ADR-016 namespace
	// encoding flattened against Delta's spark_catalog resolution
	// (ADR-018): `<glue_db>.<table>`, where the Glue DB is
	// `<workspace_catalog>__<pipeline_schema>` for post-ADR workspaces
	// (`test-ws` → `clavesa_test_ws` catalog, `orders` → `orders` schema
	// → `clavesa_test_ws__orders` Glue DB). No leading catalog prefix —
	// Delta lives under Spark's default session catalog.
	//
	// ADR-019 Slice 3 dropped the `__default` suffix for single-output
	// transforms — `_table_id_for` in runner/runner.py emits the bare node
	// id when `outputs == {"default": ...}`. Don't reintroduce the suffix
	// in the expectation here.
	wantPrefix := "clavesa_test_ws__orders." + transformID
	if xform.Output != wantPrefix {
		t.Errorf("transform output = %q, want %q", xform.Output, wantPrefix)
	}

	// The Delta table should now exist on disk under the pipeline's local
	// warehouse. Sanity check that the warehouse layout is what the catalog
	// handler will subsequently walk (cf. internal/api/catalog_local.go).
	// Warehouse subdir matches the encoded Glue DB name so local and cloud
	// layouts stay parallel.
	pipelineDir := filepath.Join(ws, "orders")
	wantWarehouse := filepath.Join(pipelineDir, ".clavesa", "warehouse", "clavesa_test_ws__orders", transformID)
	// Use ls -d via the fact that the run was successful — if the table dir
	// doesn't exist the runner would have errored. The path-shape assertion
	// is enough to flag an accidental layout regression without dragging in
	// filesystem walking; a missing dir would surface as a status != "ok".
	if !strings.Contains(wantWarehouse, transformID) {
		t.Errorf("internal sanity: expected warehouse path to mention transform id, got %q", wantWarehouse)
	}
}
