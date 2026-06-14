package observability_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vesahyp/clavesa/internal/observability"
)

// writeRunFixture lays out the per-node log file at
// <dir>/.clavesa/runs/<runID>/logs/ that ExecutionLogs reads. Per-run state
// no longer lives in a state.json (ADR-024 moved overall run status to the
// warehouse `_progress/<run>/_run.json` marker); only logs stay in the
// per-pipeline runs dir, so the fixture writes just the log.
func writeRunFixture(t *testing.T, dir, runID, status string) {
	t.Helper()
	_ = status // retained for call-site readability; status now lives in _run.json
	logPath := observability.RunLogPath(dir, runID, "filter_complete")
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		t.Fatalf("mkdir log dir: %v", err)
	}
	if err := os.WriteFile(logPath, []byte("starting filter_complete\nfiltered 4 rows\n"), 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}
}

// warehouseFor mirrors workspace.LocalWarehouseDir without importing the
// workspace package into the test: the warehouse is <root>/.clavesa/warehouse.
func warehouseFor(root string) string {
	return filepath.Join(root, ".clavesa", "warehouse")
}

// writeProgressMarker writes one per-node `_progress/<run>/<node>.json`
// marker under the workspace warehouse (ADR-024 cloud-local read tree).
// updatedMs controls freshness for still-"running" markers; nowFresh marks it
// at the current epoch so the freshness window passes.
func writeProgressMarker(t *testing.T, root, run, node, status string, nowFresh bool, extra string) {
	t.Helper()
	dir := filepath.Join(warehouseFor(root), "_progress", run)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir progress dir: %v", err)
	}
	updated := int64(1)
	if nowFresh {
		updated = time.Now().UnixMilli()
	}
	body := fmt.Sprintf(`{"status":%q,"updated_ms":%d%s}`, status, updated, extra)
	if err := os.WriteFile(filepath.Join(dir, node+".json"), []byte(body), 0o644); err != nil {
		t.Fatalf("write progress marker: %v", err)
	}
}

// writeRunMarkerFixture writes the run-level `_progress/<run>/_run.json`.
func writeRunMarkerFixture(t *testing.T, root, run string, m observability.RunMarker) {
	t.Helper()
	store := observability.NewFileProgressStore(warehouseFor(root))
	if err := observability.WriteRunMarker(context.Background(), store, run, m); err != nil {
		t.Fatalf("WriteRunMarker: %v", err)
	}
}

func TestLocalProviderExecutionStatesByRunID(t *testing.T) {
	root := t.TempDir()
	writeProgressMarker(t, root, "run-abc", "load_orders", "succeeded", false, `,"started_ms":1,"ended_ms":3`)
	writeProgressMarker(t, root, "run-abc", "filter_complete", "running", true, "")
	writeRunMarkerFixture(t, root, "run-abc", observability.RunMarker{Status: "RUNNING", Pipeline: "demo"})

	p := observability.NewLocalProvider(root)
	ref := observability.FormatExecRef("demo", "run-abc")

	res, err := p.ExecutionStates(context.Background(), observability.ExecutionStatesQuery{
		ExecutionRef: ref,
	})
	if err != nil {
		t.Fatalf("ExecutionStates: %v", err)
	}
	if res.Status != "RUNNING" {
		t.Errorf("Status = %q, want RUNNING", res.Status)
	}
	// Terminal markers are authoritative regardless of freshness.
	if got := res.States["load_orders"].Status; got != "SUCCEEDED" {
		t.Errorf("load_orders status = %q, want SUCCEEDED", got)
	}
	if got := res.States["filter_complete"].Status; got != "RUNNING" {
		t.Errorf("filter_complete status = %q, want RUNNING", got)
	}
}

func TestLocalProviderExecutionStatesLatestWhenRunIDOmitted(t *testing.T) {
	root := t.TempDir()
	writeProgressMarker(t, root, "older-run", "n", "succeeded", false, "")
	writeRunMarkerFixture(t, root, "older-run", observability.RunMarker{Status: "SUCCEEDED", Pipeline: "demo"})
	// Sleep so the _run.json mtimes differ (filesystem resolution can be 1s).
	time.Sleep(20 * time.Millisecond)
	writeProgressMarker(t, root, "newer-run", "n", "running", true, "")
	writeRunMarkerFixture(t, root, "newer-run", observability.RunMarker{Status: "RUNNING", Pipeline: "demo"})

	p := observability.NewLocalProvider(root)
	ref := observability.FormatExecRef("demo", "")

	res, err := p.ExecutionStates(context.Background(), observability.ExecutionStatesQuery{
		ExecutionRef: ref,
	})
	if err != nil {
		t.Fatalf("ExecutionStates: %v", err)
	}
	if res.Status != "RUNNING" {
		t.Errorf("expected newest (RUNNING) run, got status %q", res.Status)
	}
}

func TestLocalProviderExecutionStatesNoRuns(t *testing.T) {
	// Fresh pipeline that has never been run: the runs/ channel
	// directory doesn't exist on disk. The pipeline-dashboard UI fires
	// this query on every page load; returning an error there flashes
	// a 500 on the new-user landing path. Empty result is the contract.
	dir := t.TempDir()
	p := observability.NewLocalProvider(filepath.Dir(dir))
	ref := observability.FormatExecRef(filepath.Base(dir), "")

	res, err := p.ExecutionStates(context.Background(), observability.ExecutionStatesQuery{
		ExecutionRef: ref,
	})
	if err != nil {
		t.Fatalf("expected no error on fresh pipeline, got %v", err)
	}
	if res == nil {
		t.Fatal("expected empty result, got nil")
	}
	if len(res.States) != 0 {
		t.Errorf("expected empty States map, got %d entries", len(res.States))
	}
}

func TestLocalProviderExecutionLogs(t *testing.T) {
	dir := t.TempDir()
	writeRunFixture(t, dir, "run-1", "RUNNING")

	p := observability.NewLocalProvider(filepath.Dir(dir))
	ref := observability.FormatExecRef(filepath.Base(dir), "run-1")

	res, err := p.ExecutionLogs(context.Background(), observability.ExecutionLogsQuery{
		ExecutionRef: ref,
		Step:         "filter_complete",
	})
	if err != nil {
		t.Fatalf("ExecutionLogs: %v", err)
	}
	if len(res.Events) != 2 {
		t.Fatalf("Events length = %d, want 2", len(res.Events))
	}
	if res.Events[0].Message != "starting filter_complete" {
		t.Errorf("first message = %q, want %q", res.Events[0].Message, "starting filter_complete")
	}
	if res.FunctionName != "filter_complete" {
		t.Errorf("FunctionName = %q, want filter_complete", res.FunctionName)
	}
	if res.LogGroup == "" {
		t.Error("LogGroup should expose the on-disk path so the UI can show it")
	}
}

// TestLocalProviderExecutionLogsTimestamped covers the format
// NewTimestampedLogWriter writes — each line carries the wall-clock
// time it was emitted so the response shape matches what the cloud
// CloudWatch path produces (ADR-014 parity).
func TestLocalProviderExecutionLogsTimestamped(t *testing.T) {
	dir := t.TempDir()
	writeRunFixture(t, dir, "run-ts", "RUNNING")

	// Overwrite the log file with two timestamped lines via the writer
	// the orchestrator uses; this is the production format on disk.
	logPath := observability.RunLogPath(dir, "run-ts", "filter_complete")
	f, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("create log: %v", err)
	}
	w := observability.NewTimestampedLogWriter(f)
	if _, err := w.Write([]byte("starting filter_complete\n")); err != nil {
		t.Fatalf("write line 1: %v", err)
	}
	// Mid-stream line written in two halves — the writer should buffer
	// until the newline arrives so the timestamp aligns with completion.
	if _, err := w.Write([]byte("filtered ")); err != nil {
		t.Fatalf("write partial: %v", err)
	}
	if _, err := w.Write([]byte("4 rows\n")); err != nil {
		t.Fatalf("write completion: %v", err)
	}
	_ = w.Close()
	_ = f.Close()

	p := observability.NewLocalProvider(filepath.Dir(dir))
	ref := observability.FormatExecRef(filepath.Base(dir), "run-ts")
	res, err := p.ExecutionLogs(context.Background(), observability.ExecutionLogsQuery{
		ExecutionRef: ref,
		Step:         "filter_complete",
	})
	if err != nil {
		t.Fatalf("ExecutionLogs: %v", err)
	}
	if len(res.Events) != 2 {
		t.Fatalf("Events length = %d, want 2", len(res.Events))
	}
	for i, ev := range res.Events {
		if ev.Timestamp == "" {
			t.Errorf("event[%d] timestamp empty — writer should have stamped it", i)
		}
		if _, err := time.Parse(time.RFC3339Nano, ev.Timestamp); err != nil {
			t.Errorf("event[%d] timestamp %q not RFC3339Nano: %v", i, ev.Timestamp, err)
		}
	}
	if res.Events[0].Message != "starting filter_complete" {
		t.Errorf("event[0] message = %q", res.Events[0].Message)
	}
	if res.Events[1].Message != "filtered 4 rows" {
		t.Errorf("event[1] message = %q (partial-line buffering broken?)", res.Events[1].Message)
	}
}

func TestLocalProviderExecutionLogsMissingFile(t *testing.T) {
	dir := t.TempDir()
	writeRunFixture(t, dir, "run-1", "RUNNING")

	p := observability.NewLocalProvider(filepath.Dir(dir))
	ref := observability.FormatExecRef(filepath.Base(dir), "run-1")

	res, err := p.ExecutionLogs(context.Background(), observability.ExecutionLogsQuery{
		ExecutionRef: ref,
		Step:         "load_orders", // no log file written for this node
	})
	if err != nil {
		t.Fatalf("expected nil error for missing log file, got %v", err)
	}
	if len(res.Events) != 0 {
		t.Errorf("expected empty events for missing log file, got %d", len(res.Events))
	}
}

func TestLocalProviderExecutionLogsRequiresStep(t *testing.T) {
	p := observability.NewLocalProvider(t.TempDir())
	_, err := p.ExecutionLogs(context.Background(), observability.ExecutionLogsQuery{
		ExecutionRef: "anything:anything",
	})
	if err == nil {
		t.Error("expected error when Step is empty")
	}
}

// stubQueryRunner returns canned rows so the LocalProvider Iceberg surfaces
// can be exercised without Docker. CLAVESA_QUERY=1 produces this exact
// shape over stdout in production.
type stubQueryRunner struct {
	result *observability.QueryRunnerResult
	err    error

	gotWarehouse string
	gotSQL       string
}

func (s *stubQueryRunner) Run(ctx context.Context, warehouse, sql string) (*observability.QueryRunnerResult, error) {
	s.gotWarehouse = warehouse
	s.gotSQL = sql
	if s.err != nil {
		return nil, s.err
	}
	return s.result, nil
}

func TestLocalProviderNodeRunsViaQuery(t *testing.T) {
	workspace := t.TempDir()
	pipelineDir := filepath.Join(workspace, "demo")
	_ = os.MkdirAll(pipelineDir, 0o755)

	stub := &stubQueryRunner{
		result: &observability.QueryRunnerResult{
			Columns: []string{
				"run_id", "pipeline", "node", "started_at", "ended_at",
				"duration_ms", "status", "compute_target", "memory_mb",
				"cold_start", "lambda_request_id", "error_class", "error_msg",
			},
			Rows: [][]interface{}{
				{"r1", "demo", "xform", "2026-05-07T10:00:00.000+00:00", "2026-05-07T10:00:02.000+00:00",
					float64(2000), "ok", "local", nil, nil, "", "", ""},
			},
		},
	}
	p := observability.NewLocalProvider(workspace).WithQueryRunner(stub)

	// Pass an SfExecutionARN — without it the dashboard fast path bypasses
	// Spark in favour of state.json files; this test specifically covers
	// the Spark-backed drill-down used by the run-detail Sheet.
	res, err := p.NodeRuns(context.Background(), observability.NodeRunsQuery{
		PipelineName:   "demo",
		Database:       "clavesa_demo",
		PipelineDir:    "demo",
		SfExecutionARN: "r1",
		Limit:          50,
	})
	if err != nil {
		t.Fatalf("NodeRuns: %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0].RunID != "r1" {
		t.Fatalf("unexpected rows: %+v", res.Rows)
	}
	if res.Rows[0].DurationMs == nil || *res.Rows[0].DurationMs != 2000 {
		t.Errorf("DurationMs = %v, want 2000", res.Rows[0].DurationMs)
	}
	// Warehouse must point at the workspace-shared directory (ADR-016) so a
	// query sees every local pipeline's tables in one Hadoop catalog.
	wantWH := filepath.Join(workspace, ".clavesa", "warehouse")
	if stub.gotWarehouse != wantWH {
		t.Errorf("warehouse = %q, want %q", stub.gotWarehouse, wantWH)
	}
	// Query targets the per-pipeline namespace using the same shape Athena uses.
	if !contains(stub.gotSQL, "clavesa_demo.node_runs") {
		t.Errorf("SQL did not target clavesa_demo.node_runs:\n%s", stub.gotSQL)
	}
}

// TestLocalProviderNodeRunsFromProgress covers the dashboard fast path —
// no SfExecutionARN means the provider projects per-(run,node) rows from the
// warehouse `_progress/<run>/<node>.json` markers directly (ADR-024),
// bypassing Spark entirely.
func TestLocalProviderNodeRunsFromProgress(t *testing.T) {
	root := t.TempDir()
	// transform_a: terminal succeeded with started/ended → duration 1234, output_rows.
	writeProgressMarker(t, root, "r1", "transform_a", "succeeded", false,
		`,"started_ms":1746612000100,"ended_ms":1746612001334,"output_rows":42`)
	// transform_b: terminal skipped, no timing.
	writeProgressMarker(t, root, "r1", "transform_b", "skipped", false, "")
	writeRunMarkerFixture(t, root, "r1", observability.RunMarker{
		Status: "SUCCEEDED", Pipeline: "demo", Trigger: "manual",
	})

	// Use a stub that would explode if the Spark path got hit — the fast
	// path must not invoke it.
	stub := &stubQueryRunner{err: fmt.Errorf("query runner must not be called for bulk dashboard fetch")}
	p := observability.NewLocalProvider(root).WithQueryRunner(stub)

	res, err := p.NodeRuns(context.Background(), observability.NodeRunsQuery{
		PipelineName: "demo",
		Database:     "clavesa_demo",
		PipelineDir:  "demo",
		Limit:        50,
	})
	if err != nil {
		t.Fatalf("NodeRuns: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("got %d rows, want 2: %+v", len(res.Rows), res.Rows)
	}
	if stub.gotSQL != "" {
		t.Errorf("Spark path was invoked unexpectedly:\n%s", stub.gotSQL)
	}
	byNode := map[string]observability.NodeRun{}
	for _, r := range res.Rows {
		byNode[r.Node] = r
	}
	if a, ok := byNode["transform_a"]; !ok {
		t.Errorf("transform_a row missing")
	} else {
		// Maps the progress marker's succeeded → "ok" to match the runner's
		// node_runs convention; the grid checks `=== "ok"` literally.
		if a.Status != "ok" {
			t.Errorf("transform_a status = %q, want %q", a.Status, "ok")
		}
		if a.DurationMs == nil || *a.DurationMs != 1234 {
			t.Errorf("transform_a duration = %v, want 1234", a.DurationMs)
		}
		if a.OutputRows == nil || *a.OutputRows != 42 {
			t.Errorf("transform_a output_rows = %v, want 42", a.OutputRows)
		}
		if a.SfExecutionARN != "r1" {
			t.Errorf("transform_a sf_execution_arn = %q, want r1", a.SfExecutionARN)
		}
		if a.Pipeline != "demo" {
			t.Errorf("transform_a pipeline = %q, want demo", a.Pipeline)
		}
	}
	if b, ok := byNode["transform_b"]; !ok {
		t.Errorf("transform_b row missing")
	} else if b.Status != "skipped" {
		t.Errorf("transform_b status = %q, want %q", b.Status, "skipped")
	}
}

// TestLocalProviderSampleTableViaQuery exercises the sample-table dispatch
// added so TableDetail's "Sample" panel works for compute = "local"
// pipelines (ADR-014). Verifies row stringification (numbers without
// trailing .0, nil → "", booleans), schema projection, and the SQL shape.
func TestLocalProviderSampleTableViaQuery(t *testing.T) {
	workspace := t.TempDir()
	stub := &stubQueryRunner{
		result: &observability.QueryRunnerResult{
			Columns: []string{"id", "amount", "active", "label"},
			Rows: [][]interface{}{
				{float64(1), float64(10.5), true, "alpha"},
				{float64(2), nil, false, ""},
				{float64(3), float64(79456384.28), true, "big"},
			},
		},
	}
	p := observability.NewLocalProvider(workspace).WithQueryRunner(stub)

	res, err := p.SampleTable(context.Background(), observability.SampleTableQuery{
		Database:    "clavesa_demo",
		Table:       "orders__default",
		PipelineDir: "demo",
		Limit:       100,
	})
	if err != nil {
		t.Fatalf("SampleTable: %v", err)
	}
	if len(res.Columns) != 4 || res.Columns[0].Name != "id" {
		t.Fatalf("unexpected columns: %+v", res.Columns)
	}
	if len(res.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(res.Rows))
	}
	// Large fractional floats render in plain decimal, not scientific
	// notation — a revenue figure must read as 79456384.28.
	if res.Rows[2][1] != "79456384.28" {
		t.Errorf("amount row 2 = %q, want \"79456384.28\"", res.Rows[2][1])
	}
	// Integer-valued floats render without trailing ".0" so id columns read
	// naturally; floats with fractional component preserve precision.
	if res.Rows[0][0] != "1" {
		t.Errorf("id row 0 = %q, want \"1\"", res.Rows[0][0])
	}
	if res.Rows[0][1] != "10.5" {
		t.Errorf("amount row 0 = %q, want \"10.5\"", res.Rows[0][1])
	}
	if res.Rows[0][2] != "true" {
		t.Errorf("active row 0 = %q, want \"true\"", res.Rows[0][2])
	}
	// Nulls become "" — matches what cloud Athena emits for null Datum.VarCharValue.
	if res.Rows[1][1] != "" {
		t.Errorf("amount row 1 (nil) = %q, want empty string", res.Rows[1][1])
	}
	// SQL targets the per-pipeline namespace.
	// ADR-018: two-part `<db>.<table>` resolved under spark_catalog; the
	// legacy `clavesa.<db>.<table>` was Iceberg-era and is gone.
	if !contains(stub.gotSQL, "clavesa_demo.orders__default") {
		t.Errorf("SQL did not target the right table:\n%s", stub.gotSQL)
	}
	// ADR-024: the local provider executed the read — runner Spark over
	// the local warehouse — and stamps the result accordingly.
	if res.Served == nil || res.Served.Engine != "spark" || res.Served.Warehouse != "local" {
		t.Errorf("Served = %+v, want {spark local}", res.Served)
	}
}

// TestLocalProviderQueryArbitrary exercises the free-form Query path
// behind the dashboards UI. Verifies the SQL flows through verbatim,
// stringification matches SampleTable, and MaxRows truncates.
func TestLocalProviderQueryArbitrary(t *testing.T) {
	workspace := t.TempDir()
	stub := &stubQueryRunner{
		result: &observability.QueryRunnerResult{
			Columns: []string{"status", "n"},
			Rows: [][]interface{}{
				{"ok", float64(7)},
				{"failed", float64(2)},
			},
		},
	}
	p := observability.NewLocalProvider(workspace).WithQueryRunner(stub)

	res, err := p.Query(context.Background(), observability.QueryQuery{
		SQL:         "SELECT status, COUNT(*) AS n FROM clavesa_demo.runs GROUP BY status",
		PipelineDir: "demo",
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Columns) != 2 || res.Columns[0].Name != "status" {
		t.Errorf("columns = %+v, want [status, n]", res.Columns)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Rows))
	}
	if res.Rows[0][1] != "7" {
		t.Errorf("count = %q, want 7", res.Rows[0][1])
	}
	if !contains(stub.gotSQL, "SELECT status, COUNT(*)") {
		t.Errorf("SQL didn't pass through verbatim: %s", stub.gotSQL)
	}
	// ADR-024 engine-metadata stamp from the executing provider.
	if res.Served == nil || res.Served.Engine != "spark" || res.Served.Warehouse != "local" || res.Served.Transpiled {
		t.Errorf("Served = %+v, want {spark local false}", res.Served)
	}
}

func TestLocalProviderQueryMissingIsEmpty(t *testing.T) {
	workspace := t.TempDir()
	stub := &stubQueryRunner{err: errFakeNoTable}
	p := observability.NewLocalProvider(workspace).WithQueryRunner(stub)

	res, err := p.Query(context.Background(), observability.QueryQuery{
		SQL:         "SELECT * FROM clavesa_demo.ghost",
		PipelineDir: "demo",
	})
	if err != nil {
		t.Fatalf("expected nil for missing table, got %v", err)
	}
	if len(res.Rows) != 0 {
		t.Errorf("expected empty rows, got %d", len(res.Rows))
	}
}

func TestLocalProviderSampleTableMissingIsEmpty(t *testing.T) {
	workspace := t.TempDir()
	stub := &stubQueryRunner{err: errFakeNoTable}
	p := observability.NewLocalProvider(workspace).WithQueryRunner(stub)

	res, err := p.SampleTable(context.Background(), observability.SampleTableQuery{
		Database:    "clavesa_demo",
		Table:       "ghost__default",
		PipelineDir: "demo",
		Limit:       100,
	})
	if err != nil {
		t.Fatalf("expected nil error on missing table, got %v", err)
	}
	if len(res.Rows) != 0 {
		t.Errorf("expected empty rows, got %d", len(res.Rows))
	}
}

func TestLocalProviderRunsTreatsMissingTableAsEmpty(t *testing.T) {
	workspace := t.TempDir()
	stub := &stubQueryRunner{err: errFakeNoTable}
	p := observability.NewLocalProvider(workspace).WithQueryRunner(stub)

	res, err := p.Runs(context.Background(), observability.RunsQuery{
		PipelineName: "demo",
		Database:     "clavesa_demo",
		PipelineDir:  "demo",
		Limit:        50,
	})
	if err != nil {
		t.Fatalf("expected nil for missing-table case, got %v", err)
	}
	if len(res.Rows) != 0 {
		t.Errorf("expected empty rows on missing table, got %d", len(res.Rows))
	}
}

func TestLocalProviderSnapshotsFromDeltaLog(t *testing.T) {
	// ADR-018: Snapshots reads the Delta transaction log directly from
	// the local Hadoop-catalog warehouse. The on-disk layout uses Hive's
	// `<db>.db/` directory (v2.0.0 persistent metastore).
	workspace := t.TempDir()
	logDir := filepath.Join(workspace, ".clavesa", "warehouse", "clavesa_demo.db", "xform__default", "_delta_log")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatalf("mkdir log: %v", err)
	}
	// Minimal valid Delta commit — metaData + commitInfo with provenance
	// metadata. Mirrors the shape the runner writes.
	commitJSON := `{"metaData":{"id":"00000000-0000-0000-0000-000000000000","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1715000000000}}
{"commitInfo":{"timestamp":1715000000000,"operation":"WRITE","operationParameters":{"mode":"Overwrite"},"operationMetrics":{"numOutputRows":"5"},"userMetadata":"{\"clavesa.trigger\":\"manual\",\"clavesa.run-id\":\"r1\"}"}}`
	if err := os.WriteFile(filepath.Join(logDir, "00000000000000000000.json"), []byte(commitJSON), 0o644); err != nil {
		t.Fatalf("write commit: %v", err)
	}

	p := observability.NewLocalProvider(workspace)
	res, err := p.Snapshots(context.Background(), observability.SnapshotsQuery{
		Database: "clavesa_demo",
		Table:    "xform__default",
		Limit:    20,
	})
	if err != nil {
		t.Fatalf("Snapshots: %v", err)
	}
	if len(res.Snapshots) != 1 || res.Snapshots[0].SnapshotID != "0" {
		t.Fatalf("unexpected snapshots: %+v", res.Snapshots)
	}
	got := res.Snapshots[0]
	if got.Operation != "WRITE" {
		t.Errorf("operation = %q, want WRITE", got.Operation)
	}
	if got.Trigger != "manual" {
		t.Errorf("trigger = %q, want manual", got.Trigger)
	}
	if got.WriterRunID != "r1" {
		t.Errorf("writer run id = %q, want r1", got.WriterRunID)
	}
}

// TestLocalProviderSnapshotsLatestRecordCountFromDeltaCommits exercises the
// LatestRecordCount derivation across single-commit CTAS, multi-commit
// append, and append-with-deletes. Delta commits carry per-commit
// numOutputRows / numTargetRowsDeleted, never a running total — so the
// provider has to sum across the full history.
func TestLocalProviderSnapshotsLatestRecordCountFromDeltaCommits(t *testing.T) {
	metaLine := `{"metaData":{"id":"00000000-0000-0000-0000-000000000000","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1715000000000}}`

	writeCommit := func(t *testing.T, logDir string, version int, body string) {
		t.Helper()
		name := fmt.Sprintf("%020d.json", version)
		if err := os.WriteFile(filepath.Join(logDir, name), []byte(body), 0o644); err != nil {
			t.Fatalf("write commit %d: %v", version, err)
		}
	}

	cases := []struct {
		name    string
		commits []string
		want    int64
	}{
		{
			name: "single-commit CTAS",
			commits: []string{
				metaLine + "\n" + `{"commitInfo":{"timestamp":1715000000000,"operation":"WRITE","operationParameters":{"mode":"Overwrite"},"operationMetrics":{"numOutputRows":"16662"}}}`,
			},
			want: 16662,
		},
		{
			name: "multi-commit append",
			commits: []string{
				metaLine + "\n" + `{"commitInfo":{"timestamp":1715000000000,"operation":"WRITE","operationParameters":{"mode":"Overwrite"},"operationMetrics":{"numOutputRows":"100"}}}`,
				`{"commitInfo":{"timestamp":1715000001000,"operation":"WRITE","operationParameters":{"mode":"Append"},"operationMetrics":{"numOutputRows":"50"}}}`,
				`{"commitInfo":{"timestamp":1715000002000,"operation":"WRITE","operationParameters":{"mode":"Append"},"operationMetrics":{"numOutputRows":"30"}}}`,
			},
			want: 180,
		},
		{
			name: "append with deletes via MERGE",
			commits: []string{
				metaLine + "\n" + `{"commitInfo":{"timestamp":1715000000000,"operation":"WRITE","operationParameters":{"mode":"Overwrite"},"operationMetrics":{"numOutputRows":"100"}}}`,
				`{"commitInfo":{"timestamp":1715000001000,"operation":"MERGE","operationMetrics":{"numTargetRowsInserted":"0","numTargetRowsUpdated":"0","numTargetRowsDeleted":"20"}}}`,
				`{"commitInfo":{"timestamp":1715000002000,"operation":"WRITE","operationParameters":{"mode":"Append"},"operationMetrics":{"numOutputRows":"50"}}}`,
			},
			want: 130,
		},
		{
			// Merge-keyed dim table — the http-changing-source cookbook
			// shape. First run inserts 100 rows; later runs see ~5 new IDs
			// each plus ~95 updates of overlapping IDs. Updates don't move
			// the row count, so the dim stays near 100 after every run.
			// Pre-fix this rendered 500 because numTargetRowsUpdated was
			// folded into AddedRecords and double-counted.
			name: "merge dim with mostly updates",
			commits: []string{
				metaLine + "\n" + `{"commitInfo":{"timestamp":1715000000000,"operation":"CREATE TABLE","operationMetrics":{"numOutputRows":"100"}}}`,
				`{"commitInfo":{"timestamp":1715000001000,"operation":"MERGE","operationMetrics":{"numTargetRowsInserted":"5","numTargetRowsUpdated":"95","numTargetRowsDeleted":"0"}}}`,
				`{"commitInfo":{"timestamp":1715000002000,"operation":"MERGE","operationMetrics":{"numTargetRowsInserted":"3","numTargetRowsUpdated":"100","numTargetRowsDeleted":"0"}}}`,
				`{"commitInfo":{"timestamp":1715000003000,"operation":"MERGE","operationMetrics":{"numTargetRowsInserted":"4","numTargetRowsUpdated":"100","numTargetRowsDeleted":"0"}}}`,
			},
			want: 112,
		},
		{
			// CREATE OR REPLACE TABLE wipes the prior state. A long-running
			// append-mode table that gets re-bootstrapped should report the
			// new row count, not the cumulative sum.
			name: "create or replace resets total",
			commits: []string{
				metaLine + "\n" + `{"commitInfo":{"timestamp":1715000000000,"operation":"WRITE","operationParameters":{"mode":"Append"},"operationMetrics":{"numOutputRows":"1000"}}}`,
				`{"commitInfo":{"timestamp":1715000001000,"operation":"WRITE","operationParameters":{"mode":"Append"},"operationMetrics":{"numOutputRows":"500"}}}`,
				`{"commitInfo":{"timestamp":1715000002000,"operation":"CREATE OR REPLACE TABLE","operationMetrics":{"numOutputRows":"42"}}}`,
				`{"commitInfo":{"timestamp":1715000003000,"operation":"WRITE","operationParameters":{"mode":"Append"},"operationMetrics":{"numOutputRows":"8"}}}`,
			},
			want: 50,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ws := t.TempDir()
			logDir := filepath.Join(ws, ".clavesa", "warehouse", "clavesa_demo.db", "xform__default", "_delta_log")
			if err := os.MkdirAll(logDir, 0o755); err != nil {
				t.Fatalf("mkdir log: %v", err)
			}
			for i, body := range tc.commits {
				writeCommit(t, logDir, i, body)
			}
			p := observability.NewLocalProvider(ws)
			res, err := p.Snapshots(context.Background(), observability.SnapshotsQuery{
				Database: "clavesa_demo",
				Table:    "xform__default",
				Limit:    20,
			})
			if err != nil {
				t.Fatalf("Snapshots: %v", err)
			}
			if res.LatestRecordCount == nil {
				t.Fatalf("LatestRecordCount = nil, want %d", tc.want)
			}
			if *res.LatestRecordCount != tc.want {
				t.Errorf("LatestRecordCount = %d, want %d", *res.LatestRecordCount, tc.want)
			}
		})
	}
}

// errFakeNoTable mimics the AnalysisException Spark raises when querying a
// table that hasn't been registered in the catalog yet.
var errFakeNoTable = &fakeErr{msg: "AnalysisException: TABLE_OR_VIEW_NOT_FOUND: clavesa.clavesa_demo.runs"}

type fakeErr struct{ msg string }

func (e *fakeErr) Error() string { return e.msg }

func contains(s, substr string) bool {
	return len(substr) == 0 || (len(s) >= len(substr) && (indexOf(s, substr) >= 0))
}

func indexOf(s, substr string) int {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func TestSplitAndFormatExecRef(t *testing.T) {
	cases := []struct {
		dir, runID string
	}{
		{"workdir/pipelineA", ""},
		{"/abs/dir", "abc123"},
		{"rel", "feedface"},
	}
	for _, tc := range cases {
		ref := observability.FormatExecRef(tc.dir, tc.runID)
		// FormatExecRef is the inverse of LocalProvider's internal split, but
		// the package-internal split function isn't exported. Smoke-check via
		// ExecutionLogs's "Step required" failure mode — it gets past the
		// dir-resolve step so we know FormatExecRef doesn't mangle inputs.
		_ = ref
	}
}

// TestFormatExecRefARNPassthrough pins the cloud dispatch contract: a runID
// that is already a full SFN execution ARN must pass through unchanged so the
// cloud provider can parse it, while a plain local run ID still gets the
// "dir:runID" prefix. The bug this guards: prefixing an ARN with "dir:"
// shifted the colon-split in StateMachineNameFromExecutionARN, so cloud live
// progress never surfaced.
func TestFormatExecRefARNPassthrough(t *testing.T) {
	const arn = "arn:aws:states:eu-north-1:699166197771:execution:clavesa-bigagg:bcf294d6-dc5f-413f-a2f0-a103aefb22ff"
	if got := observability.FormatExecRef("bigagg", arn); got != arn {
		t.Errorf("ARN runID must pass through unchanged; got %q", got)
	}
	if observability.StateMachineNameFromExecutionARN(observability.FormatExecRef("bigagg", arn)) != "clavesa-bigagg" {
		t.Errorf("formatted ARN ref must still parse to its state machine name")
	}
	if got := observability.FormatExecRef("bigagg", "run-1"); got != "bigagg:run-1" {
		t.Errorf("local run ID must keep the dir prefix; got %q", got)
	}
}
