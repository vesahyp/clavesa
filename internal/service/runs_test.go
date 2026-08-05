package service

import (
	"context"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// runsTestService builds a Service rooted at a fresh temp workspace, wired
// through a resolver to two independently-configurable fakes — one for the
// local provider, one for cloud — so tests can distinguish which path a
// call actually dispatched to. wh selects the workspace warehouse (default
// local when zero-value).
func runsTestService(t *testing.T, local, cloud *fakeProvider, wh workspace.Warehouse) *Service {
	t.Helper()
	ws := t.TempDir()
	if wh == workspace.WarehouseCloud {
		if err := workspace.WriteWarehouse(ws, workspace.WarehouseCloud); err != nil {
			t.Fatalf("WriteWarehouse: %v", err)
		}
	}
	resolver := observability.NewResolver(ws, cloud, local)
	return New(ws).WithResolver(resolver)
}

func TestRunsQueryPopulation(t *testing.T) {
	local := &fakeProvider{}
	s := runsTestService(t, local, &fakeProvider{}, workspace.WarehouseLocal)

	if _, err := s.Runs(context.Background(), "demo", 10); err != nil {
		t.Fatalf("Runs: %v", err)
	}
	q := local.runsQ
	if q.PipelineName != "demo" {
		t.Errorf("PipelineName = %q, want %q", q.PipelineName, "demo")
	}
	if q.PipelineDir != "demo" {
		t.Errorf("PipelineDir = %q, want %q", q.PipelineDir, "demo")
	}
	if q.Database == "" {
		t.Error("Database must be populated (systemGlueDB fallback)")
	}
	if q.Limit != 10 {
		t.Errorf("Limit = %d, want 10", q.Limit)
	}
}

// TestRunsLimitZeroLeavesProviderDefault proves limit<=0 leaves the
// query's Limit at zero so the provider applies its own default, matching
// the /data/runs handler's behavior when no `limit` query param is set.
func TestRunsLimitZeroLeavesProviderDefault(t *testing.T) {
	local := &fakeProvider{}
	s := runsTestService(t, local, &fakeProvider{}, workspace.WarehouseLocal)

	if _, err := s.Runs(context.Background(), "demo", 0); err != nil {
		t.Fatalf("Runs: %v", err)
	}
	if local.runsQ.Limit != 0 {
		t.Errorf("Limit = %d, want 0 (provider default)", local.runsQ.Limit)
	}
}

func TestRunsNilResolverErrors(t *testing.T) {
	s := New(t.TempDir())
	_, err := s.Runs(context.Background(), "demo", 10)
	if err == nil || !strings.Contains(err.Error(), "observability resolver not configured") {
		t.Fatalf("Runs with nil resolver = %v, want an actionable resolver error", err)
	}
}

// TestNodeRunsLocalDoesNotSetExecutionARN proves the local dispatch path
// never sets SfExecutionARN on the query — that would force
// LocalProvider.NodeRuns off its cheap `_progress`-marker fast path and
// onto the Spark-container query path (local.go:263).
func TestNodeRunsLocalDoesNotSetExecutionARN(t *testing.T) {
	local := &fakeProvider{
		nodeRuns: []observability.NodeRun{
			{RunID: "run-1", Node: "trips", Status: "ok"},
			{RunID: "run-2", Node: "trips", Status: "ok"},
		},
	}
	s := runsTestService(t, local, &fakeProvider{}, workspace.WarehouseLocal)

	res, err := s.NodeRuns(context.Background(), "demo", "run-1", 50)
	if err != nil {
		t.Fatalf("NodeRuns: %v", err)
	}
	if local.nodeRunsQ.SfExecutionARN != "" {
		t.Errorf("SfExecutionARN = %q, want empty on the local fast path", local.nodeRunsQ.SfExecutionARN)
	}
	// The wrapper must filter by run id itself, since the provider returned
	// rows for both runs unfiltered.
	if len(res.Rows) != 1 || res.Rows[0].RunID != "run-1" {
		t.Fatalf("Rows = %+v, want exactly the run-1 row", res.Rows)
	}
}

// TestNodeRunsLocalEmptyRunReturnsUnfiltered proves an empty run id (the
// "all runs" request) skips the wrapper's filter entirely.
func TestNodeRunsLocalEmptyRunReturnsUnfiltered(t *testing.T) {
	local := &fakeProvider{
		nodeRuns: []observability.NodeRun{
			{RunID: "run-1", Node: "trips", Status: "ok"},
			{RunID: "run-2", Node: "trips", Status: "ok"},
		},
	}
	s := runsTestService(t, local, &fakeProvider{}, workspace.WarehouseLocal)

	res, err := s.NodeRuns(context.Background(), "demo", "", 50)
	if err != nil {
		t.Fatalf("NodeRuns: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("Rows = %+v, want both unfiltered rows", res.Rows)
	}
}

// TestNodeRunsCloudPassesExecutionARN proves the cloud dispatch path
// passes the run id straight through as SfExecutionARN — Athena filters
// server-side at no extra cost, so there's no reason to fetch-then-filter
// like the local path does.
func TestNodeRunsCloudPassesExecutionARN(t *testing.T) {
	cloud := &fakeProvider{}
	s := runsTestService(t, &fakeProvider{}, cloud, workspace.WarehouseCloud)

	if _, err := s.NodeRuns(context.Background(), "demo", "arn-or-uuid", 50); err != nil {
		t.Fatalf("NodeRuns: %v", err)
	}
	if cloud.nodeRunsQ.SfExecutionARN != "arn-or-uuid" {
		t.Errorf("SfExecutionARN = %q, want %q", cloud.nodeRunsQ.SfExecutionARN, "arn-or-uuid")
	}
}

func TestNodeRunsNilResolverErrors(t *testing.T) {
	s := New(t.TempDir())
	_, err := s.NodeRuns(context.Background(), "demo", "", 50)
	if err == nil || !strings.Contains(err.Error(), "observability resolver not configured") {
		t.Fatalf("NodeRuns with nil resolver = %v, want an actionable resolver error", err)
	}
}

// TestExecutionLogsLocalDefaultsStep proves an omitted step defaults to
// "run" on the local provider, which ignores its content (the bundle
// runner shares one log across every node in a run).
func TestExecutionLogsLocalDefaultsStep(t *testing.T) {
	local := &fakeProvider{}
	s := runsTestService(t, local, &fakeProvider{}, workspace.WarehouseLocal)

	if _, err := s.ExecutionLogs(context.Background(), "demo", "run-1", "", 100); err != nil {
		t.Fatalf("ExecutionLogs: %v", err)
	}
	if !local.execLogsCalled {
		t.Fatal("expected the local provider to be called")
	}
	if local.execLogsQ.Step != "run" {
		t.Errorf("Step = %q, want the default %q", local.execLogsQ.Step, "run")
	}
	if local.execLogsQ.MaxLines != 100 {
		t.Errorf("MaxLines = %d, want 100", local.execLogsQ.MaxLines)
	}
	wantRef := observability.FormatExecRef("demo", "run-1")
	if local.execLogsQ.ExecutionRef != wantRef {
		t.Errorf("ExecutionRef = %q, want %q", local.execLogsQ.ExecutionRef, wantRef)
	}
}

// TestExecutionLogsCloudNoStepErrorsActionably proves an omitted step on
// the cloud provider is refused before ever reaching the provider — cloud
// logs are addressed per-Lambda-step, so there's no safe default the way
// there is locally.
func TestExecutionLogsCloudNoStepErrorsActionably(t *testing.T) {
	cloud := &fakeProvider{}
	s := runsTestService(t, &fakeProvider{}, cloud, workspace.WarehouseCloud)

	_, err := s.ExecutionLogs(context.Background(), "demo", "run-1", "", 100)
	if err == nil {
		t.Fatal("expected an error for an omitted step on the cloud path")
	}
	if !strings.Contains(err.Error(), "--node") {
		t.Errorf("error = %v, want it to mention --node", err)
	}
	if cloud.execLogsCalled {
		t.Error("provider must not be called when the step is missing on cloud")
	}
}

// TestExecutionLogsCloudWithStepPassesThrough proves a supplied step on
// the cloud path reaches the provider unchanged.
func TestExecutionLogsCloudWithStepPassesThrough(t *testing.T) {
	cloud := &fakeProvider{}
	s := runsTestService(t, &fakeProvider{}, cloud, workspace.WarehouseCloud)

	if _, err := s.ExecutionLogs(context.Background(), "demo", "run-1", "trips", 0); err != nil {
		t.Fatalf("ExecutionLogs: %v", err)
	}
	if cloud.execLogsQ.Step != "trips" {
		t.Errorf("Step = %q, want %q", cloud.execLogsQ.Step, "trips")
	}
}

func TestExecutionLogsNilResolverErrors(t *testing.T) {
	s := New(t.TempDir())
	_, err := s.ExecutionLogs(context.Background(), "demo", "run-1", "trips", 0)
	if err == nil || !strings.Contains(err.Error(), "observability resolver not configured") {
		t.Fatalf("ExecutionLogs with nil resolver = %v, want an actionable resolver error", err)
	}
}
