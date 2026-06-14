package service

import (
	"errors"
	"testing"
)

// TestPrepareRun — the synchronous half of an async run produces a
// meaningful run id, a run-level outcome, and a progress store (the
// `_run.json` sink) before any DAG work.
func TestPrepareRun(t *testing.T) {
	svc, dir, id := initComputeTestWorkspace(t)
	if _, err := svc.UpdateNode(dir, id, map[string]interface{}{"sql": "SELECT 1"}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	prep, err := svc.prepareRun(dir)
	if err != nil {
		t.Fatalf("prepareRun: %v", err)
	}
	if prep.runID == "" {
		t.Error("prepareRun returned an empty run id")
	}
	if prep.outcome == nil {
		t.Error("prepareRun returned a nil run outcome")
	}
	if prep.store == nil {
		t.Error("prepareRun returned a nil progress store")
	}
	found := false
	for _, n := range prep.order {
		if n == id {
			found = true
		}
	}
	if !found {
		t.Errorf("topo order %v does not include the transform %q", prep.order, id)
	}
}

// TestStartRunGuardsConcurrentRuns — StartRun refuses a second run while
// one is already in flight for the same pipeline (the guard the
// synchronous RunPipeline got for free by blocking).
func TestStartRunGuardsConcurrentRuns(t *testing.T) {
	svc := New(t.TempDir())
	abs := svc.resolveDir("demo")

	// Simulate a run already executing for this dir.
	svc.runsMu.Lock()
	svc.runsInFlight[abs] = true
	svc.runsMu.Unlock()

	if _, err := svc.StartRun("demo"); !errors.Is(err, ErrRunInFlight) {
		t.Errorf("StartRun with a run in flight: err = %v, want ErrRunInFlight", err)
	}
}
