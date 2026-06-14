package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/vesahyp/clavesa/internal/runlock"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// initLockTestWorkspace scaffolds a manifest-less workspace with one empty
// pipeline. Manifest-less keeps prepareRun off EnsureLocalRunnerImage and
// the empty DAG keeps executeRun off the bundle container — the whole run
// path is docker-free, which is exactly what the lock-lifecycle tests need.
func initLockTestWorkspace(t *testing.T) (*Service, string) {
	t.Helper()
	svc := New(t.TempDir())
	if _, err := svc.CreatePipeline("demo", ""); err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	return svc, "demo"
}

// TestRunRejectedWhileFileLockHeld — the warehouse file lock guards runs
// across processes (GH #48: the CLI's synchronous path previously had no
// cross-process guard at all). A lock held by another holder maps onto the
// ErrRunInFlight sentinel (the HTTP 409 path) with the holder's identity in
// the message, on both the sync and async dispatch paths.
func TestRunRejectedWhileFileLockHeld(t *testing.T) {
	svc, dir := initLockTestWorkspace(t)

	// Simulate another process holding the run lock.
	lk, err := runlock.New(workspace.LocalWarehouseDir(svc.workspace), "demo")
	if err != nil {
		t.Fatalf("runlock.New: %v", err)
	}
	lease, err := lk.Acquire(context.Background(), runlock.Holder{
		RunID: "intruder-run", Compute: "local", Host: "other-host", PID: 4242,
	})
	if err != nil {
		t.Fatalf("out-of-band Acquire: %v", err)
	}
	defer lease.Release(context.Background())

	// Sync path (CLI).
	_, err = svc.RunPipelineWithOpts(context.Background(), dir, RunOpts{})
	if !errors.Is(err, ErrRunInFlight) {
		t.Fatalf("RunPipelineWithOpts under held lock: err = %v, want ErrRunInFlight", err)
	}
	for _, want := range []string{"intruder-run", "other-host"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing holder detail %q", err.Error(), want)
		}
	}

	// Async path (UI). runsInFlight is empty, so this exercises the file
	// lock, not the in-memory fast path.
	if _, err := svc.StartRunWithOpts(dir, RunOpts{}); !errors.Is(err, ErrRunInFlight) {
		t.Fatalf("StartRunWithOpts under held lock: err = %v, want ErrRunInFlight", err)
	}
}

// TestRunLockReleasedAtTerminalBeforeRollup — the GH #48 regression pin.
// The run lock AND the in-flight flag release the moment the run is
// observably terminal (channel.finish), BEFORE the recordLocalRun rollup;
// a new run dispatched while the rollup is still executing must start
// instead of 409ing.
func TestRunLockReleasedAtTerminalBeforeRollup(t *testing.T) {
	svc, dir := initLockTestWorkspace(t)

	rollupStarted := make(chan struct{}, 2)
	rollupRelease := make(chan struct{})
	svc.recordRun = func(context.Context, string, string, *runOutcome, []string, func(context.Context, string, string) (string, string)) {
		rollupStarted <- struct{}{}
		<-rollupRelease
	}

	if _, err := svc.StartRunWithOpts(dir, RunOpts{}); err != nil {
		t.Fatalf("StartRunWithOpts run 1: %v", err)
	}
	// The rollup stub entering proves the run is terminal: executeRun calls
	// channel.finish → releaseTerminal → recordRun in that order.
	select {
	case <-rollupStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("run 1 never reached the rollup")
	}

	// Run 2 dispatched while run 1's rollup is still in flight: must start.
	if _, err := svc.StartRunWithOpts(dir, RunOpts{}); err != nil {
		t.Fatalf("StartRunWithOpts run 2 during run 1's rollup: %v (GH #48 regression: lock/flag held through the rollup)", err)
	}

	close(rollupRelease)
	select {
	case <-rollupStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("run 2 never reached the rollup")
	}

	// Both runs terminal: a third run acquires cleanly (tombstone takeover).
	if _, err := svc.StartRunWithOpts(dir, RunOpts{}); err != nil {
		t.Fatalf("StartRunWithOpts run 3 after both rollups: %v", err)
	}
	select {
	case <-rollupStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("run 3 never reached the rollup")
	}
}

// TestSyncRunReleasesLock — the synchronous CLI path acquires and releases
// the lock across a full run; back-to-back runs don't self-block.
func TestSyncRunReleasesLock(t *testing.T) {
	svc, dir := initLockTestWorkspace(t)
	svc.recordRun = func(context.Context, string, string, *runOutcome, []string, func(context.Context, string, string) (string, string)) {
	}
	for i := 0; i < 2; i++ {
		if _, err := svc.RunPipelineWithOpts(context.Background(), dir, RunOpts{}); err != nil {
			t.Fatalf("RunPipelineWithOpts pass %d: %v", i+1, err)
		}
	}
}
