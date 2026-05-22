package observability

import (
	"testing"
	"time"
)

// TestWarmWorkersReporting checks Workers() reports tracked workers with
// their state, sorted, and ages — and is empty for a fresh or closed
// runner. White-box: production code only creates worker entries via
// getOrSpawn (which needs docker), so the test injects them directly.
func TestWarmWorkersReporting(t *testing.T) {
	p := NewPersistentQueryRunner("/tmp/ws")

	if got := p.Workers(); len(got) != 0 {
		t.Fatalf("fresh runner: Workers() = %d, want 0", len(got))
	}

	p.workers["/wh/a"] = &warmWorker{
		state:     workerSpawning,
		startedAt: time.Now().Add(-2 * time.Second),
	}
	p.workers["/wh/b"] = &warmWorker{
		state:     workerReady,
		startedAt: time.Now().Add(-30 * time.Second),
	}

	got := p.Workers()
	if len(got) != 2 {
		t.Fatalf("Workers() = %d, want 2", len(got))
	}
	if got[0].Warehouse != "/wh/a" || got[0].State != "spawning" {
		t.Errorf("got[0] = %+v, want {/wh/a spawning}", got[0])
	}
	if got[1].Warehouse != "/wh/b" || got[1].State != "ready" {
		t.Errorf("got[1] = %+v, want {/wh/b ready}", got[1])
	}
	if got[0].AgeMS < 1000 {
		t.Errorf("got[0].AgeMS = %d, want >= 1000 (started 2s ago)", got[0].AgeMS)
	}

	p.Close()
	if got := p.Workers(); len(got) != 0 {
		t.Errorf("closed runner: Workers() = %d, want 0", len(got))
	}
}
