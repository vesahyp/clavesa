package observability

import (
	"slices"
	"strings"
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

// hasEnvArg reports whether args contains the docker env pair
// ["-e", "<kv>"]. Exact-match on the kv string.
func hasEnvArg(args []string, kv string) bool {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == "-e" && args[i+1] == kv {
			return true
		}
	}
	return false
}

// TestWarmWorkerRunArgsLocal — local warehouse: bind-mount, Derby
// metastore network args, no AWS passthrough.
func TestWarmWorkerRunArgsLocal(t *testing.T) {
	wh := "/ws/.clavesa/warehouse"
	args := warmWorkerRunArgs("clavesa.warm-worker=/ws", wh, "img:latest",
		"cat", "syscat", "clavesa-metastore-net", "metastore:1527", nil)

	if !hasEnvArg(args, "CLAVESA_WAREHOUSE="+wh) {
		t.Errorf("missing CLAVESA_WAREHOUSE: %v", args)
	}
	if !slices.Contains(args, "-v") || !slices.Contains(args, wh+":"+wh) {
		t.Errorf("local warehouse must be bind-mounted: %v", args)
	}
	if !slices.Contains(args, "--network") || !slices.Contains(args, "clavesa-metastore-net") {
		t.Errorf("local warehouse must join the metastore network: %v", args)
	}
	if !hasEnvArg(args, "CLAVESA_METASTORE_ADDR=metastore:1527") {
		t.Errorf("missing CLAVESA_METASTORE_ADDR: %v", args)
	}
	if !hasEnvArg(args, "CLAVESA_CATALOG=cat") || !hasEnvArg(args, "CLAVESA_SYSTEM_CATALOG=syscat") {
		t.Errorf("missing catalog identifiers: %v", args)
	}
	for _, a := range args {
		if strings.HasPrefix(a, "AWS_") {
			t.Errorf("local warehouse must not forward AWS env, found %q", a)
		}
	}
	if args[len(args)-1] != "img:latest" {
		t.Errorf("image must be the final arg, got %q", args[len(args)-1])
	}
}

// TestWarmWorkerRunArgsS3 — cloud warehouse (ADR-024): CLAVESA_WAREHOUSE
// verbatim, no bind-mount, no Derby metastore args (even when spawn
// passed them), and the host-resolved AWS credential args (computed by
// spawn via runner.AWSEnvDockerArgs, passed in to keep this pure)
// appended before the image.
func TestWarmWorkerRunArgsS3(t *testing.T) {
	awsArgs := []string{"-e", "AWS_ACCESS_KEY_ID=AKIATEST", "-e", "AWS_REGION=eu-north-1"}
	wh := "s3://bkt/_workspace/_warehouse/"
	args := warmWorkerRunArgs("clavesa.warm-worker=/ws", wh, "img:latest",
		"cat", "syscat", "should-be-ignored-net", "ignored:1527", awsArgs)

	if !hasEnvArg(args, "CLAVESA_WAREHOUSE="+wh) {
		t.Errorf("CLAVESA_WAREHOUSE must pass verbatim: %v", args)
	}
	if slices.Contains(args, "-v") && slices.Contains(args, wh+":"+wh) {
		t.Errorf("s3 warehouse must not be bind-mounted: %v", args)
	}
	if slices.Contains(args, "--network") {
		t.Errorf("s3 warehouse must not join the Derby metastore network: %v", args)
	}
	if hasEnvArg(args, "CLAVESA_METASTORE_ADDR=ignored:1527") {
		t.Errorf("s3 warehouse must not get a metastore addr: %v", args)
	}
	if !hasEnvArg(args, "AWS_ACCESS_KEY_ID=AKIATEST") || !hasEnvArg(args, "AWS_REGION=eu-north-1") {
		t.Errorf("s3 warehouse must carry the provided AWS credential args: %v", args)
	}
	if args[len(args)-1] != "img:latest" {
		t.Errorf("image must be the final arg, got %q", args[len(args)-1])
	}
}
