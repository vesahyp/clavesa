package observability

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// dockerReady reports whether a working Docker daemon is reachable. The
// sidecar test needs the runner image built (make build-runner); when
// docker is absent it skips rather than failing the suite — matching the
// make test-runner gating used elsewhere (no build tag, so it still runs
// under plain `go test` when docker is present).
func dockerReady(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not installed; skipping transpile sidecar test")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("docker daemon not reachable; skipping transpile sidecar test")
	}
}

// builtRunnerImage is the local tag `make build-runner` produces. The
// docker-gated sidecar test points at it directly (the empty-workspace
// resolver path yields a per-workspace name that isn't built in CI).
const builtRunnerImage = "clavesa/transform-runner:latest"

// TestTranspileSidecarToServing spawns the real transpile server container
// and transpiles a SparkSQL statement whose datediff maps to Trino's
// DATE_DIFF — proof the sidecar round-trips SparkSQL → Athena dialect.
// Requires the runner image built locally (make build-runner).
func TestTranspileSidecarToServing(t *testing.T) {
	dockerReady(t)
	if exec.Command("docker", "image", "inspect", builtRunnerImage).Run() != nil {
		t.Skipf("%s not present; run `make build-runner` first", builtRunnerImage)
	}

	s := NewTranspileSidecar("")
	s.imageOverride = builtRunnerImage
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	out, err := s.ToServing(ctx, "SELECT datediff(d2, d1) AS n FROM t")
	if err != nil {
		var de *DialectError
		if errors.As(err, &de) {
			t.Fatalf("unexpected dialect rejection: %v", de)
		}
		t.Fatalf("ToServing transport error (is the runner image built?): %v", err)
	}
	if !strings.Contains(strings.ToUpper(out), "DATE_DIFF") {
		t.Errorf("transpiled SQL = %q, want it to contain DATE_DIFF", out)
	}
}
