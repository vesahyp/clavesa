package observability

import (
	"context"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// TestWorkspaceLabelKV — the shared workspace label value is the
// Abs-normalized root, so relative and absolute spellings of the same
// workspace produce the same label (the reaper stats exactly one path).
func TestWorkspaceLabelKV(t *testing.T) {
	if got := workspaceLabelKV("/abs/ws"); got != "clavesa.workspace=/abs/ws" {
		t.Errorf("workspaceLabelKV(/abs/ws) = %q", got)
	}
	rel := workspaceLabelKV("rel/ws")
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	want := "clavesa.workspace=" + filepath.Join(wd, "rel/ws")
	if rel != want {
		t.Errorf("workspaceLabelKV(rel/ws) = %q, want %q (Abs-normalized)", rel, want)
	}
}

// TestParseWorkspaceLabels — the `{{.ID}}\t{{.Label ...}}` parse keeps
// well-formed lines and drops anything it can't attribute (blank lines,
// missing tab, empty label value).
func TestParseWorkspaceLabels(t *testing.T) {
	out := "abc123def456\t/tmp/ws-a\n" +
		"\n" +
		"deadbeef0000\t/tmp/ws b with spaces\n" +
		"notablonthisline\n" +
		"cafe00000000\t\n"
	got := parseWorkspaceLabels(out)
	want := map[string]string{
		"abc123def456": "/tmp/ws-a",
		"deadbeef0000": "/tmp/ws b with spaces",
	}
	if len(got) != len(want) {
		t.Fatalf("parseWorkspaceLabels = %v, want %v", got, want)
	}
	for id, root := range want {
		if got[id] != root {
			t.Errorf("parseWorkspaceLabels[%s] = %q, want %q", id, got[id], root)
		}
	}
}

// TestOrphanContainerIDs — remove only on a definitive does-not-exist;
// an existing root or an ambiguous stat error (permission, I/O) keeps the
// container. Output is sorted for determinism.
func TestOrphanContainerIDs(t *testing.T) {
	byID := map[string]string{
		"id-gone-b":   "/gone/b",
		"id-gone-a":   "/gone/a",
		"id-live":     "/live",
		"id-permdeny": "/denied",
	}
	stat := func(path string) error {
		switch path {
		case "/live":
			return nil
		case "/denied":
			return fs.ErrPermission
		default:
			return fs.ErrNotExist
		}
	}
	got := orphanContainerIDs(byID, stat)
	want := []string{"id-gone-a", "id-gone-b"}
	if !slices.Equal(got, want) {
		t.Errorf("orphanContainerIDs = %v, want %v", got, want)
	}
}

// overrideReapStamp points the cross-process throttle stamp into a fresh
// tempdir (no file created yet) and restores the default on cleanup, so
// tests neither observe nor pollute the real /tmp stamp.
func overrideReapStamp(t *testing.T) string {
	t.Helper()
	orig := reapStampPath
	t.Cleanup(func() { reapStampPath = orig })
	reapStampPath = filepath.Join(t.TempDir(), "reap.stamp")
	return reapStampPath
}

// TestReapStampFresh — the pure freshness decision: younger than the TTL
// suppresses a reap, at-or-past the TTL allows one. A future mtime (clock
// skew) counts as fresh rather than triggering a reap loop.
func TestReapStampFresh(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name  string
		mtime time.Time
		want  bool
	}{
		{"just touched", now, true},
		{"within ttl", now.Add(-59 * time.Minute), true},
		{"exactly ttl", now.Add(-time.Hour), false},
		{"past ttl", now.Add(-2 * time.Hour), false},
		{"future mtime (clock skew)", now.Add(10 * time.Minute), true},
	}
	for _, tc := range cases {
		if got := reapStampFresh(tc.mtime, now, time.Hour); got != tc.want {
			t.Errorf("reapStampFresh(%s) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestReapFreshStampShortCircuits — a fresh stamp makes ReapOrphanContainers
// return (nil, nil) immediately, before any docker call. No docker required:
// if the throttle failed to short-circuit, the call would either return a
// docker error (docker absent) or leave the stamp mtime advanced.
func TestReapFreshStampShortCircuits(t *testing.T) {
	stamp := overrideReapStamp(t)
	if err := os.WriteFile(stamp, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(stamp)
	if err != nil {
		t.Fatal(err)
	}
	removed, reapErr := ReapOrphanContainers(context.Background())
	if removed != nil || reapErr != nil {
		t.Errorf("ReapOrphanContainers with fresh stamp = (%v, %v), want (nil, nil)", removed, reapErr)
	}
	after, err := os.Stat(stamp)
	if err != nil {
		t.Fatal(err)
	}
	if !after.ModTime().Equal(before.ModTime()) {
		t.Errorf("fresh stamp mtime changed (%v -> %v); short-circuit must not touch it", before.ModTime(), after.ModTime())
	}
}

// TestReapStampTouchedWhenMissingOrStale — a missing or stale stamp gets
// touched (created / mtime advanced). The touch happens before the docker
// step, so this holds with or without docker; the reap's own error is
// ignored (docker absent is the normal best-effort case).
func TestReapStampTouchedWhenMissingOrStale(t *testing.T) {
	stamp := overrideReapStamp(t)

	// Missing stamp: the call must create it.
	_, _ = ReapOrphanContainers(context.Background())
	if _, err := os.Stat(stamp); err != nil {
		t.Fatalf("stamp not created on missing-stamp reap: %v", err)
	}

	// Stale stamp: backdate past the TTL; the next call must advance mtime.
	old := time.Now().Add(-2 * reapStampTTL)
	if err := os.Chtimes(stamp, old, old); err != nil {
		t.Fatal(err)
	}
	_, _ = ReapOrphanContainers(context.Background())
	after, err := os.Stat(stamp)
	if err != nil {
		t.Fatal(err)
	}
	if !after.ModTime().After(old) {
		t.Errorf("stale stamp mtime not advanced: still %v", after.ModTime())
	}
}

// TestReapOrphanContainersDocker — docker-gated end-to-end reap: two
// labeled containers, one pointing at a live tempdir and one at a deleted
// path. The reaper must remove exactly the orphan and leave the live one
// running. Requires the runner image built locally (make build-runner);
// skips otherwise, mirroring the transpile sidecar test's gating.
func TestReapOrphanContainersDocker(t *testing.T) {
	// Point the cross-process throttle stamp at a fresh (nonexistent)
	// tempdir path so a recent real-world reap can't short-circuit this run.
	overrideReapStamp(t)
	dockerReady(t)
	if exec.Command("docker", "image", "inspect", builtRunnerImage).Run() != nil {
		t.Skipf("%s not present; run `make build-runner` first", builtRunnerImage)
	}

	liveRoot := t.TempDir()
	goneRoot := filepath.Join(t.TempDir(), "deleted-ws")
	if err := os.MkdirAll(goneRoot, 0o755); err != nil {
		t.Fatal(err)
	}

	start := func(root string) string {
		t.Helper()
		out, err := exec.Command("docker", "run", "-d", "--rm",
			"--label", workspaceLabelKV(root),
			"--entrypoint", "sleep",
			builtRunnerImage, "300").Output()
		if err != nil {
			t.Fatalf("docker run: %v", err)
		}
		id := strings.TrimSpace(string(out))
		t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", id).Run() })
		return id
	}
	liveID := start(liveRoot)
	goneID := start(goneRoot)

	// Delete the orphan's workspace after its container is up.
	if err := os.RemoveAll(goneRoot); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	removed, err := ReapOrphanContainers(ctx)
	if err != nil {
		t.Fatalf("ReapOrphanContainers: %v", err)
	}

	// Removed ids come from `docker ps` (short form); match by prefix.
	containsPrefixOf := func(fullID string) bool {
		for _, id := range removed {
			if strings.HasPrefix(fullID, id) {
				return true
			}
		}
		return false
	}
	if !containsPrefixOf(goneID) {
		t.Errorf("removed = %v, want it to include orphan %s", removed, goneID[:12])
	}
	if containsPrefixOf(liveID) {
		t.Errorf("removed = %v, must not include live-workspace container %s", removed, liveID[:12])
	}

	out, err := exec.Command("docker", "inspect", "-f", "{{.State.Running}}", liveID).Output()
	if err != nil || strings.TrimSpace(string(out)) != "true" {
		t.Errorf("live-workspace container %s not running after reap (out=%q err=%v)", liveID[:12], out, err)
	}
	if exec.Command("docker", "inspect", goneID).Run() == nil {
		t.Errorf("orphan container %s still exists after reap", goneID[:12])
	}
}
