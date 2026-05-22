package preview

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// latestSnapshotMtime + latestPipelineSourceMtime form the core
// staleness decision in ResolveUpstreamFromSnapshot. The full helper
// is harder to test in isolation because it spawns a runner; the two
// mtime helpers are the only place an off-by-one would cause a stale
// snapshot to be served as fresh, so cover them directly here.

func TestLatestSnapshotMtimeReadsNewestMetadataJSON(t *testing.T) {
	dir := t.TempDir()
	mustWriteWithTime(t, filepath.Join(dir, "v1.metadata.json"), "{}", time.Now().Add(-2*time.Hour))
	mustWriteWithTime(t, filepath.Join(dir, "v2.metadata.json"), "{}", time.Now().Add(-1*time.Hour))
	mustWriteWithTime(t, filepath.Join(dir, "v3.metadata.json"), "{}", time.Now().Add(-30*time.Minute))
	// avro / crc siblings must be ignored — Iceberg writes plenty of
	// those in the same dir and they bump on partial commits.
	mustWriteWithTime(t, filepath.Join(dir, "snap-1.avro"), "x", time.Now())

	got, ok := latestSnapshotMtime(dir)
	if !ok {
		t.Fatal("expected to find at least one metadata.json")
	}
	want := time.Now().Add(-30 * time.Minute)
	if delta := got.Sub(want); delta > time.Second || delta < -time.Second {
		t.Errorf("latest mtime mismatch: got %v, want ~%v", got, want)
	}
}

func TestLatestSnapshotMtimeMissingDir(t *testing.T) {
	_, ok := latestSnapshotMtime(filepath.Join(t.TempDir(), "does-not-exist"))
	if ok {
		t.Fatal("expected ok=false when metadata dir is missing")
	}
}

func TestLatestSnapshotMtimeEmptyDir(t *testing.T) {
	dir := t.TempDir()
	_, ok := latestSnapshotMtime(dir)
	if ok {
		t.Fatal("expected ok=false when no metadata.json files exist")
	}
}

func TestLatestPipelineSourceMtimePicksAuthoringFiles(t *testing.T) {
	dir := t.TempDir()
	// .tf, .sql, .py count; .json / unrelated files don't.
	mustWriteWithTime(t, filepath.Join(dir, "main.tf"), "module {}", time.Now().Add(-3*time.Hour))
	mustWriteWithTime(t, filepath.Join(dir, "trips.sql"), "SELECT 1", time.Now().Add(-1*time.Hour))
	mustWriteWithTime(t, filepath.Join(dir, "orchestration.tf"), "{}", time.Now().Add(-30*time.Minute))
	mustWriteWithTime(t, filepath.Join(dir, "lineage.json"), "{}", time.Now())
	mustWriteWithTime(t, filepath.Join(dir, "README.md"), "x", time.Now())

	got, err := latestPipelineSourceMtime(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := time.Now().Add(-30 * time.Minute)
	if delta := got.Sub(want); delta > time.Second || delta < -time.Second {
		t.Errorf("latest mtime should be orchestration.tf at ~-30m, got %v", got)
	}
}

func TestLatestPipelineSourceMtimeSkipsSubdirs(t *testing.T) {
	dir := t.TempDir()
	// .clavesa/ holds watermarks that bump on every run — including
	// them would make every freshly-written snapshot look stale.
	sub := filepath.Join(dir, ".clavesa")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	mustWriteWithTime(t, filepath.Join(sub, "watermark.json"), "{}", time.Now())
	mustWriteWithTime(t, filepath.Join(dir, "trips.sql"), "SELECT 1", time.Now().Add(-2*time.Hour))

	got, err := latestPipelineSourceMtime(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := time.Now().Add(-2 * time.Hour)
	if delta := got.Sub(want); delta > time.Second || delta < -time.Second {
		t.Errorf("latest mtime should ignore .clavesa/, got %v want ~%v", got, want)
	}
}

func mustWriteWithTime(t *testing.T, path, content string, mtime time.Time) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatal(err)
	}
}
