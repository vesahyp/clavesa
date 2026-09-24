package observability

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// GH #59: every long-lived helper container (metastore, warm query worker,
// transpile sidecar) carries a shared workspace label so a global reaper can
// remove orphans whose workspace directory no longer exists. The per-root
// sweeps (SweepMetastores, SweepWarmWorkers) only see the CURRENT workspace;
// containers belonging to deleted tempdir workspaces (test gates, throwaway
// verifications) leaked forever without this.

// workspaceLabel is the docker label key every long-lived clavesa helper
// container carries; its value is the Abs-normalized workspace root the
// container serves. ReapOrphanContainers removes any container whose labeled
// root no longer exists on disk.
const workspaceLabel = "clavesa.workspace"

// reaperDockerTimeout bounds each individual docker shell-out the reaper
// makes, so a wedged daemon can't hang command startup (the reaper runs
// best-effort from newServiceDeps).
const reaperDockerTimeout = 5 * time.Second

// reapStampTTL is how long a completed reap suppresses further reaps across
// processes (see the stamp-file throttle in ReapOrphanContainers).
const reapStampTTL = time.Hour

// reapStampPath is the cross-process throttle stamp for
// ReapOrphanContainers. A var (not a const) so tests can point it into a
// tempdir; production always uses the OS tempdir default.
var reapStampPath = filepath.Join(os.TempDir(), "clavesa-orphan-reap.stamp")

// reapStampFresh reports whether a stamp touched at mtime still suppresses a
// reap at now. Pure so the freshness decision is unit-testable without
// touching the filesystem or docker.
func reapStampFresh(mtime, now time.Time, ttl time.Duration) bool {
	return now.Sub(mtime) < ttl
}

// workspaceLabelValue returns the Abs-normalized workspace root used as the
// workspaceLabel value. Abs() failures (an unresolvable cwd) fall back to the
// input as-is, mirroring workspaceShortHash — the value is still
// deterministic for a given string.
func workspaceLabelValue(workspaceRoot string) string {
	abs, err := filepath.Abs(workspaceRoot)
	if err != nil {
		return workspaceRoot
	}
	return abs
}

// workspaceLabelKV returns the full `clavesa.workspace=<abs root>` label
// argument value for `docker run --label`.
func workspaceLabelKV(workspaceRoot string) string {
	return workspaceLabel + "=" + workspaceLabelValue(workspaceRoot)
}

// parseWorkspaceLabels parses the `docker ps` output produced by the format
// `{{.ID}}\t{{.Label "clavesa.workspace"}}` into an id → labeled-root map.
// Malformed lines (no tab) and empty label values are dropped — the reaper
// never acts on a container it can't attribute to a workspace path. Pure so
// the parse is unit-testable without docker.
func parseWorkspaceLabels(out string) map[string]string {
	byID := make(map[string]string)
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		id, root, ok := strings.Cut(line, "\t")
		if !ok || id == "" || root == "" {
			continue
		}
		byID[id] = root
	}
	return byID
}

// testTempDirMaxAge is how old a labeled container must be before the
// reaper treats a workspace living inside a Go test temp dir as orphaned
// even though the directory still exists (GH #93: a test process killed
// by the GH #84 Docker-VM OOM skips t.Cleanup, so the tempdir survives and
// the plain existence check below spares the container forever).
const testTempDirMaxAge = 24 * time.Hour

// goTestTempDirSegment matches the path segment Go's t.TempDir() creates
// directly under os.TempDir(): "Test<name><random digits>", e.g.
// "TestTransformPreviewCorrectness1234567890". Anchored to the whole
// segment so an unrelated directory that merely contains "Test" doesn't
// match.
var goTestTempDirSegment = regexp.MustCompile(`^Test\S*$`)

// isGoTestTempDirWorkspace reports whether root sits inside a directory
// t.TempDir() created directly under tempDir (tempDir/Test.../...). Pure —
// no filesystem access — so it's unit-testable on its own.
func isGoTestTempDirWorkspace(root, tempDir string) bool {
	rel, err := filepath.Rel(tempDir, root)
	if err != nil || rel == "." || strings.HasPrefix(rel, "..") {
		return false
	}
	first, _, _ := strings.Cut(filepath.ToSlash(rel), "/")
	return goTestTempDirSegment.MatchString(first)
}

// containerAge resolves how long a container has been running, keyed by
// its `docker ps` short ID. Injected so orphanContainerIDs stays pure and
// unit-testable without docker; ReapOrphanContainers wires it to `docker
// inspect`. An error means "age unknown" — same ambiguity policy as stat:
// never remove on ambiguity.
type containerAge func(id string) (time.Duration, error)

// orphanContainerIDs decides which containers to remove. A container is
// orphaned when either:
//
//   - its labeled workspace root definitively does not exist
//     (errors.Is fs.ErrNotExist) — the original GH #59 case; or
//   - its labeled root lives inside a Go test temp dir under tempDir AND
//     the container is at least testTempDirMaxAge old (GH #93).
//
// Any other outcome — the root exists and isn't a test temp dir, stat
// failed ambiguously (permission, I/O), or age is unknown — keeps the
// container; the reaper never removes on ambiguity. Sorted for
// deterministic output. Pure (stat + age injected) so the decision is
// unit-testable without docker or a real filesystem.
func orphanContainerIDs(byID map[string]string, stat func(string) error, tempDir string, age containerAge) []string {
	var ids []string
	for id, root := range byID {
		err := stat(root)
		if err != nil && errors.Is(err, fs.ErrNotExist) {
			ids = append(ids, id)
			continue
		}
		if err != nil || !isGoTestTempDirWorkspace(root, tempDir) {
			continue
		}
		d, ageErr := age(id)
		if ageErr != nil {
			continue
		}
		if d >= testTempDirMaxAge {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

// dockerContainerAge returns how long ago the named container was
// created, via `docker inspect -f {{.Created}}` (RFC3339Nano). Same
// short-timeout discipline as every other docker call the reaper makes.
func dockerContainerAge(ctx context.Context, id string) (time.Duration, error) {
	inspectCtx, cancel := context.WithTimeout(ctx, reaperDockerTimeout)
	defer cancel()
	out, err := exec.CommandContext(inspectCtx, "docker", "inspect", "-f", "{{.Created}}", id).Output()
	if err != nil {
		return 0, err
	}
	created, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(string(out)))
	if err != nil {
		return 0, err
	}
	return time.Since(created), nil
}

// ReapOrphanContainers removes every clavesa helper container whose labeled
// workspace directory has been deleted (GH #59: warm workers and metastores
// from tempdir test workspaces idled for days after their /tmp roots were
// removed). It lists all containers carrying the workspaceLabel — running or
// stopped, any workspace, not just the current one — stats each labeled root,
// and `docker rm -f`s the ones whose root is gone.
//
// Best-effort by contract: callers treat the returned error as non-fatal
// (docker absent / daemon down is the normal quiet case, same condition the
// per-root sweeps tolerate). Each docker call gets a short timeout so a
// wedged daemon can't hang startup. Returns the ids it removed.
//
// Reaps are throttled across processes by a stamp file in the OS tempdir
// (reapStampPath). Every CLI invocation reaches this via newServiceDeps, and
// CLI processes are numerous and short-lived, so a per-process sync.Once
// alone still meant each process paid a `docker ps` that can approach
// reaperDockerTimeout under load. Reaping is a once-in-a-while hygiene pass;
// an orphan lingering up to reapStampTTL is fine. If the stamp exists and is
// younger than reapStampTTL the call returns (nil, nil) without any docker
// work; otherwise the stamp is touched FIRST so concurrent processes racing
// the check mostly skip (a rare double-reap is harmless). Stamp I/O is
// best-effort: if the stamp can't be read or written, the reap proceeds.
func ReapOrphanContainers(ctx context.Context) (removed []string, err error) {
	if fi, statErr := os.Stat(reapStampPath); statErr == nil &&
		reapStampFresh(fi.ModTime(), time.Now(), reapStampTTL) {
		return nil, nil
	}
	// Touch before reaping; ignore failure (best-effort, never fail the
	// command over stamp I/O).
	_ = os.WriteFile(reapStampPath, nil, 0o644)
	byID, err := listWorkspaceLabeledContainers(ctx)
	if err != nil {
		return nil, err
	}
	orphans := orphanContainerIDs(byID, func(path string) error {
		_, statErr := os.Stat(path)
		return statErr
	}, os.TempDir(), func(id string) (time.Duration, error) {
		return dockerContainerAge(ctx, id)
	})
	for _, id := range orphans {
		if !removeContainer(ctx, id) {
			continue
		}
		fmt.Fprintf(os.Stderr, "clavesa: removed orphan helper container %s (workspace %s is gone, or is a test workspace older than %s)\n", id, byID[id], testTempDirMaxAge)
		removed = append(removed, id)
	}
	return removed, nil
}

// listWorkspaceLabeledContainers runs `docker ps -a` filtered to every
// container carrying workspaceLabel — running or stopped, any workspace —
// and parses the id → labeled-root map. Shared by ReapOrphanContainers and
// ReapWorkspaceContainers so both reaps see the exact same container set.
func listWorkspaceLabeledContainers(ctx context.Context) (map[string]string, error) {
	psCtx, cancel := context.WithTimeout(ctx, reaperDockerTimeout)
	defer cancel()
	out, err := exec.CommandContext(psCtx, "docker", "ps", "-a",
		"--filter", "label="+workspaceLabel,
		"--format", "{{.ID}}\t{{.Label \""+workspaceLabel+"\"}}").Output()
	if err != nil {
		// Docker not running / not installed — same condition the rest of
		// the local provider tolerates.
		return nil, fmt.Errorf("docker ps: %w", err)
	}
	return parseWorkspaceLabels(string(out)), nil
}

// removeContainer force-removes one container, short-timeout, swallowing
// the error (the normal "already gone" / daemon-hiccup case every reap
// here treats as non-fatal). Returns whether the removal reported success.
func removeContainer(ctx context.Context, id string) bool {
	rmCtx, cancel := context.WithTimeout(ctx, reaperDockerTimeout)
	defer cancel()
	return exec.CommandContext(rmCtx, "docker", "rm", "-f", id).Run() == nil
}

// underAnyRoot reports whether path equals, or sits inside, one of roots.
// Pure so the matching rule is unit-testable independent of docker.
func underAnyRoot(path string, roots []string) bool {
	for _, root := range roots {
		if path == root || strings.HasPrefix(path, root+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

// ReapWorkspaceContainers force-removes every clavesa helper container
// whose workspace label falls under one of the given roots, regardless of
// whether the labeled directory still exists. Unlike ReapOrphanContainers
// this is unthrottled and scoped by the caller rather than driven by
// existence/age — it is for an explicit, bounded teardown where the caller
// already knows exactly which workspaces are its own. The CLI integration
// test harness (tests/cli) is the one caller today: TestMain calls this
// after every test finishes, pass or fail, with the t.TempDir() roots it
// created, so containers a test spawned don't outlive the process just
// because nothing else ever told them to stop (GH #93).
func ReapWorkspaceContainers(ctx context.Context, roots []string) (removed []string, err error) {
	if len(roots) == 0 {
		return nil, nil
	}
	byID, err := listWorkspaceLabeledContainers(ctx)
	if err != nil {
		return nil, err
	}
	var targets []string
	for id, root := range byID {
		if underAnyRoot(root, roots) {
			targets = append(targets, id)
		}
	}
	sort.Strings(targets)
	for _, id := range targets {
		if !removeContainer(ctx, id) {
			continue
		}
		removed = append(removed, id)
	}
	return removed, nil
}
