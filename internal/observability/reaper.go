package observability

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
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

// orphanContainerIDs decides which containers to remove: those whose labeled
// workspace root definitively does not exist (errors.Is fs.ErrNotExist). Any
// other stat outcome — the root exists, or stat failed ambiguously
// (permission, I/O) — keeps the container; the reaper never removes on
// ambiguity. Sorted for deterministic output. Pure (stat injected) so the
// decision is unit-testable without docker or a real filesystem.
func orphanContainerIDs(byID map[string]string, stat func(string) error) []string {
	var ids []string
	for id, root := range byID {
		err := stat(root)
		if err != nil && errors.Is(err, fs.ErrNotExist) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
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
	byID := parseWorkspaceLabels(string(out))
	orphans := orphanContainerIDs(byID, func(path string) error {
		_, statErr := os.Stat(path)
		return statErr
	})
	for _, id := range orphans {
		rmCtx, rmCancel := context.WithTimeout(ctx, reaperDockerTimeout)
		rmErr := exec.CommandContext(rmCtx, "docker", "rm", "-f", id).Run()
		rmCancel()
		if rmErr != nil {
			continue
		}
		fmt.Fprintf(os.Stderr, "clavesa: removed orphan helper container %s (workspace %s no longer exists)\n", id, byID[id])
		removed = append(removed, id)
	}
	return removed, nil
}
