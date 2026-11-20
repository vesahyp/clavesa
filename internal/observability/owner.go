package observability

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/vesahyp/clavesa/internal/workspace"
)

// Owner labels (docker-leftovers review, 2026-11-18): every helper
// container a process starts carries clavesa.owner=<pid>:<start time>, and
// a container whose owner process is gone is removed. This covers every
// way a process can end without its cleanup (SIGTERM, a test timeout
// panic, an OOM kill, os.Exit, a program that never calls the release),
// instead of patching one exit path at a time. A metastore kept on
// purpose (--keep-metastore, CLAVESA_KEEP_METASTORE) gets no owner label,
// and a metastore another process still uses is kept by the same rule its
// creator's release follows (shouldReleaseMetastore).
//
// Known limit: the pid is the host's. A clavesa that runs inside a
// container with the docker socket mounted labels with its namespace pid,
// which the host cannot check; no such setup runs today.
const ownerLabel = "clavesa.owner"

// keepMetastore is set by the CLI when the command keeps its metastore
// warm (--keep-metastore or CLAVESA_KEEP_METASTORE): such a metastore has
// no owner, so it outlives the process on purpose. Atomic: the CLI's unit
// tests run commands in parallel, each setting it.
var keepMetastore atomic.Bool

// SetKeepMetastore records whether this process keeps its metastore warm.
func SetKeepMetastore(keep bool) { keepMetastore.Store(keep) }

// KeepMetastore reports what SetKeepMetastore last recorded.
func KeepMetastore() bool { return keepMetastore.Load() }

var (
	selfOwnerOnce sync.Once
	selfOwner     string
)

// processStart returns a process's start time as `ps -o lstart=` prints
// it in the C locale and UTC ("Wed Nov 18 05:50:12 2026"), the same text
// whatever the caller's TZ. found is false only when the kernel says the
// pid does not exist (kill -0 gives ESRCH); every other failure is an
// error, which callers treat as "alive" (never remove on ambiguity).
func processStart(pid int) (start string, found bool, err error) {
	if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
		return "", false, nil
	}
	cmd := exec.Command("ps", "-o", "lstart=", "-p", strconv.Itoa(pid))
	cmd.Env = append(os.Environ(), "LC_ALL=C", "TZ=UTC")
	out, err := cmd.Output()
	if err != nil {
		return "", false, err
	}
	start = strings.TrimSpace(string(out))
	if start == "" {
		return "", false, fmt.Errorf("ps printed no start time for pid %d", pid)
	}
	return start, true, nil
}

// ownerLabelKV is this process's `clavesa.owner=<pid>:<start>` label, or
// "" when its start time cannot be read (then the container is unowned
// and only the older reaper rules apply).
func ownerLabelKV() string {
	selfOwnerOnce.Do(func() {
		start, found, err := processStart(os.Getpid())
		if err == nil && found && start != "" {
			selfOwner = fmt.Sprintf("%s=%d:%s", ownerLabel, os.Getpid(), start)
		}
	})
	return selfOwner
}

// ownerAlive is how deadOwnerIDs asks whether an owner still runs; the
// real one is processAliveAs.
type ownerAlive func(pid int, start string) bool

// processAliveAs reports whether pid runs and started at start. A reused
// pid (another start time) counts as dead; an unreadable answer as alive.
func processAliveAs(pid int, start string) bool {
	got, found, err := processStart(pid)
	if err != nil {
		return true
	}
	return found && got == start
}

// parseOwner splits an owner label value "<pid>:<start>".
func parseOwner(v string) (pid int, start string, ok bool) {
	pidStr, start, cut := strings.Cut(v, ":")
	pid, err := strconv.Atoi(pidStr)
	if !cut || err != nil || pid <= 0 || start == "" {
		return 0, "", false
	}
	return pid, start, true
}

// deadOwnerIDs picks the containers whose owner label names a process
// that is gone. byID maps a container id to its label value
// "<pid>:<start>"; a value that does not parse is kept. Sorted. Pure.
func deadOwnerIDs(byID map[string]string, alive ownerAlive) []string {
	var ids []string
	for id, v := range byID {
		pid, start, ok := parseOwner(v)
		if ok && !alive(pid, start) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

// stillInUse applies the creator's release rule (shouldReleaseMetastore)
// to a container whose owner died: another running container of its
// workspace, or a run lease held by another process, keeps it. A docker or
// lease error keeps it too.
func stillInUse(ctx context.Context, id, root string, ownerPID int) bool {
	if root == "" {
		return true
	}
	others, err := runningWorkspaceContainers(ctx, root)
	if err != nil {
		return true
	}
	leases, err := readRunLeases(workspace.LocalWarehouseDir(root))
	if err != nil {
		return true
	}
	return !shouldReleaseMetastore(id, id, others, leases, time.Now(), ownerPID)
}

// ReapDeadOwnerContainers removes every helper container whose owner
// process is gone. Unthrottled: one `docker ps` plus one `ps` per owned
// container. Best-effort; returns the ids it removed.
func ReapDeadOwnerContainers(ctx context.Context) ([]string, error) {
	psCtx, cancel := context.WithTimeout(ctx, reaperDockerTimeout)
	defer cancel()
	out, err := exec.CommandContext(psCtx, "docker", "ps", "-a",
		"--filter", "label="+ownerLabel,
		"--format", "{{.ID}}\t{{.Label \""+ownerLabel+"\"}}\t{{.Label \""+workspaceLabel+"\"}}").Output()
	if err != nil {
		return nil, fmt.Errorf("docker ps: %w", err)
	}
	owners, roots := map[string]string{}, map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		f := strings.Split(line, "\t")
		if len(f) == 3 && f[0] != "" {
			owners[f[0]], roots[f[0]] = f[1], f[2]
		}
	}
	var removed []string
	for _, id := range deadOwnerIDs(owners, processAliveAs) {
		pid, _, _ := parseOwner(owners[id])
		if stillInUse(ctx, id, roots[id], pid) {
			continue
		}
		if removeContainer(ctx, id) {
			removed = append(removed, id)
		}
	}
	return removed, nil
}
