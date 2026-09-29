package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/vesahyp/clavesa/internal/workspace"
)

// GH #99: a one-shot CLI command (pipeline run, backfill, query, node
// preview, ...) that brought the per-workspace metastore container up left
// it running forever. Only `clavesa ui` ever stopped its metastore, and only
// the one it started. This file is the release side of that lifecycle: the
// process remembers which metastore containers IT created, and at exit
// removes each one that is still the container it created and that nothing
// else is using. The reuse path of EnsureMetastore records nothing, so a
// command that found the container already running (a `clavesa ui` owns it,
// or a `--keep-metastore` run left it warm) never takes it down.

// createdMetastore is what EnsureMetastore records for a container it
// started: the full container ID `docker run -d` printed, and the workspace
// root it serves (needed at release time for the label filter and the run
// lock directory).
type createdMetastore struct {
	id   string
	root string
}

// createdMetastores is the per-process registry of metastore containers
// this process created, keyed by container name. Guarded by
// createdMetastoresMu because EnsureMetastore runs from several goroutines
// (the warm worker's spawn, the UI's startup warmup, run dispatch).
var (
	createdMetastoresMu sync.Mutex
	createdMetastores   = map[string]createdMetastore{}
)

// recordCreatedMetastore registers a container EnsureMetastore just
// started. Called as soon as `docker run -d` succeeds, before the readiness
// wait: a container that never prints the Derby banner is exactly the
// orphan GH #99 is about, and the readiness error already carries the log
// tail the user needs. An empty id (docker printed nothing) is not
// recorded, since release could not verify ownership of it.
func recordCreatedMetastore(name, id, workspaceRoot string) {
	id = strings.TrimSpace(id)
	if name == "" || id == "" {
		return
	}
	createdMetastoresMu.Lock()
	defer createdMetastoresMu.Unlock()
	createdMetastores[name] = createdMetastore{id: id, root: workspaceRoot}
}

// leaseInfo is the subset of a run lock lease (internal/runlock's leaseDoc,
// file backend at `<warehouse>/_locks/<pipeline>.run.json`) the release
// decision needs: who holds it, whether it is still held, and until when.
// The JSON tags mirror runlock's so the file parses without importing that
// package's unexported document type.
type leaseInfo struct {
	Holder struct {
		PID int `json:"pid"`
	} `json:"holder"`
	ExpiresAt time.Time `json:"expires_at"`
	State     string    `json:"state"`
}

// leaseHeldByOther reports whether a lease is a live claim by some other
// process: state "held", not yet expired at now, and held by a PID other
// than ours. A lease for our own PID does not count (the command that is
// exiting is the one that held it), and a released or expired lease is no
// claim at all.
func (l leaseInfo) leaseHeldByOther(now time.Time, pid int) bool {
	return l.State == "held" && l.ExpiresAt.After(now) && l.Holder.PID != pid
}

// readRunLeases parses every `<warehouse>/_locks/*.run.json` lease file.
// A missing lock directory means no leases and is not an error. An
// unreadable or malformed lease IS an error: the caller treats that as
// ambiguity and keeps the container, the same rule the reaper applies to
// a container it cannot attribute.
func readRunLeases(warehouse string) ([]leaseInfo, error) {
	pattern := filepath.Join(warehouse, "_locks", "*.run.json")
	paths, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("glob %s: %w", pattern, err)
	}
	var leases []leaseInfo
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			return nil, fmt.Errorf("read lease %s: %w", p, err)
		}
		var l leaseInfo
		if err := json.Unmarshal(data, &l); err != nil {
			return nil, fmt.Errorf("parse lease %s: %w", p, err)
		}
		leases = append(leases, l)
	}
	return leases, nil
}

// sameContainerID reports whether two docker container IDs name the same
// container. `docker run -d` and `docker inspect -f {{.Id}}` print the full
// 64-hex ID while `docker ps -q` prints the 12-hex short form, so a prefix
// match of at least the short length is accepted in either direction.
// Empty IDs never match anything.
func sameContainerID(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	if len(a) > len(b) {
		a, b = b, a
	}
	return len(a) >= 12 && strings.HasPrefix(b, a)
}

// shouldReleaseMetastore is the pure release decision for one recorded
// metastore container (GH #99). It says yes only when every one of these
// holds:
//
//   - currentID (what the container name resolves to now) is still the
//     recordedID this process created. A `clavesa ui` started in between
//     swept and recreated the container under the same name with a new ID,
//     and that container is the UI's to stop.
//   - otherContainers (the running containers carrying this workspace's
//     label) holds nothing but our own metastore. Our own warm workers and
//     sidecar are already stopped by the time this runs, so anything left
//     belongs to another process that may be dialing the metastore.
//   - no lease in leases is held by another process at now (see
//     leaseHeldByOther): a run in flight elsewhere is using the metastore
//     whether or not its containers are labeled.
//
// Ambiguity (docker errors, unreadable leases) is resolved by the caller
// before this is reached, always in favour of keeping the container.
func shouldReleaseMetastore(recordedID, currentID string, otherContainers []string, leases []leaseInfo, now time.Time, pid int) bool {
	if !sameContainerID(recordedID, currentID) {
		return false
	}
	for _, id := range otherContainers {
		if !sameContainerID(id, recordedID) {
			return false
		}
	}
	for _, l := range leases {
		if l.leaseHeldByOther(now, pid) {
			return false
		}
	}
	return true
}

// dockerContainerID resolves a container name to its full ID via
// `docker inspect -f {{.Id}}`, short-timeout. A missing container or an
// unreachable daemon is an error; the caller keeps its hands off in both
// cases (there is nothing to remove, or it cannot tell).
func dockerContainerID(ctx context.Context, name string) (string, error) {
	inspectCtx, cancel := context.WithTimeout(ctx, reaperDockerTimeout)
	defer cancel()
	out, err := exec.CommandContext(inspectCtx, "docker", "inspect", "-f", "{{.Id}}", name).Output()
	if err != nil {
		return "", fmt.Errorf("docker inspect %s: %w", name, err)
	}
	return strings.TrimSpace(string(out)), nil
}

// runningWorkspaceContainers lists the full IDs of every RUNNING container
// carrying this workspace's label (metastore, warm workers, transpile
// sidecar of any process), short-timeout. `--no-trunc` makes the IDs
// comparable full-to-full with what `docker run -d` printed.
func runningWorkspaceContainers(ctx context.Context, workspaceRoot string) ([]string, error) {
	psCtx, cancel := context.WithTimeout(ctx, reaperDockerTimeout)
	defer cancel()
	out, err := exec.CommandContext(psCtx, "docker", "ps", "-q", "--no-trunc",
		"--filter", "label="+workspaceLabelKV(workspaceRoot)).Output()
	if err != nil {
		return nil, fmt.Errorf("docker ps: %w", err)
	}
	return strings.Fields(string(out)), nil
}

// ReleaseCreatedMetastores removes every metastore container this process
// created (see recordCreatedMetastore) that shouldReleaseMetastore clears,
// and returns the names it removed. It is the exit hook behind the CLI's
// post-command teardown (GH #99): quiet, best-effort, and never removing
// on ambiguity. A docker error on any probe keeps that container; a
// container whose name no longer resolves (a `clavesa ui` already stopped
// it) is skipped the same way. Each docker call is short-timeout so a
// wedged daemon cannot hang command exit; the caller bounds the total via
// ctx. The registry is cleared on entry so a repeat call (Run() in unit
// tests, or the UI's shutdown followed by Execute's) does no docker work.
func ReleaseCreatedMetastores(ctx context.Context) []string {
	createdMetastoresMu.Lock()
	recorded := createdMetastores
	createdMetastores = map[string]createdMetastore{}
	createdMetastoresMu.Unlock()
	if len(recorded) == 0 {
		return nil
	}
	names := make([]string, 0, len(recorded))
	for name := range recorded {
		names = append(names, name)
	}
	sort.Strings(names)

	var removed []string
	for _, name := range names {
		rec := recorded[name]
		currentID, err := dockerContainerID(ctx, name)
		if err != nil {
			continue
		}
		others, err := runningWorkspaceContainers(ctx, rec.root)
		if err != nil {
			continue
		}
		leases, err := readRunLeases(workspace.LocalWarehouseDir(rec.root))
		if err != nil {
			continue
		}
		if !shouldReleaseMetastore(rec.id, currentID, others, leases, time.Now(), os.Getpid()) {
			continue
		}
		// Remove by the recorded ID rather than the name: if a `clavesa ui`
		// recreates the container between the inspect above and this rm,
		// an ID-addressed rm misses the new container instead of taking it
		// down.
		if removeContainer(ctx, rec.id) {
			removed = append(removed, name)
		}
	}
	return removed
}
