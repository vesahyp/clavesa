package observability

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/vesahyp/clavesa/internal/runner"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// SLICE 2: the local Derby metastore as a shared network service — the
// local analog of cloud's shared Glue Data Catalog.
//
// A per-workspace Derby Network Server container ("metastore") owns
// <warehouse>/_metastore/metastore_db and serves it over JDBC inside a
// user-defined docker network. Local Spark CLIENT containers (the warm
// query worker, pipeline-run, preview, one-shot query, notebooks — wired
// in LATER slices) reach it by docker-network DNS rather than each
// embedding its own Derby, which is what lets them run side-by-side
// without the single-JVM Derby lock that an embedded metastore forces.
//
// This file is the lifecycle layer only: deterministic names, an
// idempotent Ensure, and best-effort sweep/stop. It is NOT wired into
// ui.go / run.go / the warm worker yet (later slices); it just provides
// the package-level API + tests.

// metastorePort is the JDBC port Derby Network Server listens on inside
// the container. It is NOT host-published — clients reach it by
// docker-network DNS at <containerName>:<metastorePort>.
const metastorePort = 1527

// metastoreReadyLog is the case-insensitive substring of the Derby
// Network Server banner line that signals it is accepting JDBC
// connections (full line: "… started and ready to accept connections on
// port 1527"). We poll `docker logs` for it rather than a host TCP probe
// because the metastore has no host-published port.
const metastoreReadyLog = "ready to accept connections"

// metastoreReadyTimeout bounds how long EnsureMetastore waits for the
// Derby banner before giving up and returning the log tail. Derby boots
// in a couple of seconds; the generous ceiling absorbs a cold image pull
// or a busy daemon.
const metastoreReadyTimeout = 60 * time.Second

// workspaceShortHash returns a stable docker-safe short hash of the
// absolute workspace path. Keyed names let multiple workspaces coexist
// and let later slices recompute the same container/network/addr without
// calling Ensure. The raw path can't be used directly — docker object
// names are limited to [a-zA-Z0-9_.-] — so we hash the absolute path and
// take the first 12 hex of its sha256.
//
// Abs() failures (an unresolvable cwd) fall back to the input as-is; the
// hash is still deterministic for a given string, which is all the
// naming contract requires.
func workspaceShortHash(workspaceRoot string) string {
	abs, err := filepath.Abs(workspaceRoot)
	if err != nil {
		abs = workspaceRoot
	}
	sum := sha256.Sum256([]byte(abs))
	return hex.EncodeToString(sum[:])[:12]
}

// metastoreNetworkName returns the per-workspace user-defined docker
// network name. Deterministic in the workspace path so the metastore and
// every later client container join the same network without a shared
// lookup.
func metastoreNetworkName(workspaceRoot string) string {
	return "clavesa-net-" + workspaceShortHash(workspaceRoot)
}

// metastoreContainerName returns the per-workspace metastore container
// name. Also the in-network DNS name clients dial (see MetastoreAddr).
func metastoreContainerName(workspaceRoot string) string {
	return "clavesa-metastore-" + workspaceShortHash(workspaceRoot)
}

// MetastoreNetwork returns the per-workspace docker network name a local
// Spark client container must join (`--network <name>`) to reach the
// metastore by its in-network DNS address. Pure — computed from the
// workspace path, no docker call — so the service / preview packages can
// build a client container's run args without importing the private
// naming helper. Pair it with MetastoreAddr (the CLAVESA_METASTORE_ADDR
// value) and an EnsureMetastore call that guarantees something is
// actually listening on that network.
func MetastoreNetwork(workspaceRoot string) string {
	return metastoreNetworkName(workspaceRoot)
}

// workspaceRootForWarehouse recovers the workspace root from a warehouse
// directory. LocalWarehouseDir lays the warehouse out at
// `<root>/.clavesa/warehouse`, so the root is two directories up. Used by
// the query / preview client launch sites that only carry the warehouse
// path (not the workspace root) to key the metastore the same way Ensure
// did. Empty input returns empty so callers can skip the metastore wiring.
func workspaceRootForWarehouse(warehouse string) string {
	if strings.TrimSpace(warehouse) == "" {
		return ""
	}
	return filepath.Dir(filepath.Dir(warehouse))
}

// EnsureMetastoreForWarehouse is the warehouse-keyed convenience the
// preview package uses: it recovers the workspace root from the warehouse
// path (LocalWarehouseDir's `<root>/.clavesa/warehouse` layout), loads the
// workspace name to pick the metastore image, ensures the metastore, and
// returns the (network, addr) pair a client container joins. Both are ""
// when the warehouse path can't be mapped to a workspace or Ensure fails,
// so the caller falls back to embedded Derby. Best-effort by contract.
func EnsureMetastoreForWarehouse(ctx context.Context, warehouse string) (network, addr string) {
	root := workspaceRootForWarehouse(warehouse)
	if root == "" {
		return "", ""
	}
	name := ""
	if m, _ := workspace.Load(root); m != nil {
		name = m.Name
	}
	a, err := EnsureMetastore(ctx, root, name)
	if err != nil {
		return "", ""
	}
	return MetastoreNetwork(root), a
}

// MetastoreAddr returns the in-network DNS address clients put in
// CLAVESA_METASTORE_ADDR ("<containerName>:1527"). Pure — computed from
// the workspace path, no docker call — so later slices can wire a client
// container's env without first calling EnsureMetastore (Ensure is what
// guarantees something is actually listening at that address).
func MetastoreAddr(workspaceRoot string) string {
	return fmt.Sprintf("%s:%d", metastoreContainerName(workspaceRoot), metastorePort)
}

// metastoreImage resolves the workspace-scoped runner image tag the
// metastore container runs (it REUSES the runner image, selecting Derby
// Network Server mode via CLAVESA_METASTORE_SERVER=1). Mirrors
// persistentDockerQueryRunner.resolveImage: lazy, fresh per call, falls
// back to the empty-name image when the workspace manifest isn't
// readable yet.
func metastoreImage(workspaceName string) string {
	return runner.LocalImageName(workspaceName) + ":latest"
}

// EnsureMetastore brings up (or reuses) the per-workspace Derby metastore
// container and returns the in-network address clients dial. Idempotent:
//
//   - Ensures the per-workspace docker network exists (tolerating a race
//     where two callers create it concurrently).
//   - If the container is already running, returns its addr immediately.
//   - If it exists but stopped, removes it and recreates (a half-dead
//     container would otherwise hold the Derby lock without serving).
//   - Starts it, then polls `docker logs` for Derby's "ready to accept
//     connections" banner with a bounded timeout, returning the log tail
//     on failure.
//
// Best-effort by contract: every failure is a real error the caller can
// surface, but an empty/whitespace workspaceRoot is a programming error
// and returns immediately without touching docker.
func EnsureMetastore(ctx context.Context, workspaceRoot, workspaceName string) (string, error) {
	if strings.TrimSpace(workspaceRoot) == "" {
		return "", fmt.Errorf("metastore: empty workspaceRoot")
	}
	net := metastoreNetworkName(workspaceRoot)
	name := metastoreContainerName(workspaceRoot)

	if err := ensureMetastoreNetwork(ctx, net); err != nil {
		return "", err
	}

	switch metastoreContainerStatus(ctx, name) {
	case "running":
		// Already serving — reuse it.
		return MetastoreAddr(workspaceRoot), nil
	case "stopped":
		// Exists but not serving. A stopped container still holds the
		// container name (so `docker run --name` would clash) and may
		// have left the Derby db lock in an ambiguous state; remove and
		// recreate from scratch.
		_ = exec.Command("docker", "rm", "-f", name).Run()
	}

	warehouse := workspace.LocalWarehouseDir(workspaceRoot)
	// The Derby db lives under the same warehouse the runner/query
	// containers mount, at <warehouse>/_metastore/metastore_db. Ensure
	// the warehouse root exists so the bind mount has a host directory to
	// project (docker would otherwise create it root-owned).
	if err := os.MkdirAll(warehouse, 0o755); err != nil {
		return "", fmt.Errorf("create warehouse dir: %w", err)
	}

	args := []string{
		"run", "-d",
		"--name", name,
		"--network", net,
		// Stable in-network alias so clients can dial a known name even
		// if the container name convention ever shifts. The container
		// name already resolves on a user-defined network; the explicit
		// alias is belt-and-suspenders and matches MetastoreAddr.
		"--network-alias", name,
		"-e", "CLAVESA_METASTORE_SERVER=1",
		"-e", "CLAVESA_WAREHOUSE=" + warehouse,
		// Mount the warehouse at the SAME absolute path it has on the
		// host (matches run.go / local_query_warm.go), so the Derby db
		// path the metastore opens is identical to the path clients
		// reference. The metastore needs no AWS creds — it only serves
		// the local _metastore dir — so we deliberately omit the ~/.aws
		// mount and AWS_* passthrough the transform runner uses.
		"-v", warehouse + ":" + warehouse,
		metastoreImage(workspaceName),
	}
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("docker run metastore: %w\nstderr: %s", err, stderr.String())
	}

	if err := waitMetastoreReady(ctx, name, metastoreReadyTimeout); err != nil {
		logs := dockerTailLogs(name, 30)
		// Leave the container in place on a readiness failure so the
		// caller (and the user) can `docker logs` it; StopMetastore /
		// the next EnsureMetastore's stopped-container branch will reap
		// it. Returning the tail makes the timeout actionable.
		return "", fmt.Errorf("metastore %s: %w\ncontainer logs (tail):\n%s", name, err, logs)
	}
	return MetastoreAddr(workspaceRoot), nil
}

// ensureMetastoreNetwork creates the per-workspace user-defined network
// if it doesn't already exist. Tolerates the concurrent-create race: two
// callers can both see "absent" and both issue `docker network create`;
// the loser gets an "already exists" error that we treat as success.
func ensureMetastoreNetwork(ctx context.Context, net string) error {
	if exec.CommandContext(ctx, "docker", "network", "inspect", net).Run() == nil {
		return nil
	}
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "docker", "network", "create", net)
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		// Lost the create race (or a stale inspect) — re-inspect; if the
		// network is present now, the create "failure" is benign.
		if strings.Contains(stderr.String(), "already exists") {
			return nil
		}
		if exec.CommandContext(ctx, "docker", "network", "inspect", net).Run() == nil {
			return nil
		}
		return fmt.Errorf("docker network create %s: %w\nstderr: %s", net, err, stderr.String())
	}
	return nil
}

// metastoreContainerStatus reports whether the named container is
// "running", "stopped" (exists but not running), or "" (absent / docker
// unreachable). Uses `docker inspect -f {{.State.Running}}`: a non-zero
// exit means no such container, which we map to "".
func metastoreContainerStatus(ctx context.Context, name string) string {
	out, err := exec.CommandContext(ctx, "docker", "inspect", "-f", "{{.State.Running}}", name).Output()
	if err != nil {
		return ""
	}
	if strings.TrimSpace(string(out)) == "true" {
		return "running"
	}
	return "stopped"
}

// waitMetastoreReady polls `docker logs` for the Derby "ready to accept
// connections" banner with a bounded timeout and a short interval,
// respecting ctx. Returns nil once the banner appears, ctx.Err() on
// cancellation, or a timeout error otherwise. We poll logs (not a host
// TCP probe) because the metastore port is not host-published.
func waitMetastoreReady(ctx context.Context, name string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if metastoreLogReady(dockerTailLogs(name, 200)) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("did not become ready within %s", timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
}

// metastoreLogReady reports whether a chunk of Derby Network Server logs
// contains the "ready to accept connections" banner. Factored out as a
// pure helper so the readiness parse can be unit-tested without docker.
// Case-insensitive — Derby's exact casing has shifted across versions.
func metastoreLogReady(logs string) bool {
	return strings.Contains(strings.ToLower(logs), metastoreReadyLog)
}

// SweepMetastores stops and removes any stale metastore container for
// this workspace, best-effort. Called at startup/init (mirrors
// SweepWarmWorkers) so a SIGKILL'd prior session doesn't leave a
// container holding the Derby db lock and blocking a fresh Ensure.
//
// The per-workspace network is intentionally left in place: it is cheap
// (no running process, no disk), the next EnsureMetastore reuses it, and
// removing it would race any client container still attached from a
// half-torn-down prior session. StopMetastore / SweepMetastores own the
// container's lifecycle; the network outlives them.
func SweepMetastores(workspaceRoot string) {
	if strings.TrimSpace(workspaceRoot) == "" {
		return
	}
	name := metastoreContainerName(workspaceRoot)
	// `docker rm -f` removes whether running or stopped; a missing
	// container is a benign non-zero exit we ignore.
	out, err := exec.Command("docker", "ps", "-aq", "--filter", "name=^/"+name+"$").Output()
	if err != nil {
		// Docker not running / not installed — same condition the rest of
		// the local provider tolerates.
		return
	}
	if len(strings.Fields(string(out))) == 0 {
		return
	}
	fmt.Fprintf(os.Stderr, "clavesa: removing stale metastore container from prior session\n")
	_ = exec.Command("docker", "rm", "-f", name).Run()
}

// StopMetastore removes the per-workspace metastore container, for
// explicit teardown (e.g. `clavesa ui` shutdown in a later slice).
// Best-effort: a missing container or unreachable daemon is a no-op. The
// network is left in place for the same reason SweepMetastores keeps it.
func StopMetastore(workspaceRoot string) {
	if strings.TrimSpace(workspaceRoot) == "" {
		return
	}
	_ = exec.Command("docker", "rm", "-f", metastoreContainerName(workspaceRoot)).Run()
}
