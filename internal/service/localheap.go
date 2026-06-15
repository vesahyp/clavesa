package service

import (
	"context"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// localHeapArgs returns the `-e CLAVESA_JVM_HEAP_MB=…` docker run flag for a
// locally-launched runner container, or nil to leave the runner on its own
// default.
//
// Why this exists (GH #58): the runner's spark-class sizes the Spark JVM heap
// to ~75% of the container's memory limit, but clavesa launches local
// containers *uncapped* (no --memory, no Lambda env), so spark-class finds no
// limit and lands on its 1 GB fallback — every local run is throttled to 1 GB
// even on a 16 GB host. Local compute is clavesa's big-backfill /
// cost-per-billion path, exactly where a 1 GB heap hurts most (a shuffle-heavy
// transform GC-thrashes to a heartbeat-death, the #24 failure mode recurring
// locally).
//
// We size the heap here, on the orchestrating side, rather than capping the
// container with --memory: a hard cap risks OOM-killing a run that was healthy
// before, whereas a larger -Xmx only raises the ceiling (the JVM still commits
// lazily). The size derives from the Docker VM's total memory (the real
// ceiling on macOS, where the host's physical RAM overshoots the VM), not the
// host's, so we never ask for a heap the daemon can't back.
//
// reserveMB is memory to leave for the VM's other tenants. A local-warehouse
// run never has the VM to itself: it shares it with the Derby metastore
// container (~0.3 GB) AND, under `clavesa ui`, a long-lived warm query-worker
// Spark JVM (~1 GB RSS), and back-to-back runs briefly overlap two run
// containers at the run-lock boundary. An over-large heap there is committed
// lazily but a shuffle can grow it toward -Xmx, and 2×(heap+overhead) plus the
// warm worker plus the metastore then exceeds a small Docker Desktop VM and the
// OOM-killer takes out the metastore mid-run. So reserveSharedVMMB is large
// enough to keep the heap modest (a ~2.4 GB heap on a 7.6 GB VM — still 2.4× the
// old 1 GB floor, and it scales up on bigger hosts). The `--compute local`
// dispatcher reads the cloud catalog directly with no co-resident metastore or
// warm worker, and its runs serialize on the warehouse lock, so it reserves
// only OS/daemon slack (reserveStandaloneMB). An explicit CLAVESA_JVM_HEAP_MB
// always wins.
const (
	reserveSharedVMMB   = 4608
	reserveStandaloneMB = 768
)

func localHeapArgs(reserveMB int) []string {
	if v, ok := os.LookupEnv("CLAVESA_JVM_HEAP_MB"); ok && strings.TrimSpace(v) != "" {
		return []string{"-e", "CLAVESA_JVM_HEAP_MB=" + v}
	}
	if mb := autoLocalHeapMB(reserveMB); mb > 0 {
		return []string{"-e", "CLAVESA_JVM_HEAP_MB=" + strconv.Itoa(mb)}
	}
	return nil
}

// autoLocalHeapMB derives a Spark JVM heap (MB) from the Docker VM's total
// memory, or 0 when it can't be determined.
func autoLocalHeapMB(reserveMB int) int {
	return heapFromVMTotalMB(dockerVMTotalMB(), reserveMB)
}

// heapFromVMTotalMB is the pure heap-sizing policy: 75% of what's left after
// the reserve (the remaining 25% covers this container's own
// metaspace/off-heap/python, mirroring spark-class's in-container ratio).
// Returns 0 when the total is unknown or the result wouldn't beat the runner's
// own 1 GB fallback by a worthwhile margin.
func heapFromVMTotalMB(totalMB, reserveMB int) int {
	if totalMB <= 0 {
		return 0
	}
	budget := totalMB - reserveMB
	if budget <= 0 {
		return 0
	}
	heap := budget * 75 / 100
	if heap < 1536 {
		return 0
	}
	return heap
}

var (
	dockerVMTotalOnce sync.Once
	dockerVMTotalVal  int
)

// dockerVMTotalMB returns the Docker daemon's total memory in MB (the VM
// ceiling on Docker Desktop), memoized for the process. 0 on any failure —
// callers treat that as "unknown, leave the heap to the runner default". Uses
// its own short-lived context so a caller's cancelled context can't poison the
// memoized value.
func dockerVMTotalMB() int {
	dockerVMTotalOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, "docker", "info", "--format", "{{.MemTotal}}").Output()
		if err != nil {
			return
		}
		n, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
		if err != nil || n <= 0 {
			return
		}
		dockerVMTotalVal = int(n / 1024 / 1024)
	})
	return dockerVMTotalVal
}
