package observability

import (
	"fmt"
	"regexp"
	"testing"
)

// dockerObjectName is the charset docker allows for container / network
// names: must start alphanumeric, then alphanumerics plus _.- . Our
// derived names must satisfy it (the raw workspace path doesn't, which is
// why we hash).
var dockerObjectName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)

// TestMetastoreNamesStable checks the derived names are deterministic in
// the workspace path: same path → identical names across calls.
func TestMetastoreNamesStable(t *testing.T) {
	const ws = "/Users/vesa/some/workspace"
	if a, b := metastoreNetworkName(ws), metastoreNetworkName(ws); a != b {
		t.Fatalf("network name not stable: %q vs %q", a, b)
	}
	if a, b := metastoreContainerName(ws), metastoreContainerName(ws); a != b {
		t.Fatalf("container name not stable: %q vs %q", a, b)
	}
	if a, b := MetastoreAddr(ws), MetastoreAddr(ws); a != b {
		t.Fatalf("addr not stable: %q vs %q", a, b)
	}
}

// TestMetastoreNamesDistinct checks different workspace paths derive
// different names, so two workspaces can't collide on the same container
// or network.
func TestMetastoreNamesDistinct(t *testing.T) {
	a := "/Users/vesa/workspace-a"
	b := "/Users/vesa/workspace-b"
	if metastoreNetworkName(a) == metastoreNetworkName(b) {
		t.Fatalf("distinct paths share a network name: %q", metastoreNetworkName(a))
	}
	if metastoreContainerName(a) == metastoreContainerName(b) {
		t.Fatalf("distinct paths share a container name: %q", metastoreContainerName(a))
	}
	if MetastoreAddr(a) == MetastoreAddr(b) {
		t.Fatalf("distinct paths share an addr: %q", MetastoreAddr(a))
	}
}

// TestMetastoreNamesDockerSafe checks the derived network / container
// names satisfy docker's name charset even when the workspace path is
// full of characters docker rejects (slashes, spaces, dots).
func TestMetastoreNamesDockerSafe(t *testing.T) {
	paths := []string{
		"/Users/vesa/Repositories/clavesa dev",
		"/tmp/clavesa-verify-metastore",
		"relative/path",
		"/with.dots/and-dashes_under",
	}
	for _, p := range paths {
		for _, name := range []string{metastoreNetworkName(p), metastoreContainerName(p)} {
			if !dockerObjectName.MatchString(name) {
				t.Errorf("name %q (from %q) is not docker-safe", name, p)
			}
		}
	}
}

// TestMetastoreAddrShape checks the addr is exactly
// "<containerName>:1527" — the value clients put in
// CLAVESA_METASTORE_ADDR.
func TestMetastoreAddrShape(t *testing.T) {
	const ws = "/Users/vesa/workspace"
	want := fmt.Sprintf("%s:%d", metastoreContainerName(ws), metastorePort)
	if got := MetastoreAddr(ws); got != want {
		t.Fatalf("MetastoreAddr = %q, want %q", got, want)
	}
	if metastorePort != 1527 {
		t.Fatalf("metastorePort = %d, want 1527", metastorePort)
	}
}

// TestMetastoreNetworkMatchesPrivate checks the exported MetastoreNetwork
// wrapper returns exactly the private metastoreNetworkName value — the
// service / preview packages rely on this to build `--network` args that
// land the client on the same network EnsureMetastore created.
func TestMetastoreNetworkMatchesPrivate(t *testing.T) {
	for _, ws := range []string{"/Users/vesa/workspace", "/tmp/clavesa-x", "relative/path"} {
		if got, want := MetastoreNetwork(ws), metastoreNetworkName(ws); got != want {
			t.Fatalf("MetastoreNetwork(%q) = %q, want %q", ws, got, want)
		}
	}
}

// TestWorkspaceRootForWarehouse checks the warehouse → workspace-root
// recovery inverts LocalWarehouseDir's `<root>/.clavesa/warehouse` layout
// and returns "" for empty input (so callers skip the metastore wiring).
func TestWorkspaceRootForWarehouse(t *testing.T) {
	root := "/Users/vesa/ws"
	wh := root + "/.clavesa/warehouse"
	if got := workspaceRootForWarehouse(wh); got != root {
		t.Fatalf("workspaceRootForWarehouse(%q) = %q, want %q", wh, got, root)
	}
	if got := workspaceRootForWarehouse(""); got != "" {
		t.Fatalf("workspaceRootForWarehouse(\"\") = %q, want \"\"", got)
	}
}

// TestMetastoreLogReady exercises the pure readiness-parse helper:
// case-insensitive substring match on the Derby banner, no false
// positive on unrelated log noise.
func TestMetastoreLogReady(t *testing.T) {
	cases := []struct {
		name string
		logs string
		want bool
	}{
		{"derby banner lowercase", "apache derby network server - 10.x started and ready to accept connections on port 1527", true},
		{"derby banner mixed case", "Apache Derby Network Server started and Ready To Accept Connections on port 1527", true},
		{"booting only", "Apache Derby Network Server starting on port 1527", false},
		{"empty", "", false},
		{"unrelated noise", "WARN  org.apache.spark something something", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := metastoreLogReady(c.logs); got != c.want {
				t.Fatalf("metastoreLogReady(%q) = %v, want %v", c.logs, got, c.want)
			}
		})
	}
}
