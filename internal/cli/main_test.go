package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestMain keeps the package's tests off the real current-workspace file
// ($XDG_CONFIG_HOME/clavesa/current-workspace, else ~/.config/...).
// `workspace init` and `workspace use` record the workspace there, and
// parallel tests cannot t.Setenv: before this, every parallel init wrote
// the developer's real file (this Mac's pointed at a deleted test temp
// dir), and a parallel test that resolved its workspace through the file
// could write into another test's temp dir while it was being removed
// ("TempDir RemoveAll cleanup: directory not empty", public CI on
// v2.27.9 and v2.27.13).
//
// XDG_CONFIG_HOME points at a temp dir in which "clavesa" is a plain
// file, so the state file can be neither written nor read: init and use
// print their best-effort "could not record current workspace" note,
// and resolution never finds a workspace through it. A test that needs
// the state file calls withXDGConfig, which gives it a directory of its
// own (t.Setenv, so such a test cannot be parallel).
func TestMain(m *testing.M) {
	os.Exit(runIsolated(m))
}

func runIsolated(m *testing.M) int {
	dir, err := os.MkdirTemp("", "clavesa-cli-test-xdg-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "TestMain: temp XDG_CONFIG_HOME:", err)
		return 1
	}
	defer os.RemoveAll(dir)
	if err := os.WriteFile(filepath.Join(dir, "clavesa"), []byte("tests: no state file here\n"), 0o444); err != nil {
		fmt.Fprintln(os.Stderr, "TestMain: block the state dir:", err)
		return 1
	}
	os.Setenv("XDG_CONFIG_HOME", dir)
	return m.Run()
}
