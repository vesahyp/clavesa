package cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
)

// newResolveTestCmd returns a fresh cobra.Command with the same
// `--workspace` persistent flag the real CLI exposes. The tests below
// use it as the `cmd` argument to resolveWorkspaceRoot — they assert
// the precedence rules, so a barebones command tree is enough.
func newResolveTestCmd() *cobra.Command {
	c := &cobra.Command{Use: "test"}
	c.PersistentFlags().String("workspace", "", "")
	return c
}

// writeManifest plants a minimal clavesa.json so walkUpForManifest +
// readWorkspaceStateFile + the state-file Stat probe accept the dir.
func writeManifest(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(dir, "clavesa.json"),
		[]byte(`{"name":"test","cloud":"local","version":1}`+"\n"),
		0o644,
	); err != nil {
		t.Fatal(err)
	}
}

// withChdir cd's into dir for the test and restores the original cwd
// on cleanup. Caller is responsible for not invoking this from a
// t.Parallel test — cwd is process-global.
func withChdir(t *testing.T, dir string) {
	t.Helper()
	orig, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(orig) })
}

// withEnv sets an env var for the test and restores it on cleanup.
func withEnv(t *testing.T, key, val string) {
	t.Helper()
	orig, had := os.LookupEnv(key)
	if err := os.Setenv(key, val); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if had {
			_ = os.Setenv(key, orig)
		} else {
			_ = os.Unsetenv(key)
		}
	})
}

// withXDGConfig points $XDG_CONFIG_HOME at the test's tempdir so
// writeWorkspaceStateFile + workspaceStateFile target a per-test
// location, never touching the real ~/.config/clavesa/.
func withXDGConfig(t *testing.T) {
	t.Helper()
	withEnv(t, "XDG_CONFIG_HOME", t.TempDir())
}

// resolveWorkspaceRoot precedence: cwd-walk for clavesa.json must
// beat the global state file. Standing inside a workspace operates on
// that workspace; the state file is only the fallback for when you
// aren't inside any. Pre-fix, the order was reversed and
// `workspace destroy` from inside one workspace targeted whatever
// was last-used.
func TestResolveWorkspaceRoot_CwdWalkBeatsStateFile(t *testing.T) {
	withXDGConfig(t)
	// Unset to be sure the env var doesn't shadow the cwd-walk.
	withEnv(t, "CLAVESA_WORKSPACE", "")
	_ = os.Unsetenv("CLAVESA_WORKSPACE")

	cwdWS := t.TempDir()
	writeManifest(t, cwdWS)

	stateWS := t.TempDir()
	writeManifest(t, stateWS)
	if err := writeWorkspaceStateFile(stateWS); err != nil {
		t.Fatalf("seed state file: %v", err)
	}

	withChdir(t, cwdWS)

	got, err := resolveWorkspaceRoot(newResolveTestCmd())
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	// On macOS /tmp is a symlink to /private/tmp; compare via EvalSymlinks.
	want, _ := filepath.EvalSymlinks(cwdWS)
	gotR, _ := filepath.EvalSymlinks(got)
	if gotR != want {
		t.Fatalf("resolved = %q, want cwd workspace %q", gotR, want)
	}
}

// When cwd is NOT inside any workspace, the state file is the right
// fallback. The current-workspace pin keeps the CLI usable across
// terminals; this asserts the cwd-walk doesn't kill that path.
func TestResolveWorkspaceRoot_StateFileWhenCwdNotAWorkspace(t *testing.T) {
	withXDGConfig(t)
	_ = os.Unsetenv("CLAVESA_WORKSPACE")

	bareCwd := t.TempDir() // no clavesa.json, no parent walk hit
	stateWS := t.TempDir()
	writeManifest(t, stateWS)
	if err := writeWorkspaceStateFile(stateWS); err != nil {
		t.Fatalf("seed state file: %v", err)
	}

	withChdir(t, bareCwd)

	got, err := resolveWorkspaceRoot(newResolveTestCmd())
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	want, _ := filepath.EvalSymlinks(stateWS)
	gotR, _ := filepath.EvalSymlinks(got)
	if gotR != want {
		t.Fatalf("resolved = %q, want state-file workspace %q", gotR, want)
	}
}

// --workspace flag is the explicit override and beats everything,
// including a cwd that is itself a workspace.
func TestResolveWorkspaceRoot_FlagBeatsAll(t *testing.T) {
	withXDGConfig(t)

	flagWS := t.TempDir()
	writeManifest(t, flagWS)

	cwdWS := t.TempDir()
	writeManifest(t, cwdWS)

	envWS := t.TempDir()
	writeManifest(t, envWS)
	withEnv(t, "CLAVESA_WORKSPACE", envWS)

	stateWS := t.TempDir()
	writeManifest(t, stateWS)
	if err := writeWorkspaceStateFile(stateWS); err != nil {
		t.Fatalf("seed state file: %v", err)
	}

	withChdir(t, cwdWS)

	cmd := newResolveTestCmd()
	if err := cmd.ParseFlags([]string{"--workspace", flagWS}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	got, err := resolveWorkspaceRoot(cmd)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	want, _ := filepath.EvalSymlinks(flagWS)
	gotR, _ := filepath.EvalSymlinks(got)
	if gotR != want {
		t.Fatalf("resolved = %q, want --workspace flag value %q", gotR, want)
	}
}

// $CLAVESA_WORKSPACE beats the cwd-walk and the state file (the
// per-shell pin is more specific than either), and with no --workspace
// flag set, the env var wins.
func TestResolveWorkspaceRoot_EnvBeatsCwdWalkAndStateFile(t *testing.T) {
	withXDGConfig(t)

	envWS := t.TempDir()
	writeManifest(t, envWS)
	withEnv(t, "CLAVESA_WORKSPACE", envWS)

	cwdWS := t.TempDir()
	writeManifest(t, cwdWS)

	stateWS := t.TempDir()
	writeManifest(t, stateWS)
	if err := writeWorkspaceStateFile(stateWS); err != nil {
		t.Fatalf("seed state file: %v", err)
	}

	withChdir(t, cwdWS)

	got, err := resolveWorkspaceRoot(newResolveTestCmd())
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	want, _ := filepath.EvalSymlinks(envWS)
	gotR, _ := filepath.EvalSymlinks(got)
	if gotR != want {
		t.Fatalf("resolved = %q, want $CLAVESA_WORKSPACE value %q", gotR, want)
	}
}
