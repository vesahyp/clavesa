package cli

import (
	"bytes"
	"strings"
	"testing"
)

// TestDeployHelp / TestPlanHelp confirm the top-level deploy/plan commands
// render their --help without error (no AWS, no terraform).
func TestDeployHelp(t *testing.T) {
	t.Parallel()
	cmd := newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"deploy", "--help"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("deploy --help: %v", err)
	}
	s := out.String()
	for _, want := range []string{"--yes", "--plan-only", "workspace"} {
		if !strings.Contains(s, want) {
			t.Errorf("deploy --help missing %q\n%s", want, s)
		}
	}
}

func TestPlanHelp(t *testing.T) {
	t.Parallel()
	cmd := newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"plan", "--help"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("plan --help: %v", err)
	}
	s := out.String()
	if !strings.Contains(s, "Plan the whole workspace") {
		t.Errorf("plan --help missing long text\n%s", s)
	}
	// plan has no apply, so no confirmation flag.
	if strings.Contains(s, "--yes") {
		t.Errorf("plan --help should not advertise --yes\n%s", s)
	}
}

// TestDeployNoWorkspace / TestPlanNoWorkspace confirm a clean error when run
// against a dir with no clavesa.json — the workspace deployFlow preflight
// short-circuits before any terraform/AWS call.
func TestDeployNoWorkspace(t *testing.T) {
	t.Parallel()
	ws := t.TempDir() // no clavesa.json
	err := Run([]string{"deploy", "--workspace", ws})
	if err == nil {
		t.Fatal("deploy outside a workspace: expected an error")
	}
	if !strings.Contains(err.Error(), "not a clavesa workspace") {
		t.Errorf("deploy error = %q, want it to mention 'not a clavesa workspace'", err)
	}
}

func TestPlanNoWorkspace(t *testing.T) {
	t.Parallel()
	ws := t.TempDir() // no clavesa.json
	err := Run([]string{"plan", "--workspace", ws})
	if err == nil {
		t.Fatal("plan outside a workspace: expected an error")
	}
	if !strings.Contains(err.Error(), "not a clavesa workspace") {
		t.Errorf("plan error = %q, want it to mention 'not a clavesa workspace'", err)
	}
}
