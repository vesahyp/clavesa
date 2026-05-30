package service

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// initUpgradeTestWorkspace lays out a workspace whose shell (main.tf +
// variables.tf) is stale, so the shell upgrade has something to rewrite.
// It writes the manifest directly rather than calling workspace.Init to
// avoid the Docker image build Init does; CreatePipeline only needs the
// manifest's catalog field.
func initUpgradeTestWorkspace(t *testing.T) string {
	t.Helper()
	ws := t.TempDir()
	manifest := `{"name":"up-ws","cloud":"aws","version":1,"catalog":"clavesa_up_ws","system_catalog":"clavesa_up_ws_system"}` + "\n"
	if err := os.WriteFile(filepath.Join(ws, "clavesa.json"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	mainTF := `module "workspace" {
  source         = "./.clavesa/modules/v0.1.0/workspace/aws"
  workspace_name = var.workspace_name
}
`
	if err := os.WriteFile(filepath.Join(ws, "main.tf"), []byte(mainTF), 0o644); err != nil {
		t.Fatal(err)
	}
	varsTF := `variable "workspace_name" {
  default = "up-ws"
}

variable "runner_version" {
  description = "Transform runner image version tag (must be built locally before apply)."
  default     = "v0.1.0"
}
`
	if err := os.WriteFile(filepath.Join(ws, "variables.tf"), []byte(varsTF), 0o644); err != nil {
		t.Fatal(err)
	}
	return ws
}

// addListablePipeline creates a pipeline and a transform node in it so
// ListPipelines surfaces it. CreatePipeline alone leaves a node-less
// directory that scanPipelines skips. Uses the real service authoring
// API (AddNode + UpdateNode), not a hand-authored .tf fixture (project
// hard rule).
func addListablePipeline(t *testing.T, svc *Service, name string) {
	t.Helper()
	if _, err := svc.CreatePipeline(name, ""); err != nil {
		t.Fatalf("CreatePipeline %s: %v", name, err)
	}
	if _, err := svc.AddNode(name, "transform", "t1"); err != nil {
		t.Fatalf("AddNode in %s: %v", name, err)
	}
	if _, err := svc.UpdateNode(name, "t1", map[string]interface{}{"sql": "SELECT 1 AS x"}); err != nil {
		t.Fatalf("UpdateNode in %s: %v", name, err)
	}
}

func TestUpgradeWorkspace_ShellOnly(t *testing.T) {
	ws := initUpgradeTestWorkspace(t)
	svc := New(ws)
	if _, err := svc.CreatePipeline("bronze", ""); err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}

	res, err := svc.UpgradeWorkspace("", false)
	if err != nil {
		t.Fatalf("UpgradeWorkspace shell-only: %v", err)
	}
	if res.TargetVersion != ModuleVersion {
		t.Errorf("TargetVersion = %q, want %q", res.TargetVersion, ModuleVersion)
	}
	if res.PrevVersion != "v0.1.0" {
		t.Errorf("PrevVersion = %q, want v0.1.0", res.PrevVersion)
	}
	if res.WorkspaceRewritten == 0 {
		t.Errorf("WorkspaceRewritten = 0, want > 0 (main.tf + variables.tf were stale)")
	}
	if res.Pipelines != nil {
		t.Errorf("Pipelines = %v, want nil for shell-only", res.Pipelines)
	}
}

func TestUpgradeWorkspace_FullWalksEveryPipeline(t *testing.T) {
	ws := initUpgradeTestWorkspace(t)
	svc := New(ws)
	for _, name := range []string{"bronze", "silver"} {
		addListablePipeline(t, svc, name)
	}

	res, err := svc.UpgradeWorkspace("", true)
	if err != nil {
		t.Fatalf("UpgradeWorkspace full: %v", err)
	}
	if len(res.Pipelines) != 2 {
		t.Fatalf("len(Pipelines) = %d, want 2", len(res.Pipelines))
	}
	for _, p := range res.Pipelines {
		if p.Err != "" {
			t.Errorf("pipeline %s reported error: %s", p.Name, p.Err)
		}
		if p.TargetRef != ModuleVersion {
			t.Errorf("pipeline %s TargetRef = %q, want %q", p.Name, p.TargetRef, ModuleVersion)
		}
	}
}

func TestUpgradeWorkspace_ContinueOnError(t *testing.T) {
	ws := initUpgradeTestWorkspace(t)
	svc := New(ws)
	addListablePipeline(t, svc, "good")

	// A "broken" pipeline that ListPipelines surfaces (real transform
	// node, valid HCL) but UpgradePipeline fails on. We create it through
	// the real API, then (a) stale its module-source version so the
	// upgrade actually has a rewrite to perform, and (b) make main.tf
	// read-only so that rewrite's WriteFile fails. Deterministic and
	// independent of graph/topology subtleties. Proves continue-on-error:
	// this row gets Err set while "good" upgrades cleanly and the method
	// returns no error.
	//
	// (A syntactically-broken .tf can't exercise this path — scanPipelines
	// silently drops dirs whose HCL won't parse, so they never get listed.
	// The failure has to come from a step past the parse.)
	addListablePipeline(t, svc, "broken")
	brokenMain := filepath.Join(ws, "broken", "main.tf")
	data, err := os.ReadFile(brokenMain)
	if err != nil {
		t.Fatal(err)
	}
	// Rewrite the embedded module version to a stale one so UpgradePipeline
	// detects a substitution and tries to write the file back.
	staled := []byte(strings.ReplaceAll(string(data), "/modules/"+ModuleVersion+"/", "/modules/v0.1.0/"))
	if string(staled) == string(data) {
		t.Fatalf("could not stale module version in broken/main.tf:\n%s", data)
	}
	if err := os.WriteFile(brokenMain, staled, 0o644); err != nil {
		t.Fatal(err)
	}
	// Make the file read-only so the upgrade's rewrite WriteFile fails.
	if err := os.Chmod(brokenMain, 0o400); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(brokenMain, 0o644) })

	res, err := svc.UpgradeWorkspace("", true)
	if err != nil {
		t.Fatalf("UpgradeWorkspace must not return an error on a per-pipeline failure, got: %v", err)
	}

	var sawBroken, sawGood bool
	for _, p := range res.Pipelines {
		switch p.Name {
		case "broken":
			sawBroken = true
			if p.Err == "" {
				t.Errorf("broken pipeline row has empty Err, want a failure message")
			}
		case "good":
			sawGood = true
			if p.Err != "" {
				t.Errorf("good pipeline failed unexpectedly: %s", p.Err)
			}
		}
	}
	if !sawBroken {
		t.Errorf("broken pipeline not present in results: %+v", res.Pipelines)
	}
	if !sawGood {
		t.Errorf("good pipeline not present in results: %+v", res.Pipelines)
	}
}
