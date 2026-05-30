package workspace

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeTestWorkspaceForUpgrade lays down the minimal workspace shell that
// Upgrade operates on: a clavesa.json manifest, a main.tf with a stale
// module source line, and a variables.tf with a stale runner_version
// default. No Docker, no modules extraction dependency beyond what
// Upgrade itself drives.
func writeTestWorkspaceForUpgrade(t *testing.T, moduleVer, runnerVer string) string {
	t.Helper()
	root := t.TempDir()
	manifest := `{"name":"up","cloud":"aws","version":1,"catalog":"clavesa_up","system_catalog":"clavesa_up_system"}` + "\n"
	if err := os.WriteFile(filepath.Join(root, "clavesa.json"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	mainTF := `module "workspace" {
  source         = "./.clavesa/modules/` + moduleVer + `/workspace/aws"
}
`
	if err := os.WriteFile(filepath.Join(root, "main.tf"), []byte(mainTF), 0o644); err != nil {
		t.Fatal(err)
	}
	varsTF := `variable "runner_version" {
  description = "Transform runner image version tag (must be built locally before apply)."
  default     = "` + runnerVer + `"
}
`
	if err := os.WriteFile(filepath.Join(root, "variables.tf"), []byte(varsTF), 0o644); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestUpgrade_RewritesRunnerVersionDefault(t *testing.T) {
	root := writeTestWorkspaceForUpgrade(t, "v0.1.0", "v0.1.0")
	prev, rewritten, err := Upgrade(root, "v2.3.0")
	if err != nil {
		t.Fatalf("Upgrade: %v", err)
	}
	if prev != "v0.1.0" {
		t.Errorf("prev = %q, want v0.1.0", prev)
	}
	if rewritten < 2 {
		t.Errorf("rewritten = %d, want >= 2 (main.tf + variables.tf)", rewritten)
	}
	varsData, err := os.ReadFile(filepath.Join(root, "variables.tf"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(varsData), `default     = "v2.3.0"`) {
		t.Errorf("variables.tf runner_version not bumped:\n%s", varsData)
	}
	if strings.Contains(string(varsData), "v0.1.0") {
		t.Errorf("variables.tf still has old version:\n%s", varsData)
	}
	// Idempotent: a second Upgrade at the same target rewrites nothing.
	_, rewritten2, err := Upgrade(root, "v2.3.0")
	if err != nil {
		t.Fatalf("second Upgrade: %v", err)
	}
	if rewritten2 != 0 {
		t.Errorf("second Upgrade rewritten = %d, want 0 (idempotent)", rewritten2)
	}
}
