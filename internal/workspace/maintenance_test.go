package workspace

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestScaffoldMaintenancePipeline(t *testing.T) {
	root := t.TempDir()
	if err := scaffoldMaintenancePipeline(root, "clavesa_demo", "clavesa_demo_system", "v9.9.9"); err != nil {
		t.Fatalf("scaffold: %v", err)
	}

	dir := filepath.Join(root, MaintenancePipelineDir)
	mainTF, err := os.ReadFile(filepath.Join(dir, "main.tf"))
	if err != nil {
		t.Fatalf("read main.tf: %v", err)
	}
	main := string(mainTF)
	for _, want := range []string{
		`module "compact"`,
		`language           = "python"`,
		`python             = file("transforms/compact.py")`,
		`output_definitions = {}`,
		`inputs             = {}`,
		`catalog        = "clavesa_demo"`,
		`system_catalog = "clavesa_demo_system"`,
		`data "terraform_remote_state" "workspace"`,
	} {
		if !strings.Contains(main, want) {
			t.Errorf("main.tf missing %q", want)
		}
	}
	// The module source must point at the extracted transform module relative
	// to the pipeline dir (one level under the workspace root).
	if !strings.Contains(main, "transform/aws") {
		t.Errorf("main.tf module source does not reference the transform module: %s", main)
	}

	py, err := os.ReadFile(filepath.Join(dir, "transforms", "compact.py"))
	if err != nil {
		t.Fatalf("read compact.py: %v", err)
	}
	for _, want := range []string{
		"def transform(spark, inputs):",
		"OPTIMIZE",
		"VACUUM",
		"retentionDurationCheck.enabled",
		"return {}",
	} {
		if !strings.Contains(string(py), want) {
			t.Errorf("compact.py missing %q", want)
		}
	}

	vars, err := os.ReadFile(filepath.Join(dir, "variables.tf"))
	if err != nil {
		t.Fatalf("read variables.tf: %v", err)
	}
	if !strings.Contains(string(vars), "trigger_schedule") {
		t.Errorf("variables.tf missing trigger_schedule")
	}
}

func TestScaffoldMaintenancePipelineDoesNotClobber(t *testing.T) {
	root := t.TempDir()
	if err := scaffoldMaintenancePipeline(root, "c", "c_system", "v1"); err != nil {
		t.Fatalf("first scaffold: %v", err)
	}
	edited := filepath.Join(root, MaintenancePipelineDir, "transforms", "compact.py")
	if err := os.WriteFile(edited, []byte("# user edit\n"), 0o644); err != nil {
		t.Fatalf("user edit: %v", err)
	}
	// Re-running init must preserve the edit (write-if-absent).
	if err := scaffoldMaintenancePipeline(root, "c", "c_system", "v1"); err != nil {
		t.Fatalf("second scaffold: %v", err)
	}
	got, err := os.ReadFile(edited)
	if err != nil {
		t.Fatalf("read edited: %v", err)
	}
	if string(got) != "# user edit\n" {
		t.Errorf("scaffold clobbered a user edit: %q", got)
	}
}
