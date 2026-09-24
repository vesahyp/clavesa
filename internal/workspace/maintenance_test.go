package workspace

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestScaffoldMaintenancePipeline(t *testing.T) {
	root := t.TempDir()
	m := &Manifest{Name: "demo", Catalog: "clavesa_demo", SystemCatalog: "clavesa_demo_system"}
	if err := scaffoldMaintenancePipeline(root, m, "v9.9.9"); err != nil {
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

	// No backend on the manifest: no backend.tf (ADR-025).
	if _, err := os.Stat(filepath.Join(dir, "backend.tf")); !os.IsNotExist(err) {
		t.Errorf("backend.tf written for a no-backend workspace (stat err=%v)", err)
	}
}

// TestScaffoldMaintenancePipelineWithBackend covers the ADR-025 emit for
// the one pipeline scaffold Init writes directly (not via CreatePipeline):
// main.tf drops the local terraform_remote_state block and a clavesa-owned
// backend.tf carries both the pipeline's own "s3" backend and the
// workspace remote-state read.
func TestScaffoldMaintenancePipelineWithBackend(t *testing.T) {
	root := t.TempDir()
	m := &Manifest{
		Name: "analytics", Catalog: "clavesa_analytics", SystemCatalog: "clavesa_analytics_system",
		Backend: &Backend{Type: "s3", Bucket: "analytics-tfstate", Region: "eu-north-1"},
	}
	if err := scaffoldMaintenancePipeline(root, m, "v9.9.9"); err != nil {
		t.Fatalf("scaffold: %v", err)
	}
	dir := filepath.Join(root, MaintenancePipelineDir)

	main, err := os.ReadFile(filepath.Join(dir, "main.tf"))
	if err != nil {
		t.Fatal(err)
	}
	// The inline data-source *declaration* is gone (it now lives in
	// backend.tf), but the module's *reference* to its output stays —
	// Terraform merges all .tf files in a directory, so the reference
	// still resolves.
	if strings.Contains(string(main), `data "terraform_remote_state" "workspace" {`) {
		t.Errorf("main.tf still declares terraform_remote_state inline for a backend-configured workspace:\n%s", main)
	}
	if strings.Contains(string(main), `backend "local"`) {
		t.Errorf("main.tf carries backend \"local\" for a backend-configured workspace:\n%s", main)
	}
	if !strings.Contains(string(main), "data.terraform_remote_state.workspace.outputs.pipeline_bucket") {
		t.Errorf("main.tf lost the reference to the workspace's pipeline_bucket output:\n%s", main)
	}

	backendTF, err := os.ReadFile(filepath.Join(dir, "backend.tf"))
	if err != nil {
		t.Fatalf("backend.tf not written: %v", err)
	}
	if !strings.Contains(string(backendTF), `key          = "clavesa/analytics/pipelines/_maintenance.tfstate"`) {
		t.Errorf("backend.tf missing the _maintenance pipeline state key:\n%s", backendTF)
	}
	if !strings.Contains(string(backendTF), `data "terraform_remote_state" "workspace"`) {
		t.Errorf("backend.tf missing the workspace remote-state data source:\n%s", backendTF)
	}
}

func TestScaffoldMaintenancePipelineDoesNotClobber(t *testing.T) {
	root := t.TempDir()
	m := &Manifest{Name: "c", Catalog: "c", SystemCatalog: "c_system"}
	if err := scaffoldMaintenancePipeline(root, m, "v1"); err != nil {
		t.Fatalf("first scaffold: %v", err)
	}
	edited := filepath.Join(root, MaintenancePipelineDir, "transforms", "compact.py")
	if err := os.WriteFile(edited, []byte("# user edit\n"), 0o644); err != nil {
		t.Fatalf("user edit: %v", err)
	}
	// Re-running init must preserve the edit (write-if-absent).
	if err := scaffoldMaintenancePipeline(root, m, "v1"); err != nil {
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
