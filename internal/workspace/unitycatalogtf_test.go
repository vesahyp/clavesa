package workspace

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRenderUnityCatalogTF(t *testing.T) {
	m := &Manifest{Name: "ws", Databricks: &Databricks{Host: "https://dbc-1.cloud.databricks.com", Catalog: "analytics", Credential: "dbx-sp"}}
	got := RenderUnityCatalogTF(m, "v9.9.9", "app-id", "env:SECRET")
	for _, want := range []string{
		`source               = "./.clavesa/modules/v9.9.9/unity-catalog/aws"`,
		`databricks_host      = "https://dbc-1.cloud.databricks.com"`,
		`databricks_client_id = "app-id"`,
		`bucket               = module.workspace.pipeline_bucket`,
		`catalog              = "analytics"`,
		`create_catalog       = false`,
		`external_location    = null`,
		`create_runner_secret = true`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("unity_catalog.tf lacks %q:\n%s", want, got)
		}
	}
	// main.tf holds this root's one required_providers block; a second
	// one is a Terraform error.
	if strings.Contains(got, "required_providers") || strings.Contains(got, `provider "databricks"`) {
		t.Errorf("unity_catalog.tf must not configure providers at the root:\n%s", got)
	}

	m.Databricks.ExternalLocation = "analytics_bucket"
	got = RenderUnityCatalogTF(m, "v9.9.9", "app-id", "arn:aws:secretsmanager:us-east-2:1:secret:x")
	if !strings.Contains(got, `external_location    = "analytics_bucket"`) || !strings.Contains(got, `create_runner_secret = false`) {
		t.Errorf("admin location / arn: secret not rendered:\n%s", got)
	}
	if !strings.Contains(got, `readers              = []`) {
		t.Errorf("no readers must render an empty list:\n%s", got)
	}
	m.Databricks.Readers = []string{"analysts"}
	if got = RenderUnityCatalogTF(m, "v9.9.9", "app-id", "env:S"); !strings.Contains(got, `readers              = ["analysts"]`) {
		t.Errorf("readers not rendered:\n%s", got)
	}
}

func TestSyncUnityCatalogTF(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, UnityCatalogTFFile)
	m := &Manifest{Name: "ws", Databricks: &Databricks{Host: "https://h", Catalog: "c", Credential: "x"}}
	if err := SyncUnityCatalogTF(root, m, "v1.0.0", "id", "env:S"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("not written: %v", err)
	}
	if err := SyncUnityCatalogTF(root, m, "", "id", "env:S"); err == nil {
		t.Error("empty module version accepted")
	}
	m.Databricks = nil
	if err := SyncUnityCatalogTF(root, m, "", "", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("not removed: %v", err)
	}
	if err := SyncUnityCatalogTF(root, m, "", "", ""); err != nil {
		t.Errorf("removing a missing file: %v", err)
	}
}

func TestCurrentModuleVersion(t *testing.T) {
	root := t.TempDir()
	m := &Manifest{Name: "ws"}
	if err := os.WriteFile(filepath.Join(root, "main.tf"), []byte(WorkspaceMainTF(m, "v2.22.0")), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := CurrentModuleVersion(root)
	if err != nil || got != "v2.22.0" {
		t.Errorf("CurrentModuleVersion = %q, %v; want v2.22.0", got, err)
	}
}
