package workspace

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/access"
)

func TestSyncLakeFormationTF(t *testing.T) {
	root := t.TempDir()
	m := &Manifest{Name: "smoke2"}
	path := filepath.Join(root, LakeFormationTFFile)

	if err := SyncLakeFormationTF(root, m, "v9.9.9"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("no access block, yet %s written", LakeFormationTFFile)
	}

	m.Access = &access.Config{Tags: []string{"open", "ip", "raw"}}
	if err := SyncLakeFormationTF(root, m, "v9.9.9"); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`source          = "./.clavesa/modules/v9.9.9/lake-formation/aws"`,
		`role_name       = "clavesa-lf-register-smoke2"`,
		`output "lf_register_role_arn"`,
		`output "lf_system_location_arn"`,
		`output "lf_tag_key"`,
		`tag_key         = "clavesa_smoke2"`,
		`tag_values      = ["ip", "open", "raw", "unreviewed"]`,
		`system_database = module.workspace.system_pipelines_db`,
	} {
		if !strings.Contains(string(b), want) {
			t.Errorf("missing %q", want)
		}
	}

	m.Access = nil
	if err := SyncLakeFormationTF(root, m, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("block removed, file kept")
	}
}

func TestLakeFormationRoleNameFitsIAM(t *testing.T) {
	n := LakeFormationRoleName(strings.Repeat("w", 80))
	if len(n) > 64 || !strings.HasPrefix(n, "clavesa-lf-register-") {
		t.Errorf("role name %q (%d chars)", n, len(n))
	}
}
