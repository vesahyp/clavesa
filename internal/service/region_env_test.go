package service

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/vesahyp/clavesa/internal/workspace"
)

// TestTerraformEnvCarriesWorkspaceRegion: every Terraform run of a
// workspace applies in its recorded region (GH #107), over an exported
// AWS_REGION, and a workspace with no recorded region gets the one in its
// workspace stack state.
func TestTerraformEnvCarriesWorkspaceRegion(t *testing.T) {
	t.Setenv("AWS_REGION", "eu-north-1")
	t.Setenv("AWS_DEFAULT_REGION", "eu-north-1")
	dir := t.TempDir()
	m := &workspace.Manifest{Name: "ws", Cloud: "aws", Version: 1, Catalog: "clavesa_ws", SystemCatalog: "clavesa_ws_system"}
	if err := workspace.SaveManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	svc := New(dir)

	env, err := svc.TerraformEnv(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !containsString(env, "AWS_REGION=eu-north-1") {
		t.Error("no recorded region and no state: TerraformEnv should keep the exported AWS_REGION")
	}

	state := `{"version": 4, "outputs": {"pipeline_bucket": {"value": "ws-clavesa"}},
	  "resources": [{"mode": "managed", "type": "aws_s3_bucket", "name": "b",
	    "instances": [{"attributes": {"bucket": "ws-clavesa", "region": "us-east-2"}}]}]}`
	if err := os.WriteFile(filepath.Join(dir, "terraform.tfstate"), []byte(state), 0o644); err != nil {
		t.Fatal(err)
	}
	env, err = svc.TerraformEnv(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !containsString(env, "AWS_REGION=us-east-2") || !containsString(env, "AWS_DEFAULT_REGION=us-east-2") {
		t.Errorf("TerraformEnv lacks the workspace region us-east-2")
	}
	if containsString(env, "AWS_REGION=eu-north-1") || containsString(env, "AWS_DEFAULT_REGION=eu-north-1") {
		t.Errorf("TerraformEnv still carries the exported eu-north-1")
	}
	if got := workspace.RecordedRegion(dir); got != "us-east-2" {
		t.Errorf("derived region not persisted: RecordedRegion = %q", got)
	}
}
