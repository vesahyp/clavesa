package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/workspace"
)

// GH #107: a workspace records its AWS region and every Terraform run and
// AWS call of it uses that region.

func TestWorkspaceInitRegion(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	// The name reuses TestWorkspaceInitWithBackend's, so the runner image
	// init builds is the same tag and no new image is left behind.
	if err := Run([]string{"workspace", "init", "remote-ws", "--workspace", ws, "--region", "us-east-2"}); err != nil {
		t.Fatalf("workspace init --region: %v", err)
	}
	if got := workspace.RecordedRegion(ws); got != "us-east-2" {
		t.Errorf("recorded region = %q, want us-east-2", got)
	}
}

func TestWorkspaceInitRejectsBadRegion(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	err := Run([]string{"workspace", "init", "bad-ws", "--workspace", ws, "--region", "useast2"})
	if err == nil || !strings.Contains(err.Error(), "not an AWS region name") {
		t.Fatalf("workspace init --region useast2 = %v, want a region-name error", err)
	}
	if _, statErr := os.Stat(filepath.Join(ws, "clavesa.json")); !os.IsNotExist(statErr) {
		t.Errorf("a rejected init left clavesa.json behind (stat err=%v)", statErr)
	}
}

// regionWorkspace writes a manifest and, when state is not empty, a local
// workspace stack state.
func regionWorkspace(t *testing.T, region, state string) string {
	t.Helper()
	dir := t.TempDir()
	m := &workspace.Manifest{Name: "ws", Cloud: "aws", Version: 1, Catalog: "clavesa_ws", SystemCatalog: "clavesa_ws_system", Region: region}
	if err := workspace.SaveManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	if state != "" {
		if err := os.WriteFile(filepath.Join(dir, "terraform.tfstate"), []byte(state), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

const eastState = `{"version": 4, "outputs": {"pipeline_bucket": {"value": "ws-clavesa"}},
  "resources": [{"mode": "managed", "type": "aws_s3_bucket", "name": "b",
    "instances": [{"attributes": {"bucket": "ws-clavesa", "region": "us-east-2"}}]}]}`

// TestDeployFlowEnvCarriesRegion: the workspace and every pipeline stack
// apply in the workspace's region, over the one in Env.
func TestDeployFlowEnvCarriesRegion(t *testing.T) {
	t.Parallel()
	ws := regionWorkspace(t, "us-east-2", "")
	for _, tfDir := range []string{ws, filepath.Join(ws, "orders")} {
		d := deployFlow{WorkspaceRoot: ws, TfDir: tfDir, Env: []string{"PATH=/bin", "AWS_REGION=eu-north-1", "AWS_DEFAULT_REGION=eu-north-1"}}
		env := strings.Join(d.env(), "\n")
		if !strings.Contains(env, "AWS_REGION=us-east-2") || !strings.Contains(env, "AWS_DEFAULT_REGION=us-east-2") || strings.Contains(env, "eu-north-1") {
			t.Errorf("%s: terraform env =\n%s\nwant AWS_REGION and AWS_DEFAULT_REGION us-east-2 only", tfDir, env)
		}
	}
}

// TestDeployFlowRecordsRegionAfterApply: the workspace stack's apply
// records the region its state is in; a pipeline stack's apply does not
// touch clavesa.json.
func TestDeployFlowRecordsRegionAfterApply(t *testing.T) {
	ws := regionWorkspace(t, "", eastState)
	var out, errOut strings.Builder

	pipe := deployFlow{WorkspaceRoot: ws, TfDir: filepath.Join(ws, "orders"), Out: &out, Err: &errOut}
	pipe.recordRegion()
	m, err := workspace.Load(ws)
	if err != nil || m.Region != "" {
		t.Fatalf("pipeline apply wrote region %q (%v)", m.Region, err)
	}

	d := deployFlow{WorkspaceRoot: ws, TfDir: ws, Out: &out, Err: &errOut}
	d.recordRegion()
	if got := workspace.RecordedRegion(ws); got != "us-east-2" {
		t.Errorf("recorded region = %q, want us-east-2 (stdout %q, stderr %q)", got, out.String(), errOut.String())
	}
	if !strings.Contains(out.String(), "Recorded AWS region us-east-2") {
		t.Errorf("stdout = %q, want the recorded-region line", out.String())
	}
}

// TestApplyWorkspaceRegion: the CLI's process environment takes the
// workspace's recorded region over an exported AWS_REGION.
func TestApplyWorkspaceRegion(t *testing.T) {
	ws := regionWorkspace(t, "us-east-2", "")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_REGION", "eu-north-1")
	t.Setenv("AWS_DEFAULT_REGION", "")
	applyWorkspaceAWSProfile(ws)
	if got := os.Getenv("AWS_REGION"); got != "us-east-2" {
		t.Errorf("AWS_REGION = %q, want us-east-2", got)
	}
	if got := os.Getenv("AWS_DEFAULT_REGION"); got != "us-east-2" {
		t.Errorf("AWS_DEFAULT_REGION = %q, want us-east-2", got)
	}

	// No recorded region and nothing deployed: the shell's choice stands.
	bare := regionWorkspace(t, "", "")
	t.Setenv("AWS_REGION", "eu-west-1")
	applyWorkspaceAWSProfile(bare)
	if got := os.Getenv("AWS_REGION"); got != "eu-west-1" {
		t.Errorf("AWS_REGION = %q, want the exported eu-west-1 kept", got)
	}
}
