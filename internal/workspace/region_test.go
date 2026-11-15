package workspace

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// regionManifest writes a minimal clavesa.json at a fresh temp dir.
func regionManifest(t *testing.T, m Manifest) string {
	t.Helper()
	dir := t.TempDir()
	if m.Name == "" {
		m.Name = "ws"
	}
	m.Version = manifestVersion
	m.Catalog = DefaultCatalog(m.Name)
	m.SystemCatalog = DefaultSystemCatalog(m.Catalog)
	if err := SaveManifest(dir, &m); err != nil {
		t.Fatal(err)
	}
	return dir
}

// ucpubLikeState is the shape of a real workspace stack's state: the
// bucket records its region, an IAM role has a global ARN, and the runner
// image names the ECR registry.
const ucpubLikeState = `{
  "version": 4,
  "outputs": {
    "pipeline_bucket": {"value": "ucpub-clavesa", "type": "string"},
    "runner_image": {"value": "123456789012.dkr.ecr.us-east-2.amazonaws.com/clavesa-ucpub/transform-runner:latest", "type": "string"}
  },
  "resources": [
    {"mode": "managed", "type": "aws_iam_role", "name": "uc", "instances": [{"attributes": {"arn": "arn:aws:iam::123456789012:role/uc"}}]},
    {"mode": "managed", "type": "aws_s3_bucket", "name": "other", "instances": [{"attributes": {"bucket": "elsewhere", "region": "eu-west-1"}}]},
    {"mode": "managed", "type": "aws_s3_bucket", "name": "workspace_bucket", "instances": [{"attributes": {"bucket": "ucpub-clavesa", "region": "us-east-2"}}]}
  ]
}`

func TestRegionFromState(t *testing.T) {
	cases := []struct {
		name, state, want string
	}{
		{"pipeline bucket region wins", ucpubLikeState, "us-east-2"},
		{"any aws resource region", `{"resources": [
			{"mode": "data", "type": "aws_region", "name": "x", "instances": [{"attributes": {"region": "ap-south-1"}}]},
			{"mode": "managed", "type": "aws_athena_workgroup", "name": "w", "instances": [{"attributes": {"region": "eu-central-1"}}]}]}`, "eu-central-1"},
		{"regional ARN", `{"resources": [{"mode": "managed", "type": "aws_sfn_state_machine", "name": "p",
			"instances": [{"attributes": {"arn": "arn:aws:states:ca-central-1:123456789012:stateMachine:p"}}]}]}`, "ca-central-1"},
		{"ECR registry of the runner image", `{"outputs": {"runner_image": {"value": "1.dkr.ecr.sa-east-1.amazonaws.com/r:1"}}}`, "sa-east-1"},
		{"no region at all", `{"outputs": {"pipeline_bucket": {"value": "b"}}, "resources": [
			{"mode": "managed", "type": "aws_iam_role", "name": "r", "instances": [{"attributes": {"arn": "arn:aws:iam::123456789012:role/r"}}]}]}`, ""},
		{"not JSON", `nope`, ""},
	}
	for _, c := range cases {
		if got := RegionFromState([]byte(c.state)); got != c.want {
			t.Errorf("%s: RegionFromState = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestRecordRegion(t *testing.T) {
	dir := regionManifest(t, Manifest{})
	if err := RecordRegion(dir, "us-east-2"); err != nil {
		t.Fatal(err)
	}
	if got := RecordedRegion(dir); got != "us-east-2" {
		t.Errorf("RecordedRegion = %q, want us-east-2", got)
	}
	if err := RecordRegion(dir, "not a region"); err == nil {
		t.Error("RecordRegion accepted a value that is not a region")
	}
	if err := RecordRegion(t.TempDir(), "us-east-2"); err == nil {
		t.Error("RecordRegion wrote into a directory with no clavesa.json")
	}
}

// TestResolveRegionDerivesFromLocalState: a workspace deployed before the
// field existed gets its region from its state, once, and keeps it.
func TestResolveRegionDerivesFromLocalState(t *testing.T) {
	dir := regionManifest(t, Manifest{})
	if got := ResolveRegion(context.Background(), dir); got != "" {
		t.Fatalf("undeployed workspace: ResolveRegion = %q, want \"\"", got)
	}
	if err := os.WriteFile(filepath.Join(dir, "terraform.tfstate"), []byte(ucpubLikeState), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := ResolveRegion(context.Background(), dir); got != "us-east-2" {
		t.Fatalf("ResolveRegion = %q, want us-east-2", got)
	}
	m, err := Load(dir)
	if err != nil || m.Region != "us-east-2" {
		t.Fatalf("clavesa.json region = %+v, %v; want us-east-2 persisted", m, err)
	}
	// The recorded value wins from now on, even over a changed state.
	if err := os.Remove(filepath.Join(dir, "terraform.tfstate")); err != nil {
		t.Fatal(err)
	}
	if got := ResolveRegion(context.Background(), dir); got != "us-east-2" {
		t.Errorf("after derive: ResolveRegion = %q, want us-east-2", got)
	}
}

// TestResolveRegionS3BackendUsesStackNotBucketRegion: the state bucket's
// region is not the workspace's region.
func TestResolveRegionS3BackendUsesStackNotBucketRegion(t *testing.T) {
	dir := regionManifest(t, Manifest{Backend: &Backend{Type: "s3", Bucket: "tfstate", Region: "eu-north-1"}})
	var gotRegion, gotKey string
	restore := SetStateGetterForTest(func(_ context.Context, bucket, region, key string) ([]byte, error) {
		gotRegion, gotKey = region, key
		return []byte(ucpubLikeState), nil
	})
	defer restore()
	if got := ResolveRegion(context.Background(), dir); got != "us-east-2" {
		t.Fatalf("ResolveRegion = %q, want us-east-2", got)
	}
	if gotRegion != "eu-north-1" || gotKey != "clavesa/ws/workspace.tfstate" {
		t.Errorf("state read from region %q key %q, want the backend's eu-north-1 and the workspace key", gotRegion, gotKey)
	}
	if got := RecordedRegion(dir); got != "us-east-2" {
		t.Errorf("RecordedRegion = %q, want us-east-2 persisted", got)
	}
}

func TestResolveRegionHostedTreeCopy(t *testing.T) {
	dir := t.TempDir()
	data, _ := json.Marshal(Deployed{Version: DeployedVersion, Region: "us-east-2"})
	if err := os.WriteFile(filepath.Join(dir, DeployedFile), data, 0o644); err != nil {
		t.Fatal(err)
	}
	if got := ResolveRegion(context.Background(), dir); got != "us-east-2" {
		t.Errorf("ResolveRegion = %q, want deployed.json's us-east-2", got)
	}
}

// isolateAWSConfig points the SDK at an empty config with the given
// profile region, so the test sees only what it sets.
func isolateAWSConfig(t *testing.T, profileRegion string) {
	t.Helper()
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config")
	body := "[default]\n"
	if profileRegion != "" {
		body += "region = " + profileRegion + "\n"
	}
	if err := os.WriteFile(cfg, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_CONFIG_FILE", cfg)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
}

// TestLoadAWSConfigRegionPrecedence: recorded region > AWS_REGION >
// the profile's region.
func TestLoadAWSConfigRegionPrecedence(t *testing.T) {
	isolateAWSConfig(t, "eu-north-1")
	ctx := context.Background()
	dir := regionManifest(t, Manifest{})

	region := func() string {
		t.Helper()
		cfg, err := LoadAWSConfig(ctx, dir)
		if err != nil {
			t.Fatal(err)
		}
		return cfg.Region
	}
	if got := region(); got != "eu-north-1" {
		t.Errorf("nothing recorded, no env: region = %q, want the profile's eu-north-1", got)
	}
	t.Setenv("AWS_REGION", "eu-west-1")
	if got := region(); got != "eu-west-1" {
		t.Errorf("nothing recorded: region = %q, want AWS_REGION's eu-west-1", got)
	}
	if err := RecordRegion(dir, "us-east-2"); err != nil {
		t.Fatal(err)
	}
	if got := region(); got != "us-east-2" {
		t.Errorf("recorded: region = %q, want the recorded us-east-2 over AWS_REGION", got)
	}
}

func TestSetRegionEnv(t *testing.T) {
	env := []string{"PATH=/bin", "AWS_REGION=eu-north-1", "AWS_DEFAULT_REGION=eu-north-1", "AWS_PROFILE=p"}
	got := SetRegionEnv(env, "us-east-2")
	want := []string{"PATH=/bin", "AWS_PROFILE=p", "AWS_REGION=us-east-2", "AWS_DEFAULT_REGION=us-east-2"}
	if !slices.Equal(got, want) {
		t.Errorf("SetRegionEnv = %v, want %v", got, want)
	}
	if got := SetRegionEnv(env, ""); !slices.Equal(got, env) {
		t.Errorf("SetRegionEnv with no region changed env: %v", got)
	}
}
