package workspace

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPipelineBucketFromTfstate(t *testing.T) {
	dir := t.TempDir()
	tfstate := `{
  "version": 4,
  "outputs": {
    "pipeline_bucket": { "value": "clavesa-demo-12345", "type": "string" },
    "runner_image":    { "value": "ignored", "type": "string" }
  }
}`
	if err := os.WriteFile(filepath.Join(dir, "terraform.tfstate"), []byte(tfstate), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := PipelineBucket(dir); got != "clavesa-demo-12345" {
		t.Fatalf("PipelineBucket = %q, want %q", got, "clavesa-demo-12345")
	}
}

func TestPipelineBucketEmptyOnMissingTfstate(t *testing.T) {
	if got := PipelineBucket(t.TempDir()); got != "" {
		t.Fatalf("PipelineBucket on missing tfstate = %q, want \"\"", got)
	}
}

func TestPipelineBucketEmptyOnMissingOutput(t *testing.T) {
	dir := t.TempDir()
	tfstate := `{"version": 4, "outputs": {}}`
	if err := os.WriteFile(filepath.Join(dir, "terraform.tfstate"), []byte(tfstate), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := PipelineBucket(dir); got != "" {
		t.Fatalf("PipelineBucket on no-outputs tfstate = %q, want \"\"", got)
	}
}

func TestPipelineBucketEmptyOnMalformedTfstate(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "terraform.tfstate"), []byte("not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := PipelineBucket(dir); got != "" {
		t.Fatalf("PipelineBucket on malformed tfstate = %q, want \"\"", got)
	}
}
