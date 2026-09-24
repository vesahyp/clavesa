package workspace

import (
	"context"
	"encoding/json"
	"errors"
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

// ---------------------------------------------------------------------------
// ReadStackState (ADR-025, "Read side: one function")
// ---------------------------------------------------------------------------

// writeManifest writes a minimal clavesa.json at root, with a backend when
// backend is non-nil. Catalog/SystemCatalog are filled so Load doesn't
// trigger its auto-migrate rewrite mid-test.
func writeManifest(t *testing.T, root, name string, backend *Backend) {
	t.Helper()
	m := Manifest{
		Name:          name,
		Cloud:         "aws",
		Version:       manifestVersion,
		Catalog:       DefaultCatalog(name),
		SystemCatalog: DefaultSystemCatalog(DefaultCatalog(name)),
		Backend:       backend,
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, manifestFile), append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestReadStackStateLocalPresent(t *testing.T) {
	dir := t.TempDir()
	writeManifest(t, dir, "demo", nil)
	want := `{"outputs":{}}`
	if err := os.WriteFile(filepath.Join(dir, "terraform.tfstate"), []byte(want), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := ReadStackState(context.Background(), dir, dir)
	if err != nil {
		t.Fatalf("ReadStackState: %v", err)
	}
	if string(got) != want {
		t.Fatalf("ReadStackState = %q, want %q", got, want)
	}
}

func TestReadStackStateLocalAbsent(t *testing.T) {
	dir := t.TempDir()
	writeManifest(t, dir, "demo", nil)
	got, err := ReadStackState(context.Background(), dir, dir)
	if err != nil {
		t.Fatalf("ReadStackState: %v", err)
	}
	if got != nil {
		t.Fatalf("ReadStackState = %q, want nil", got)
	}
}

// TestReadStackStateLegacyNoManifest checks a workspace with no clavesa.json
// at all — a legacy directory that predates the manifest — is still treated
// as local, exactly like one whose manifest carries no backend.
func TestReadStackStateLegacyNoManifest(t *testing.T) {
	dir := t.TempDir()
	want := `{"outputs":{"pipeline_bucket":{"value":"b","type":"string"}}}`
	if err := os.WriteFile(filepath.Join(dir, "terraform.tfstate"), []byte(want), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := ReadStackState(context.Background(), dir, dir)
	if err != nil {
		t.Fatalf("ReadStackState: %v", err)
	}
	if string(got) != want {
		t.Fatalf("ReadStackState = %q, want %q", got, want)
	}
}

func TestReadStackStateRemotePresent(t *testing.T) {
	dir := t.TempDir()
	backend := &Backend{Type: "s3", Bucket: "tfstate-bucket", Region: "eu-north-1"}
	writeManifest(t, dir, "analytics", backend)

	want := []byte(`{"outputs":{"pipeline_bucket":{"value":"remote-bucket","type":"string"}}}`)
	var gotBucket, gotRegion, gotKey string
	restore := SetStateGetterForTest(func(ctx context.Context, bucket, region, key string) ([]byte, error) {
		gotBucket, gotRegion, gotKey = bucket, region, key
		return want, nil
	})
	defer restore()

	got, err := ReadStackState(context.Background(), dir, dir)
	if err != nil {
		t.Fatalf("ReadStackState: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("ReadStackState = %q, want %q", got, want)
	}
	if gotBucket != "tfstate-bucket" || gotRegion != "eu-north-1" {
		t.Fatalf("getter called with bucket=%q region=%q", gotBucket, gotRegion)
	}
	if want := "clavesa/analytics/workspace.tfstate"; gotKey != want {
		t.Fatalf("getter called with key=%q, want %q", gotKey, want)
	}
}

func TestReadStackStateRemoteNotFound(t *testing.T) {
	dir := t.TempDir()
	backend := &Backend{Type: "s3", Bucket: "tfstate-bucket", Region: "eu-north-1"}
	writeManifest(t, dir, "analytics", backend)

	restore := SetStateGetterForTest(func(ctx context.Context, bucket, region, key string) ([]byte, error) {
		return nil, nil // NoSuchKey
	})
	defer restore()

	got, err := ReadStackState(context.Background(), dir, dir)
	if err != nil {
		t.Fatalf("ReadStackState: %v", err)
	}
	if got != nil {
		t.Fatalf("ReadStackState = %q, want nil", got)
	}
}

func TestReadStackStateRemoteError(t *testing.T) {
	dir := t.TempDir()
	backend := &Backend{Type: "s3", Bucket: "tfstate-bucket", Region: "eu-north-1"}
	writeManifest(t, dir, "analytics", backend)

	wantErr := errors.New("access denied")
	restore := SetStateGetterForTest(func(ctx context.Context, bucket, region, key string) ([]byte, error) {
		return nil, wantErr
	})
	defer restore()

	_, err := ReadStackState(context.Background(), dir, dir)
	if !errors.Is(err, wantErr) {
		t.Fatalf("ReadStackState err = %v, want %v", err, wantErr)
	}
}

// TestReadStackStateRemoteKeyPipelineVsWorkspace checks the workspace key is
// used when stackDir is the workspace root and the pipeline key otherwise
// (ADR-025 "State keys").
func TestReadStackStateRemoteKeyPipelineVsWorkspace(t *testing.T) {
	dir := t.TempDir()
	backend := &Backend{Type: "s3", Bucket: "tfstate-bucket", Region: "eu-north-1"}
	writeManifest(t, dir, "analytics", backend)
	pipelineDir := filepath.Join(dir, "trips")
	if err := os.MkdirAll(pipelineDir, 0o755); err != nil {
		t.Fatal(err)
	}

	var gotKey string
	restore := SetStateGetterForTest(func(ctx context.Context, bucket, region, key string) ([]byte, error) {
		gotKey = key
		return []byte(`{"outputs":{}}`), nil
	})
	defer restore()

	if _, err := ReadStackState(context.Background(), dir, pipelineDir); err != nil {
		t.Fatalf("ReadStackState: %v", err)
	}
	if want := "clavesa/analytics/pipelines/trips.tfstate"; gotKey != want {
		t.Fatalf("pipeline stackDir key = %q, want %q", gotKey, want)
	}

	if _, err := ReadStackState(context.Background(), dir, dir); err != nil {
		t.Fatalf("ReadStackState: %v", err)
	}
	if want := "clavesa/analytics/workspace.tfstate"; gotKey != want {
		t.Fatalf("workspace stackDir key = %q, want %q", gotKey, want)
	}
}

// TestReadStackStateRemoteCacheHit checks a second read within the TTL
// reuses the cached bytes instead of calling the getter again.
func TestReadStackStateRemoteCacheHit(t *testing.T) {
	dir := t.TempDir()
	backend := &Backend{Type: "s3", Bucket: "tfstate-bucket", Region: "eu-north-1"}
	writeManifest(t, dir, "analytics", backend)

	calls := 0
	restore := SetStateGetterForTest(func(ctx context.Context, bucket, region, key string) ([]byte, error) {
		calls++
		return []byte(`{"outputs":{}}`), nil
	})
	defer restore()

	if _, err := ReadStackState(context.Background(), dir, dir); err != nil {
		t.Fatalf("ReadStackState (1st): %v", err)
	}
	if _, err := ReadStackState(context.Background(), dir, dir); err != nil {
		t.Fatalf("ReadStackState (2nd): %v", err)
	}
	if calls != 1 {
		t.Fatalf("getter called %d times within TTL, want 1", calls)
	}
}

func TestPipelineBucketRemote(t *testing.T) {
	dir := t.TempDir()
	backend := &Backend{Type: "s3", Bucket: "tfstate-bucket", Region: "eu-north-1"}
	writeManifest(t, dir, "analytics", backend)

	restore := SetStateGetterForTest(func(ctx context.Context, bucket, region, key string) ([]byte, error) {
		return []byte(`{"outputs":{"pipeline_bucket":{"value":"remote-bucket","type":"string"}}}`), nil
	})
	defer restore()

	if got := PipelineBucket(dir); got != "remote-bucket" {
		t.Fatalf("PipelineBucket = %q, want %q", got, "remote-bucket")
	}
}

func TestPipelineBucketRemoteEmptyOnNotFound(t *testing.T) {
	dir := t.TempDir()
	backend := &Backend{Type: "s3", Bucket: "tfstate-bucket", Region: "eu-north-1"}
	writeManifest(t, dir, "analytics", backend)

	restore := SetStateGetterForTest(func(ctx context.Context, bucket, region, key string) ([]byte, error) {
		return nil, nil
	})
	defer restore()

	if got := PipelineBucket(dir); got != "" {
		t.Fatalf("PipelineBucket = %q, want \"\"", got)
	}
}
