package service

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCreatePipelineEmitsTriggerBatchWindowDefault guards the default that
// makes docs/cookbook/s3-trigger.md work as-written. Without
// a non-null default here, an S3-event-driven pipeline deploys with the
// EventBridge rule + SQS queue intact but no poller Lambda to drain them
// — messages accumulate and no SFN execution ever fires. The recipe
// doesn't tell the user to set trigger_batch_window themselves.
func TestCreatePipelineEmitsTriggerBatchWindowDefault(t *testing.T) {
	ws := t.TempDir()
	// Init the manifest so CreatePipeline doesn't bail. The pipeline
	// service only needs the manifest's catalog field.
	manifest := `{"name":"smoke-ws","cloud":"aws","version":1,"catalog":"clavesa_smoke_ws"}` + "\n"
	if err := os.WriteFile(filepath.Join(ws, "clavesa.json"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	svc := New(ws)
	if _, err := svc.CreatePipeline("demo", ""); err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	body, err := os.ReadFile(filepath.Join(ws, "demo", "variables.tf"))
	if err != nil {
		t.Fatalf("read variables.tf: %v", err)
	}
	if !strings.Contains(string(body), `default     = "rate(1 minute)"`) {
		t.Errorf("variables.tf missing the rate(1 minute) default — S3-trigger pipelines would deploy without a poller.\nvariables.tf:\n%s", body)
	}
}
