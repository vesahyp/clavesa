package pipelinestatus_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"

	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/pipelinestatus"
)

// recordingSFN answers DescribeExecution with SUCCEEDED and records the ARN
// it was asked about.
type recordingSFN struct{ described []string }

func (f *recordingSFN) DescribeExecution(_ context.Context, in *sfn.DescribeExecutionInput, _ ...func(*sfn.Options)) (*sfn.DescribeExecutionOutput, error) {
	f.described = append(f.described, aws.ToString(in.ExecutionArn))
	start := time.Date(2026, 11, 8, 17, 22, 12, 0, time.UTC)
	return &sfn.DescribeExecutionOutput{
		ExecutionArn: in.ExecutionArn,
		Status:       sfntypes.ExecutionStatusSucceeded,
		StartDate:    &start,
	}, nil
}

func (f *recordingSFN) GetExecutionHistory(context.Context, *sfn.GetExecutionHistoryInput, ...func(*sfn.Options)) (*sfn.GetExecutionHistoryOutput, error) {
	return &sfn.GetExecutionHistoryOutput{}, nil
}

// TestExecutionStatesShortCloudRunIDResolvesToExecutionARN covers the
// run-detail sheet on a cloud warehouse: the dashboard opens it with the
// runs table's short `run_id`, and the states endpoint must read that run
// through its SFN execution ARN. Before the fix the short id missed both
// DescribeExecution and the ARN-keyed `_progress` tree, and a finished run
// read as RUNNING forever. A cloud-local `local-` id stays a bare run id.
func TestExecutionStatesShortCloudRunIDResolvesToExecutionARN(t *testing.T) {
	const (
		smARN   = "arn:aws:states:us-east-2:111122223333:stateMachine:clavesa-demo"
		shortID = "7f75c266-6c49-4428-bebf-e9bf5c75aaf5"
		execARN = "arn:aws:states:us-east-2:111122223333:execution:clavesa-demo:" + shortID
	)

	ws := t.TempDir()
	pipelineDir := filepath.Join(ws, "demo")
	if err := os.MkdirAll(pipelineDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeLocalPipeline(t, pipelineDir)
	if err := workspaceWriteCloud(t, ws); err != nil {
		t.Fatalf("write cloud warehouse: %v", err)
	}
	state := `{"resources":[{"type":"aws_sfn_state_machine","name":"pipeline","instances":[{"attributes":{"arn":"` + smARN + `"}}]}]}`
	if err := os.WriteFile(filepath.Join(pipelineDir, "terraform.tfstate"), []byte(state), 0o644); err != nil {
		t.Fatalf("write tfstate: %v", err)
	}

	fake := &recordingSFN{}
	cloud := observability.NewCloudProvider(nil, "", fake, nil)
	resolver := observability.NewResolver(ws, cloud, observability.NewLocalProvider(ws))
	h := pipelinestatus.NewHandler(ws).WithResolver(resolver)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	get := func(run string) observability.ExecutionStatesResult {
		t.Helper()
		q := url.Values{}
		q.Set("dir", "demo")
		q.Set("run", run)
		req := httptest.NewRequest(http.MethodGet, "/pipeline/execution/states?"+q.Encode(), nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("run %s: status %d: %s", run, w.Code, w.Body.String())
		}
		var res observability.ExecutionStatesResult
		if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return res
	}

	res := get(shortID)
	if res.Status != "SUCCEEDED" {
		t.Errorf("short id: status = %q, want SUCCEEDED", res.Status)
	}
	if res.RunID != execARN {
		t.Errorf("short id: run_id = %q, want %q", res.RunID, execARN)
	}
	if len(fake.described) != 1 || fake.described[0] != execARN {
		t.Errorf("DescribeExecution calls = %v, want [%s]", fake.described, execARN)
	}

	// The full ARN still works and is passed through unchanged.
	if res := get(execARN); res.Status != "SUCCEEDED" || res.RunID != execARN {
		t.Errorf("ARN: got status %q run_id %q", res.Status, res.RunID)
	}

	// A cloud-local run is keyed by its own id: no SFN lookup.
	fake.described = nil
	if res := get("local-abc123"); res.RunID != "local-abc123" {
		t.Errorf("cloud-local: run_id = %q, want local-abc123", res.RunID)
	}
	if len(fake.described) != 0 {
		t.Errorf("cloud-local: DescribeExecution called with %v", fake.described)
	}
}
