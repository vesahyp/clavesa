package service

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDatabricksJobs_RoundTrip(t *testing.T) {
	svc := newDatabricksWorkspace(t, &stubDatabricks{})
	dir := filepath.Join(svc.workspace, "orders")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "main.tf"), []byte("# pipeline\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := svc.GetDatabricksJobs("orders")
	if err != nil || len(got) != 0 {
		t.Fatalf("no file: %v, %v", got, err)
	}
	if err := svc.SetDatabricksJobs("orders", []int64{459311252754064, 2}); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(dir, DatabricksJobsFile))
	if !strings.Contains(string(data), "databricks_jobs = [459311252754064, 2]") {
		t.Errorf("file:\n%s", data)
	}
	got, err = svc.GetDatabricksJobs("orders")
	if err != nil || len(got) != 2 || got[0] != 459311252754064 {
		t.Errorf("read back %v, %v", got, err)
	}
	if err := svc.SetDatabricksJobs("orders", []int64{-1}); err == nil {
		t.Error("a negative job ID was accepted")
	}
	if err := svc.SetDatabricksJobs("orders", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, DatabricksJobsFile)); !os.IsNotExist(err) {
		t.Error("clearing did not remove the file")
	}
	if err := svc.SetDatabricksJobs("nope", []int64{1}); err == nil {
		t.Error("a missing pipeline was accepted")
	}
}

func TestGetDatabricksRun(t *testing.T) {
	svc := newDatabricksWorkspace(t, &stubDatabricks{})
	if _, err := svc.ConnectDatabricks(context.Background(), DatabricksConnectInput{
		Host: "https://dbc-1.cloud.databricks.com", Catalog: "analytics", Credential: "dbx-sp",
	}); err != nil {
		t.Fatal(err)
	}
	prev := readDatabricksRunObject
	t.Cleanup(func() { readDatabricksRunObject = prev })

	var gotBucket, gotKey string
	readDatabricksRunObject = func(_ context.Context, bucket, key string) ([]byte, error) {
		gotBucket, gotKey = bucket, key
		return []byte(`{"tables":[{"full_name":"a.b.c","action":"registered","url":"u"}],"jobs":[{"job_id":459311252754064,"run_id":162074173789529,"run_page_url":"r"}],"status":"ok"}`), nil
	}
	run, err := svc.GetDatabricksRun(context.Background(), "", "arn:exec")
	if err != nil {
		t.Fatal(err)
	}
	if gotBucket != "ws-clavesa" || gotKey != "_progress/arn:exec/_databricks.json" {
		t.Errorf("read s3://%s/%s", gotBucket, gotKey)
	}
	if !run.Found || run.Jobs[0].JobID != 459311252754064 || run.Jobs[0].RunID != 162074173789529 || run.Tables[0].FullName != "a.b.c" {
		t.Errorf("run = %+v", run)
	}

	// A short run ID (all the run page has until the run row loads) is
	// turned into the execution ARN the runner keyed the record by.
	prevARN := executionARN
	t.Cleanup(func() { executionARN = prevARN })
	executionARN = func(_ context.Context, awsName, id string) (string, error) {
		return "arn:aws:states:us-east-2:1:execution:" + awsName + ":" + id, nil
	}
	if _, err := svc.GetDatabricksRun(context.Background(), "uc_orders", "02cfc32a"); err != nil {
		t.Fatal(err)
	}
	if gotKey != "_progress/arn:aws:states:us-east-2:1:execution:clavesa-uc_orders:02cfc32a/_databricks.json" {
		t.Errorf("short ID read %s", gotKey)
	}

	readDatabricksRunObject = func(context.Context, string, string) ([]byte, error) { return nil, nil }
	run, err = svc.GetDatabricksRun(context.Background(), "", "arn:other")
	if err != nil || run.Found {
		t.Errorf("absent record: %+v, %v", run, err)
	}
}
