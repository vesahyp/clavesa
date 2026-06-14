package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"

	"github.com/vesahyp/clavesa/internal/api"
)

// fakeGlueClient returns canned responses without hitting AWS. The catalog
// handler's interface is small enough that a struct of slices suffices.
type fakeGlueClient struct {
	databases []string
	tables    map[string][]gluetypes.Table
}

func (f *fakeGlueClient) GetDatabases(_ context.Context, _ *glue.GetDatabasesInput, _ ...func(*glue.Options)) (*glue.GetDatabasesOutput, error) {
	dbs := make([]gluetypes.Database, 0, len(f.databases))
	for _, name := range f.databases {
		dbs = append(dbs, gluetypes.Database{Name: aws.String(name)})
	}
	return &glue.GetDatabasesOutput{DatabaseList: dbs}, nil
}

func (f *fakeGlueClient) GetTables(_ context.Context, in *glue.GetTablesInput, _ ...func(*glue.Options)) (*glue.GetTablesOutput, error) {
	return &glue.GetTablesOutput{TableList: f.tables[aws.ToString(in.DatabaseName)]}, nil
}

// writeEnvMode declares the workspace warehouse the catalog reads.
// The catalog lists the cloud (Glue) half on a cloud warehouse and the
// local (on-disk) half on a local warehouse, per the EnvModeToggle
// contract — so a test that wants the cloud half must say so. Writes
// the legacy `mode` key deliberately, so the pre-ADR-024 fallback read
// path stays covered.
func writeEnvMode(t *testing.T, workspace, warehouse string) {
	t.Helper()
	dir := filepath.Join(workspace, ".clavesa")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir .clavesa: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "environment.json"),
		[]byte(`{"mode":"`+warehouse+`"}`+"\n"), 0o644); err != nil {
		t.Fatalf("write environment.json: %v", err)
	}
}

// TestCatalogCloudHalfSkippedInLocalMode locks the env-gating that fixes
// double-listing: in local mode the catalog does not list Glue (cloud)
// tables even though a Glue client is wired and would return some. Without
// the gate, a table that is both deployed and run locally shows twice.
func TestCatalogCloudHalfSkippedInLocalMode(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "clavesa.json"),
		[]byte(`{"name":"test","cloud":"aws","version":1,"catalog":"clavesa_test"}`+"\n"), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	writeEnvMode(t, workspace, "local")
	fake := &fakeGlueClient{
		databases: []string{"clavesa_test__cloud"},
		tables: map[string][]gluetypes.Table{
			"clavesa_test__cloud": {{
				Name:       aws.String("xform__default"),
				Parameters: map[string]string{"table_type": "ICEBERG"},
				StorageDescriptor: &gluetypes.StorageDescriptor{
					Location: aws.String("s3://bkt/cloud/xform__default"),
				},
			}},
		},
	}
	h := api.NewCatalogHandler(fake).WithWorkspace(workspace)
	res := h.Tables(context.Background())
	if len(res.Tables) != 0 {
		t.Fatalf("local mode must skip the Glue cloud half; got %d tables", len(res.Tables))
	}
}

// localPipelineTF is a minimal compute = "local" pipeline that the catalog
// walker classifies as local.
const localPipelineTF = `variable "pipeline_name" { type = string default = "demo" }

module "src" {
  source        = "clavesa/source/aws"
  pipeline_name = var.pipeline_name
  bucket        = "in"
  prefix        = "raw/"
  format        = "csv"
}

module "xform" {
  source        = "clavesa/transform/aws"
  pipeline_name = var.pipeline_name
  inputs        = { primary = module.src.outputs["default"] }
  compute       = "local"
  language      = "sparksql"
  sql           = "SELECT 1"
}
`

// minimal Delta _delta_log/00000000000000000000.json — protocol + metaData
// + commitInfo + add, newline-delimited. The metaData carries a JSON-
// encoded Spark schema string with two columns. Backslash escapes are
// needed because the schemaString itself is JSON nested inside JSON.
const deltaInitialCommit = `{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"abc","format":{"provider":"parquet"},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"amount\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{}}}
{"commitInfo":{"timestamp":1746615600000,"operation":"CREATE TABLE","userMetadata":"{\"trigger\":\"manual\",\"run-id\":\"run-1\"}"}}
{"add":{"path":"part-00000.snappy.parquet","partitionValues":{},"size":42,"modificationTime":1746615600000,"dataChange":true}}
`

func TestCatalogListLocalTables(t *testing.T) {
	workspace := t.TempDir()
	// Workspace manifest with catalog identifier — required post-v0.18.
	if err := os.WriteFile(filepath.Join(workspace, "clavesa.json"),
		[]byte(`{"name":"test","cloud":"aws","version":1,"catalog":"clavesa_test"}`+"\n"), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	pipelineDir := filepath.Join(workspace, "demo")
	if err := os.MkdirAll(pipelineDir, 0o755); err != nil {
		t.Fatalf("mkdir pipeline: %v", err)
	}
	if err := os.WriteFile(filepath.Join(pipelineDir, "main.tf"), []byte(localPipelineTF), 0o644); err != nil {
		t.Fatalf("write tf: %v", err)
	}

	// Lay out a Delta-style table at the encoded warehouse path:
	//   <pipelineDir>/.clavesa/warehouse/clavesa_test__demo/xform__default/_delta_log/00000000000000000000.json
	logDir := filepath.Join(pipelineDir, ".clavesa", "warehouse", "clavesa_test__demo", "xform__default", "_delta_log")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatalf("mkdir log: %v", err)
	}
	if err := os.WriteFile(filepath.Join(logDir, "00000000000000000000.json"), []byte(deltaInitialCommit), 0o644); err != nil {
		t.Fatalf("write commit: %v", err)
	}

	h := api.NewCatalogHandler(nil).WithWorkspace(workspace)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/workspace/tables", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}

	var res api.CatalogResponse
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(res.Tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(res.Tables))
	}
	got := res.Tables[0]
	if got.Database != "clavesa_test__demo" {
		t.Errorf("Database = %q, want clavesa_test__demo", got.Database)
	}
	if got.Name != "xform__default" {
		t.Errorf("Name = %q, want xform__default", got.Name)
	}
	if got.OwningPipeline != "demo" {
		t.Errorf("OwningPipeline = %q, want demo", got.OwningPipeline)
	}
	if got.OwningNode != "xform" {
		t.Errorf("OwningNode = %q, want xform", got.OwningNode)
	}
	if got.OutputKey != "default" {
		t.Errorf("OutputKey = %q, want default", got.OutputKey)
	}
	if got.TableType != "DELTA" {
		t.Errorf("TableType = %q, want DELTA", got.TableType)
	}
	if len(got.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(got.Columns))
	}
	if got.UpdateTime == nil {
		t.Error("UpdateTime should be populated from the latest commit's timestamp")
	}
	if res.AWSAvailable {
		t.Error("AWSAvailable should be false when no Glue client is wired")
	}
}

func TestCatalogIgnoresCloudPipelines(t *testing.T) {
	// A compute=lambda (default) pipeline must NOT contribute to the local
	// catalog walk — those tables live in Glue, not the workspace.
	workspace := t.TempDir()
	pipelineDir := filepath.Join(workspace, "cloud")
	if err := os.MkdirAll(pipelineDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	cloudTF := `variable "pipeline_name" { type = string default = "cloud" }
module "src"   { source = "clavesa/source/aws"    pipeline_name = var.pipeline_name bucket = "x" prefix = "y/" format = "csv" }
module "xform" { source = "clavesa/transform/aws" pipeline_name = var.pipeline_name inputs = { primary = module.src.outputs["default"] } compute = "lambda" language = "sparksql" sql = "SELECT 1" }
`
	_ = os.WriteFile(filepath.Join(pipelineDir, "main.tf"), []byte(cloudTF), 0o644)
	// Even with a stray warehouse dir, nothing should surface.
	_ = os.MkdirAll(filepath.Join(pipelineDir, ".clavesa", "warehouse", "clavesa_cloud", "xform__default", "_delta_log"), 0o755)
	_ = os.WriteFile(
		filepath.Join(pipelineDir, ".clavesa", "warehouse", "clavesa_cloud", "xform__default", "_delta_log", "00000000000000000000.json"),
		[]byte(deltaInitialCommit),
		0o644,
	)

	h := api.NewCatalogHandler(nil).WithWorkspace(workspace)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/workspace/tables", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	var res api.CatalogResponse
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(res.Tables) != 0 {
		t.Errorf("expected 0 local tables for cloud-compute pipeline, got %d", len(res.Tables))
	}
}

// TestCatalogStampsCloudPipelineMeta verifies ADR-014 parity for cloud
// tables: the freshness_sla declared in the user's local .tf and the
// pipeline's `dir` (relative to workspace root) flow onto Glue-sourced
// tables, not just local-warehouse ones. Without this the freshness chip
// only ever lights up for compute = "local" pipelines.
func TestCatalogStampsCloudPipelineMeta(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "clavesa.json"),
		[]byte(`{"name":"test","cloud":"aws","version":1,"catalog":"clavesa_test"}`+"\n"), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	// The catalog reads the workspace warehouse (the EnvModeToggle
	// contract): cloud lists Glue, local lists the on-disk tables. This
	// test exercises the cloud half, so declare a cloud warehouse.
	writeEnvMode(t, workspace, "cloud")
	// Convention: pipeline directory name == var.pipeline_name. The walker
	// uses the dir basename for Glue DB matching, so they must agree.
	pipelineDir := filepath.Join(workspace, "cloud")
	if err := os.MkdirAll(pipelineDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Lambda compute, freshness_sla declared on the transform node.
	cloudTF := `variable "pipeline_name" { type = string default = "cloud" }
module "src" {
  source        = "clavesa/source/aws"
  pipeline_name = var.pipeline_name
  bucket        = "in"
  prefix        = "raw/"
  format        = "csv"
}
module "xform" {
  source        = "clavesa/transform/aws"
  pipeline_name = var.pipeline_name
  inputs        = { primary = module.src.outputs["default"] }
  compute       = "lambda"
  language      = "sparksql"
  sql           = "SELECT 1"
  freshness_sla = "4h"
}
`
	if err := os.WriteFile(filepath.Join(pipelineDir, "main.tf"), []byte(cloudTF), 0o644); err != nil {
		t.Fatalf("write tf: %v", err)
	}

	fake := &fakeGlueClient{
		databases: []string{"clavesa_test__cloud"},
		tables: map[string][]gluetypes.Table{
			"clavesa_test__cloud": {{
				Name: aws.String("xform__default"),
				Parameters: map[string]string{
					"table_type": "ICEBERG",
				},
				StorageDescriptor: &gluetypes.StorageDescriptor{
					Location: aws.String("s3://bkt/cloud/xform__default"),
					Columns: []gluetypes.Column{
						{Name: aws.String("id"), Type: aws.String("bigint")},
					},
				},
			}},
		},
	}

	h := api.NewCatalogHandler(fake).WithWorkspace(workspace)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/workspace/tables", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}

	var res api.CatalogResponse
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(res.Tables) != 1 {
		t.Fatalf("expected 1 cloud table, got %d", len(res.Tables))
	}
	got := res.Tables[0]
	if got.OwningPipeline != "cloud" {
		t.Errorf("OwningPipeline = %q, want cloud", got.OwningPipeline)
	}
	// Dir parity — cloud tables previously had no dir, so the snapshots
	// hook fell through to the cloud Resolver path no matter what; the new
	// stamp lets dir thread through consistently.
	if got.Dir != "cloud" {
		t.Errorf("Dir = %q, want cloud", got.Dir)
	}
	// Freshness parity — 4h → 14400s.
	if got.FreshnessSLASeconds != 14400 {
		t.Errorf("FreshnessSLASeconds = %d, want 14400", got.FreshnessSLASeconds)
	}
}
