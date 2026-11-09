package service

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/workspace"
)

func connectDatabricksForTest(t *testing.T, ws string) {
	t.Helper()
	m, err := workspace.Load(ws)
	if err != nil || m == nil {
		m = &workspace.Manifest{Name: "ws"}
	}
	m.Databricks = &workspace.Databricks{Host: "https://dbc-1.cloud.databricks.com", Catalog: "analytics", Credential: "dbx-sp"}
	if err := workspace.SaveManifest(ws, m); err != nil {
		t.Fatal(err)
	}
	if _, err := New(ws).AddCredential(CredentialSpec{Name: "dbx-sp", Kind: "databricks-oauth", ClientID: "app-id", Secret: "file:.clavesa/credentials/dbx.secret"}); err != nil {
		t.Fatal(err)
	}
}

func TestAddSourceUCURLNeedsDatabricks(t *testing.T) {
	t.Parallel()
	svc := New(t.TempDir())
	_, err := svc.AddSource(SourceSpec{Name: "orders", URL: "uc://main.sales.orders"})
	if err == nil || !strings.Contains(err.Error(), "clavesa workspace connect databricks") {
		t.Fatalf("AddSource without Databricks = %v, want error naming `clavesa workspace connect databricks`", err)
	}
}

func TestAddSourceUCURLPromotes(t *testing.T) {
	t.Parallel()
	ws, _ := pipelineForAttachTest(t)
	connectDatabricksForTest(t, ws)
	svc := New(ws)
	got, err := svc.AddSource(SourceSpec{Name: "orders", URL: "uc://main.sales.orders"})
	if err != nil {
		t.Fatalf("AddSource: %v", err)
	}
	if got.Kind != "unity-catalog" || got.Table != "main.sales.orders" || got.URL != "" || got.Format != "" {
		t.Errorf("stored = %#v", got)
	}
	if _, err := svc.AddSource(SourceSpec{Name: "bad", URL: "uc://main.orders"}); err == nil {
		t.Error("two-part uc:// URL should be refused")
	}
	if _, err := svc.AddSource(SourceSpec{Name: "viakind", Kind: "unity-catalog", Table: "main.sales.items"}); err != nil {
		t.Errorf("--kind/--table path: %v", err)
	}
}

func TestSyncOrchestrationEmitsUCTableDescriptor(t *testing.T) {
	t.Parallel()
	ws, dir := pipelineForAttachTest(t)
	connectDatabricksForTest(t, ws)
	svc := New(ws)
	if _, err := svc.AddSource(SourceSpec{Name: "orders", URL: "uc://main.sales.orders"}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AttachSource(dir, "orders", "t1", "raw"); err != nil {
		t.Fatal(err)
	}
	if err := svc.SyncOrchestration(dir, ""); err != nil {
		t.Fatalf("SyncOrchestration: %v", err)
	}
	body, _ := os.ReadFile(filepath.Join(ws, dir, "orchestration.tf"))
	want := `raw = { kind = "uc_table", table = "main.sales.orders" }`
	if !strings.Contains(string(body), want) {
		t.Errorf("orchestration.tf missing %s\n%s", want, body)
	}
}

func TestBuildInputsUCTable(t *testing.T) {
	ws := t.TempDir()
	connectDatabricksForTest(t, ws)
	svc := New(ws)
	if _, err := svc.AddSource(SourceSpec{Name: "orders", URL: "uc://main.sales.orders"}); err != nil {
		t.Fatal(err)
	}
	inputs, err := svc.buildInputs(context.Background(), mirrorTestGraph("orders"), "t", map[string]string{}, map[string]string{}, "cat")
	if err != nil {
		t.Fatalf("buildInputs: %v", err)
	}
	d, ok := inputs["x"].(map[string]any)
	if !ok || d["kind"] != "uc_table" || d["table"] != "main.sales.orders" || len(d) != 2 {
		t.Errorf("inputs[x] = %#v", inputs["x"])
	}
}
