package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/databricks"
	"github.com/vesahyp/clavesa/internal/workspace"
)

type fakeTables map[string]databricks.Table

func (f fakeTables) GetTable(_ context.Context, name string) (databricks.Table, error) {
	if t, ok := f[name]; ok {
		return t, nil
	}
	return databricks.Table{}, &databricks.APIError{Status: 404, Message: "TABLE_DOES_NOT_EXIST"}
}

func TestUCFullName(t *testing.T) {
	for _, c := range []struct{ uc, want string }{
		{"a.b.c", "a.b.c"},
		{"sales.t", "cat.sales.t"},
		{"t", "cat.orders.t"},
		{"", "cat.orders.node"},
	} {
		if got := ucFullName("cat", "orders", c.uc, "node"); got != c.want {
			t.Errorf("ucFullName(%q) = %q, want %q", c.uc, got, c.want)
		}
	}
}

// The runner role gets write on an existing external output table's
// folder and on a new table's unity_catalog_location, nothing for a
// managed table or the workspace bucket, and the bucket's KMS key.
func TestSyncDatabricksAccess(t *testing.T) {
	svc := newDatabricksWorkspace(t, &stubDatabricks{})
	m, err := workspace.Load(svc.workspace)
	if err != nil {
		t.Fatal(err)
	}
	m.Databricks = &workspace.Databricks{Host: "https://dbc-1.cloud.databricks.com", Catalog: "cat", Credential: "dbx-sp", PublishTables: true}
	if err := workspace.SaveManifest(svc.workspace, m); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(svc.workspace, "orders")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	mainTF := `module "daily" {
  source = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v0.18.0"
  name   = "daily"
  sql    = "SELECT 1"
  output_definitions = {
    default = { unity_catalog_table = "team.sales.daily" }
  }
}

module "shadow" {
  source = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v0.18.0"
  name   = "shadow"
  sql    = "SELECT 1"
  output_definitions = {
    default = { unity_catalog_table = "sales.daily_clavesa", unity_catalog_location = "s3://team-bucket/clavesa/daily/" }
  }
}

module "managed" {
  source = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v0.18.0"
  name   = "managed"
  sql    = "SELECT 1"
}
`
	if err := os.WriteFile(filepath.Join(dir, "main.tf"), []byte(mainTF), 0o644); err != nil {
		t.Fatal(err)
	}
	prevT, prevK := newDatabricksTableGetter, bucketKMSKey
	newDatabricksTableGetter = func(_, _, _ string) tableGetter {
		return fakeTables{
			"team.sales.daily":    {TableType: "EXTERNAL", StorageLocation: "s3://team-bucket/tables/daily"},
			"cat.orders.managed":  {TableType: "MANAGED", StorageLocation: "s3://dbx-managed/x"},
			"cat.sales.daily_old": {TableType: "EXTERNAL"},
		}
	}
	bucketKMSKey = func(_ context.Context, bucket string) (string, error) {
		if bucket == "team-bucket" {
			return "arn:aws:kms:us-east-2:1:key/abc", nil
		}
		return "", errors.New("unexpected bucket " + bucket)
	}
	t.Cleanup(func() { newDatabricksTableGetter, bucketKMSKey = prevT, prevK })

	acc, err := svc.SyncDatabricksAccess(context.Background(), "orders")
	if err != nil {
		t.Fatalf("SyncDatabricksAccess: %v", err)
	}
	want := []string{"s3://team-bucket/clavesa/daily", "s3://team-bucket/tables/daily"}
	if strings.Join(acc.Write, ",") != strings.Join(want, ",") {
		t.Errorf("write = %v, want %v", acc.Write, want)
	}
	if len(acc.Read) != 0 || strings.Join(acc.KMSKeys, ",") != "arn:aws:kms:us-east-2:1:key/abc" {
		t.Errorf("read = %v, kms = %v", acc.Read, acc.KMSKeys)
	}
	body, err := os.ReadFile(filepath.Join(dir, DatabricksAccessFile))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `databricks_s3_write = ["s3://team-bucket/clavesa/daily", "s3://team-bucket/tables/daily"]`) {
		t.Errorf("tfvars:\n%s", body)
	}

	m.Databricks = nil
	if err := workspace.SaveManifest(svc.workspace, m); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.SyncDatabricksAccess(context.Background(), "orders"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, DatabricksAccessFile)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("%s must go when the connection does: %v", DatabricksAccessFile, err)
	}
}

type fakeLister struct {
	fakeTables
	schemas map[string][]databricks.Table
}

func (f fakeLister) ListTables(_ context.Context, catalog, schema string) ([]databricks.Table, error) {
	if ts, ok := f.schemas[catalog+"."+schema]; ok {
		return ts, nil
	}
	return nil, &databricks.APIError{Status: 404, Message: "SCHEMA_DOES_NOT_EXIST"}
}

// A shadow table named with unity_catalog_table lives outside the
// pipeline's schema; the listing finds it there, and a missing pipeline
// schema is no error when every output names its own table.
func TestDatabricksTablesFindsExplicitOutputTables(t *testing.T) {
	svc := newDatabricksWorkspace(t, &stubDatabricks{})
	m, err := workspace.Load(svc.workspace)
	if err != nil {
		t.Fatal(err)
	}
	m.Databricks = &workspace.Databricks{Host: "https://dbc-1.cloud.databricks.com", Catalog: "cat", Credential: "dbx-sp", PublishTables: true}
	if err := workspace.SaveManifest(svc.workspace, m); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(svc.workspace, "events")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	mainTF := `module "daily" {
  source = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v0.18.0"
  name   = "daily"
  sql    = "SELECT 1"
  output_definitions = {
    default = { unity_catalog_table = "team.sales.daily_events_clavesa" }
  }
}
`
	if err := os.WriteFile(filepath.Join(dir, "main.tf"), []byte(mainTF), 0o644); err != nil {
		t.Fatal(err)
	}
	prev := newDatabricksLister
	newDatabricksLister = func(_, _, _ string) tableLister {
		return fakeLister{fakeTables: fakeTables{
			"team.sales.daily_events_clavesa": {FullName: "team.sales.daily_events_clavesa", TableType: "EXTERNAL"},
		}}
	}
	t.Cleanup(func() { newDatabricksLister = prev })

	rows, err := svc.DatabricksTables(context.Background())
	if err != nil {
		t.Fatalf("DatabricksTables: %v", err)
	}
	var found bool
	for _, r := range rows {
		if r.Pipeline != "events" {
			continue
		}
		found = true
		if r.SchemaError != "" {
			t.Errorf("schema error = %q, want none", r.SchemaError)
		}
		if len(r.Tables) != 1 || r.Tables[0].FullName != "team.sales.daily_events_clavesa" || !r.Tables[0].ByClavesa {
			t.Errorf("tables = %+v", r.Tables)
		}
		if r.Tables[0].URL != "https://dbc-1.cloud.databricks.com/explore/data/team/sales/daily_events_clavesa" {
			t.Errorf("url = %q", r.Tables[0].URL)
		}
	}
	if !found {
		t.Fatalf("no row for pipeline events: %+v", rows)
	}
}
