package service

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/databricks"
	"github.com/vesahyp/clavesa/internal/workspace"
)

type fakeDropper struct {
	tables  []databricks.Table
	deleted []string
	listed  string
}

func (f *fakeDropper) ListTables(_ context.Context, catalog, schema string) ([]databricks.Table, error) {
	f.listed = catalog + "." + schema
	return f.tables, nil
}
func (f *fakeDropper) DeleteTable(_ context.Context, name string) error {
	f.deleted = append(f.deleted, name)
	return nil
}

func TestUnregisterPipelineDatabricksTables(t *testing.T) {
	svc := newDatabricksWorkspace(t, &stubDatabricks{})
	if err := os.MkdirAll(filepath.Join(svc.workspace, "uc_scale"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(svc.workspace, "uc_scale", "variables.tf"),
		[]byte("variable \"schema\" {\n  type    = string\n  default = \"scale\"\n}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	fake := &fakeDropper{tables: []databricks.Table{{FullName: "cat.scale.daily_events"}}}
	prev := newDatabricksDropper
	newDatabricksDropper = func(_, _, _ string) tableDropper { return fake }
	t.Cleanup(func() { newDatabricksDropper = prev })

	set := func(create bool) {
		m, _ := workspace.Load(svc.workspace)
		m.Databricks = &workspace.Databricks{Host: "https://dbc-1.cloud.databricks.com", Catalog: "cat", Credential: "dbx-sp", CreateCatalog: create}
		if err := workspace.SaveManifest(svc.workspace, m); err != nil {
			t.Fatal(err)
		}
	}

	// The team's catalog: nothing is touched.
	set(false)
	if got, err := svc.UnregisterPipelineDatabricksTables(context.Background(), "uc_scale"); err != nil || len(got) != 0 || fake.listed != "" {
		t.Fatalf("team catalog: got %v, err %v, listed %q", got, err, fake.listed)
	}
	// clavesa's own catalog: the pipeline's schema is emptied.
	set(true)
	got, err := svc.UnregisterPipelineDatabricksTables(context.Background(), "uc_scale")
	if err != nil || strings.Join(got, ",") != "cat.scale.daily_events" || fake.listed != "cat.scale" {
		t.Fatalf("create-catalog: got %v, err %v, listed %q", got, err, fake.listed)
	}
}
