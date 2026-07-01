package service

import (
	"context"
	"testing"

	"github.com/vesahyp/clavesa/internal/observability"
)

// TestServiceTablesStateForDir exercises the service wiring: it must dispatch
// through the resolver-picked provider and thread the pipeline name / dir /
// limit onto the Tables query. The provider's own row shaping is pinned in
// the observability-package tests; this only verifies the plumbing.
func TestServiceTablesStateForDir(t *testing.T) {
	f := &fakeProvider{
		tables: []observability.TableInfo{
			{Pipeline: "demo", Node: "trips", OutputKey: "default", TableName: "trips", FileCount: ri64(3), TotalBytes: ri64(3072)},
		},
	}
	svc := dashService(t, f)

	res, err := svc.TablesStateForDir(context.Background(), "demo", "demo", 25)
	if err != nil {
		t.Fatalf("TablesStateForDir: %v", err)
	}
	if f.tablesQ.PipelineName != "demo" || f.tablesQ.PipelineDir != "demo" || f.tablesQ.Limit != 25 {
		t.Errorf("query mis-wired: %+v", f.tablesQ)
	}
	if f.tablesQ.Database == "" {
		t.Error("expected the system Glue DB to be stamped onto the query")
	}
	if res == nil || len(res.Rows) != 1 || res.Rows[0].TableName != "trips" {
		t.Fatalf("expected one table 'trips', got %+v", res)
	}
	if res.Rows[0].FileCount == nil || *res.Rows[0].FileCount != 3 {
		t.Fatalf("expected file_count=3, got %+v", res.Rows[0].FileCount)
	}
}
