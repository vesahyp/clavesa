package service

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/databricks"
	"github.com/vesahyp/clavesa/internal/workspace"
)

type fakeCleaner struct {
	objects []databricks.ExternalMetadata
	deleted []string
	listed  int
}

func (f *fakeCleaner) ListExternalMetadata(context.Context) ([]databricks.ExternalMetadata, error) {
	f.listed++
	return f.objects, nil
}

func (f *fakeCleaner) DeleteExternalMetadata(_ context.Context, name string) error {
	f.deleted = append(f.deleted, name)
	return nil
}

// GH #108: destroy deletes the lineage objects the pipeline's runs made,
// chosen by their clavesa.pipeline property (not by a name prefix, which
// would also match pipeline p1_x); deploy deletes only the steps of nodes
// the pipeline no longer has.
func TestRemovePipelineLineage(t *testing.T) {
	svc := newDatabricksWorkspace(t, &stubDatabricks{})
	step := func(pipeline, node string) databricks.ExternalMetadata {
		return databricks.ExternalMetadata{Name: "clavesa_" + pipeline + "_" + node, EntityType: "clavesa transform",
			Properties: map[string]string{"clavesa.pipeline": pipeline, "clavesa.node": node}}
	}
	fake := &fakeCleaner{objects: []databricks.ExternalMetadata{
		step("p1", "orders_raw"),
		step("p1", "compact"), // a task since removed from the job
		{Name: "clavesa_p1_in_raw", EntityType: "clavesa source (s3)",
			Properties: map[string]string{"clavesa.pipeline": "p1", "clavesa.input": "raw"}},
		step("p1_x", "orders_raw"),            // another pipeline, same name prefix
		{Name: "clavesa_p1_hand_made", EntityType: "clavesa transform"}, // no properties: not clavesa's
	}}
	prev := newLineageCleaner
	newLineageCleaner = func(_, _, _ string) lineageCleaner { return fake }
	t.Cleanup(func() { newLineageCleaner = prev })

	// No Databricks connection: nothing to remove, no call.
	if got, err := svc.RemovePipelineLineage(context.Background(), "p1", nil); err != nil || len(got) != 0 || fake.listed != 0 {
		t.Fatalf("not connected: got %v, err %v, listed %d", got, err, fake.listed)
	}
	m, _ := workspace.Load(svc.workspace)
	m.Databricks = &workspace.Databricks{Host: "https://dbc-1.cloud.databricks.com", Catalog: "cat", Credential: "dbx-sp"}
	if err := workspace.SaveManifest(svc.workspace, m); err != nil {
		t.Fatal(err)
	}

	// Deploy: only the step of the removed node goes.
	got, err := svc.RemovePipelineLineage(context.Background(), "p1", map[string]bool{"orders_raw": true})
	if err != nil || strings.Join(got, ",") != "clavesa_p1_compact" {
		t.Fatalf("prune: got %v, err %v", got, err)
	}

	// Destroy: every object of p1, nothing else.
	fake.deleted = nil
	got, err = svc.RemovePipelineLineage(context.Background(), "p1", nil)
	sort.Strings(got)
	want := "clavesa_p1_compact,clavesa_p1_in_raw,clavesa_p1_orders_raw"
	if err != nil || strings.Join(got, ",") != want || strings.Join(fake.deleted, ",") != "clavesa_p1_orders_raw,clavesa_p1_compact,clavesa_p1_in_raw" {
		t.Fatalf("destroy: got %v (deleted %v), err %v; want %s", got, fake.deleted, err, want)
	}
}
