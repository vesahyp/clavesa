package service

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// snapshotWorkspace is a local-warehouse workspace with a local hosted
// block; hosted adds keys to it ("snapshot": true).
func snapshotWorkspace(t *testing.T, hosted string) string {
	t.Helper()
	ws := t.TempDir()
	block := `{"url":"https://ui.clavesa.dev","kind":"local","publisher_role_arn":"arn:aws:iam::111122223333:role/clavesa-ui-publisher","data_bucket":"clavesa-ui-data-111122223333","region":"eu-north-1"` + hosted + `}`
	write := func(rel, body string) {
		p := filepath.Join(ws, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("clavesa.json", `{"name":"site-traffic","cloud":"aws","version":1,"catalog":"clavesa_site","hosted":`+block+`}`)
	write(".clavesa/dashboards/runs.json", `{"slug":"runs","title":"Runs","datasets":[{"name":"n","dir":"traffic","sql":"SELECT COUNT(*) AS n FROM clavesa_site_system__pipelines.runs"}],"widgets":[]}`)
	if err := workspace.WriteWarehouse(ws, workspace.WarehouseLocal); err != nil {
		t.Fatal(err)
	}
	return ws
}

// snapProvider is fakeProvider with a Snapshots answer.
type snapProvider struct{ *fakeProvider }

func (p snapProvider) Snapshots(context.Context, observability.SnapshotsQuery) (*observability.SnapshotsResult, error) {
	n := int64(7)
	return &observability.SnapshotsResult{LatestRecordCount: &n}, nil
}

func snapshotService(ws string, catalog CatalogSnapshotFunc) (*Service, *fakeProvider) {
	local := &fakeProvider{queryRes: &observability.QueryResult{Columns: []observability.SampleTableColumn{{Name: "n"}}, Rows: [][]string{{"12"}}}}
	return New(ws).WithResolver(observability.NewResolver(ws, &fakeProvider{}, snapProvider{local})).WithCatalogSnapshot(catalog), local
}

func catalogWithPath(ws string) CatalogSnapshotFunc {
	return func(context.Context) (json.RawMessage, []CatalogTableRef, error) {
		body := `{"tables":[{"database":"clavesa_site__traffic","name":"daily","dir":"traffic","location":"` + filepath.Join(ws, ".clavesa/warehouse/daily") + `","note":"` + filepath.Join(ws, "traffic") + `"}]}`
		return json.RawMessage(body), []CatalogTableRef{{Database: "clavesa_site__traffic", Table: "daily", Dir: "traffic"}}, nil
	}
}

// Finding 1: an existing hosted block with no "snapshot" publishes none.
func TestDrillSnapshotIsOptIn(t *testing.T) {
	ws := snapshotWorkspace(t, "")
	svc, _ := snapshotService(ws, catalogWithPath(ws))
	if snap := svc.drillSnapshot(context.Background(), time.Now()); snap != nil {
		t.Errorf("snapshot without hosted.snapshot: %+v", snap)
	}
	sum := &WorkspaceSummary{Name: "site-traffic", Kind: "local"}
	if d := svc.SummaryDrill(context.Background(), sum); d.Snapshot != nil {
		t.Errorf("drill carries a snapshot without hosted.snapshot")
	}
}

// The snapshot publishes the dashboard values (widget results).
func TestDrillSnapshotPublishesWidgets(t *testing.T) {
	ws := snapshotWorkspace(t, `,"snapshot":true`)
	svc, _ := snapshotService(ws, catalogWithPath(ws))
	snap := svc.drillSnapshot(context.Background(), time.Now())
	if snap == nil || len(snap.Catalog) == 0 || len(snap.Widgets) == 0 {
		t.Fatalf("snapshot: %+v", snap)
	}
}

// Finding 3: the snapshot has its own time; a slow part is left out with
// a note, and the publish still goes, with a live upload context.
func TestDrillSnapshotBudget(t *testing.T) {
	prev := SnapshotBudget
	SnapshotBudget = 50 * time.Millisecond
	t.Cleanup(func() { SnapshotBudget = prev })
	ws := snapshotWorkspace(t, `,"snapshot":true`)
	slow := func(ctx context.Context) (json.RawMessage, []CatalogTableRef, error) {
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}
	svc, _ := snapshotService(ws, slow)
	start := time.Now()
	snap := svc.drillSnapshot(context.Background(), time.Now())
	if time.Since(start) > 2*time.Second {
		t.Fatalf("the snapshot took %v, over its budget", time.Since(start))
	}
	if snap == nil || len(snap.Catalog) != 0 || !strings.Contains(strings.Join(snap.Notes, "|"), "out of its time") {
		t.Errorf("snapshot = %+v, want no catalog and a note that time ran out", snap)
	}

	// The upload has its own context: a summary whose context is spent
	// is still published.
	put := &fakePutter{}
	var putCtxErr error = context.Canceled
	prevPut := newSummaryPutter
	newSummaryPutter = func(ctx context.Context, _, _ string, _ HostedWellKnown) (SummaryPutter, error) {
		putCtxErr = ctx.Err()
		return put, nil
	}
	t.Cleanup(func() { newSummaryPutter = prevPut })
	spent, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := svc.PublishSummary(spent, &WorkspaceSummary{Name: "site-traffic", Kind: "local", GeneratedAt: "2026-11-17T04:00:00Z"}); err != nil {
		t.Fatalf("publish with a spent context: %v", err)
	}
	if putCtxErr != nil || !strings.Contains(string(put.body), `"site-traffic"`) {
		t.Errorf("upload context err %v, body %q", putCtxErr, put.body)
	}
}

// Finding 4: no path on the Mac in what is published.
func TestDrillSnapshotNoMacPaths(t *testing.T) {
	ws := snapshotWorkspace(t, `,"snapshot":true`)
	svc, _ := snapshotService(ws, catalogWithPath(ws))
	snap := svc.drillSnapshot(context.Background(), time.Now())
	if snap == nil || strings.Contains(string(snap.Catalog), ws) || strings.Contains(string(snap.Catalog), "location") {
		t.Fatalf("catalog = %s, want no location and no workspace path", snap.Catalog)
	}
	if !strings.Contains(string(snap.Catalog), `"note":"traffic"`) {
		t.Errorf("a path under the workspace was not made relative: %s", snap.Catalog)
	}

	// The publish drops a snapshot that still names the workspace root.
	put := &fakePutter{}
	prevPut := newSummaryPutter
	newSummaryPutter = func(context.Context, string, string, HostedWellKnown) (SummaryPutter, error) { return put, nil }
	t.Cleanup(func() { newSummaryPutter = prevPut })
	abs, _ := filepath.Abs(ws)
	sum := &WorkspaceSummary{Name: "site-traffic", Kind: "local", Drill: &SummaryDrill{
		Pipelines: map[string]PipelineDrill{},
		Snapshot:  &DrillSnapshot{Notes: []string{"see " + abs}},
	}}
	if _, err := svc.PublishSummary(context.Background(), sum); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(put.body), abs) || strings.Contains(string(put.body), `"snapshot"`) {
		t.Errorf("published body names the workspace path: %s", put.body)
	}
}

// Re-running enable keeps, unless --snapshot or --no-snapshot is given,
// the snapshot switch.
func TestEnableHostedKeepsChoices(t *testing.T) {
	prev := &workspace.Hosted{Snapshot: true}
	h := &workspace.Hosted{}
	keepHostedChoices(prev, h, HostedInput{})
	if !h.Snapshot {
		t.Errorf("kept %+v", h)
	}
	off := false
	h = &workspace.Hosted{}
	keepHostedChoices(prev, h, HostedInput{Snapshot: &off})
	if h.Snapshot {
		t.Errorf("--no-snapshot: %+v", h)
	}
}
