package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/observability"
)

// fakeProvider is a Provider stub for the dashboards tests: Query and
// Exec are configurable/recording, the rest panic (unreached).
type fakeProvider struct {
	execSQLs  []string
	querySQLs []string
	queryRes  *observability.QueryResult
	queryErr  error
	execErr   error
}

func (f *fakeProvider) Query(_ context.Context, q observability.QueryQuery) (*observability.QueryResult, error) {
	f.querySQLs = append(f.querySQLs, q.SQL)
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	if f.queryRes != nil {
		return f.queryRes, nil
	}
	return &observability.QueryResult{Columns: []observability.SampleTableColumn{}, Rows: [][]string{}}, nil
}
func (f *fakeProvider) Exec(_ context.Context, q observability.ExecQuery) error {
	f.execSQLs = append(f.execSQLs, q.SQL)
	return f.execErr
}
func (f *fakeProvider) NodeRuns(context.Context, observability.NodeRunsQuery) (*observability.NodeRunsResult, error) {
	panic("unused")
}
func (f *fakeProvider) Runs(context.Context, observability.RunsQuery) (*observability.RunsResult, error) {
	panic("unused")
}
func (f *fakeProvider) Tables(context.Context, observability.TablesQuery) (*observability.TablesResult, error) {
	panic("unused")
}
func (f *fakeProvider) Snapshots(context.Context, observability.SnapshotsQuery) (*observability.SnapshotsResult, error) {
	panic("unused")
}
func (f *fakeProvider) ColumnStats(context.Context, observability.ColumnStatsQuery) (*observability.ColumnStatsResult, error) {
	panic("unused")
}
func (f *fakeProvider) SampleTable(context.Context, observability.SampleTableQuery) (*observability.SampleTableResult, error) {
	panic("unused")
}
func (f *fakeProvider) ExecutionStates(context.Context, observability.ExecutionStatesQuery) (*observability.ExecutionStatesResult, error) {
	panic("unused")
}
func (f *fakeProvider) ExecutionLogs(context.Context, observability.ExecutionLogsQuery) (*observability.ExecutionLogsResult, error) {
	panic("unused")
}

// dashService builds a Service rooted at a fresh temp workspace, wired to
// the fake provider through a resolver. The temp workspace has no
// environment.json, so the resolver dispatches local — the fake is the
// local provider.
func dashService(t *testing.T, f *fakeProvider) *Service {
	t.Helper()
	ws := t.TempDir()
	resolver := observability.NewResolver(ws, f, f)
	return New(ws).WithResolver(resolver)
}

func TestNormalizeDashboardFileLegacy(t *testing.T) {
	f := dashboardFile{
		Title:              "Pipeline runs",
		DefaultPipelineDir: "demo",
		Widgets: []DashboardWidget{
			{ID: "a", Type: "big_number", SQL: "SELECT 1 AS n", ValueField: "n",
				Layout: DashboardWidgetLayout{W: 3, H: 2}},
			{ID: "b", Type: "big_number", SQL: "SELECT 1 AS n", ValueField: "n",
				Layout: DashboardWidgetLayout{X: 3, W: 3, H: 2}},
			{ID: "c", Type: "bar", SQL: "SELECT x, y FROM t", XField: "x", YField: "y",
				Layout: DashboardWidgetLayout{Y: 2, W: 6, H: 4}},
		},
	}
	d := normalizeDashboardFile("pipeline-runs-demo", f)

	// Widgets a and b share identical SQL → one dataset; c is distinct.
	if len(d.Datasets) != 2 {
		t.Fatalf("want 2 synthesized datasets, got %d: %+v", len(d.Datasets), d.Datasets)
	}
	for _, ds := range d.Datasets {
		if ds.Dir != "demo" {
			t.Errorf("dataset %q: want dir=demo, got %q", ds.Name, ds.Dir)
		}
	}
	if d.Widgets[0].Dataset != d.Widgets[1].Dataset {
		t.Errorf("widgets with identical SQL should share a dataset: %q vs %q",
			d.Widgets[0].Dataset, d.Widgets[1].Dataset)
	}
	if d.Widgets[2].Dataset == d.Widgets[0].Dataset {
		t.Errorf("widget c has distinct SQL but got the same dataset as a")
	}
	for _, w := range d.Widgets {
		if w.SQL != "" {
			t.Errorf("widget %q: inline SQL should be cleared after normalization", w.ID)
		}
		if w.Dataset == "" {
			t.Errorf("widget %q: dataset reference not set", w.ID)
		}
	}
}

func TestNormalizeDashboardFileAlreadyDatasets(t *testing.T) {
	f := dashboardFile{
		Title:    "Revenue",
		Datasets: []DashboardDataset{{Name: "rev", Dir: "demo", SQL: "SELECT 1"}},
		Widgets: []DashboardWidget{
			{ID: "a", Type: "table", Dataset: "rev", Layout: DashboardWidgetLayout{W: 6, H: 4}},
		},
	}
	d := normalizeDashboardFile("revenue", f)
	if len(d.Datasets) != 1 || d.Datasets[0].Name != "rev" {
		t.Fatalf("datasets-shaped file should pass through, got %+v", d.Datasets)
	}
	if d.Widgets[0].Dataset != "rev" {
		t.Errorf("widget dataset ref changed: %q", d.Widgets[0].Dataset)
	}
}

func TestValidateDashboard(t *testing.T) {
	ok := Dashboard{
		Slug:     "revenue",
		Title:    "Revenue",
		Datasets: []DashboardDataset{{Name: "rev", Dir: "demo", SQL: "SELECT 1"}},
		Widgets: []DashboardWidget{
			{ID: "w1", Type: "table", Dataset: "rev", Layout: DashboardWidgetLayout{W: 6, H: 4}},
		},
	}
	if err := validateDashboard(ok); err != nil {
		t.Fatalf("valid dashboard rejected: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*Dashboard)
	}{
		{"bad slug", func(d *Dashboard) { d.Slug = "Bad Slug" }},
		{"duplicate dataset", func(d *Dashboard) {
			d.Datasets = append(d.Datasets, DashboardDataset{Name: "rev", Dir: "demo", SQL: "SELECT 2"})
		}},
		{"dataset empty sql", func(d *Dashboard) { d.Datasets[0].SQL = "  " }},
		{"dataset empty dir", func(d *Dashboard) { d.Datasets[0].Dir = "" }},
		{"unknown widget type", func(d *Dashboard) { d.Widgets[0].Type = "pie" }},
		{"dangling dataset ref", func(d *Dashboard) { d.Widgets[0].Dataset = "nope" }},
		{"widget no id", func(d *Dashboard) { d.Widgets[0].ID = "" }},
		{"layout off grid", func(d *Dashboard) { d.Widgets[0].Layout = DashboardWidgetLayout{X: 8, W: 6, H: 2} }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := ok
			d.Datasets = append([]DashboardDataset(nil), ok.Datasets...)
			d.Widgets = append([]DashboardWidget(nil), ok.Widgets...)
			c.mutate(&d)
			if err := validateDashboard(d); err == nil {
				t.Errorf("expected %s to be rejected", c.name)
			}
		})
	}
}

func TestSaveDashboardEmitsMerge(t *testing.T) {
	f := &fakeProvider{}
	s := dashService(t, f)
	d := Dashboard{
		Slug:     "revenue",
		Title:    "Revenue",
		Datasets: []DashboardDataset{{Name: "rev", Dir: "demo", SQL: "SELECT 1"}},
		Widgets: []DashboardWidget{
			{ID: "w1", Type: "table", Dataset: "rev", Layout: DashboardWidgetLayout{W: 6, H: 4}},
		},
	}
	// GetDashboard (the read-back) needs a row to parse.
	f.queryRes = &observability.QueryResult{
		Columns: []observability.SampleTableColumn{{Name: "slug"}, {Name: "title"}, {Name: "spec"}, {Name: "updated_at"}},
		Rows: [][]string{{
			"revenue", "Revenue",
			`{"datasets":[{"name":"rev","dir":"demo","sql":"SELECT 1"}],"widgets":[{"id":"w1","type":"table","title":"","dataset":"rev","layout":{"x":0,"y":0,"w":6,"h":4}}]}`,
			"2026-05-19 00:00:00",
		}},
	}
	got, err := s.SaveDashboard(context.Background(), d)
	if err != nil {
		t.Fatalf("SaveDashboard: %v", err)
	}
	if got.Slug != "revenue" || len(got.Datasets) != 1 || len(got.Widgets) != 1 {
		t.Fatalf("read-back mismatch: %+v", got)
	}

	var sawCreate, sawMerge bool
	for _, q := range f.execSQLs {
		if strings.HasPrefix(q, "CREATE TABLE IF NOT EXISTS") {
			sawCreate = true
		}
		if strings.HasPrefix(q, "MERGE INTO") && strings.Contains(q, "'revenue'") {
			sawMerge = true
		}
	}
	if !sawCreate {
		t.Errorf("expected a CREATE TABLE statement, got: %v", f.execSQLs)
	}
	if !sawMerge {
		t.Errorf("expected a MERGE INTO with the slug, got: %v", f.execSQLs)
	}
}

func TestSaveDashboardRejectsInvalid(t *testing.T) {
	f := &fakeProvider{}
	s := dashService(t, f)
	_, err := s.SaveDashboard(context.Background(), Dashboard{Slug: "Bad Slug"})
	if err == nil {
		t.Fatal("expected an invalid dashboard to be rejected before any write")
	}
	if len(f.execSQLs) != 0 {
		t.Errorf("validation should fail before touching the provider, got: %v", f.execSQLs)
	}
}

func TestListDashboards(t *testing.T) {
	f := &fakeProvider{
		queryRes: &observability.QueryResult{
			Columns: []observability.SampleTableColumn{{Name: "slug"}, {Name: "title"}},
			Rows:    [][]string{{"revenue", "Revenue"}, {"pipeline-runs-demo", "Pipeline runs"}},
		},
	}
	s := dashService(t, f)
	got, err := s.ListDashboards(context.Background())
	if err != nil {
		t.Fatalf("ListDashboards: %v", err)
	}
	if len(got) != 2 || got[0].Slug != "pipeline-runs-demo" || got[1].Slug != "revenue" {
		t.Fatalf("want 2 dashboards sorted by slug, got %+v", got)
	}
}

func TestGetDashboardNotFound(t *testing.T) {
	f := &fakeProvider{queryRes: &observability.QueryResult{Rows: [][]string{}}}
	s := dashService(t, f)
	_, err := s.GetDashboard(context.Background(), "missing")
	if err == nil {
		t.Fatal("expected not-found error")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("not-found error should satisfy errors.Is(os.ErrNotExist), got %v", err)
	}
}

func TestListDashboardsMissingTableIsEmpty(t *testing.T) {
	f := &fakeProvider{queryErr: errors.New("TABLE_OR_VIEW_NOT_FOUND: dashboards")}
	s := dashService(t, f)
	got, err := s.ListDashboards(context.Background())
	if err != nil {
		t.Fatalf("a missing table should render empty, not error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("want empty list, got %+v", got)
	}
}

func TestImportLegacyDashboards(t *testing.T) {
	f := &fakeProvider{
		queryRes: &observability.QueryResult{
			Columns: []observability.SampleTableColumn{{Name: "slug"}, {Name: "title"}},
			Rows:    [][]string{{"pipeline-runs-demo", "Pipeline runs"}},
		},
	}
	s := dashService(t, f)

	// Drop a legacy dashboard file into the workspace.
	dir := filepath.Join(s.workspace, ".clavesa", "dashboards")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	legacy := `{"title":"Pipeline runs","default_pipeline_dir":"demo","widgets":[` +
		`{"id":"a","type":"big_number","sql":"SELECT 1 AS n","value_field":"n","layout":{"x":0,"y":0,"w":3,"h":2}}]}`
	if err := os.WriteFile(filepath.Join(dir, "pipeline-runs-demo.json"), []byte(legacy), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := s.ListDashboards(context.Background()); err != nil {
		t.Fatalf("ListDashboards (with migration): %v", err)
	}

	// The legacy file was MERGEd into the table.
	var sawMerge bool
	for _, q := range f.execSQLs {
		if strings.HasPrefix(q, "MERGE INTO") && strings.Contains(q, "'pipeline-runs-demo'") {
			sawMerge = true
		}
	}
	if !sawMerge {
		t.Errorf("expected the legacy file to be MERGEd, exec SQLs: %v", f.execSQLs)
	}
	// The directory was moved aside so the import does not repeat.
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("legacy dashboards dir should be moved aside after import")
	}
	if _, err := os.Stat(dir + ".imported"); err != nil {
		t.Errorf("expected .imported backup dir: %v", err)
	}
}
