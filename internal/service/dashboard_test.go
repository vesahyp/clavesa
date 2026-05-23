package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
		{"unknown widget type", func(d *Dashboard) { d.Widgets[0].Type = "sankey" }},
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

func TestExpandPlaceholders(t *testing.T) {
	t.Run("substitutes named placeholders as quoted literals", func(t *testing.T) {
		out, err := expandPlaceholders(
			`SELECT * FROM t WHERE ts BETWEEN {{start}} AND {{end}} AND site = {{site}}`,
			map[string]string{
				"start": "2026-05-01T00:00:00Z",
				"end":   "2026-05-08T00:00:00Z",
				"site":  "acme",
			})
		if err != nil {
			t.Fatalf("expandPlaceholders: %v", err)
		}
		want := `SELECT * FROM t WHERE ts BETWEEN '2026-05-01T00:00:00Z' AND '2026-05-08T00:00:00Z' AND site = 'acme'`
		if out != want {
			t.Errorf("\n got: %s\nwant: %s", out, want)
		}
	})

	t.Run("dotted names work for time_range start/end", func(t *testing.T) {
		out, err := expandPlaceholders(
			`WHERE ts >= {{tr.start}} AND ts <= {{tr.end}}`,
			map[string]string{"tr.start": "2026-01-01T00:00:00Z", "tr.end": "2026-02-01T00:00:00Z"},
		)
		if err != nil {
			t.Fatalf("expandPlaceholders: %v", err)
		}
		if !strings.Contains(out, "'2026-01-01T00:00:00Z'") || !strings.Contains(out, "'2026-02-01T00:00:00Z'") {
			t.Errorf("dotted placeholders not substituted: %s", out)
		}
	})

	t.Run("missing key fails loud with the name", func(t *testing.T) {
		_, err := expandPlaceholders(`SELECT {{unknown}}`, map[string]string{})
		if err == nil {
			t.Fatal("expected error for missing placeholder")
		}
		if !strings.Contains(err.Error(), "unknown") {
			t.Errorf("error should name the placeholder: %v", err)
		}
	})

	t.Run("unsafe characters in value are rejected", func(t *testing.T) {
		for _, bad := range []string{
			`x'; DROP TABLE t;--`,
			`a"b`,
			`a;b`,
			"a\nb",
		} {
			_, err := expandPlaceholders(`SELECT {{x}}`, map[string]string{"x": bad})
			if err == nil {
				t.Errorf("expected rejection for %q", bad)
			}
		}
	})

	t.Run("sql without placeholders passes through unchanged", func(t *testing.T) {
		sql := `SELECT COUNT(*) FROM t WHERE active = true`
		out, err := expandPlaceholders(sql, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if out != sql {
			t.Errorf("sql changed unexpectedly: %s", out)
		}
	})
}

func TestResolveTimePreset(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		preset   string
		wantSpan time.Duration
	}{
		{"last_24h", 24 * time.Hour},
		{"last_7d", 7 * 24 * time.Hour},
		{"last_30d", 30 * 24 * time.Hour},
		{"last_90d", 90 * 24 * time.Hour},
		{"", 30 * 24 * time.Hour}, // default
		{"bogus", 30 * 24 * time.Hour},
	} {
		t.Run(c.preset, func(t *testing.T) {
			start, end := resolveTimePreset(c.preset, now)
			s, _ := time.Parse(time.RFC3339, start)
			e, _ := time.Parse(time.RFC3339, end)
			if got := e.Sub(s); got != c.wantSpan {
				t.Errorf("span: got %v, want %v", got, c.wantSpan)
			}
			if !e.Equal(now) {
				t.Errorf("end should equal now, got %v", e)
			}
		})
	}
}

func TestResolveControlDefaults(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	controls := []DashboardControl{
		{Name: "tr", Type: "time_range", Default: "last_7d"},
		{Name: "site", Type: "select", Default: "acme", Options: []string{"acme", "globex"}},
		{Name: "region", Type: "select", Options: []string{"eu", "us"}},
	}
	t.Run("fills declared defaults when params are empty", func(t *testing.T) {
		out := map[string]string{}
		resolveControlDefaults(controls, out, now)
		if _, ok := out["tr.start"]; !ok {
			t.Error("tr.start should be filled")
		}
		if _, ok := out["tr.end"]; !ok {
			t.Error("tr.end should be filled")
		}
		if out["site"] != "acme" {
			t.Errorf("site default: got %q, want acme", out["site"])
		}
		if out["region"] != "eu" {
			t.Errorf("region with no Default should use first option, got %q", out["region"])
		}
	})

	t.Run("explicit params win over declared defaults", func(t *testing.T) {
		out := map[string]string{
			"tr.start": "2025-01-01T00:00:00Z",
			"tr.end":   "2025-02-01T00:00:00Z",
			"site":     "globex",
		}
		resolveControlDefaults(controls, out, now)
		if out["tr.start"] != "2025-01-01T00:00:00Z" {
			t.Errorf("explicit start overwritten: %q", out["tr.start"])
		}
		if out["site"] != "globex" {
			t.Errorf("explicit site overwritten: %q", out["site"])
		}
	})
}

func TestValidateDashboardControls(t *testing.T) {
	base := Dashboard{
		Slug:     "demo",
		Title:    "Demo",
		Datasets: []DashboardDataset{{Name: "rev", Dir: "demo", SQL: "SELECT 1"}},
		Widgets: []DashboardWidget{
			{ID: "w1", Type: "table", Dataset: "rev", Layout: DashboardWidgetLayout{W: 6, H: 4}},
		},
	}

	for _, c := range []struct {
		name     string
		controls []DashboardControl
		ok       bool
	}{
		{"time_range valid", []DashboardControl{{Name: "tr", Type: "time_range", Default: "last_7d"}}, true},
		{"select with options valid", []DashboardControl{{Name: "s", Type: "select", Options: []string{"a", "b"}}}, true},
		{"select with sql valid", []DashboardControl{{Name: "s", Type: "select", SQL: "SELECT DISTINCT site FROM t", Dir: "demo"}}, true},
		{"select without sql or options rejected", []DashboardControl{{Name: "s", Type: "select"}}, false},
		{"select with sql but no dir rejected", []DashboardControl{{Name: "s", Type: "select", SQL: "SELECT 1"}}, false},
		{"unknown type rejected", []DashboardControl{{Name: "s", Type: "slider"}}, false},
		{"duplicate name rejected", []DashboardControl{{Name: "x", Type: "time_range"}, {Name: "x", Type: "select", Options: []string{"a"}}}, false},
		{"invalid name rejected", []DashboardControl{{Name: "Bad Name", Type: "time_range"}}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			d := base
			d.Controls = c.controls
			err := validateDashboard(d)
			if c.ok && err != nil {
				t.Errorf("expected ok, got %v", err)
			}
			if !c.ok && err == nil {
				t.Error("expected validation error, got nil")
			}
		})
	}
}

func TestRenderDashboardSubstitutes(t *testing.T) {
	f := &fakeProvider{}
	s := dashService(t, f)

	// Seed GetDashboard's read-back with a dashboard that has a control
	// and a dataset referencing its placeholder.
	spec := `{"datasets":[{"name":"rev","dir":"demo","sql":"SELECT * FROM t WHERE ts >= {{tr.start}}"}],` +
		`"widgets":[{"id":"w1","type":"table","title":"","dataset":"rev","layout":{"x":0,"y":0,"w":6,"h":4}}],` +
		`"controls":[{"name":"tr","type":"time_range","default":"last_7d"}]}`
	f.queryRes = &observability.QueryResult{
		Columns: []observability.SampleTableColumn{{Name: "slug"}, {Name: "title"}, {Name: "spec"}, {Name: "updated_at"}},
		Rows:    [][]string{{"demo", "Demo", spec, "2026-05-23 00:00:00"}},
	}

	if _, err := s.RenderDashboard(context.Background(), "demo", nil); err != nil {
		t.Fatalf("RenderDashboard: %v", err)
	}

	var sawSubstituted bool
	for _, q := range f.querySQLs {
		if strings.Contains(q, "{{") {
			t.Errorf("query still contains a {{placeholder}}: %s", q)
		}
		if strings.Contains(q, "WHERE ts >= '20") {
			sawSubstituted = true
		}
	}
	if !sawSubstituted {
		t.Errorf("expected an expanded WHERE ts >= '<timestamp>', got: %v", f.querySQLs)
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
