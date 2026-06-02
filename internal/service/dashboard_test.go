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

	// nodeRuns is returned by NodeRuns when set (tests that exercise the
	// rightsize path). nodeRunsQ records the last query so the test can
	// assert IncludeMetrics is forced on.
	nodeRuns  []observability.NodeRun
	nodeRunsQ observability.NodeRunsQuery
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
func (f *fakeProvider) NodeRuns(_ context.Context, q observability.NodeRunsQuery) (*observability.NodeRunsResult, error) {
	f.nodeRunsQ = q
	return &observability.NodeRunsResult{Rows: f.nodeRuns}, nil
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

	// world_map is a valid widget type with the right shape (Slice F).
	withMap := ok
	withMap.Widgets = []DashboardWidget{{
		ID: "m1", Type: "world_map", Dataset: "rev",
		RegionField: "country", ValueField: "hits",
		Layout: DashboardWidgetLayout{W: 6, H: 5},
	}}
	if err := validateDashboard(withMap); err != nil {
		t.Fatalf("world_map widget rejected: %v", err)
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

// TestSaveGetDeleteDashboardFile exercises the file-backed CRUD round-trip
// (ADR-021). No Provider and no Docker — definition storage is the
// filesystem; only RenderDashboard touches a Provider.
func TestSaveGetDeleteDashboardFile(t *testing.T) {
	s := dashService(t, &fakeProvider{})
	d := Dashboard{
		Slug:     "revenue",
		Title:    "Revenue",
		Datasets: []DashboardDataset{{Name: "rev", Dir: "demo", SQL: "SELECT 1"}},
		Widgets: []DashboardWidget{
			{ID: "w1", Type: "table", Dataset: "rev", Layout: DashboardWidgetLayout{W: 6, H: 4}},
		},
		Controls: []DashboardControl{{Name: "tr", Type: "time_range", Default: "last_7d"}},
	}
	got, err := s.SaveDashboard(context.Background(), d)
	if err != nil {
		t.Fatalf("SaveDashboard: %v", err)
	}
	if got.Slug != "revenue" || len(got.Datasets) != 1 || len(got.Widgets) != 1 || len(got.Controls) != 1 {
		t.Fatalf("read-back mismatch: %+v", got)
	}

	// The definition landed in the registry directory as a file.
	file := filepath.Join(s.workspace, ".clavesa", "dashboards", "revenue.json")
	if _, err := os.Stat(file); err != nil {
		t.Fatalf("expected dashboard file at %s: %v", file, err)
	}

	// List shows it.
	list, err := s.ListDashboards(context.Background())
	if err != nil || len(list) != 1 || list[0].Slug != "revenue" {
		t.Fatalf("ListDashboards = %+v / %v", list, err)
	}

	// Get reads it back with full shape.
	rt, err := s.GetDashboard(context.Background(), "revenue")
	if err != nil {
		t.Fatalf("GetDashboard: %v", err)
	}
	if rt.Title != "Revenue" || rt.Datasets[0].SQL != "SELECT 1" || rt.Controls[0].Name != "tr" {
		t.Fatalf("GetDashboard round-trip mismatch: %+v", rt)
	}

	// Delete removes it.
	if err := s.DeleteDashboard(context.Background(), "revenue"); err != nil {
		t.Fatalf("DeleteDashboard: %v", err)
	}
	if _, err := s.GetDashboard(context.Background(), "revenue"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Get after Delete = %v, want os.ErrNotExist", err)
	}
}

func TestSaveDashboardRejectsInvalid(t *testing.T) {
	s := dashService(t, &fakeProvider{})
	_, err := s.SaveDashboard(context.Background(), Dashboard{Slug: "Bad Slug"})
	if err == nil {
		t.Fatal("expected an invalid dashboard to be rejected before any write")
	}
	// Nothing should have been written to the registry.
	if list, _ := s.ListDashboards(context.Background()); len(list) != 0 {
		t.Errorf("validation should fail before writing a file, got: %+v", list)
	}
}

func TestListDashboards(t *testing.T) {
	s := dashService(t, &fakeProvider{})
	for _, slug := range []string{"revenue", "pipeline-runs-demo"} {
		d := Dashboard{
			Slug:     slug,
			Title:    slug,
			Datasets: []DashboardDataset{{Name: "ds", Dir: "demo", SQL: "SELECT 1"}},
			Widgets:  []DashboardWidget{{ID: "w", Type: "table", Dataset: "ds", Layout: DashboardWidgetLayout{W: 6, H: 4}}},
		}
		if _, err := s.SaveDashboard(context.Background(), d); err != nil {
			t.Fatalf("SaveDashboard %q: %v", slug, err)
		}
	}
	got, err := s.ListDashboards(context.Background())
	if err != nil {
		t.Fatalf("ListDashboards: %v", err)
	}
	if len(got) != 2 || got[0].Slug != "pipeline-runs-demo" || got[1].Slug != "revenue" {
		t.Fatalf("want 2 dashboards sorted by slug, got %+v", got)
	}
}

func TestGetDashboardNotFound(t *testing.T) {
	s := dashService(t, &fakeProvider{})
	_, err := s.GetDashboard(context.Background(), "missing")
	if err == nil {
		t.Fatal("expected not-found error")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("not-found error should satisfy errors.Is(os.ErrNotExist), got %v", err)
	}
}

func TestListDashboardsEmptyWorkspaceIsEmpty(t *testing.T) {
	s := dashService(t, &fakeProvider{})
	got, err := s.ListDashboards(context.Background())
	if err != nil {
		t.Fatalf("a workspace with no registry should render empty, not error: %v", err)
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
		// Canonical now-<n><unit> form (Slice E).
		{"now-5m", 5 * time.Minute},
		{"now-15m", 15 * time.Minute},
		{"now-1h", time.Hour},
		{"now-3h", 3 * time.Hour},
		{"now-24h", 24 * time.Hour},
		{"now-7d", 7 * 24 * time.Hour},
		{"now-30d", 30 * 24 * time.Hour},
		{"now-2w", 14 * 24 * time.Hour},
		{"now-90d", 90 * 24 * time.Hour},
		// Legacy preset keys still resolve (back-compat with saved
		// dashboards written before Slice E).
		{"last_24h", 24 * time.Hour},
		{"last_7d", 7 * 24 * time.Hour},
		{"last_30d", 30 * 24 * time.Hour},
		{"last_90d", 90 * 24 * time.Hour},
		// Empty + bogus fall back to 30d (don't crash a freshly-added
		// control whose Default the author hasn't set yet).
		{"", 30 * 24 * time.Hour},
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

func TestParseRelativeRejectsBadInput(t *testing.T) {
	for _, in := range []string{
		"now+1h",
		"now-",
		"now-1y",        // year not supported
		"now-1.5h",      // fractional not supported
		"now-0h",        // zero not allowed
		"1h",            // missing now- prefix
		"now-abc",       // non-numeric
	} {
		t.Run(in, func(t *testing.T) {
			if _, err := ParseRelative(in); err == nil {
				t.Errorf("expected error for %q", in)
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

	// Save a dashboard with a control and a dataset referencing its
	// placeholder. RenderDashboard reads the definition from the file
	// registry, then executes the widget SQL through the (fake) Provider.
	d := Dashboard{
		Slug:     "demo",
		Title:    "Demo",
		Datasets: []DashboardDataset{{Name: "rev", Dir: "demo", SQL: "SELECT * FROM t WHERE ts >= {{tr.start}}"}},
		Widgets:  []DashboardWidget{{ID: "w1", Type: "table", Dataset: "rev", Layout: DashboardWidgetLayout{W: 6, H: 4}}},
		Controls: []DashboardControl{{Name: "tr", Type: "time_range", Default: "last_7d"}},
	}
	if _, err := s.SaveDashboard(context.Background(), d); err != nil {
		t.Fatalf("SaveDashboard: %v", err)
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

// TestMigrateLegacyDashboards verifies the one-time consolidation
// (ADR-021) seeds the registry from both legacy locations — the
// `.clavesa/dashboards.imported/` backup the old system-table importer
// left behind, and the workspace-root `dashboards/` authoring directory —
// without clobbering across sources.
func TestMigrateLegacyDashboards(t *testing.T) {
	s := dashService(t, &fakeProvider{})

	// Legacy backup location (datasets-shape file).
	imported := filepath.Join(s.workspace, ".clavesa", "dashboards.imported")
	if err := os.MkdirAll(imported, 0o755); err != nil {
		t.Fatal(err)
	}
	foo := `{"title":"Foo","datasets":[{"name":"d","dir":"demo","sql":"SELECT 1"}],` +
		`"widgets":[{"id":"w","type":"table","dataset":"d","layout":{"x":0,"y":0,"w":6,"h":4}}]}`
	if err := os.WriteFile(filepath.Join(imported, "foo.json"), []byte(foo), 0o644); err != nil {
		t.Fatal(err)
	}

	// Workspace-root authoring location (legacy per-widget-SQL shape).
	rootDir := filepath.Join(s.workspace, "dashboards")
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		t.Fatal(err)
	}
	bar := `{"title":"Bar","default_pipeline_dir":"demo","widgets":[` +
		`{"id":"a","type":"big_number","sql":"SELECT 1 AS n","value_field":"n","layout":{"x":0,"y":0,"w":3,"h":2}}]}`
	if err := os.WriteFile(filepath.Join(rootDir, "bar.json"), []byte(bar), 0o644); err != nil {
		t.Fatal(err)
	}

	list, err := s.ListDashboards(context.Background())
	if err != nil {
		t.Fatalf("ListDashboards (with migration): %v", err)
	}
	if len(list) != 2 || list[0].Slug != "bar" || list[1].Slug != "foo" {
		t.Fatalf("want migrated [bar foo], got %+v", list)
	}

	// Both now live in the canonical registry directory.
	for _, slug := range []string{"foo", "bar"} {
		if _, err := os.Stat(filepath.Join(s.workspace, ".clavesa", "dashboards", slug+".json")); err != nil {
			t.Errorf("expected %s.json in the registry: %v", slug, err)
		}
	}
	// Originals are copied, not deleted.
	if _, err := os.Stat(filepath.Join(imported, "foo.json")); err != nil {
		t.Errorf("legacy backup should be preserved, not moved: %v", err)
	}
	if _, err := os.Stat(filepath.Join(rootDir, "bar.json")); err != nil {
		t.Errorf("workspace-root original should be preserved, not moved: %v", err)
	}

	// The legacy bar dashboard normalized to the datasets shape on read.
	got, err := s.GetDashboard(context.Background(), "bar")
	if err != nil {
		t.Fatalf("GetDashboard bar: %v", err)
	}
	if len(got.Datasets) != 1 || got.Widgets[0].Dataset == "" {
		t.Errorf("legacy bar not normalized to datasets shape: %+v", got)
	}
}

// TestMigrateSkipsWhenRegistryPopulated confirms the migration is a no-op
// once the registry already holds files — a hand-authored or
// already-migrated registry is not re-seeded from legacy locations.
func TestMigrateSkipsWhenRegistryPopulated(t *testing.T) {
	s := dashService(t, &fakeProvider{})

	// Pre-seed the registry directly.
	d := Dashboard{
		Slug:     "kept",
		Title:    "Kept",
		Datasets: []DashboardDataset{{Name: "d", Dir: "demo", SQL: "SELECT 1"}},
		Widgets:  []DashboardWidget{{ID: "w", Type: "table", Dataset: "d", Layout: DashboardWidgetLayout{W: 6, H: 4}}},
	}
	if _, err := s.SaveDashboard(context.Background(), d); err != nil {
		t.Fatalf("SaveDashboard: %v", err)
	}

	// Drop a legacy file that should be ignored because the registry is
	// already populated.
	rootDir := filepath.Join(s.workspace, "dashboards")
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		t.Fatal(err)
	}
	legacy := `{"title":"Ignored","datasets":[{"name":"d","dir":"demo","sql":"SELECT 1"}],` +
		`"widgets":[{"id":"w","type":"table","dataset":"d","layout":{"x":0,"y":0,"w":6,"h":4}}]}`
	if err := os.WriteFile(filepath.Join(rootDir, "ignored.json"), []byte(legacy), 0o644); err != nil {
		t.Fatal(err)
	}

	list, err := s.ListDashboards(context.Background())
	if err != nil {
		t.Fatalf("ListDashboards: %v", err)
	}
	if len(list) != 1 || list[0].Slug != "kept" {
		t.Errorf("populated registry should not re-seed from legacy, got %+v", list)
	}
}

// recordingParser records every SQL it is asked to parse and rejects any
// that still carries an unexpanded `{{` control placeholder — mimicking
// the real Spark parser, which errors on `{{`.
type recordingParser struct{ seen []string }

func (p *recordingParser) Parse(_ context.Context, sql string) error {
	p.seen = append(p.seen, sql)
	if strings.Contains(sql, "{{") {
		return &observability.ParseError{Message: "unexpanded placeholder: " + sql}
	}
	return nil
}

// TestSaveDashboardExpandsPlaceholdersBeforeParseCheck guards the save-time
// parse-check against control placeholders. Dataset SQL carries
// `{{period.start}}`-style placeholders that are not valid SQL; the save
// validator must expand them (from the controls' defaults, as RenderDashboard
// does) before parsing, or every dashboard with a control fails to save with
// a spurious PARSE_SYNTAX_ERROR on `{{`.
func TestSaveDashboardExpandsPlaceholdersBeforeParseCheck(t *testing.T) {
	p := &recordingParser{}
	s := dashService(t, &fakeProvider{}).WithSQLParser(p)
	d := Dashboard{
		Slug:     "ph",
		Title:    "Placeholders",
		Controls: []DashboardControl{{Name: "tr", Type: "time_range", Default: "last_7d"}},
		Datasets: []DashboardDataset{{
			Name: "ds", Dir: "demo",
			SQL: "SELECT COUNT(*) AS n FROM t WHERE d >= {{tr.start}} AND d < {{tr.end}}",
		}},
	}
	if _, err := s.SaveDashboard(context.Background(), d); err != nil {
		t.Fatalf("save with placeholder SQL should succeed (placeholders expanded before parse-check): %v", err)
	}
	if len(p.seen) == 0 {
		t.Fatal("parser was never invoked")
	}
	for _, sql := range p.seen {
		if strings.Contains(sql, "{{") {
			t.Errorf("parser received unexpanded placeholder SQL: %q", sql)
		}
	}
}
