package service

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/vesahyp/clavesa/internal/dashboardsql"
	"github.com/vesahyp/clavesa/internal/identutil"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// Dashboards are stored as one row per dashboard in the `dashboards`
// system Iceberg table, in the workspace system catalog DB
// (`<system_catalog>__pipelines`, alongside runs / node_runs). Local
// workspaces keep it in the local Iceberg warehouse; cloud workspaces in
// S3 Iceberg. This replaced the old `.clavesa/dashboards/*.json`
// file store, which could not be shared between teammates and had no
// access model — a system table inherits the same Glue/Lake Formation
// grants that govern data.
//
// The dashboard spec (datasets + widgets) is a JSON document in the
// `spec` column. A dashboard is one document and a save must be atomic;
// Iceberg has no cross-table transaction, so normalizing widgets into
// their own table would make a save a non-atomic multi-row write.
// `slug` and `title` are real columns so listing is a cheap projection.

const dashboardsSystemTable = "dashboards"

// dashboardWidgetTypes is the set of widget types the UI knows how to
// render. Validation rejects anything else at save time so a typo can't
// silently produce a blank widget.
var dashboardWidgetTypes = map[string]bool{
	"big_number":  true,
	"line":        true,
	"bar":         true,
	"stacked_bar": true,
	"bar_line":    true,
	"pie":         true,
	"donut":       true,
	"table":       true,
}

// dashboardControlTypes is the set of dashboard-level control types the
// editor and renderer know how to handle. Mirrors the widget enum —
// unknown types fail validation at save.
var dashboardControlTypes = map[string]bool{
	"time_range": true,
	"select":     true,
}

// DashboardWidgetLayout positions a widget on the 12-column grid.
// 0-indexed: x in [0,12), w in [1,12], x+w <= 12.
type DashboardWidgetLayout struct {
	X int `json:"x"`
	Y int `json:"y"`
	W int `json:"w"`
	H int `json:"h"`
}

// DashboardDataset is a named, reusable SQL query. Widgets bind to a
// dataset by name; two widgets on one dataset share a single execution.
// Each dataset carries its own pipeline Dir, so one dashboard can blend
// tables from multiple pipelines and mix local + cloud.
type DashboardDataset struct {
	Name string `json:"name"`
	Dir  string `json:"dir"`
	SQL  string `json:"sql"`
}

// DashboardWidget is one chart/table on a dashboard. It binds to a
// dataset by name; the *Field hints map result columns to the renderer.
// SeriesFields are the value columns a stacked_bar stacks per x;
// LineField is the line series of a bar_line combo.
type DashboardWidget struct {
	ID           string                `json:"id"`
	Type         string                `json:"type"`
	Title        string                `json:"title"`
	Dataset      string                `json:"dataset"`
	ValueField   string                `json:"value_field,omitempty"`
	XField       string                `json:"x_field,omitempty"`
	YField       string                `json:"y_field,omitempty"`
	SeriesFields []string              `json:"series_fields,omitempty"`
	LineField    string                `json:"line_field,omitempty"`
	Layout       DashboardWidgetLayout `json:"layout"`
	// SQL is a decode-only legacy field — pre-datasets dashboards carried
	// inline per-widget SQL. normalizeDashboardFile lifts it into a
	// synthesized dataset; it is never re-emitted once normalized.
	SQL string `json:"sql,omitempty"`
}

// DashboardControl is a dashboard-level filter the viewer sets at the
// top of the page. Its current value is substituted into dataset SQL as
// the named placeholder `{{<name>}}` (or, for `time_range`,
// `{{<name>.start}}` and `{{<name>.end}}`). URL-syncable so a filtered
// view is shareable.
//
//   - `time_range`: `Default` is a preset key (`last_24h` / `last_7d` /
//     `last_30d` / `last_90d`) resolved to a start/end ISO timestamp at
//     render time.
//   - `select`: options come from running `SQL` against `Dir` (first
//     column used); a non-empty `Options` slice is a static fallback when
//     no SQL is set.
type DashboardControl struct {
	Name    string   `json:"name"`
	Type    string   `json:"type"`
	Label   string   `json:"label,omitempty"`
	Default string   `json:"default,omitempty"`
	Dir     string   `json:"dir,omitempty"`
	SQL     string   `json:"sql,omitempty"`
	Options []string `json:"options,omitempty"`
}

// Dashboard is the full spec returned by GetDashboard.
type Dashboard struct {
	Slug      string             `json:"slug"`
	Title     string             `json:"title"`
	Datasets  []DashboardDataset `json:"datasets"`
	Widgets   []DashboardWidget  `json:"widgets"`
	Controls  []DashboardControl `json:"controls,omitempty"`
	UpdatedAt string             `json:"updated_at,omitempty"`
}

// DashboardSummary is one entry in ListDashboards.
type DashboardSummary struct {
	Slug  string `json:"slug"`
	Title string `json:"title"`
}

// dashboardFile is the on-disk JSON shape used by the legacy file store
// and accepted as an import/apply input. It tolerates both the legacy
// shape (default_pipeline_dir + per-widget sql) and the datasets shape.
type dashboardFile struct {
	Title              string             `json:"title"`
	DefaultPipelineDir string             `json:"default_pipeline_dir,omitempty"`
	Datasets           []DashboardDataset `json:"datasets,omitempty"`
	Widgets            []DashboardWidget  `json:"widgets"`
	Controls           []DashboardControl `json:"controls,omitempty"`
}

// dashboardSpecJSON is the document stored in the `spec` column.
type dashboardSpecJSON struct {
	Datasets []DashboardDataset `json:"datasets"`
	Widgets  []DashboardWidget  `json:"widgets"`
	Controls []DashboardControl `json:"controls,omitempty"`
}

// WithResolver wires the observability resolver the dashboards store
// needs to dispatch its catalog reads/writes to the cloud (Athena) or
// local (runner-Spark) provider. Without it the dashboard methods
// return a configuration error.
func (s *Service) WithResolver(r *observability.Resolver) *Service {
	s.dashResolver = r
	return s
}

// ListDashboards returns every dashboard, sorted by slug.
func (s *Service) ListDashboards(ctx context.Context) ([]DashboardSummary, error) {
	prov, err := s.dashboardProvider()
	if err != nil {
		return nil, err
	}
	if err := s.importLegacyDashboards(ctx, prov); err != nil {
		return nil, err
	}
	res, err := prov.Query(ctx, observability.QueryQuery{
		SQL:         fmt.Sprintf("SELECT slug, title FROM %s ORDER BY slug", s.dashboardTableRef()),
		PipelineDir: s.workspace,
	})
	if err != nil {
		if isMissingDashboardsTable(err) {
			return []DashboardSummary{}, nil
		}
		return nil, fmt.Errorf("list dashboards: %w", err)
	}
	out := make([]DashboardSummary, 0, len(res.Rows))
	for _, row := range res.Rows {
		if len(row) < 2 {
			continue
		}
		out = append(out, DashboardSummary{Slug: row[0], Title: row[1]})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Slug < out[j].Slug })
	return out, nil
}

// GetDashboard reads one dashboard by slug. Returns a wrapped
// os.ErrNotExist when the slug is unknown so callers dispatch 404.
func (s *Service) GetDashboard(ctx context.Context, slug string) (Dashboard, error) {
	if !validDashboardSlug(slug) {
		return Dashboard{}, fmt.Errorf("invalid dashboard slug %q", slug)
	}
	prov, err := s.dashboardProvider()
	if err != nil {
		return Dashboard{}, err
	}
	if err := s.importLegacyDashboards(ctx, prov); err != nil {
		return Dashboard{}, err
	}
	res, err := prov.Query(ctx, observability.QueryQuery{
		SQL: fmt.Sprintf("SELECT slug, title, spec, CAST(updated_at AS STRING) FROM %s WHERE slug = %s",
			s.dashboardTableRef(), s.sqlString(slug)),
		PipelineDir: s.workspace,
	})
	if err != nil {
		if isMissingDashboardsTable(err) {
			return Dashboard{}, &notRegisteredError{kind: "dashboard", name: slug}
		}
		return Dashboard{}, fmt.Errorf("get dashboard: %w", err)
	}
	if len(res.Rows) == 0 || len(res.Rows[0]) < 3 {
		return Dashboard{}, &notRegisteredError{kind: "dashboard", name: slug}
	}
	row := res.Rows[0]
	var spec dashboardSpecJSON
	if err := json.Unmarshal([]byte(row[2]), &spec); err != nil {
		return Dashboard{}, fmt.Errorf("parse dashboard %q spec: %w", slug, err)
	}
	d := Dashboard{
		Slug:     row[0],
		Title:    row[1],
		Datasets: spec.Datasets,
		Widgets:  spec.Widgets,
		Controls: spec.Controls,
	}
	if len(row) > 3 {
		d.UpdatedAt = row[3]
	}
	if d.Datasets == nil {
		d.Datasets = []DashboardDataset{}
	}
	if d.Widgets == nil {
		d.Widgets = []DashboardWidget{}
	}
	return d, nil
}

// SaveDashboard creates or replaces a dashboard. The slug is the key —
// a save with an existing slug overwrites. Returns the stored dashboard.
func (s *Service) SaveDashboard(ctx context.Context, d Dashboard) (Dashboard, error) {
	if err := validateDashboard(d); err != nil {
		return Dashboard{}, err
	}
	prov, err := s.dashboardProvider()
	if err != nil {
		return Dashboard{}, err
	}
	if err := s.ensureDashboardTable(ctx, prov); err != nil {
		return Dashboard{}, err
	}
	if err := s.writeDashboard(ctx, prov, d); err != nil {
		return Dashboard{}, err
	}
	return s.GetDashboard(ctx, d.Slug)
}

// ApplyDashboardFile parses a dashboard JSON document (legacy or
// datasets shape), stamps the given slug, and saves it — the CLI's
// `dashboards apply` authoring path, symmetric with the UI builder.
func (s *Service) ApplyDashboardFile(ctx context.Context, slug string, data []byte) (Dashboard, error) {
	d, err := parseDashboardFile(slug, data)
	if err != nil {
		return Dashboard{}, err
	}
	return s.SaveDashboard(ctx, d)
}

// DeleteDashboard removes a dashboard. Returns a wrapped os.ErrNotExist
// when the slug is unknown.
func (s *Service) DeleteDashboard(ctx context.Context, slug string) error {
	if !validDashboardSlug(slug) {
		return fmt.Errorf("invalid dashboard slug %q", slug)
	}
	if _, err := s.GetDashboard(ctx, slug); err != nil {
		return err
	}
	prov, err := s.dashboardProvider()
	if err != nil {
		return err
	}
	return prov.Exec(ctx, observability.ExecQuery{
		SQL:         fmt.Sprintf("DELETE FROM %s WHERE slug = %s", s.dashboardTableRef(), s.sqlString(slug)),
		PipelineDir: s.workspace,
	})
}

// RenderedWidget is one widget's executed result — the data behind a
// chart, or the error that stopped it. Columns/Rows mirror QueryResult.
type RenderedWidget struct {
	WidgetID string     `json:"widget_id"`
	Title    string     `json:"title"`
	Type     string     `json:"type"`
	Dataset  string     `json:"dataset"`
	Columns  []string   `json:"columns"`
	Rows     [][]string `json:"rows"`
	Error    string     `json:"error,omitempty"`
}

// DashboardRender is a whole dashboard with every widget's dataset
// executed — the payload behind `clavesa dashboards render`.
type DashboardRender struct {
	Slug    string           `json:"slug"`
	Title   string           `json:"title"`
	Widgets []RenderedWidget `json:"widgets"`
}

// RenderDashboard executes every widget's bound dataset and returns the
// results. Datasets are executed once and shared across the widgets that
// reference them — the same execute-once the UI gets from its query
// cache. Used by `clavesa dashboards render` for cron / CI smoke
// tests; a widget whose dataset errors carries the error inline rather
// than failing the whole render.
//
// `params` are substituted into dataset SQL via {{name}} placeholders.
// Keys not provided are filled from the dashboard's declared control
// defaults (e.g. a `last_30d` time_range expands to {start, end} at
// now); explicit caller values always win. A dataset whose SQL
// references an unknown placeholder fails with a clear error inline,
// same as a query failure.
func (s *Service) RenderDashboard(ctx context.Context, slug string, params map[string]string) (DashboardRender, error) {
	d, err := s.GetDashboard(ctx, slug)
	if err != nil {
		return DashboardRender{}, err
	}
	prov, err := s.dashboardProvider()
	if err != nil {
		return DashboardRender{}, err
	}
	// Fill declared-control defaults for any param the caller did not
	// set, so `clavesa dashboards render <slug>` with no flags Just
	// Works against a dashboard with controls.
	effective := map[string]string{}
	for k, v := range params {
		effective[k] = v
	}
	resolveControlDefaults(d.Controls, effective, time.Now())
	datasets := map[string]DashboardDataset{}
	for _, ds := range d.Datasets {
		datasets[ds.Name] = ds
	}
	// Execute each dataset at most once.
	type execd struct {
		cols []string
		rows [][]string
		err  error
	}
	results := map[string]execd{}
	out := DashboardRender{Slug: d.Slug, Title: d.Title}
	for _, w := range d.Widgets {
		rw := RenderedWidget{WidgetID: w.ID, Title: w.Title, Type: w.Type, Dataset: w.Dataset}
		ds, ok := datasets[w.Dataset]
		if !ok {
			rw.Error = fmt.Sprintf("widget references unknown dataset %q", w.Dataset)
			out.Widgets = append(out.Widgets, rw)
			continue
		}
		r, done := results[w.Dataset]
		if !done {
			expanded, expErr := expandPlaceholders(ds.SQL, effective)
			if expErr != nil {
				r = execd{err: expErr}
			} else {
				res, qErr := prov.Query(ctx, observability.QueryQuery{
					SQL:         expanded,
					PipelineDir: ds.Dir,
					MaxRows:     10_000,
				})
				if qErr != nil {
					r = execd{err: qErr}
				} else {
					cols := make([]string, len(res.Columns))
					for i, c := range res.Columns {
						cols[i] = c.Name
					}
					r = execd{cols: cols, rows: res.Rows}
				}
			}
			results[w.Dataset] = r
		}
		if r.err != nil {
			rw.Error = r.err.Error()
		} else {
			rw.Columns = r.cols
			rw.Rows = r.rows
		}
		out.Widgets = append(out.Widgets, rw)
	}
	return out, nil
}

// writeDashboard MERGEs one dashboard row into the system table. MERGE
// is supported by both Athena Iceberg and Spark Iceberg, so create and
// replace are one atomic statement on either backend.
func (s *Service) writeDashboard(ctx context.Context, prov observability.Provider, d Dashboard) error {
	specBytes, err := json.Marshal(dashboardSpecJSON{Datasets: d.Datasets, Widgets: d.Widgets, Controls: d.Controls})
	if err != nil {
		return fmt.Errorf("encode dashboard spec: %w", err)
	}
	now := "current_timestamp"
	if s.dashResolver.IsLocal() {
		now = "current_timestamp()"
	}
	ref := s.dashboardTableRef()
	sql := fmt.Sprintf(
		"MERGE INTO %s t USING (SELECT %s AS slug, %s AS title, %s AS spec) s "+
			"ON t.slug = s.slug "+
			"WHEN MATCHED THEN UPDATE SET title = s.title, spec = s.spec, updated_at = %s "+
			"WHEN NOT MATCHED THEN INSERT (slug, title, spec, updated_at, updated_by) "+
			"VALUES (s.slug, s.title, s.spec, %s, NULL)",
		ref, s.sqlString(d.Slug), s.sqlString(d.Title), s.sqlString(string(specBytes)), now, now)
	return prov.Exec(ctx, observability.ExecQuery{SQL: sql, PipelineDir: s.workspace})
}

// dashboardProvider returns the cloud-or-local provider for the
// workspace's environment mode.
func (s *Service) dashboardProvider() (observability.Provider, error) {
	if s.dashResolver == nil {
		return nil, fmt.Errorf("dashboards: observability resolver not configured")
	}
	return s.dashResolver.For(s.workspace)
}

// systemGlueDB returns the workspace system catalog DB
// (`<system_catalog>__pipelines`) — where runs / node_runs / dashboards
// all live.
func (s *Service) systemGlueDB() string {
	catalog := ""
	if m, _ := workspace.Load(s.workspace); m != nil {
		catalog = m.SystemCatalogIdentifier()
	}
	if catalog == "" {
		// No manifest (bare directory) — defensive fallback. Fresh
		// workspaces always have a manifest, so this only bites tests
		// that skip workspace init.
		return "clavesa_system__pipelines"
	}
	return identutil.EncodeGlueDatabase(catalog, "pipelines")
}

// dashboardTableRef is the fully-qualified `dashboards` table identifier
// in the form each backend expects: the runner's Hadoop catalog uses a
// three-part `clavesa.<db>.<table>` name; Athena uses a quoted
// two-part `"<db>"."<table>"`. This mirrors how CloudProvider.Runs
// addresses `"<db>"."runs"` and the runner addresses `clavesa.<db>.runs`.
func (s *Service) dashboardTableRef() string {
	db := s.systemGlueDB()
	if s.dashResolver != nil && s.dashResolver.IsLocal() {
		return fmt.Sprintf("clavesa.%s.%s", db, dashboardsSystemTable)
	}
	return fmt.Sprintf("%q.%q", db, dashboardsSystemTable)
}

// ensureDashboardTable creates the system table if it does not exist.
// Idempotent and guarded so the DDL round-trip is paid at most once per
// process. On the local backend the system namespace is created first
// (the dashboards table can be the first thing written to it); on cloud
// the Glue DB already exists from workspace deploy and the table needs
// an explicit S3 location.
func (s *Service) ensureDashboardTable(ctx context.Context, prov observability.Provider) error {
	s.dashMu.Lock()
	ready := s.dashTableReady
	s.dashMu.Unlock()
	if ready {
		return nil
	}

	db := s.systemGlueDB()
	cols := "slug STRING, title STRING, spec STRING, updated_at TIMESTAMP, updated_by STRING"
	if s.dashResolver.IsLocal() {
		if err := prov.Exec(ctx, observability.ExecQuery{
			SQL:         fmt.Sprintf("CREATE NAMESPACE IF NOT EXISTS clavesa.%s", db),
			PipelineDir: s.workspace,
		}); err != nil {
			return fmt.Errorf("create system namespace: %w", err)
		}
		if err := prov.Exec(ctx, observability.ExecQuery{
			SQL:         fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) USING iceberg", s.dashboardTableRef(), cols),
			PipelineDir: s.workspace,
		}); err != nil {
			return fmt.Errorf("create dashboards table: %w", err)
		}
	} else {
		bucket := workspace.PipelineBucket(s.workspace)
		if bucket == "" {
			return fmt.Errorf("dashboards: workspace is not deployed — cannot create the dashboards table (run `clavesa workspace deploy`)")
		}
		loc := fmt.Sprintf("s3://%s/_system/pipelines/%s", bucket, dashboardsSystemTable)
		if err := prov.Exec(ctx, observability.ExecQuery{
			SQL: fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS %s (%s) LOCATION '%s' TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet')",
				s.dashboardTableRef(), cols, loc),
			PipelineDir: s.workspace,
		}); err != nil {
			return fmt.Errorf("create dashboards table: %w", err)
		}
	}

	s.dashMu.Lock()
	s.dashTableReady = true
	s.dashMu.Unlock()
	return nil
}

// importLegacyDashboards migrates a pre-table workspace: any
// `.clavesa/dashboards/*.json` files are written into the system
// table once, then the directory is moved aside so the import does not
// repeat. Best-effort per file; a malformed file is skipped with a
// stderr note rather than blocking the whole import. Runs at most once
// per process.
func (s *Service) importLegacyDashboards(ctx context.Context, prov observability.Provider) error {
	s.dashMu.Lock()
	done := s.dashImported
	s.dashMu.Unlock()
	if done {
		return nil
	}

	dir := filepath.Join(s.workspace, ".clavesa", "dashboards")
	entries, err := os.ReadDir(dir)
	if err != nil {
		// No legacy directory — nothing to migrate.
		s.dashMu.Lock()
		s.dashImported = true
		s.dashMu.Unlock()
		return nil
	}

	imported := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		slug := strings.TrimSuffix(e.Name(), ".json")
		data, readErr := os.ReadFile(filepath.Join(dir, e.Name()))
		if readErr != nil {
			fmt.Fprintf(os.Stderr, "clavesa: skip dashboard import %s: %v\n", e.Name(), readErr)
			continue
		}
		d, parseErr := parseDashboardFile(slug, data)
		if parseErr != nil {
			fmt.Fprintf(os.Stderr, "clavesa: skip dashboard import %s: %v\n", e.Name(), parseErr)
			continue
		}
		if err := s.ensureDashboardTable(ctx, prov); err != nil {
			return fmt.Errorf("import legacy dashboards: %w", err)
		}
		if err := s.writeDashboard(ctx, prov, d); err != nil {
			return fmt.Errorf("import dashboard %s: %w", e.Name(), err)
		}
		imported++
	}

	// Move the directory aside so the next read does not re-import. The
	// files are kept (not deleted) under `.imported` as a safety copy.
	aside := dir + ".imported"
	if _, statErr := os.Stat(aside); statErr == nil {
		_ = os.RemoveAll(dir)
	} else {
		_ = os.Rename(dir, aside)
	}
	if imported > 0 {
		fmt.Fprintf(os.Stderr, "clavesa: migrated %d dashboard(s) into the system table\n", imported)
	}

	s.dashMu.Lock()
	s.dashImported = true
	s.dashMu.Unlock()
	return nil
}

// parseDashboardFile decodes a dashboard JSON document (legacy or
// datasets shape), normalizes it to the datasets shape, and stamps the
// slug from the filename.
func parseDashboardFile(slug string, data []byte) (Dashboard, error) {
	var f dashboardFile
	if err := json.Unmarshal(data, &f); err != nil {
		return Dashboard{}, fmt.Errorf("parse JSON: %w", err)
	}
	d := normalizeDashboardFile(slug, f)
	if err := validateDashboard(d); err != nil {
		return Dashboard{}, err
	}
	return d, nil
}

// normalizeDashboardFile converts a legacy dashboard (per-widget inline
// SQL + a single default_pipeline_dir) into the datasets shape. Widgets
// already carrying a `dataset` reference and a populated `datasets`
// array pass through untouched. Legacy widgets get one synthesized
// dataset per distinct SQL string — widgets with identical SQL share a
// dataset, which is the execute-once win.
func normalizeDashboardFile(slug string, f dashboardFile) Dashboard {
	d := Dashboard{
		Slug:     slug,
		Title:    f.Title,
		Datasets: f.Datasets,
		Widgets:  f.Widgets,
		Controls: f.Controls,
	}
	if d.Title == "" {
		d.Title = slug
	}
	if d.Datasets == nil {
		d.Datasets = []DashboardDataset{}
	}
	if d.Widgets == nil {
		d.Widgets = []DashboardWidget{}
	}

	// Already datasets-shaped — nothing to synthesize.
	if len(d.Datasets) > 0 {
		for i := range d.Widgets {
			d.Widgets[i].SQL = ""
		}
		return d
	}

	bySQL := map[string]string{} // sql -> dataset name
	for i := range d.Widgets {
		w := &d.Widgets[i]
		if w.Dataset != "" || w.SQL == "" {
			w.SQL = ""
			continue
		}
		name, ok := bySQL[w.SQL]
		if !ok {
			name = fmt.Sprintf("ds%d", len(d.Datasets)+1)
			bySQL[w.SQL] = name
			d.Datasets = append(d.Datasets, DashboardDataset{
				Name: name,
				Dir:  f.DefaultPipelineDir,
				SQL:  w.SQL,
			})
		}
		w.Dataset = name
		w.SQL = ""
	}
	return d
}

// validateDashboard enforces the dashboard invariants: a valid slug,
// uniquely-named datasets with non-empty SQL, and widgets that reference
// an existing dataset, carry a known type, and sit within the grid.
func validateDashboard(d Dashboard) error {
	if !validDashboardSlug(d.Slug) {
		return fmt.Errorf("invalid dashboard slug %q (lowercase letters, digits, dash, underscore; max 64)", d.Slug)
	}
	names := map[string]bool{}
	for _, ds := range d.Datasets {
		if !validDatasetName(ds.Name) {
			return fmt.Errorf("invalid dataset name %q (lowercase letters, digits, dash, underscore)", ds.Name)
		}
		if names[ds.Name] {
			return fmt.Errorf("duplicate dataset name %q", ds.Name)
		}
		names[ds.Name] = true
		if strings.TrimSpace(ds.Dir) == "" {
			return fmt.Errorf("dataset %q: dir is required", ds.Name)
		}
		if strings.TrimSpace(ds.SQL) == "" {
			return fmt.Errorf("dataset %q: sql is required", ds.Name)
		}
	}
	for _, w := range d.Widgets {
		if strings.TrimSpace(w.ID) == "" {
			return fmt.Errorf("widget: id is required")
		}
		if !dashboardWidgetTypes[w.Type] {
			return fmt.Errorf("widget %q: unknown type %q", w.ID, w.Type)
		}
		if !names[w.Dataset] {
			return fmt.Errorf("widget %q: references unknown dataset %q", w.ID, w.Dataset)
		}
		l := w.Layout
		if l.W < 1 || l.H < 1 || l.X < 0 || l.Y < 0 || l.X+l.W > 12 {
			return fmt.Errorf("widget %q: layout out of the 12-column grid", w.ID)
		}
	}
	ctlNames := map[string]bool{}
	for _, c := range d.Controls {
		if !validDatasetName(c.Name) {
			return fmt.Errorf("invalid control name %q (lowercase letters, digits, dash, underscore)", c.Name)
		}
		if ctlNames[c.Name] {
			return fmt.Errorf("duplicate control name %q", c.Name)
		}
		ctlNames[c.Name] = true
		if !dashboardControlTypes[c.Type] {
			return fmt.Errorf("control %q: unknown type %q", c.Name, c.Type)
		}
		if c.Type == "select" {
			// A `select` control needs either an inline options list or a
			// SQL query (with its pipeline dir) to populate the dropdown —
			// otherwise the viewer can't pick anything.
			if len(c.Options) == 0 && strings.TrimSpace(c.SQL) == "" {
				return fmt.Errorf("control %q: select needs sql or options", c.Name)
			}
			if strings.TrimSpace(c.SQL) != "" && strings.TrimSpace(c.Dir) == "" {
				return fmt.Errorf("control %q: dir is required when sql is set", c.Name)
			}
		}
	}
	return nil
}

// validDashboardSlug mirrors the dashboards-handler slug rule: lowercase
// letters, digits, dash, underscore; 1-64 chars. Guards the filename and
// the SQL string literal.
func validDashboardSlug(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for _, c := range s {
		switch {
		case c == '-' || c == '_':
		case c >= 'a' && c <= 'z':
		case c >= '0' && c <= '9':
		default:
			return false
		}
	}
	return true
}

// validDatasetName is the same character class as a slug — dataset names
// are referenced by widgets and embedded in the spec JSON.
func validDatasetName(s string) bool {
	return validDashboardSlug(s)
}

// sqlString quotes a Go string as a SQL string literal for the current
// backend. The two dialects escape differently and getting it wrong
// silently corrupts the stored spec:
//
//   - Spark SQL (local runner) uses backslash escapes. `”` is NOT an
//     escaped quote there — `'a”b'` lexes as two literals and yields
//     `ab`. Escape backslash first, then the quote.
//   - Athena / Trino (cloud) uses ANSI doubling: `”` is the escaped
//     quote and backslash is a literal character.
//
// The spec JSON is the value that actually exercises this — it carries
// both single quotes (from widget SQL) and backslashes (from JSON's
// \uXXXX escapes of `<`/`>`/`&`).
func (s *Service) sqlString(v string) string {
	if s.dashResolver != nil && s.dashResolver.IsLocal() {
		v = strings.ReplaceAll(v, `\`, `\\`)
		v = strings.ReplaceAll(v, `'`, `\'`)
		return "'" + v + "'"
	}
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
}

// expandPlaceholders delegates to the leaf-package implementation
// shared with internal/api/dashboards.go (C13, 2026-05-24).
func expandPlaceholders(sql string, params map[string]string) (string, error) {
	expanded, err := dashboardsql.ExpandPlaceholders(sql, params)
	if err != nil {
		return "", err
	}
	return expanded, nil
}

// resolveControlDefaults expands a dashboard's declared control
// defaults into a param map. Already-set keys in `out` are kept (an
// explicit --param or URL value wins over the dashboard's default).
// `time_range` controls produce two params (`<name>.start` and
// `<name>.end`); the preset key is interpreted at `now`.
func resolveControlDefaults(controls []DashboardControl, out map[string]string, now time.Time) {
	for _, c := range controls {
		switch c.Type {
		case "time_range":
			startKey := c.Name + ".start"
			endKey := c.Name + ".end"
			_, hasStart := out[startKey]
			_, hasEnd := out[endKey]
			if hasStart && hasEnd {
				continue
			}
			start, end := resolveTimePreset(c.Default, now)
			if !hasStart {
				out[startKey] = start
			}
			if !hasEnd {
				out[endKey] = end
			}
		case "select":
			if _, ok := out[c.Name]; ok {
				continue
			}
			if c.Default != "" {
				out[c.Name] = c.Default
			} else if len(c.Options) > 0 {
				out[c.Name] = c.Options[0]
			}
		}
	}
}

// resolveTimePreset turns a preset key (`last_24h` / `last_7d` /
// `last_30d` / `last_90d`) into a {start, end} pair of ISO timestamps
// at `now`. Unknown values default to `last_30d` — that keeps a
// freshly-added time_range control workable even before the author
// touches the default.
func resolveTimePreset(preset string, now time.Time) (start, end string) {
	end = now.UTC().Format(time.RFC3339)
	var d time.Duration
	switch preset {
	case "last_24h":
		d = 24 * time.Hour
	case "last_7d":
		d = 7 * 24 * time.Hour
	case "last_90d":
		d = 90 * 24 * time.Hour
	case "last_30d":
		d = 30 * 24 * time.Hour
	default:
		d = 30 * 24 * time.Hour
	}
	start = now.UTC().Add(-d).Format(time.RFC3339)
	return start, end
}

// isMissingDashboardsTable classifies a query error as "the dashboards
// table does not exist yet" so a fresh workspace renders an empty list
// instead of a 500. The local provider already maps this to an empty
// result; cloud surfaces the Athena error text.
func isMissingDashboardsTable(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	for _, marker := range []string{
		"Table or view not found",
		"TABLE_OR_VIEW_NOT_FOUND",
		"does not exist",
		"NoSuchTableException",
		"not found",
	} {
		if strings.Contains(s, marker) {
			return true
		}
	}
	return false
}
