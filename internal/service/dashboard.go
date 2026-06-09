package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/vesahyp/clavesa/internal/dashboards"
	"github.com/vesahyp/clavesa/internal/dashboardsql"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/servingsql"
)

// Dashboards are workspace-level IaC definitions (ADR-021): one JSON file
// per dashboard under `.clavesa/dashboards/<slug>.json`, read directly
// through the file-backed dashboards.Store. A dashboard is a definition,
// not runtime data. It lives as code beside the source/credential
// registries (ADR-017) and pipeline `.tf`, version-controlled and
// promoted via the repo. This replaced the prior system Delta table,
// whose cloud write path was Athena (which cannot write Delta).
//
// The widget-SQL execution path (RenderDashboard, the /api/dashboards/query
// route) is unchanged. It still dispatches through the observability
// Provider so each dataset runs on the warm Spark worker locally or on
// Athena in the cloud (ADR-014). Only the *definition* storage moved off
// the catalog and onto the filesystem.

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
	"world_map":   true,
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
	// RegionField + TooltipField are world_map-only; ISO 3166-1 alpha-2
	// or alpha-3 country code in the region column, a numeric metric in
	// value_field (already declared above). Both omitempty so other
	// widget types serialise unchanged.
	RegionField  string                `json:"region_field,omitempty"`
	TooltipField string                `json:"tooltip_field,omitempty"`
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

// WithResolver wires the observability resolver RenderDashboard uses to
// dispatch widget SQL to the cloud (Athena) or local (runner-Spark)
// provider (ADR-014). Definition CRUD is file-backed (ADR-021) and does
// not need it; only RenderDashboard returns a configuration error when
// it is absent.
func (s *Service) WithResolver(r *observability.Resolver) *Service {
	s.dashResolver = r
	return s
}

// dashboardStore returns the workspace-rooted file registry. Dashboards
// live at the workspace level (ADR-021), not per-pipeline: a dataset
// already carries its own `dir`, so one dashboard can read across
// pipelines.
func (s *Service) dashboardStore() *dashboards.Store {
	return dashboards.New(s.workspace)
}

// ListDashboards returns every dashboard, sorted by slug. Reads the
// registry directory directly: no Provider, no catalog query.
func (s *Service) ListDashboards(ctx context.Context) ([]DashboardSummary, error) {
	s.migrateLegacyDashboards()
	store := s.dashboardStore()
	slugs, err := store.List()
	if err != nil {
		return nil, fmt.Errorf("list dashboards: %w", err)
	}
	out := make([]DashboardSummary, 0, len(slugs))
	for _, slug := range slugs {
		data, err := store.Get(slug)
		if err != nil {
			// Skip an unreadable file rather than failing the whole list;
			// GetDashboard surfaces the parse error on demand.
			fmt.Fprintf(os.Stderr, "clavesa: skip dashboard %q in list: %v\n", slug, err)
			continue
		}
		d, err := parseDashboardFile(slug, data)
		if err != nil {
			fmt.Fprintf(os.Stderr, "clavesa: skip dashboard %q in list: %v\n", slug, err)
			continue
		}
		out = append(out, DashboardSummary{Slug: d.Slug, Title: d.Title})
	}
	// store.List already returns slugs sorted, but parse-skips don't
	// reorder, so the summaries stay sorted by slug.
	return out, nil
}

// GetDashboard reads one dashboard by slug from the registry. Returns a
// wrapped os.ErrNotExist when the slug is unknown so callers dispatch 404.
func (s *Service) GetDashboard(ctx context.Context, slug string) (Dashboard, error) {
	if !validDashboardSlug(slug) {
		return Dashboard{}, fmt.Errorf("invalid dashboard slug %q", slug)
	}
	s.migrateLegacyDashboards()
	data, err := s.dashboardStore().Get(slug)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Dashboard{}, &notRegisteredError{kind: "dashboard", name: slug}
		}
		return Dashboard{}, fmt.Errorf("get dashboard: %w", err)
	}
	d, err := parseDashboardFile(slug, data)
	if err != nil {
		return Dashboard{}, fmt.Errorf("parse dashboard %q: %w", slug, err)
	}
	if d.Datasets == nil {
		d.Datasets = []DashboardDataset{}
	}
	if d.Widgets == nil {
		d.Widgets = []DashboardWidget{}
	}
	return d, nil
}

// SaveDashboard creates or replaces a dashboard definition file. The slug
// is the key — a save with an existing slug overwrites. Returns the
// stored dashboard.
func (s *Service) SaveDashboard(ctx context.Context, d Dashboard) (Dashboard, error) {
	if err := validateDashboard(d); err != nil {
		return Dashboard{}, err
	}
	// SQL parse-check is best-effort: a definition file must save even
	// when no parser is wired or the warm worker is unavailable. A real
	// parse error (parser present, SQL bad) still blocks. See
	// validateDashboardSQL.
	if err := s.validateDashboardSQL(ctx, d); err != nil {
		return Dashboard{}, err
	}
	data, err := marshalDashboardFile(d)
	if err != nil {
		return Dashboard{}, err
	}
	if err := s.dashboardStore().Save(d.Slug, data); err != nil {
		return Dashboard{}, fmt.Errorf("save dashboard: %w", err)
	}
	return s.GetDashboard(ctx, d.Slug)
}

// transpileServingTemplate transpiles a Spark serving-SQL TEMPLATE (with
// {{name}} placeholders intact) to the Trino/Athena serving dialect and
// restores the placeholders, returning a Trino template. Placeholders are
// sentinelized to string literals so sqlglot can parse the template, then
// restored after transpile; because the cache (wired into the transpiler)
// keys on the sentinelized template, the cached entry is param-independent
// — render-time fills the same template with runtime params and never
// re-transpiles. Pass-through when no transpiler is wired (TranspileServing
// returns the sentinelized input unchanged, which desentinelizes back to
// the original template).
func (s *Service) transpileServingTemplate(ctx context.Context, sparkTemplate string) (string, error) {
	sent := servingsql.SentinelizeTemplate(sparkTemplate)
	out, err := s.TranspileServing(ctx, sent)
	if err != nil {
		return "", err
	}
	return servingsql.DesentinelizeTrino(out), nil
}

// validateDashboardSQL runs a two-step author-time gate over every
// dataset's SQL (and any select-control SQL): a Spark /parse check in the
// author dialect, then a sqlglot transpile gate that confirms the SQL is
// portable to the Trino/Athena serving dialect AND populates the transpile
// cache as a side effect (render re-derives from that cache, never
// re-transpiling). Failures are aggregated so the user sees every bad
// dataset in one shot — saving a dashboard with three broken datasets
// one-by-one would be infuriating.
//
// Unlike the prior Athena-EXPLAIN portability gate (ADR-022), the transpile
// gate runs on BOTH local and cloud workspaces — portability is a property
// of the SQL, not the deployment, so it is checked at author time even
// locally. This closes the ADR-022 gap where local-authored serving SQL
// only broke after a cloud deploy.
//
// No-op when neither a parser nor a transpiler is wired (CLI integration
// tests, dry runs). Transport failures (warm worker dead, sidecar down)
// are logged but never block the save — render-time still surfaces real
// errors.
func (s *Service) validateDashboardSQL(ctx context.Context, d Dashboard) error {
	// Nothing wired to validate against — common in CLI/unit paths with no
	// docker.
	if s.sqlParser == nil && s.transpiler == nil {
		return nil
	}

	// Expand control-placeholder defaults for the Spark PARSE check. Dataset /
	// control SQL carries `{{name}}` placeholders (e.g. `{{period.start}}`)
	// that are not valid SQL until substituted — the warm worker chokes on
	// `{{`. Resolve the dashboard's declared control defaults exactly as
	// RenderDashboard does, so we parse-check the SQL the engine will actually
	// run rather than the raw template. (The transpile gate, by contrast,
	// runs on the RAW template — the sentinel scheme handles `{{ }}` and keeps
	// the cache param-independent.)
	effective := map[string]string{}
	resolveControlDefaults(d.Controls, effective, time.Now())

	var failures []string
	checkOne := func(label, sql, dir string) {
		expanded, expErr := expandPlaceholders(sql, effective)
		if expErr != nil {
			// An unresolved placeholder is a control-wiring issue, not a SQL
			// syntax error the user can fix in the editor — render-time
			// surfaces it. Skip the parse-check rather than block the save.
			fmt.Fprintf(os.Stderr, "warn: SQL parse-check skipped for %s (unresolved placeholder: %v)\n", label, expErr)
			return
		}
		// Step 1: Spark /parse on the expanded SQL. Invalid Spark is the
		// author's first problem (best error positions), so on a parse error
		// we report it and skip the transpile gate entirely.
		if err := s.ValidateSQL(ctx, expanded); err != nil {
			var pe *ParseError
			if errors.As(err, &pe) {
				failures = append(failures, label+": "+pe.Message)
				return
			}
			fmt.Fprintf(os.Stderr, "warn: SQL parse-check skipped for %s: %v\n", label, err)
			return
		}
		// Step 2: transpile-portability gate on the RAW template (not the
		// expanded form — we transpile the template so the cache is
		// param-independent; the sentinel scheme handles the `{{ }}`). A
		// successful transpile is discarded: its side effect is populating the
		// cache, which render re-derives from.
		if _, err := s.transpileServingTemplate(ctx, sql); err != nil {
			var de *DialectError
			if errors.As(err, &de) {
				failures = append(failures, label+": "+de.Message)
				return
			}
			// A sidecar-down / transport failure must NOT block authoring —
			// log and move on so the save still succeeds (mirrors the prior
			// EXPLAIN transport-failure handling).
			fmt.Fprintf(os.Stderr, "warn: transpile check skipped for %s: %v\n", label, err)
		}
	}
	for _, ds := range d.Datasets {
		if strings.TrimSpace(ds.SQL) == "" {
			continue
		}
		checkOne(fmt.Sprintf("dataset %q", ds.Name), ds.SQL, ds.Dir)
	}
	for _, c := range d.Controls {
		if c.Type != "select" || strings.TrimSpace(c.SQL) == "" {
			continue
		}
		checkOne(fmt.Sprintf("control %q", c.Name), c.SQL, c.Dir)
	}
	if len(failures) == 0 {
		return nil
	}
	return &ParseError{Message: "Dashboard SQL validation failed:\n  " + strings.Join(failures, "\n  ")}
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

// DeleteDashboard removes a dashboard's definition file. Returns a
// wrapped os.ErrNotExist when the slug is unknown.
func (s *Service) DeleteDashboard(ctx context.Context, slug string) error {
	if !validDashboardSlug(slug) {
		return fmt.Errorf("invalid dashboard slug %q", slug)
	}
	s.migrateLegacyDashboards()
	err := s.dashboardStore().Delete(slug)
	if errors.Is(err, os.ErrNotExist) {
		return &notRegisteredError{kind: "dashboard", name: slug}
	}
	return err
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

// servingTemplate returns the dialect-appropriate SQL template for the
// workspace environment mode: on a cloud workspace the cached Trino
// transpilation of the Spark template (populated at save), on a local
// workspace the Spark template unchanged (local serving runs Spark). A
// nil/local resolver means local. Errors only on a cloud transpile failure
// (surfaced as the widget's inline error).
func (s *Service) servingTemplate(ctx context.Context, sparkTemplate string) (string, error) {
	if s.dashResolver == nil || s.dashResolver.IsLocal() {
		return sparkTemplate, nil
	}
	return s.transpileServingTemplate(ctx, sparkTemplate)
}

// RenderDashboard executes every widget's bound dataset and returns the
// results. Datasets are executed once and shared across the widgets that
// reference them — the same execute-once the UI gets from its query
// cache. Used by `clavesa dashboards render` for cron / CI smoke
// tests; a widget whose dataset errors carries the error inline rather
// than failing the whole render.
//
// On a cloud workspace each dataset's SQL is transpiled to the Trino
// serving dialect (cached from save) before execution; a local workspace
// runs the authored Spark unchanged. The single-dialect contract
// (ADR-014) keeps the response shape identical either way — only the SQL
// handed to the Provider differs.
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
			// Resolve the dialect-appropriate template first: Trino
			// (cached from save) on cloud, the authored Spark on local.
			serving, sErr := s.servingTemplate(ctx, ds.SQL)
			if sErr != nil {
				r = execd{err: sErr}
			} else {
				expanded, expErr := expandPlaceholders(serving, effective)
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

// dashboardProvider returns the cloud-or-local provider for the
// workspace's environment mode. Used only by RenderDashboard, the widget
// SQL execution path, which stays env-dispatched (ADR-014). Definition
// CRUD no longer touches a Provider.
func (s *Service) dashboardProvider() (observability.Provider, error) {
	if s.dashResolver == nil {
		return nil, fmt.Errorf("dashboards: observability resolver not configured")
	}
	return s.dashResolver.For(s.workspace)
}

// marshalDashboardFile serializes a Dashboard into its canonical on-disk
// JSON shape (the datasets shape parseDashboardFile reads back). Title +
// datasets + widgets + controls are persisted; the slug is carried by the
// filename, not the body, mirroring how sources omit Name from their JSON.
func marshalDashboardFile(d Dashboard) ([]byte, error) {
	f := dashboardFile{
		Title:    d.Title,
		Datasets: d.Datasets,
		Widgets:  d.Widgets,
		Controls: d.Controls,
	}
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode dashboard: %w", err)
	}
	return data, nil
}

// migrateLegacyDashboards is the one-time, best-effort consolidation of
// pre-ADR-021 dashboard locations into the canonical registry directory
// (`.clavesa/dashboards/`). Guarded so it runs at most once per process.
//
// If the registry already has files, it does nothing. Otherwise it seeds
// the registry, without clobbering, from, in order:
//   1. `.clavesa/dashboards.imported/*.json` (the backup the old
//      importLegacyDashboards left behind when it moved the directory
//      aside after writing to the system table), then
//   2. `dashboards/*.json` (the workspace-root authoring location a user
//      hand-edits, e.g. analytics/web-traffic/dashboards/heineli.json).
//
// Originals are copied, not deleted. A bad file is skipped with a stderr
// note, never fatal. This recovers existing dashboards into the canonical
// home without ever blocking a read.
func (s *Service) migrateLegacyDashboards() {
	s.dashMu.Lock()
	done := s.dashImported
	s.dashMu.Unlock()
	if done {
		return
	}
	defer func() {
		s.dashMu.Lock()
		s.dashImported = true
		s.dashMu.Unlock()
	}()

	store := s.dashboardStore()
	// Registry already populated: nothing to migrate.
	if existing, err := store.List(); err == nil && len(existing) > 0 {
		return
	}

	migrated := 0
	for _, src := range []string{
		filepath.Join(s.workspace, ".clavesa", "dashboards.imported"),
		filepath.Join(s.workspace, "dashboards"),
	} {
		entries, err := os.ReadDir(src)
		if err != nil {
			continue // source location absent; try the next
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
				continue
			}
			slug := strings.TrimSuffix(e.Name(), ".json")
			if !validDashboardSlug(slug) {
				continue
			}
			// Don't clobber a slug an earlier source already seeded.
			if _, err := store.Get(slug); err == nil {
				continue
			}
			data, readErr := os.ReadFile(filepath.Join(src, e.Name()))
			if readErr != nil {
				fmt.Fprintf(os.Stderr, "clavesa: skip dashboard migration %s: %v\n", e.Name(), readErr)
				continue
			}
			// Validate before writing so a malformed legacy file doesn't
			// land an unreadable entry in the registry.
			if _, parseErr := parseDashboardFile(slug, data); parseErr != nil {
				fmt.Fprintf(os.Stderr, "clavesa: skip dashboard migration %s: %v\n", e.Name(), parseErr)
				continue
			}
			if err := store.Save(slug, data); err != nil {
				fmt.Fprintf(os.Stderr, "clavesa: skip dashboard migration %s: %v\n", e.Name(), err)
				continue
			}
			migrated++
		}
	}
	if migrated > 0 {
		fmt.Fprintf(os.Stderr, "clavesa: migrated %d dashboard(s) into %s\n", migrated, store.Dir())
	}
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

// resolveTimePreset turns a canonical `now-<n><unit>` expression (or
// one of the legacy preset keys `last_24h` / `last_7d` / `last_30d` /
// `last_90d`) into a {start, end} pair of ISO timestamps at `now`.
// Empty or invalid input defaults to `now-30d` so a freshly-added
// time_range control works before the author touches the default.
//
// Thin wrapper over ResolveTimeRange — kept for stable internal call
// sites (resolveControlDefaults). New callers should reach for
// ResolveTimeRange directly.
func resolveTimePreset(preset string, now time.Time) (start, end string) {
	return ResolveTimeRange(preset, now)
}
