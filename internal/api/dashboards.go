package api

import (
	"context"
	"errors"
	"net/http"
	"os"

	"github.com/vesahyp/clavesa/internal/dashboardsql"
	"github.com/vesahyp/clavesa/internal/httputil"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/service"
)

// queryMaxRowsCap bounds how many rows any one widget query can return to
// the UI even if the user's SQL has no LIMIT. Protects the browser from a
// `SELECT * FROM big_table` mistake.
const queryMaxRowsCap = 10_000

// DashboardWidgetLayout positions a widget on a 12-column grid. 0-indexed:
// x in [0,12), w in [1,12], x+w <= 12.
type DashboardWidgetLayout struct {
	X int `json:"x"`
	Y int `json:"y"`
	W int `json:"w"`
	H int `json:"h"`
}

// DashboardDataset is a named, reusable SQL query. Each carries its own
// pipeline dir, so one dashboard can blend tables from multiple pipelines
// and mix local + cloud.
type DashboardDataset struct {
	Name string `json:"name"`
	Dir  string `json:"dir"`
	SQL  string `json:"sql"`
}

// DashboardWidget is one chart/table. It binds to a dataset by name;
// the *_field hints map result columns to the renderer.
type DashboardWidget struct {
	ID           string   `json:"id"`
	Type         string   `json:"type"`
	Title        string   `json:"title"`
	Dataset      string   `json:"dataset"`
	ValueField   string   `json:"value_field,omitempty"`
	XField       string   `json:"x_field,omitempty"`
	YField       string   `json:"y_field,omitempty"`
	SeriesFields []string `json:"series_fields,omitempty"`
	LineField    string   `json:"line_field,omitempty"`
	// RegionField + TooltipField are world_map-only (mirrors the service
	// struct). Missing here meant the API silently dropped region_field
	// when re-serializing a dashboard, blanking the choropleth.
	RegionField  string                `json:"region_field,omitempty"`
	TooltipField string                `json:"tooltip_field,omitempty"`
	Layout       DashboardWidgetLayout `json:"layout"`
}

// DashboardControl is a dashboard-level filter substituted into dataset
// SQL as the placeholder `{{<name>}}` (or `{{<name>.start}}` /
// `{{<name>.end}}` for `time_range`).
type DashboardControl struct {
	Name    string   `json:"name"`
	Type    string   `json:"type"`
	Label   string   `json:"label,omitempty"`
	Default string   `json:"default,omitempty"`
	Dir     string   `json:"dir,omitempty"`
	SQL     string   `json:"sql,omitempty"`
	Options []string `json:"options,omitempty"`
}

// Dashboard is the full spec — body of GET/PUT/POST /api/dashboards{,/slug}.
type Dashboard struct {
	Slug      string             `json:"slug"`
	Title     string             `json:"title"`
	Datasets  []DashboardDataset `json:"datasets"`
	Widgets   []DashboardWidget  `json:"widgets"`
	Controls  []DashboardControl `json:"controls,omitempty"`
	UpdatedAt string             `json:"updated_at,omitempty"`
}

// DashboardSummary is one entry in GET /api/dashboards.
type DashboardSummary struct {
	Slug  string `json:"slug"`
	Title string `json:"title"`
}

// DashboardStore is the service-layer interface the dashboards handler
// depends on. Mirrors service.Service's dashboard methods with the api
// package's own Dashboard shape (the cli package bridges the two).
type DashboardStore interface {
	ListDashboards(ctx context.Context) ([]DashboardSummary, error)
	GetDashboard(ctx context.Context, slug string) (Dashboard, error)
	SaveDashboard(ctx context.Context, d Dashboard) (Dashboard, error)
	DeleteDashboard(ctx context.Context, slug string) error
}

// DashboardsListResponse is the body of GET /api/dashboards.
type DashboardsListResponse struct {
	Dashboards []DashboardSummary `json:"dashboards"`
}

// DashboardQueryRequest is the body of POST /api/dashboards/query. The dir
// scopes the query seam's pipeline reference — it is the pipeline dir of
// the dataset the widget is bound to. The UI resolves widget → dataset →
// {dir, sql} and posts that here, so cross-pipeline datasets just work.
//
// `Params` carries the current dashboard control values. The handler
// substitutes `{{name}}` tokens in `SQL` against this map before
// dispatching to the query seam; unknown placeholders fail with a clear
// 400 so a typo in dataset SQL surfaces in the UI.
type DashboardQueryRequest struct {
	Dir    string            `json:"dir"`
	SQL    string            `json:"sql"`
	Params map[string]string `json:"params,omitempty"`
}

// QueryFunc runs one ad-hoc SQL statement through the shared service seam
// (service.Service.Query): provider dispatch by the workspace warehouse
// plus the ADR-023 SparkSQL→Trino portability gate / transpile — the exact
// path `clavesa query` and POST /data/query ride (ADR-015; mirrors
// dataquery.QueryFunc). Wired via WithQueryService from cli/ui.go.
type QueryFunc func(ctx context.Context, sql, dir string, maxRows int) (*observability.QueryResult, error)

// DashboardsHandler serves the dashboards endpoints. CRUD goes through the
// service-layer DashboardStore (the `dashboards` system Iceberg table);
// the query route runs widget SQL through the shared ad-hoc query seam
// (service.Query via WithQueryService). The handler itself only expands
// `{{control}}` placeholders — dialect policy, provider dispatch, and the
// Served/Transpiled stamping all live in the seam, so `/dashboards/query`,
// `/data/query`, and `clavesa query` cannot drift (P2-N2, 2026-07-02
// review: the previous private dispatch copy had already skipped the
// local portability gate).
type DashboardsHandler struct {
	store   DashboardStore
	queryFn QueryFunc
}

// NewDashboardsHandler wires the handler against a service-layer store.
func NewDashboardsHandler(store DashboardStore) *DashboardsHandler {
	return &DashboardsHandler{store: store}
}

// WithQueryService routes POST /dashboards/query through the shared ad-hoc
// query seam (service.Service.Query). Without it the query route answers
// 500 — production wiring (cli/ui.go) always provides it.
func (h *DashboardsHandler) WithQueryService(fn QueryFunc) *DashboardsHandler {
	h.queryFn = fn
	return h
}

// RegisterRoutes mounts the dashboards endpoints under the api mux.
func (h *DashboardsHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /dashboards", h.list)
	mux.HandleFunc("POST /dashboards", h.create)
	mux.HandleFunc("GET /dashboards/{slug}", h.get)
	mux.HandleFunc("PUT /dashboards/{slug}", h.put)
	mux.HandleFunc("DELETE /dashboards/{slug}", h.delete)
	mux.HandleFunc("POST /dashboards/query", h.query)
}

func (h *DashboardsHandler) list(w http.ResponseWriter, r *http.Request) {
	out, err := h.store.ListDashboards(r.Context())
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "list dashboards: "+err.Error())
		return
	}
	if out == nil {
		out = []DashboardSummary{}
	}
	httputil.WriteJSON(w, http.StatusOK, DashboardsListResponse{Dashboards: out})
}

func (h *DashboardsHandler) get(w http.ResponseWriter, r *http.Request) {
	slug := r.PathValue("slug")
	d, err := h.store.GetDashboard(r.Context(), slug)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			httputil.WriteError(w, http.StatusNotFound, "dashboard not found: "+slug)
			return
		}
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, d)
}

// create is POST /dashboards — fails with 409 when the slug is taken, so a
// "New dashboard" never silently clobbers an existing one. Replace goes
// through PUT.
func (h *DashboardsHandler) create(w http.ResponseWriter, r *http.Request) {
	d, ok := httputil.DecodeJSON[Dashboard](w, r)
	if !ok {
		return
	}
	if _, err := h.store.GetDashboard(r.Context(), d.Slug); err == nil {
		httputil.WriteError(w, http.StatusConflict, "dashboard already exists: "+d.Slug)
		return
	} else if !errors.Is(err, os.ErrNotExist) {
		// A non-not-found error here (bad slug, backend down) is worth
		// surfacing rather than masking with the save attempt.
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	h.save(w, r, d)
}

// put is PUT /dashboards/{slug} — create-or-replace. The path slug is
// authoritative; a slug in the body is overwritten so the stored row
// always matches the URL.
func (h *DashboardsHandler) put(w http.ResponseWriter, r *http.Request) {
	d, ok := httputil.DecodeJSON[Dashboard](w, r)
	if !ok {
		return
	}
	d.Slug = r.PathValue("slug")
	h.save(w, r, d)
}

func (h *DashboardsHandler) save(w http.ResponseWriter, r *http.Request, d Dashboard) {
	stored, err := h.store.SaveDashboard(r.Context(), d)
	if err != nil {
		// Validation failures (bad slug, dangling dataset ref, unknown
		// widget type) are the common case — 400. A genuine backend
		// failure also lands here; 400 keeps parity with the sources
		// handler and the message carries the detail.
		httputil.WriteServiceError(w, err, http.StatusBadRequest)
		return
	}
	httputil.WriteJSON(w, http.StatusOK, stored)
}

func (h *DashboardsHandler) delete(w http.ResponseWriter, r *http.Request) {
	slug := r.PathValue("slug")
	err := h.store.DeleteDashboard(r.Context(), slug)
	if err == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if errors.Is(err, os.ErrNotExist) {
		httputil.WriteError(w, http.StatusNotFound, "dashboard not found: "+slug)
		return
	}
	httputil.WriteError(w, http.StatusInternalServerError, err.Error())
}

func (h *DashboardsHandler) query(w http.ResponseWriter, r *http.Request) {
	req, ok := httputil.DecodeJSON[DashboardQueryRequest](w, r)
	if !ok {
		return
	}
	// dir is required for both backends so the request fails the same way
	// regardless of the inspected pipeline's compute attr (ADR-014).
	if !httputil.RequireFields(w, map[string]string{"sql": req.SQL, "dir": req.Dir}) {
		return
	}

	if h.queryFn == nil {
		httputil.WriteError(w, http.StatusInternalServerError, "no query service configured for this request")
		return
	}

	expanded, err := expandDashboardPlaceholders(req.SQL, req.Params)
	if err != nil {
		// A typo in a dataset's {{name}} is the common case — 400 with
		// the placeholder name so the UI can surface it inline rather
		// than failing the whole dashboard.
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	// The seam applies the ADR-023 dialect policy on BOTH warehouses
	// (transpile-and-dispatch-Trino on cloud, portability-gate-then-run-
	// Spark on local) and stamps Served/Transpiled itself (ADR-024).
	res, err := h.queryFn(r.Context(), expanded, req.Dir, queryMaxRowsCap)
	if err != nil {
		// A dialect rejection (SparkSQL the ADR-023 transpiler can't map
		// to Trino/Athena) is the author's problem — 400, mirroring
		// /data/query, so the widget editor surfaces it inline instead
		// of as a server fault.
		var de *service.DialectError
		if errors.As(err, &de) {
			httputil.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, res)
}

// expandDashboardPlaceholders delegates to the leaf-package implementation
// shared with internal/service/dashboard.go (C13, 2026-05-24).
func expandDashboardPlaceholders(sql string, params map[string]string) (string, error) {
	return dashboardsql.ExpandPlaceholders(sql, params)
}
