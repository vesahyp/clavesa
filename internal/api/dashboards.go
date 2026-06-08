package api

import (
	"context"
	"errors"
	"net/http"
	"os"

	"github.com/vesahyp/clavesa/internal/dashboardsql"
	"github.com/vesahyp/clavesa/internal/httputil"
	"github.com/vesahyp/clavesa/internal/observability"
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
	ID           string                `json:"id"`
	Type         string                `json:"type"`
	Title        string                `json:"title"`
	Dataset      string                `json:"dataset"`
	ValueField   string                `json:"value_field,omitempty"`
	XField       string                `json:"x_field,omitempty"`
	YField       string                `json:"y_field,omitempty"`
	SeriesFields []string              `json:"series_fields,omitempty"`
	LineField    string                `json:"line_field,omitempty"`
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
// depends on. Mirrors service.Service's dashboard methods so internal/api
// stays free of an internal/service import (the cli package bridges them).
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
// scopes the Provider dispatch — it is the pipeline dir of the dataset the
// widget is bound to. The UI resolves widget → dataset → {dir, sql} and
// posts that here, so cross-pipeline datasets just work.
//
// `Params` carries the current dashboard control values. The handler
// substitutes `{{name}}` tokens in `SQL` against this map before
// dispatching to the Provider; unknown placeholders fail with a clear
// 400 so a typo in dataset SQL surfaces in the UI.
type DashboardQueryRequest struct {
	Dir    string            `json:"dir"`
	SQL    string            `json:"sql"`
	Params map[string]string `json:"params,omitempty"`
}

// DashboardsHandler serves the dashboards endpoints. CRUD goes through the
// service-layer DashboardStore (the `dashboards` system Iceberg table);
// the query route runs widget SQL through the observability Provider.
type DashboardsHandler struct {
	store    DashboardStore
	cloud    observability.Provider
	resolver *observability.Resolver
}

// NewDashboardsHandler wires the handler against a service-layer store and
// a CloudProvider fallback for query requests without a resolver dispatch.
func NewDashboardsHandler(store DashboardStore, cloud observability.Provider) *DashboardsHandler {
	return &DashboardsHandler{store: store, cloud: cloud}
}

// WithResolver enables per-request cloud/local provider dispatch for the
// query route.
func (h *DashboardsHandler) WithResolver(r *observability.Resolver) *DashboardsHandler {
	h.resolver = r
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
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
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

	provider := h.providerFor(req.Dir)
	if provider == nil {
		httputil.WriteError(w, http.StatusInternalServerError, "no provider configured for this request")
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

	res, err := provider.Query(r.Context(), observability.QueryQuery{
		SQL:         expanded,
		PipelineDir: req.Dir,
		MaxRows:     queryMaxRowsCap,
	})
	if err != nil {
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

// providerFor dispatches the query through the resolver (cloud or local
// per the workspace environment mode); falls back to the cloud-only
// provider when no resolver is wired.
func (h *DashboardsHandler) providerFor(dir string) observability.Provider {
	if h.resolver != nil && dir != "" {
		if p, err := h.resolver.For(dir); err == nil {
			return p
		}
	}
	return h.cloud
}
