package api

import (
	"context"
	"net/http"

	"github.com/vesahyp/clavesa/internal/httputil"
)

// Optimizer is the service-layer interface internal/api depends on for the
// Delta-maintenance feature. Mirrors service.Service.OptimizeTable so
// internal/api stays free of an internal/service import.
type Optimizer interface {
	OptimizeTable(ctx context.Context, req OptimizeRequest) ([]OptimizeTableResult, error)
}

// OptimizeRequest mirrors service.OptimizeRequest at the api boundary. With no
// Node it targets every transform output; Recluster migrates a table to (or
// re-applies) liquid clustering before compacting; Vacuum additionally prunes
// tombstoned files past the retention window.
type OptimizeRequest struct {
	Dir         string `json:"dir"`
	Node        string `json:"node,omitempty"`
	Recluster   bool   `json:"recluster,omitempty"`
	Vacuum      bool   `json:"vacuum,omitempty"`
	RetainHours int    `json:"retain_hours,omitempty"`
}

// OptimizeTableResult mirrors service.OptimizeTableResult. Same field tags so
// the UI parses the JSON straight off the wire.
type OptimizeTableResult struct {
	Table     string `json:"table"`
	Node      string `json:"node"`
	OutputKey string `json:"output_key"`
	Operation string `json:"operation"`
	Vacuumed  bool   `json:"vacuumed"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
}

// OptimizeHandler serves the /pipelines/optimize endpoint.
type OptimizeHandler struct {
	svc Optimizer
}

// NewOptimizeHandler returns a handler wired to a service-layer optimize
// implementation.
func NewOptimizeHandler(svc Optimizer) *OptimizeHandler {
	return &OptimizeHandler{svc: svc}
}

// RegisterRoutes mounts the optimize endpoint under the api mux.
func (h *OptimizeHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /pipelines/optimize", h.optimize)
}

type optimizeResponse struct {
	Results []OptimizeTableResult `json:"results"`
}

// POST /pipelines/optimize
// Body: { dir, node?, recluster?, vacuum?, retain_hours? }
func (h *OptimizeHandler) optimize(w http.ResponseWriter, r *http.Request) {
	req, ok := httputil.DecodeJSON[OptimizeRequest](w, r)
	if !ok {
		return
	}
	if !httputil.RequireFields(w, map[string]string{"dir": req.Dir}) {
		return
	}
	results, err := h.svc.OptimizeTable(r.Context(), req)
	if err != nil {
		httputil.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	if results == nil {
		results = []OptimizeTableResult{}
	}
	httputil.WriteJSON(w, http.StatusOK, optimizeResponse{Results: results})
}
