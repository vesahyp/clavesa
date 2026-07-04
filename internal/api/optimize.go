package api

import (
	"context"
	"net/http"

	"github.com/vesahyp/clavesa/internal/httputil"
	"github.com/vesahyp/clavesa/internal/service"
)

// Optimizer is the service-layer interface internal/api depends on for the
// Delta-maintenance feature. Satisfied by *service.Service directly — the
// api package already imports internal/service (handler.go), so the former
// field-for-field bridge DTOs guarded a boundary that doesn't exist
// (C2-P2-11, 2026-07-02 review).
type Optimizer interface {
	OptimizeTable(ctx context.Context, req service.OptimizeRequest) ([]service.OptimizeTableResult, error)
}

// optimizeBody is the HTTP body of the optimize endpoint. It exists
// (rather than decoding into service.OptimizeRequest) because the service
// type carries no JSON tags — `retain_hours` wouldn't decode onto
// RetainHours. With no Node it targets every transform output; Recluster
// migrates a table to (or re-applies) liquid clustering before compacting;
// Vacuum additionally prunes tombstoned files past the retention window.
type optimizeBody struct {
	Dir         string `json:"dir"`
	Node        string `json:"node,omitempty"`
	Recluster   bool   `json:"recluster,omitempty"`
	Vacuum      bool   `json:"vacuum,omitempty"`
	RetainHours int    `json:"retain_hours,omitempty"`
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
	Results []service.OptimizeTableResult `json:"results"`
}

// POST /pipelines/optimize
// Body: { dir, node?, recluster?, vacuum?, retain_hours? }
func (h *OptimizeHandler) optimize(w http.ResponseWriter, r *http.Request) {
	req, ok := httputil.DecodeJSON[optimizeBody](w, r)
	if !ok {
		return
	}
	if !httputil.RequireFields(w, map[string]string{"dir": req.Dir}) {
		return
	}
	results, err := h.svc.OptimizeTable(r.Context(), service.OptimizeRequest{
		Dir:         req.Dir,
		Node:        req.Node,
		Recluster:   req.Recluster,
		Vacuum:      req.Vacuum,
		RetainHours: req.RetainHours,
	})
	if err != nil {
		httputil.WriteServiceError(w, err, http.StatusBadGateway)
		return
	}
	if results == nil {
		results = []service.OptimizeTableResult{}
	}
	httputil.WriteJSON(w, http.StatusOK, optimizeResponse{Results: results})
}
