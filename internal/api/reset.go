package api

import (
	"context"
	"net/http"

	"github.com/vesahyp/clavesa/internal/httputil"
	"github.com/vesahyp/clavesa/internal/service"
)

// Resetter is the service-layer interface internal/api depends on for the
// pipeline reset feature. Satisfied by *service.Service directly — the api
// package already imports internal/service (handler.go), so the former
// field-for-field bridge DTOs guarded a boundary that doesn't exist
// (C2-P2-11, 2026-07-02 review).
type Resetter interface {
	PipelineResetPlan(ctx context.Context, req service.PipelineResetRequest) (*service.PipelineResetResult, error)
	PipelineReset(ctx context.Context, req service.PipelineResetRequest) (*service.PipelineResetResult, error)
}

// pipelineResetBody is the HTTP body of both reset endpoints. It exists
// (rather than decoding into service.PipelineResetRequest) because the
// service type carries no JSON tags — `include_watermarks` wouldn't
// decode onto IncludeWatermarks. Node empty = every transform node;
// IncludeWatermarks also clears the consumer-side CDF watermarks so
// incremental inputs replay from the start.
type pipelineResetBody struct {
	Dir               string `json:"dir"`
	Node              string `json:"node,omitempty"`
	IncludeWatermarks bool   `json:"include_watermarks,omitempty"`
}

func (b pipelineResetBody) toService() service.PipelineResetRequest {
	return service.PipelineResetRequest{
		Dir:               b.Dir,
		Node:              b.Node,
		IncludeWatermarks: b.IncludeWatermarks,
	}
}

// ResetHandler serves the /pipeline/reset endpoints.
type ResetHandler struct {
	svc Resetter
}

// NewResetHandler returns a handler wired to a service-layer reset
// implementation.
func NewResetHandler(svc Resetter) *ResetHandler {
	return &ResetHandler{svc: svc}
}

// RegisterRoutes mounts plan / execute under the api mux. Plan is a POST
// (not GET) because it shares the request body with execute — the UI's
// confirm modal sends the same JSON to both.
func (h *ResetHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /pipeline/reset/plan", h.plan)
	mux.HandleFunc("POST /pipeline/reset", h.reset)
}

// POST /pipeline/reset/plan
// Body: { dir, node?, include_watermarks? }
func (h *ResetHandler) plan(w http.ResponseWriter, r *http.Request) {
	req, ok := httputil.DecodeJSON[pipelineResetBody](w, r)
	if !ok {
		return
	}
	if !httputil.RequireFields(w, map[string]string{"dir": req.Dir}) {
		return
	}
	res, err := h.svc.PipelineResetPlan(r.Context(), req.toService())
	if err != nil {
		httputil.WriteServiceError(w, err, http.StatusBadGateway)
		return
	}
	httputil.WriteJSON(w, http.StatusOK, normalizeResetResult(res))
}

// POST /pipeline/reset
// Body: { dir, node?, include_watermarks? }
func (h *ResetHandler) reset(w http.ResponseWriter, r *http.Request) {
	req, ok := httputil.DecodeJSON[pipelineResetBody](w, r)
	if !ok {
		return
	}
	if !httputil.RequireFields(w, map[string]string{"dir": req.Dir}) {
		return
	}
	res, err := h.svc.PipelineReset(r.Context(), req.toService())
	if err != nil {
		httputil.WriteServiceError(w, err, http.StatusBadGateway)
		return
	}
	httputil.WriteJSON(w, http.StatusOK, normalizeResetResult(res))
}

// normalizeResetResult guarantees non-nil slices so the UI's Zod boundary
// sees `[]` rather than `null`.
func normalizeResetResult(res *service.PipelineResetResult) *service.PipelineResetResult {
	if res == nil {
		res = &service.PipelineResetResult{}
	}
	if res.TablesDropped == nil {
		res.TablesDropped = []service.ResetTarget{}
	}
	if res.WatermarksCleared == nil {
		res.WatermarksCleared = []service.WatermarkTarget{}
	}
	return res
}
