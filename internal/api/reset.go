package api

import (
	"context"
	"net/http"

	"github.com/vesahyp/clavesa/internal/httputil"
)

// Resetter is the service-layer interface internal/api depends on for the
// pipeline reset feature. Mirrors the relevant service.Service methods so
// internal/api stays free of an internal/service import.
type Resetter interface {
	PipelineResetPlan(ctx context.Context, req PipelineResetRequest) (*PipelineResetResult, error)
	PipelineReset(ctx context.Context, req PipelineResetRequest) (*PipelineResetResult, error)
}

// PipelineResetRequest mirrors service.PipelineResetRequest at the api
// boundary. Node empty = every transform node; IncludeWatermarks also
// clears the consumer-side CDF watermarks so incremental inputs replay
// from the start.
type PipelineResetRequest struct {
	Dir               string `json:"dir"`
	Node              string `json:"node,omitempty"`
	IncludeWatermarks bool   `json:"include_watermarks,omitempty"`
}

// ResetTarget mirrors service.ResetTarget. Same field tags so the UI
// parses the JSON straight off the wire.
type ResetTarget struct {
	Node      string `json:"node"`
	OutputKey string `json:"output_key"`
	Table     string `json:"table"`
	GlueDB    string `json:"glue_db"`
	Location  string `json:"location"`
}

// WatermarkTarget mirrors service.WatermarkTarget.
type WatermarkTarget struct {
	Consumer string `json:"consumer"`
	Alias    string `json:"alias"`
	Path     string `json:"path"`
}

// PipelineResetResult mirrors service.PipelineResetResult — the plan
// (POST /pipeline/reset/plan) or the receipt (POST /pipeline/reset).
type PipelineResetResult struct {
	Pipeline          string            `json:"pipeline"`
	Mode              string            `json:"mode"`
	TablesDropped     []ResetTarget     `json:"tables_dropped"`
	WatermarksCleared []WatermarkTarget `json:"watermarks_cleared"`
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
	req, ok := httputil.DecodeJSON[PipelineResetRequest](w, r)
	if !ok {
		return
	}
	if !httputil.RequireFields(w, map[string]string{"dir": req.Dir}) {
		return
	}
	res, err := h.svc.PipelineResetPlan(r.Context(), req)
	if err != nil {
		httputil.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, normalizeResetResult(res))
}

// POST /pipeline/reset
// Body: { dir, node?, include_watermarks? }
func (h *ResetHandler) reset(w http.ResponseWriter, r *http.Request) {
	req, ok := httputil.DecodeJSON[PipelineResetRequest](w, r)
	if !ok {
		return
	}
	if !httputil.RequireFields(w, map[string]string{"dir": req.Dir}) {
		return
	}
	res, err := h.svc.PipelineReset(r.Context(), req)
	if err != nil {
		httputil.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, normalizeResetResult(res))
}

// normalizeResetResult guarantees non-nil slices so the UI's Zod boundary
// sees `[]` rather than `null`.
func normalizeResetResult(res *PipelineResetResult) *PipelineResetResult {
	if res == nil {
		res = &PipelineResetResult{}
	}
	if res.TablesDropped == nil {
		res.TablesDropped = []ResetTarget{}
	}
	if res.WatermarksCleared == nil {
		res.WatermarksCleared = []WatermarkTarget{}
	}
	return res
}
