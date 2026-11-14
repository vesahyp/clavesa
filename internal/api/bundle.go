package api

import (
	"context"
	"net/http"

	"github.com/vesahyp/clavesa/internal/httputil"
)

// BundleService is what the bundle page needs from the service layer
// (ADR-031 section 11; ADR-015: the same calls as clavesa bundle plan).
type BundleService interface {
	// PlanBundle plans a bundle folder for a target; the result is the
	// report clavesa bundle plan --json prints.
	PlanBundle(ctx context.Context, dir, target string) (any, error)
	// BundlePipelines lists the workspace's pipelines generated from
	// bundle jobs.
	BundlePipelines() (any, error)
}

// BundleHandler serves the read-only bundle page.
type BundleHandler struct{ svc BundleService }

// NewBundleHandler returns the handler.
func NewBundleHandler(svc BundleService) *BundleHandler { return &BundleHandler{svc: svc} }

// RegisterRoutes mounts GET /bundle/plan and GET /bundle/pipelines.
func (h *BundleHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /bundle/plan", h.plan)
	mux.HandleFunc("GET /bundle/pipelines", h.pipelines)
}

func (h *BundleHandler) plan(w http.ResponseWriter, r *http.Request) {
	dir := r.URL.Query().Get("bundle")
	if dir == "" {
		httputil.WriteError(w, http.StatusBadRequest, "bundle: give the folder with databricks.yml")
		return
	}
	// A bundle that does not plan is an answer, not a server error: the page
	// shows the message (the same one clavesa bundle plan prints).
	rep, err := h.svc.PlanBundle(r.Context(), dir, r.URL.Query().Get("target"))
	if err != nil {
		httputil.WriteJSON(w, http.StatusOK, map[string]any{"error": err.Error()})
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]any{"report": rep})
}

func (h *BundleHandler) pipelines(w http.ResponseWriter, r *http.Request) {
	out, err := h.svc.BundlePipelines()
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, out)
}
