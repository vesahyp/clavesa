package api

import (
	"net/http"

	"github.com/vesahyp/clavesa/internal/httputil"
)

// RunnerRequirementsService is the service-layer interface internal/api
// depends on for the runner's Python requirements file. Mirrors
// service.Service methods so internal/api stays free of an
// internal/service import.
type RunnerRequirementsService interface {
	RunnerRequirements() (string, error)
	SetRunnerRequirements(content string) error
	ListRunnerRequirements() ([]string, error)
}

// RunnerHandler serves the /runner/requirements endpoints.
type RunnerHandler struct {
	svc RunnerRequirementsService
}

// NewRunnerHandler returns a handler wired to a service-layer registry.
func NewRunnerHandler(svc RunnerRequirementsService) *RunnerHandler {
	return &RunnerHandler{svc: svc}
}

// RegisterRoutes mounts the runner-requirements routes under the api mux.
func (h *RunnerHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /runner/requirements", h.get)
	mux.HandleFunc("PUT /runner/requirements", h.set)
}

// runnerRequirementsResponse is the shared shape returned by both GET and
// PUT: the raw file content plus the parsed list of meaningful lines.
// Requirements is always a non-nil array ([] when empty).
type runnerRequirementsResponse struct {
	Content      string   `json:"content"`
	Requirements []string `json:"requirements"`
}

type runnerRequirementsRequest struct {
	Content string `json:"content"`
}

func (h *RunnerHandler) get(w http.ResponseWriter, r *http.Request) {
	resp, err := h.read()
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "read runner requirements: "+err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, resp)
}

func (h *RunnerHandler) set(w http.ResponseWriter, r *http.Request) {
	req, ok := httputil.DecodeJSON[runnerRequirementsRequest](w, r)
	if !ok {
		return
	}
	if err := h.svc.SetRunnerRequirements(req.Content); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "write runner requirements: "+err.Error())
		return
	}
	resp, err := h.read()
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "read runner requirements: "+err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, resp)
}

// read assembles the response shape from the service, normalising the
// requirements list to a non-nil array.
func (h *RunnerHandler) read() (runnerRequirementsResponse, error) {
	content, err := h.svc.RunnerRequirements()
	if err != nil {
		return runnerRequirementsResponse{}, err
	}
	reqs, err := h.svc.ListRunnerRequirements()
	if err != nil {
		return runnerRequirementsResponse{}, err
	}
	if reqs == nil {
		reqs = []string{}
	}
	return runnerRequirementsResponse{Content: content, Requirements: reqs}, nil
}
