package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"

	"github.com/vesahyp/clavesa/internal/httputil"
	"github.com/vesahyp/clavesa/internal/notebooks"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// NotebookRegistry is the service-layer interface internal/api depends on
// for the notebooks registry. Mirrors service.Service methods so the api
// package stays free of an internal/service import.
//
// Concrete types are pulled from internal/notebooks (the storage package)
// and internal/observability (for CellResult) — both are one-way deps
// (api → them, never the reverse) so no cycle. Cheaper than maintaining a
// parallel set of nbformat types in the api boundary; the .ipynb shape IS
// the wire shape the UI consumes.
type NotebookRegistry interface {
	ListNotebooks() ([]notebooks.Summary, error)
	GetNotebook(name string) (*notebooks.Notebook, error)
	CreateNotebook(name string) (*notebooks.Notebook, error)
	SaveNotebook(nb *notebooks.Notebook) (*notebooks.Notebook, error)
	DeleteNotebook(name string) error
	ClearOutputs(name string) (*notebooks.Notebook, error)
	RunCell(ctx context.Context, name, cellID string) (*CellRunResult, error)
	CancelCell(ctx context.Context, name, cellRunID string) error
	StopNotebookSession(ctx context.Context, name string) error
	GraduateCell(notebookName, cellID, pipelineDir, transformName string) error
}

// CellRunResult bundles the freshly-updated cell + the runner CellResult.
// Mirrors service.CellRunResult; we re-declare here so the api package
// doesn't need to import internal/service.
type CellRunResult struct {
	Cell   notebooks.Cell           `json:"cell"`
	Result observability.CellResult `json:"result"`
	// Served — engine + warehouse the cell ran against (ADR-024).
	// Response-envelope only, never persisted into the .ipynb.
	Served *observability.Served `json:"served,omitempty"`
}

// NotebooksHandler serves /notebooks* endpoints.
type NotebooksHandler struct {
	svc NotebookRegistry
}

// NewNotebooksHandler returns a handler wired to a service-layer registry.
func NewNotebooksHandler(svc NotebookRegistry) *NotebooksHandler {
	return &NotebooksHandler{svc: svc}
}

// RegisterRoutes mounts CRUD + cell-execution routes under the api mux.
func (h *NotebooksHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /notebooks", h.list)
	mux.HandleFunc("POST /notebooks", h.create)
	mux.HandleFunc("GET /notebooks/{name}", h.get)
	mux.HandleFunc("PATCH /notebooks/{name}", h.save)
	mux.HandleFunc("DELETE /notebooks/{name}", h.delete)
	mux.HandleFunc("POST /notebooks/{name}/clear-outputs", h.clearOutputs)
	mux.HandleFunc("POST /notebooks/{name}/cells/{cellID}/run", h.runCell)
	mux.HandleFunc("POST /notebooks/{name}/cells/{cellRunID}/cancel", h.cancelCell)
	mux.HandleFunc("DELETE /notebooks/{name}/session", h.stopSession)
	mux.HandleFunc("POST /notebooks/{name}/cells/{cellID}/graduate", h.graduate)
}

type graduateRequest struct {
	Pipeline      string `json:"pipeline"`
	TransformName string `json:"transform_name"`
}

func (h *NotebooksHandler) graduate(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cellID := r.PathValue("cellID")
	var req graduateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	if err := h.svc.GraduateCell(name, cellID, req.Pipeline, req.TransformName); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			httputil.WriteError(w, http.StatusNotFound, err.Error())
			return
		}
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]string{
		"pipeline":       req.Pipeline,
		"transform_name": req.TransformName,
	})
}

type notebooksListResponse struct {
	Notebooks []notebooks.Summary `json:"notebooks"`
}

func (h *NotebooksHandler) list(w http.ResponseWriter, r *http.Request) {
	list, err := h.svc.ListNotebooks()
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "list notebooks: "+err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, notebooksListResponse{Notebooks: list})
}

type createNotebookRequest struct {
	Name string `json:"name"`
}

func (h *NotebooksHandler) create(w http.ResponseWriter, r *http.Request) {
	var req createNotebookRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	nb, err := h.svc.CreateNotebook(req.Name)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, nb)
}

func (h *NotebooksHandler) get(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	nb, err := h.svc.GetNotebook(name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			httputil.WriteError(w, http.StatusNotFound, "notebook not found: "+name)
			return
		}
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, nb)
}

// save accepts the full notebook body on PATCH and writes it. The cell IDs
// in the request body are authoritative; the server doesn't try to diff
// by position — the UI is expected to PATCH the full body it loaded plus
// edits, IDs intact. Path's `name` overrides any name in the body so
// callers can't accidentally PATCH /notebooks/a with a body claiming b.
func (h *NotebooksHandler) save(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var nb notebooks.Notebook
	if err := json.NewDecoder(r.Body).Decode(&nb); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	nb.Name = name
	out, err := h.svc.SaveNotebook(&nb)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			httputil.WriteError(w, http.StatusNotFound, "notebook not found: "+name)
			return
		}
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, out)
}

func (h *NotebooksHandler) delete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	err := h.svc.DeleteNotebook(name)
	if err == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if errors.Is(err, os.ErrNotExist) {
		httputil.WriteError(w, http.StatusNotFound, "notebook not found: "+name)
		return
	}
	httputil.WriteError(w, http.StatusInternalServerError, err.Error())
}

func (h *NotebooksHandler) clearOutputs(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	nb, err := h.svc.ClearOutputs(name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			httputil.WriteError(w, http.StatusNotFound, "notebook not found: "+name)
			return
		}
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, nb)
}

func (h *NotebooksHandler) runCell(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cellID := r.PathValue("cellID")
	res, err := h.svc.RunCell(r.Context(), name, cellID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			httputil.WriteError(w, http.StatusNotFound, "notebook not found: "+name)
			return
		}
		// Cloud warehouse on an undeployed workspace (ADR-024): the
		// request is well-formed but the workspace state can't satisfy
		// it — a user-actionable 409, not a server fault.
		if errors.Is(err, workspace.ErrWarehouseUndeployed) {
			httputil.WriteError(w, http.StatusConflict, err.Error())
			return
		}
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, res)
}

func (h *NotebooksHandler) cancelCell(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cellRunID := r.PathValue("cellRunID")
	if err := h.svc.CancelCell(r.Context(), name, cellRunID); err != nil {
		if errors.Is(err, workspace.ErrWarehouseUndeployed) {
			httputil.WriteError(w, http.StatusConflict, err.Error())
			return
		}
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]bool{"cancelled": true})
}

func (h *NotebooksHandler) stopSession(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := h.svc.StopNotebookSession(r.Context(), name); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
