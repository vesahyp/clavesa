package api

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strconv"

	"github.com/vesahyp/clavesa/internal/httputil"
	"github.com/vesahyp/clavesa/internal/preview"
)

// SourceRegistry is the service-layer interface the api package depends on
// for the ADR-017 source registry. Mirrors the relevant service.Service
// methods so internal/api stays free of an internal/service import.
type SourceRegistry interface {
	AddSource(spec SourceSpec) (SourceSpec, error)
	UpdateSource(name string, spec SourceSpec) (SourceSpec, error)
	ListSources() ([]SourceSpec, error)
	GetSource(name string) (SourceSpec, error)
	DeleteSource(name string, force bool) error
	AttachSource(dir, name, toNode, alias string) error
	PreviewRegistrySource(ctx context.Context, name string, offset, limit int) (*preview.PreviewResult, error)
}

// SourceSpec mirrors sources.Spec / service.SourceSpec at the api
// boundary. Field tags must stay byte-identical to the storage shape
// — the UI parses with the same names.
type SourceSpec struct {
	Name                      string   `json:"name"`
	Kind                      string   `json:"kind"`
	URL                       string   `json:"url,omitempty"`
	Bucket                    string   `json:"bucket,omitempty"`
	Prefix                    string   `json:"prefix,omitempty"`
	Format                    string   `json:"format,omitempty"`
	Credentials               string   `json:"credentials,omitempty"`
	Partitions                []string `json:"partitions,omitempty"`
	StartFrom                 string   `json:"start_from,omitempty"`
	ManageBucketNotifications bool     `json:"manage_bucket_notifications,omitempty"`
}

// SourceUsage names a pipeline that references the source. Returned in
// the 409 Conflict body of DELETE so the UI can render "used by N
// pipelines" inline.
type SourceUsage struct {
	PipelineDir string   `json:"pipeline_dir"`
	NodeIDs     []string `json:"node_ids"`
}

// SourcesHandler serves the /sources endpoints.
type SourcesHandler struct {
	svc SourceRegistry
}

// NewSourcesHandler returns a handler wired to a service-layer registry.
func NewSourcesHandler(svc SourceRegistry) *SourcesHandler {
	return &SourcesHandler{svc: svc}
}

// RegisterRoutes mounts CRUD + attach routes under the api mux.
func (h *SourcesHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /sources", h.list)
	mux.HandleFunc("POST /sources", h.register)
	mux.HandleFunc("GET /sources/{name}", h.get)
	mux.HandleFunc("GET /sources/{name}/preview", h.preview)
	mux.HandleFunc("PUT /sources/{name}", h.update)
	mux.HandleFunc("DELETE /sources/{name}", h.delete)
	mux.HandleFunc("POST /sources/{name}/attach", h.attach)
}

type sourcesListResponse struct {
	Sources []SourceSpec `json:"sources"`
}

func (h *SourcesHandler) list(w http.ResponseWriter, r *http.Request) {
	list, err := h.svc.ListSources()
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "list sources: "+err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, sourcesListResponse{Sources: list})
}

func (h *SourcesHandler) get(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	spec, err := h.svc.GetSource(name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			httputil.WriteError(w, http.StatusNotFound, "source not found: "+name)
			return
		}
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, spec)
}

func (h *SourcesHandler) register(w http.ResponseWriter, r *http.Request) {
	req, ok := httputil.DecodeJSON[SourceSpec](w, r)
	if !ok {
		return
	}
	// Don't default Kind here — service.AddSource sniffs `s3://` URLs
	// and promotes kind=s3 with bucket/prefix derivation. Forcing
	// Kind="http" here would short-circuit that and 400 every UI POST
	// of an s3:// URL (the registerSource client doesn't know the
	// kind, only the URL).
	stored, err := h.svc.AddSource(req)
	if err != nil {
		httputil.WriteServiceError(w, err, http.StatusBadRequest)
		return
	}
	httputil.WriteJSON(w, http.StatusOK, stored)
}

// preview samples a registered source's raw data standalone — the HTTP
// twin of `clavesa source preview <name>`.
func (h *SourcesHandler) preview(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	limit := 50
	if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && v > 0 {
		limit = v
	}
	res, err := h.svc.PreviewRegistrySource(r.Context(), name, offset, limit)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			httputil.WriteError(w, http.StatusNotFound, "source not found: "+name)
			return
		}
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, res)
}

// update overwrites an existing source. The {name} path segment is the
// fixed registry key — a `name` in the body is ignored, since renaming
// is a delete + re-register, not an edit.
func (h *SourcesHandler) update(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	req, ok := httputil.DecodeJSON[SourceSpec](w, r)
	if !ok {
		return
	}
	stored, err := h.svc.UpdateSource(name, req)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			httputil.WriteError(w, http.StatusNotFound, "source not found: "+name)
			return
		}
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, stored)
}

type deleteSourceResponse struct {
	Usages []SourceUsage `json:"usages,omitempty"`
}

// inUseConflicter lets the api layer pull the structured usage list out
// of a service-side ErrSourceInUse without importing internal/service.
type inUseConflicter interface {
	error
	InUseUsages() []SourceUsage
}

func (h *SourcesHandler) delete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	force := r.URL.Query().Get("force") == "1"
	err := h.svc.DeleteSource(name, force)
	if err == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if errors.Is(err, os.ErrNotExist) {
		httputil.WriteError(w, http.StatusNotFound, "source not found: "+name)
		return
	}
	// 409 + structured body when in use, so the UI can render the
	// dependency list inline.
	if c, ok := err.(inUseConflicter); ok {
		httputil.WriteJSON(w, http.StatusConflict, deleteSourceResponse{Usages: c.InUseUsages()})
		return
	}
	httputil.WriteError(w, http.StatusInternalServerError, err.Error())
}

type attachRequest struct {
	Dir   string `json:"dir"`
	To    string `json:"to"`
	Alias string `json:"alias"`
}

func (h *SourcesHandler) attach(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	req, ok := httputil.DecodeJSON[attachRequest](w, r)
	if !ok {
		return
	}
	if !httputil.RequireFields(w, map[string]string{"dir": req.Dir, "to": req.To}) {
		return
	}
	if err := h.svc.AttachSource(req.Dir, name, req.To, req.Alias); err != nil {
		httputil.WriteServiceError(w, err, http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
