package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"

	"github.com/vesahyp/clavesa/internal/httputil"
)

// CredentialRegistry is the service-layer interface internal/api depends
// on for the credentials registry. Mirrors service.Service methods so
// internal/api stays free of an internal/service import.
type CredentialRegistry interface {
	AddCredential(spec CredentialSpec) (CredentialSpec, error)
	ListCredentials() ([]CredentialSpec, error)
	GetCredential(name string) (CredentialSpec, error)
	DeleteCredential(name string, force bool) error
}

// CredentialSpec mirrors credentials.Spec at the api boundary. The
// `secret` field carries only the *reference* (arn:/env:/file: prefix);
// secret material never crosses the API boundary.
type CredentialSpec struct {
	Name        string `json:"name"`
	Kind        string `json:"kind"`
	HeaderName  string `json:"header_name,omitempty"`
	ValuePrefix string `json:"value_prefix,omitempty"`
	Secret      string `json:"secret"`
	// Backend is a derived discriminator (arn|env|file) the UI uses to
	// label the backend without re-parsing the prefix.
	Backend string `json:"backend,omitempty"`
}

// CredentialUsage names a source that references the credential.
type CredentialUsage struct {
	SourceName string `json:"source_name"`
}

// credentialInUseConflicter mirrors inUseConflicter for the credentials
// registry. The api cli bridge converts service.ErrCredentialInUse into
// an implementor of this interface.
type credentialInUseConflicter interface {
	error
	InUseUsages() []CredentialUsage
}

// CredentialsHandler serves the /credentials endpoints.
type CredentialsHandler struct {
	svc CredentialRegistry
}

// NewCredentialsHandler returns a handler wired to a service-layer registry.
func NewCredentialsHandler(svc CredentialRegistry) *CredentialsHandler {
	return &CredentialsHandler{svc: svc}
}

// RegisterRoutes mounts CRUD routes under the api mux.
func (h *CredentialsHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /credentials", h.list)
	mux.HandleFunc("POST /credentials", h.register)
	mux.HandleFunc("GET /credentials/{name}", h.get)
	mux.HandleFunc("DELETE /credentials/{name}", h.delete)
}

type credentialsListResponse struct {
	Credentials []CredentialSpec `json:"credentials"`
}

func (h *CredentialsHandler) list(w http.ResponseWriter, r *http.Request) {
	list, err := h.svc.ListCredentials()
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "list credentials: "+err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, credentialsListResponse{Credentials: list})
}

func (h *CredentialsHandler) get(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	spec, err := h.svc.GetCredential(name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			httputil.WriteError(w, http.StatusNotFound, "credential not found: "+name)
			return
		}
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, spec)
}

func (h *CredentialsHandler) register(w http.ResponseWriter, r *http.Request) {
	var req CredentialSpec
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	if req.Kind == "" {
		req.Kind = "header"
	}
	stored, err := h.svc.AddCredential(req)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, stored)
}

type deleteCredentialResponse struct {
	Usages []CredentialUsage `json:"usages,omitempty"`
}

func (h *CredentialsHandler) delete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	force := r.URL.Query().Get("force") == "1"
	err := h.svc.DeleteCredential(name, force)
	if err == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if errors.Is(err, os.ErrNotExist) {
		httputil.WriteError(w, http.StatusNotFound, "credential not found: "+name)
		return
	}
	if c, ok := err.(credentialInUseConflicter); ok {
		httputil.WriteJSON(w, http.StatusConflict, deleteCredentialResponse{Usages: c.InUseUsages()})
		return
	}
	httputil.WriteError(w, http.StatusInternalServerError, err.Error())
}
