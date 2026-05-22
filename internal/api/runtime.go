package api

import (
	"net/http"

	"github.com/vesahyp/clavesa/internal/httputil"
	"github.com/vesahyp/clavesa/internal/observability"
)

// WorkerLister is the dependency RuntimeHandler needs. The warm-Spark
// query runner (`observability.persistentDockerQueryRunner`) satisfies
// it; the interface keeps internal/api decoupled from that unexported
// concrete type.
type WorkerLister interface {
	Workers() []observability.WorkerStatus
}

// AWSIdentity is the server process's effective AWS identity, resolved
// once at startup via sts:GetCallerIdentity. Surfaced by
// GET /runtime/identity so the UI can show which account / profile the
// server is operating as — the fast answer to "why did this 403?".
type AWSIdentity struct {
	// Available is false when no AWS credentials resolved (local-only
	// mode, or GetCallerIdentity failed) — the UI hides the chip then.
	Available bool   `json:"available"`
	AccountID string `json:"account_id,omitempty"`
	ARN       string `json:"arn,omitempty"`
	// Profile is the AWS_PROFILE the server process was started with,
	// empty when unset (default profile / instance role).
	Profile string `json:"profile,omitempty"`
}

// RuntimeHandler serves /runtime/* — ambient process state the UI's
// header surfaces: the warm-Spark worker spawn status (a "Starting
// Spark…" hint instead of an unexplained first-query freeze) and the
// server's effective AWS identity.
type RuntimeHandler struct {
	workers  WorkerLister
	identity AWSIdentity
}

// NewRuntimeHandler wires a handler against a worker lister and the
// resolved AWS identity. workers may be nil — the workers endpoint then
// reports an empty list (the UI indicator stays hidden). A zero-value
// identity reports Available=false (local-only mode).
func NewRuntimeHandler(workers WorkerLister, identity AWSIdentity) *RuntimeHandler {
	return &RuntimeHandler{workers: workers, identity: identity}
}

// RegisterRoutes mounts the runtime routes on mux.
func (h *RuntimeHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /runtime/workers", h.listWorkers)
	mux.HandleFunc("GET /runtime/identity", h.getIdentity)
}

// getIdentity reports the server's effective AWS identity. Static —
// resolved once at startup — so the UI fetches it once, no polling.
func (h *RuntimeHandler) getIdentity(w http.ResponseWriter, _ *http.Request) {
	httputil.WriteJSON(w, http.StatusOK, h.identity)
}

type runtimeWorkersResponse struct {
	Workers []observability.WorkerStatus `json:"workers"`
}

// listWorkers reports the warm-Spark workers and their spawn state. The
// list is empty in a fresh session (no query has spawned a worker yet),
// after a worker is evicted, and when no warm runner is wired (CLI
// one-shots) — all fine; the UI indicator just stays hidden.
func (h *RuntimeHandler) listWorkers(w http.ResponseWriter, _ *http.Request) {
	workers := []observability.WorkerStatus{}
	if h.workers != nil {
		if got := h.workers.Workers(); got != nil {
			workers = got
		}
	}
	httputil.WriteJSON(w, http.StatusOK, runtimeWorkersResponse{Workers: workers})
}
