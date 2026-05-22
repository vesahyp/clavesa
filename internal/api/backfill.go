package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/vesahyp/clavesa/internal/httputil"
)

// Backfiller is the service-layer interface internal/api depends on for
// the backfill feature. Mirrors the relevant service.Service methods so
// internal/api stays free of an internal/service import.
type Backfiller interface {
	BackfillStage(ctx context.Context, req BackfillStageRequest) (*BackfillRun, error)
	BackfillList(ctx context.Context, dir string) ([]BackfillRun, error)
	BackfillDiff(ctx context.Context, dir, runID string) (*BackfillDiff, error)
	BackfillDedupCheck(ctx context.Context, dir, runID, col string) (*BackfillDedupCheckResult, error)
	BackfillPromote(ctx context.Context, dir, runID string, opts BackfillPromoteOpts) error
	BackfillDiscard(ctx context.Context, dir, runID string) error
}

// BackfillColumnInfo mirrors service.BackfillColumnInfo. Surfaced inside
// BackfillDiff so the UI can populate a real column dropdown on the
// append-mode promote screen.
type BackfillColumnInfo struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// BackfillDedupCheckResult mirrors service.BackfillDedupCheckResult.
type BackfillDedupCheckResult struct {
	MatchingRows int64 `json:"matching_rows"`
	NewRows      int64 `json:"new_rows"`
}

// BackfillStageRequest mirrors service.BackfillStageRequest at the api
// boundary. The runner reads only partitions inside [from, to] and writes
// to a parallel staging table (unless Direct=true).
type BackfillStageRequest struct {
	Dir    string   `json:"dir"`
	Node   string   `json:"node"`
	From   []string `json:"from"`
	To     []string `json:"to"`
	Direct bool     `json:"direct,omitempty"`
}

// BackfillRun mirrors service.BackfillRun. Same field tags so the UI
// parses the JSON straight off the wire.
type BackfillRun struct {
	RunID          string `json:"run_id"`
	Pipeline       string `json:"pipeline"`
	Node           string `json:"node"`
	OutputKey      string `json:"output_key"`
	From           []string `json:"from_cursor"`
	To             []string `json:"to_cursor"`
	Direct         bool   `json:"direct"`
	TargetTable    string `json:"target_table"`
	CanonicalTable string `json:"canonical_table"`
	StartedAt      string `json:"started_at,omitempty"`
	StoppedAt      string `json:"stopped_at,omitempty"`
	Status         string `json:"status"`
	RowsWritten    int64  `json:"rows_written,omitempty"`
	ErrorMsg       string `json:"error_msg,omitempty"`
}

// BackfillDiff mirrors service.BackfillDiff.
type BackfillDiff struct {
	RunID           string               `json:"run_id"`
	StagingTable    string               `json:"staging_table"`
	CanonicalTable  string               `json:"canonical_table"`
	StagingRows     int64                `json:"staging_rows"`
	CanonicalRows   int64                `json:"canonical_rows"`
	SchemaMatches   bool                 `json:"schema_matches"`
	SchemaDiff      string               `json:"schema_diff,omitempty"`
	OutputMode      string               `json:"output_mode"`
	MergeKeys       []string             `json:"merge_keys,omitempty"`
	MatchingKeyRows int64                `json:"matching_key_rows,omitempty"`
	NewKeyRows      int64                `json:"new_key_rows,omitempty"`
	StagingColumns  []BackfillColumnInfo `json:"staging_columns,omitempty"`
}

// BackfillPromoteOpts mirrors service.BackfillPromoteOpts.
type BackfillPromoteOpts struct {
	ForceDedup      string `json:"force_dedup,omitempty"`
	AllowDuplicates bool   `json:"allow_duplicates,omitempty"`
}

// BackfillHandler serves the /backfills endpoints.
type BackfillHandler struct {
	svc Backfiller
}

// NewBackfillHandler returns a handler wired to a service-layer backfill
// implementation.
func NewBackfillHandler(svc Backfiller) *BackfillHandler {
	return &BackfillHandler{svc: svc}
}

// RegisterRoutes mounts list / stage / diff / promote / discard under the
// api mux. Run IDs travel as path params on the per-run routes so the UI
// can render bookmarkable URLs at /backfills?dir=…&run=<id>.
func (h *BackfillHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /backfills", h.list)
	mux.HandleFunc("POST /backfills/stage", h.stage)
	mux.HandleFunc("GET /backfills/{run_id}/diff", h.diff)
	mux.HandleFunc("GET /backfills/{run_id}/dedup-check", h.dedupCheck)
	mux.HandleFunc("POST /backfills/{run_id}/promote", h.promote)
	mux.HandleFunc("POST /backfills/{run_id}/discard", h.discard)
}

type backfillsListResponse struct {
	Backfills []BackfillRun `json:"backfills"`
}

// GET /backfills?dir=<pipeline_dir>
func (h *BackfillHandler) list(w http.ResponseWriter, r *http.Request) {
	dir := strings.TrimSpace(r.URL.Query().Get("dir"))
	if dir == "" {
		httputil.WriteError(w, http.StatusBadRequest, "dir is required")
		return
	}
	runs, err := h.svc.BackfillList(r.Context(), dir)
	if err != nil {
		httputil.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, backfillsListResponse{Backfills: runs})
}

// POST /backfills/stage
// Body: { dir, node, from: [cursor...], to: [cursor...], direct? }
func (h *BackfillHandler) stage(w http.ResponseWriter, r *http.Request) {
	var req BackfillStageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	if req.Dir == "" || req.Node == "" || len(req.From) == 0 || len(req.To) == 0 {
		httputil.WriteError(w, http.StatusBadRequest, "dir, node, from, and to are required")
		return
	}
	run, err := h.svc.BackfillStage(r.Context(), req)
	if err != nil {
		// BackfillStage returns the partially-populated run alongside the
		// error when the Lambda itself returned a FunctionError — surface
		// both so the UI can show the error message inline rather than
		// just a generic 502.
		if run != nil {
			httputil.WriteJSON(w, http.StatusBadGateway, run)
			return
		}
		httputil.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, run)
}

// GET /backfills/{run_id}/diff?dir=<pipeline_dir>
func (h *BackfillHandler) diff(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("run_id")
	dir := strings.TrimSpace(r.URL.Query().Get("dir"))
	if runID == "" || dir == "" {
		httputil.WriteError(w, http.StatusBadRequest, "dir and run_id are required")
		return
	}
	d, err := h.svc.BackfillDiff(r.Context(), dir, runID)
	if err != nil {
		httputil.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, d)
}

// GET /backfills/{run_id}/dedup-check?dir=<pipeline_dir>&col=<column>
// Returns the matching/new-key counts the user would get if they
// promoted with --force-dedup <col>. Cheap-ish (two Athena queries);
// fired live as the user picks a column on the promote screen.
func (h *BackfillHandler) dedupCheck(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("run_id")
	dir := strings.TrimSpace(r.URL.Query().Get("dir"))
	col := strings.TrimSpace(r.URL.Query().Get("col"))
	if runID == "" || dir == "" || col == "" {
		httputil.WriteError(w, http.StatusBadRequest, "dir, run_id, and col are required")
		return
	}
	res, err := h.svc.BackfillDedupCheck(r.Context(), dir, runID, col)
	if err != nil {
		httputil.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, res)
}

type promoteBody struct {
	Dir             string `json:"dir"`
	ForceDedup      string `json:"force_dedup,omitempty"`
	AllowDuplicates bool   `json:"allow_duplicates,omitempty"`
}

// POST /backfills/{run_id}/promote
// Body: { dir, force_dedup?, allow_duplicates? }
func (h *BackfillHandler) promote(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("run_id")
	var body promoteBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	if body.Dir == "" || runID == "" {
		httputil.WriteError(w, http.StatusBadRequest, "dir and run_id are required")
		return
	}
	if err := h.svc.BackfillPromote(r.Context(), body.Dir, runID, BackfillPromoteOpts{
		ForceDedup:      body.ForceDedup,
		AllowDuplicates: body.AllowDuplicates,
	}); err != nil {
		httputil.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type discardBody struct {
	Dir string `json:"dir"`
}

// POST /backfills/{run_id}/discard
// Body: { dir }
func (h *BackfillHandler) discard(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("run_id")
	var body discardBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	if body.Dir == "" || runID == "" {
		httputil.WriteError(w, http.StatusBadRequest, "dir and run_id are required")
		return
	}
	if err := h.svc.BackfillDiscard(r.Context(), body.Dir, runID); err != nil {
		httputil.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
