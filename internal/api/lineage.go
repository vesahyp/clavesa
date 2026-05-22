package api

import (
	"net/http"

	"github.com/vesahyp/clavesa/internal/httputil"
)

// LineageResponse wraps the edges array plus the queried pipeline's own
// ADR-016 namespace (catalog + schema). The UI uses catalog/schema to
// label a node's output table — deriving it from a `via_table` is wrong
// for a pipeline that only reads cross-pipeline (the via_table then
// points at the upstream pipeline's schema).
type LineageResponse struct {
	Edges   []LineageEdge `json:"edges"`
	Catalog string        `json:"catalog,omitempty"`
	Schema  string        `json:"schema,omitempty"`
}

// GET /pipeline/lineage?dir=<path>
//
// Returns the directed table-to-table lineage edges for the pipeline at dir.
// Derived purely from .tf — no Glue, no Athena, no SFN — so the same response
// shape works for cloud and local pipelines (ADR-014). The UI's TableDetail
// page filters edges by `via_table` to render upstream and downstream
// neighbors of one table.
func (h *Handler) GetLineage(w http.ResponseWriter, r *http.Request) {
	if h.lineager == nil {
		httputil.WriteError(w, http.StatusInternalServerError, "lineage not wired: Handler.WithLineage was not called")
		return
	}
	dir := h.resolve(r.URL.Query().Get("dir"))
	if dir == "" {
		httputil.WriteError(w, http.StatusBadRequest, "missing required query param: dir")
		return
	}
	res, err := h.lineager.Lineage(dir)
	if err != nil {
		httputil.WriteError(w, parseStatus(err), err.Error())
		return
	}
	if res == nil {
		res = &LineageResponse{}
	}
	if res.Edges == nil {
		res.Edges = []LineageEdge{}
	}
	httputil.WriteJSON(w, http.StatusOK, res)
}
