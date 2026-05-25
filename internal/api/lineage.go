package api

import (
	"net/http"

	"github.com/vesahyp/clavesa/internal/httputil"
	"github.com/vesahyp/clavesa/internal/lineagetype"
)

// LineageResponse is an alias for the canonical leaf-package type so
// existing call sites keep compiling. New code imports lineagetype.
type LineageResponse = lineagetype.Response

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
	// Normalise an empty graph to `{"edges":[]}` — the UI Zod schema
	// treats null as a schema violation. service.Lineage today never
	// returns (nil, nil), but a test-only Lineager (fakeLineager) does
	// pass nil edges intentionally to exercise this seam, so keep the
	// guard. (Revisit if service guarantees non-nil through the
	// interface.)
	if res == nil {
		res = &lineagetype.Response{Edges: []lineagetype.Edge{}}
	}
	if res.Edges == nil {
		res.Edges = []lineagetype.Edge{}
	}
	httputil.WriteJSON(w, http.StatusOK, res)
}
