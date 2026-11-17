package hosted

import (
	"encoding/json"
	"net/http"

	"github.com/vesahyp/clavesa/internal/httputil"
	"github.com/vesahyp/clavesa/internal/service"
)

// snapshotRoutes answers the two reads of a local workspace that come
// from its published snapshot (ADR-036), before the summary-backed mux:
//
//   - GET /workspace/tables: the catalog listing as the Mac gave it;
//   - POST /dashboards/query: the stored result of that dataset (its dir
//     and SQL as the spec stores them), with the control defaults of the
//     publish. Nothing runs: a dataset with no stored result is 403.
//
// Everything else goes to next.
//
// A workspace the registry marks personal_data serves no widget result,
// whatever the Mac sent (slice 5 review, finding 2): the registry is the
// hosted side's own word.
func snapshotRoutes(e Entry, snap *service.DrillSnapshot, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/workspace/tables" && (r.Method == http.MethodGet || r.Method == http.MethodHead):
			if snap == nil || len(snap.Catalog) == 0 {
				httputil.WriteError(w, http.StatusNotFound, "not kept for local workspaces: the published summary has no catalog snapshot")
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(snap.Catalog)
			return
		case r.URL.Path == "/dashboards/query" && r.Method == http.MethodPost:
			var req struct {
				Dir string `json:"dir"`
				SQL string `json:"sql"`
			}
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
				httputil.WriteError(w, http.StatusBadRequest, "invalid JSON body")
				return
			}
			if snap != nil && !e.PersonalData {
				if res, ok := snap.Widgets[service.DashboardWidgetKey(req.Dir, req.SQL)]; ok && res != nil {
					httputil.WriteJSON(w, http.StatusOK, res)
					return
				}
			}
			httputil.WriteError(w, http.StatusForbidden,
				"this local workspace's hosted dashboards show only the widget results the Mac published (ADR-036)")
			return
		}
		next.ServeHTTP(w, r)
	})
}
