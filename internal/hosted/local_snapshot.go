package hosted

import (
	"encoding/json"
	"net/http"
	"strings"

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
// A workspace whose snapshot holds no widget result (no snapshot, or a
// Mac that published none) lists no dashboards, and a link to one answers
// 403 with noPublishedDashboards, which the dashboard page shows as a
// notice: every widget would fail otherwise (2026-11-19).

// noPublishedDashboards is what a link to a dashboard answers when the
// Mac published no widget result of the workspace.
const noPublishedDashboards = "This dashboard's values are not published from the Mac; it opens in the local clavesa UI"

func snapshotRoutes(e Entry, snap *service.DrillSnapshot, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		published := snap != nil && len(snap.Widgets) > 0
		switch {
		case !published && r.URL.Path == "/dashboards" && (r.Method == http.MethodGet || r.Method == http.MethodHead):
			httputil.WriteJSON(w, http.StatusOK, map[string][]struct{}{"dashboards": {}})
			return
		case !published && strings.HasPrefix(r.URL.Path, "/dashboards/"):
			httputil.WriteError(w, http.StatusForbidden, noPublishedDashboards)
			return
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
			if snap != nil {
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
