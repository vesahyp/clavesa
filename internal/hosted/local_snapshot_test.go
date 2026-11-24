package hosted

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/service"
)

// TestLocalSnapshotRoutes (ADR-036): a local workspace's dashboards and
// catalog come from the published snapshot; a widget whose dataset is
// not stored is 403, and nothing runs.
func TestLocalSnapshotRoutes(t *testing.T) {
	spec := `{"slug":"traffic","title":"Traffic","datasets":[{"name":"views","dir":"traffic","sql":"SELECT day, n FROM daily"}],"widgets":[]}`
	n := int64(42)
	sum := service.WorkspaceSummary{
		Name: "site-traffic", Kind: "local",
		Drill: &service.SummaryDrill{
			Version:   service.DrillVersion,
			Tree:      map[string]string{"clavesa.json": `{"name":"site-traffic","cloud":"aws","version":1}`, ".clavesa/dashboards/traffic.json": spec},
			Pipelines: map[string]service.PipelineDrill{},
			Snapshot: &service.DrillSnapshot{
				At:        "2026-11-17T04:00:00Z",
				Catalog:   json.RawMessage(`{"tables":[{"database":"c__traffic","name":"daily"}],"aws_available":true}`),
				Snapshots: map[string]*observability.SnapshotsResult{"c__traffic.daily": {LatestRecordCount: &n}},
				Widgets: map[string]*observability.QueryResult{
					service.DashboardWidgetKey("traffic", "SELECT day, n FROM daily"): {Columns: []observability.SampleTableColumn{{Name: "day"}, {Name: "n"}}, Rows: [][]string{{"2026-11-16", "7"}}},
				},
			},
		},
	}
	body, _ := json.Marshal(sum)
	ws, _, err := OpenLocal(context.Background(), filepath.Join(t.TempDir(), "site-traffic"), Entry{Name: "site-traffic", Kind: KindLocal}, body)
	if err != nil {
		t.Fatal(err)
	}
	do := func(method, path, b string) (int, string) {
		w := httptest.NewRecorder()
		ws.Handler.ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(b)))
		return w.Code, w.Body.String()
	}
	if c, b := do(http.MethodGet, "/workspace/tables", ""); c != 200 || !strings.Contains(b, `"daily"`) {
		t.Errorf("GET /workspace/tables: %d %s", c, b)
	}
	if c, b := do(http.MethodPost, "/dashboards/query", `{"dir":"traffic","sql":"  SELECT day, n FROM daily\n"}`); c != 200 || !strings.Contains(b, "2026-11-16") {
		t.Errorf("stored widget: %d %s", c, b)
	}
	if c, _ := do(http.MethodPost, "/dashboards/query", `{"dir":"traffic","sql":"SELECT * FROM raw"}`); c != http.StatusForbidden {
		t.Errorf("other SQL: %d, want 403", c)
	}
	if c, b := do(http.MethodGet, "/dashboards", ""); c != 200 || !strings.Contains(b, `"traffic"`) {
		t.Errorf("GET /dashboards: %d %s", c, b)
	}
	if c, b := do(http.MethodGet, "/data/tables/c__traffic/daily/snapshots?limit=20", ""); c != 200 || !strings.Contains(b, "42") {
		t.Errorf("snapshots: %d %s", c, b)
	}
	if c, _ := do(http.MethodGet, "/data/tables/c__traffic/other/snapshots", ""); c == 200 {
		t.Errorf("snapshots of a table not kept: 200")
	}
	if c, b := do(http.MethodGet, "/workspace", ""); c != 200 || !strings.Contains(b, `"snapshot_at":"2026-11-17T04:00:00Z"`) {
		t.Errorf("GET /workspace: %d %s", c, b)
	}

	// A drill with no snapshot: the catalog is not kept, widgets are 403.
	sum.Drill.Snapshot = nil
	body, _ = json.Marshal(sum)
	ws, _, err = OpenLocal(context.Background(), filepath.Join(t.TempDir(), "site-traffic"), Entry{Name: "site-traffic", Kind: KindLocal}, body)
	if err != nil {
		t.Fatal(err)
	}
	if c, b := do(http.MethodGet, "/workspace/tables", ""); c != 404 || !strings.Contains(b, "not kept") {
		t.Errorf("no snapshot, GET /workspace/tables: %d %s", c, b)
	}
	if c, _ := do(http.MethodPost, "/dashboards/query", `{"dir":"traffic","sql":"SELECT day, n FROM daily"}`); c != http.StatusForbidden {
		t.Errorf("no snapshot, widget: %d, want 403", c)
	}
}

// A local workspace whose snapshot holds no widget result lists no
// dashboards, and a link to one (its spec or a widget query) answers 403
// with the notice, so no widget fails.
func TestLocalSnapshotHidesUnpublishedDashboards(t *testing.T) {
	spec := `{"slug":"traffic","title":"Traffic","datasets":[],"widgets":[]}`
	sum := service.WorkspaceSummary{
		Name: "site-traffic", Kind: "local",
		Drill: &service.SummaryDrill{
			Version:   service.DrillVersion,
			Tree:      map[string]string{"clavesa.json": `{"name":"site-traffic","cloud":"aws","version":1}`, ".clavesa/dashboards/traffic.json": spec},
			Pipelines: map[string]service.PipelineDrill{},
			Snapshot:  &service.DrillSnapshot{At: "2026-11-17T04:00:00Z"},
		},
	}
	body, _ := json.Marshal(sum)
	ws, _, err := OpenLocal(context.Background(), filepath.Join(t.TempDir(), "site-traffic"), Entry{Name: "site-traffic", Kind: KindLocal}, body)
	if err != nil {
		t.Fatal(err)
	}
	do := func(method, path, b string) (int, string) {
		w := httptest.NewRecorder()
		ws.Handler.ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(b)))
		return w.Code, w.Body.String()
	}
	if c, b := do(http.MethodGet, "/dashboards", ""); c != 200 || strings.Contains(b, "traffic") || !strings.Contains(b, `"dashboards":[]`) {
		t.Errorf("GET /dashboards: %d %s, want an empty list", c, b)
	}
	for _, req := range [][3]string{{http.MethodGet, "/dashboards/traffic", ""}, {http.MethodPost, "/dashboards/query", `{"dir":"traffic","sql":"SELECT 1"}`}} {
		if c, b := do(req[0], req[1], req[2]); c != http.StatusForbidden || !strings.Contains(b, "not published from the Mac") {
			t.Errorf("%s %s: %d %s, want 403 with the notice", req[0], req[1], c, b)
		}
	}
}
