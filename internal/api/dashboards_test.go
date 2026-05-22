package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"testing"

	"github.com/vesahyp/clavesa/internal/api"
	"github.com/vesahyp/clavesa/internal/observability"
)

// fakeStore is an in-memory api.DashboardStore for the handler tests.
type fakeStore struct {
	dashboards map[string]api.Dashboard
}

func newFakeStore() *fakeStore { return &fakeStore{dashboards: map[string]api.Dashboard{}} }

func (s *fakeStore) ListDashboards(context.Context) ([]api.DashboardSummary, error) {
	out := make([]api.DashboardSummary, 0, len(s.dashboards))
	for _, d := range s.dashboards {
		out = append(out, api.DashboardSummary{Slug: d.Slug, Title: d.Title})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Slug < out[j].Slug })
	return out, nil
}

func (s *fakeStore) GetDashboard(_ context.Context, slug string) (api.Dashboard, error) {
	d, ok := s.dashboards[slug]
	if !ok {
		return api.Dashboard{}, fmt.Errorf("dashboard %q: %w", slug, os.ErrNotExist)
	}
	return d, nil
}

func (s *fakeStore) SaveDashboard(_ context.Context, d api.Dashboard) (api.Dashboard, error) {
	if d.Slug == "" {
		return api.Dashboard{}, fmt.Errorf("invalid dashboard slug")
	}
	s.dashboards[d.Slug] = d
	return d, nil
}

func (s *fakeStore) DeleteDashboard(_ context.Context, slug string) error {
	if _, ok := s.dashboards[slug]; !ok {
		return fmt.Errorf("dashboard %q: %w", slug, os.ErrNotExist)
	}
	delete(s.dashboards, slug)
	return nil
}

// fakeQueryProvider is enough Provider surface to exercise the dashboards
// query route. Other Provider methods aren't reachable from the dashboards
// handler so they panic — failing loud beats silently returning empty.
type fakeQueryProvider struct {
	gotSQL string
	gotDir string
	result *observability.QueryResult
	err    error
}

func (f *fakeQueryProvider) Query(_ context.Context, q observability.QueryQuery) (*observability.QueryResult, error) {
	f.gotSQL = q.SQL
	f.gotDir = q.PipelineDir
	if f.err != nil {
		return nil, f.err
	}
	return f.result, nil
}

func (f *fakeQueryProvider) NodeRuns(context.Context, observability.NodeRunsQuery) (*observability.NodeRunsResult, error) {
	panic("unused")
}
func (f *fakeQueryProvider) Runs(context.Context, observability.RunsQuery) (*observability.RunsResult, error) {
	panic("unused")
}
func (f *fakeQueryProvider) Tables(context.Context, observability.TablesQuery) (*observability.TablesResult, error) {
	panic("unused")
}
func (f *fakeQueryProvider) Snapshots(context.Context, observability.SnapshotsQuery) (*observability.SnapshotsResult, error) {
	panic("unused")
}
func (f *fakeQueryProvider) ColumnStats(context.Context, observability.ColumnStatsQuery) (*observability.ColumnStatsResult, error) {
	panic("unused")
}
func (f *fakeQueryProvider) SampleTable(context.Context, observability.SampleTableQuery) (*observability.SampleTableResult, error) {
	panic("unused")
}
func (f *fakeQueryProvider) ExecutionStates(context.Context, observability.ExecutionStatesQuery) (*observability.ExecutionStatesResult, error) {
	panic("unused")
}
func (f *fakeQueryProvider) ExecutionLogs(context.Context, observability.ExecutionLogsQuery) (*observability.ExecutionLogsResult, error) {
	panic("unused")
}
func (f *fakeQueryProvider) Exec(context.Context, observability.ExecQuery) error {
	panic("unused")
}

func newDashboardsMux(t *testing.T, store api.DashboardStore, p observability.Provider) *http.ServeMux {
	t.Helper()
	h := api.NewDashboardsHandler(store, p)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	return mux
}

func sampleDashboard(slug string) api.Dashboard {
	return api.Dashboard{
		Slug:     slug,
		Title:    "Pipeline runs",
		Datasets: []api.DashboardDataset{{Name: "runs", Dir: "demo", SQL: "SELECT 1 AS n"}},
		Widgets: []api.DashboardWidget{{
			ID: "w1", Type: "big_number", Title: "Failures", Dataset: "runs",
			ValueField: "n", Layout: api.DashboardWidgetLayout{W: 3, H: 2},
		}},
	}
}

// TestDashboardsListEmpty surfaces the first-run case — no dashboards yet.
// The list endpoint should 200 with an empty array, not 404.
func TestDashboardsListEmpty(t *testing.T) {
	mux := newDashboardsMux(t, newFakeStore(), &fakeQueryProvider{})
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/dashboards", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", rr.Code, rr.Body.String())
	}
	var got api.DashboardsListResponse
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Dashboards) != 0 {
		t.Errorf("expected 0 dashboards, got %d", len(got.Dashboards))
	}
}

// TestDashboardsListAndGet stores two dashboards, asserts list returns them
// sorted by slug and detail returns the full datasets-shaped spec.
func TestDashboardsListAndGet(t *testing.T) {
	store := newFakeStore()
	store.dashboards["pipeline-runs"] = sampleDashboard("pipeline-runs")
	store.dashboards["sessions"] = sampleDashboard("sessions")
	mux := newDashboardsMux(t, store, &fakeQueryProvider{})

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/dashboards", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("list status = %d (body: %s)", rr.Code, rr.Body.String())
	}
	var list api.DashboardsListResponse
	if err := json.NewDecoder(rr.Body).Decode(&list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(list.Dashboards) != 2 || list.Dashboards[0].Slug != "pipeline-runs" {
		t.Fatalf("want 2 dashboards sorted by slug, got %+v", list.Dashboards)
	}

	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/dashboards/pipeline-runs", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("get status = %d (body: %s)", rr.Code, rr.Body.String())
	}
	var d api.Dashboard
	if err := json.NewDecoder(rr.Body).Decode(&d); err != nil {
		t.Fatalf("decode get: %v", err)
	}
	if d.Slug != "pipeline-runs" || len(d.Datasets) != 1 || len(d.Widgets) != 1 {
		t.Errorf("unexpected dashboard: %+v", d)
	}
}

// TestDashboardsGetMissing returns 404, not 500 — the UI differentiates
// "the slug you bookmarked is gone" from "server is broken."
func TestDashboardsGetMissing(t *testing.T) {
	mux := newDashboardsMux(t, newFakeStore(), &fakeQueryProvider{})
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/dashboards/ghost", nil))
	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rr.Code)
	}
}

// TestDashboardsCreate creates a dashboard, then asserts a second create on
// the same slug is rejected with 409 rather than silently clobbering.
func TestDashboardsCreate(t *testing.T) {
	mux := newDashboardsMux(t, newFakeStore(), &fakeQueryProvider{})

	body, _ := json.Marshal(sampleDashboard("revenue"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/dashboards", bytes.NewReader(body)))
	if rr.Code != http.StatusOK {
		t.Fatalf("create status = %d (body: %s)", rr.Code, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/dashboards", bytes.NewReader(body)))
	if rr.Code != http.StatusConflict {
		t.Errorf("duplicate create status = %d, want 409", rr.Code)
	}
}

// TestDashboardsPutAndDelete exercises create-or-replace via PUT and
// removal via DELETE, including the 404 on deleting an unknown slug.
func TestDashboardsPutAndDelete(t *testing.T) {
	store := newFakeStore()
	mux := newDashboardsMux(t, store, &fakeQueryProvider{})

	body, _ := json.Marshal(sampleDashboard("revenue"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPut, "/dashboards/revenue", bytes.NewReader(body)))
	if rr.Code != http.StatusOK {
		t.Fatalf("put status = %d (body: %s)", rr.Code, rr.Body.String())
	}
	if _, ok := store.dashboards["revenue"]; !ok {
		t.Fatal("PUT did not store the dashboard")
	}

	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodDelete, "/dashboards/revenue", nil))
	if rr.Code != http.StatusNoContent {
		t.Errorf("delete status = %d, want 204", rr.Code)
	}

	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodDelete, "/dashboards/revenue", nil))
	if rr.Code != http.StatusNotFound {
		t.Errorf("delete-missing status = %d, want 404", rr.Code)
	}
}

// TestDashboardsPutSlugFromPath confirms the path slug wins over a stale
// slug in the request body.
func TestDashboardsPutSlugFromPath(t *testing.T) {
	store := newFakeStore()
	mux := newDashboardsMux(t, store, &fakeQueryProvider{})

	d := sampleDashboard("body-slug")
	body, _ := json.Marshal(d)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPut, "/dashboards/path-slug", bytes.NewReader(body)))
	if rr.Code != http.StatusOK {
		t.Fatalf("put status = %d (body: %s)", rr.Code, rr.Body.String())
	}
	if _, ok := store.dashboards["path-slug"]; !ok {
		t.Errorf("dashboard should be stored under the path slug, got %v", store.dashboards)
	}
}

// TestDashboardsQueryDispatch confirms the POST query route reaches the
// Provider with the SQL + dir from the request body.
func TestDashboardsQueryDispatch(t *testing.T) {
	prov := &fakeQueryProvider{
		result: &observability.QueryResult{
			Columns: []observability.SampleTableColumn{{Name: "n"}},
			Rows:    [][]string{{"42"}},
		},
	}
	mux := newDashboardsMux(t, newFakeStore(), prov)

	body := `{"dir":"demo","sql":"SELECT 42 AS n"}`
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/dashboards/query", bytes.NewReader([]byte(body))))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", rr.Code, rr.Body.String())
	}
	if prov.gotSQL != "SELECT 42 AS n" {
		t.Errorf("provider got SQL = %q", prov.gotSQL)
	}
	if prov.gotDir != "demo" {
		t.Errorf("provider got dir = %q, want demo", prov.gotDir)
	}
}

// TestDashboardsQueryEmptySQL rejects at 400.
func TestDashboardsQueryEmptySQL(t *testing.T) {
	mux := newDashboardsMux(t, newFakeStore(), &fakeQueryProvider{})
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/dashboards/query", bytes.NewReader([]byte(`{"dir":"demo","sql":""}`))))
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

// TestDashboardsQueryEmptyDir rejects at 400 — without a dir we don't know
// which Provider serves the request (ADR-014 symmetric reject).
func TestDashboardsQueryEmptyDir(t *testing.T) {
	mux := newDashboardsMux(t, newFakeStore(), &fakeQueryProvider{})
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/dashboards/query", bytes.NewReader([]byte(`{"dir":"","sql":"SELECT 1"}`))))
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}
