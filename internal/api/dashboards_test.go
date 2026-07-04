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
	"github.com/vesahyp/clavesa/internal/service"
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

// fakeQuerySeam records what the handler hands the shared ad-hoc query
// seam (service.Service.Query in production) and plays back a canned
// result or error.
type fakeQuerySeam struct {
	gotSQL     string
	gotDir     string
	gotMaxRows int
	result     *observability.QueryResult
	err        error
}

func (f *fakeQuerySeam) fn(_ context.Context, sql, dir string, maxRows int) (*observability.QueryResult, error) {
	f.gotSQL = sql
	f.gotDir = dir
	f.gotMaxRows = maxRows
	if f.err != nil {
		return nil, f.err
	}
	return f.result, nil
}

func newDashboardsMux(t *testing.T, store api.DashboardStore, seam *fakeQuerySeam) *http.ServeMux {
	t.Helper()
	h := api.NewDashboardsHandler(store)
	if seam != nil {
		h = h.WithQueryService(seam.fn)
	}
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
	mux := newDashboardsMux(t, newFakeStore(), nil)
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
	mux := newDashboardsMux(t, store, nil)

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
	mux := newDashboardsMux(t, newFakeStore(), nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/dashboards/ghost", nil))
	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rr.Code)
	}
}

// TestDashboardsCreate creates a dashboard, then asserts a second create on
// the same slug is rejected with 409 rather than silently clobbering.
func TestDashboardsCreate(t *testing.T) {
	mux := newDashboardsMux(t, newFakeStore(), nil)

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
	mux := newDashboardsMux(t, store, nil)

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
	mux := newDashboardsMux(t, store, nil)

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
// shared query seam with the SQL + dir from the request body and the
// route's row cap. Dialect policy / transpile behavior lives inside the
// seam (service.Query) and is pinned by internal/service/query_test.go.
func TestDashboardsQueryDispatch(t *testing.T) {
	seam := &fakeQuerySeam{
		result: &observability.QueryResult{
			Columns: []observability.SampleTableColumn{{Name: "n"}},
			Rows:    [][]string{{"42"}},
		},
	}
	mux := newDashboardsMux(t, newFakeStore(), seam)

	body := `{"dir":"demo","sql":"SELECT 42 AS n"}`
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/dashboards/query", bytes.NewReader([]byte(body))))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", rr.Code, rr.Body.String())
	}
	if seam.gotSQL != "SELECT 42 AS n" {
		t.Errorf("seam got SQL = %q", seam.gotSQL)
	}
	if seam.gotDir != "demo" {
		t.Errorf("seam got dir = %q, want demo", seam.gotDir)
	}
	if seam.gotMaxRows <= 0 {
		t.Errorf("seam got maxRows = %d, want the route's positive row cap", seam.gotMaxRows)
	}
}

// TestDashboardsQueryExpandsPlaceholders confirms `{{control}}` tokens are
// substituted before the SQL reaches the seam — placeholder expansion is
// the one query concern the handler still owns.
func TestDashboardsQueryExpandsPlaceholders(t *testing.T) {
	seam := &fakeQuerySeam{result: &observability.QueryResult{}}
	mux := newDashboardsMux(t, newFakeStore(), seam)

	body := `{"dir":"demo","sql":"SELECT * FROM t WHERE env = {{env}}","params":{"env":"prod"}}`
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/dashboards/query", bytes.NewReader([]byte(body))))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body: %s", rr.Code, rr.Body.String())
	}
	if want := "SELECT * FROM t WHERE env = 'prod'"; seam.gotSQL != want {
		t.Errorf("seam got SQL = %q, want %q", seam.gotSQL, want)
	}
}

// TestDashboardsQueryDialectRejection maps a *service.DialectError from the
// seam to 400 — a non-portable SparkSQL widget query is the author's
// problem (ADR-023), surfaced inline in the editor, not a server fault.
func TestDashboardsQueryDialectRejection(t *testing.T) {
	seam := &fakeQuerySeam{err: &service.DialectError{Message: "PIVOT has no Trino equivalent", Line: 1, Col: 8}}
	mux := newDashboardsMux(t, newFakeStore(), seam)

	body := `{"dir":"demo","sql":"SELECT 1"}`
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/dashboards/query", bytes.NewReader([]byte(body))))

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body: %s)", rr.Code, rr.Body.String())
	}
}

// TestDashboardsQueryOpaqueSeamError keeps non-dialect seam failures at 500.
func TestDashboardsQueryOpaqueSeamError(t *testing.T) {
	seam := &fakeQuerySeam{err: fmt.Errorf("provider exploded")}
	mux := newDashboardsMux(t, newFakeStore(), seam)

	body := `{"dir":"demo","sql":"SELECT 1"}`
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/dashboards/query", bytes.NewReader([]byte(body))))

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (body: %s)", rr.Code, rr.Body.String())
	}
}

// TestDashboardsQueryUnwired answers 500 when no query seam is injected —
// production wiring (cli/ui.go) always provides one.
func TestDashboardsQueryUnwired(t *testing.T) {
	mux := newDashboardsMux(t, newFakeStore(), nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/dashboards/query", bytes.NewReader([]byte(`{"dir":"demo","sql":"SELECT 1"}`))))
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rr.Code)
	}
}

// TestDashboardsQueryEmptySQL rejects at 400.
func TestDashboardsQueryEmptySQL(t *testing.T) {
	mux := newDashboardsMux(t, newFakeStore(), nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/dashboards/query", bytes.NewReader([]byte(`{"dir":"demo","sql":""}`))))
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

// TestDashboardsQueryEmptyDir rejects at 400 — without a dir we don't know
// which Provider serves the request (ADR-014 symmetric reject).
func TestDashboardsQueryEmptyDir(t *testing.T) {
	mux := newDashboardsMux(t, newFakeStore(), nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/dashboards/query", bytes.NewReader([]byte(`{"dir":"","sql":"SELECT 1"}`))))
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}
