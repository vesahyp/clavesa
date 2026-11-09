package dataquery_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/vesahyp/clavesa/internal/dataquery"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/runtiming"
	"github.com/vesahyp/clavesa/internal/service"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// TestRunTimingsHandler drives GET /data/run-timings against a temp local
// workspace through the real service seam, the way cli/ui.go wires it.
func TestRunTimingsHandler(t *testing.T) {
	t.Setenv("CLAVESA_RUN_HISTORY_DIR", t.TempDir())
	ws := t.TempDir()
	now := time.Now()
	for i, r := range []runtiming.Record{
		{Schema: 1, RunID: "run-1", Pipeline: "traffic", Status: "SUCCEEDED", StartedAt: now.Add(-3 * time.Hour).Format(time.RFC3339), TotalMs: 9000,
			Nodes: []runtiming.Node{{Node: "clean", Timing: json.RawMessage(`{"phases":{"write":4000}}`)}}},
		{Schema: 1, RunID: "run-2", Pipeline: "traffic", Status: "SUCCEEDED", StartedAt: now.Add(-1 * time.Hour).Format(time.RFC3339), TotalMs: 8000},
		{Schema: 1, RunID: "run-x", Pipeline: "other", Status: "SUCCEEDED", StartedAt: now.Format(time.RFC3339)},
	} {
		rec := r
		path := filepath.Join(workspace.LocalWarehouseDir(ws), "_progress", rec.RunID, runtiming.FileName)
		if err := runtiming.Write(&rec, path); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	svc := service.New(ws)
	h := dataquery.NewHandler(&mockS3Client{}, nil, "").(*dataquery.Handler).
		WithResolver(observability.NewResolver(ws, nil, observability.NewLocalProvider(ws))).
		WithRunTimingsService(func(ctx context.Context, dir string, limit int, since time.Duration) ([]runtiming.Record, error) {
			return svc.RunTimings(ctx, dir, service.RunTimingsQuery{Limit: limit, Since: since})
		})

	get := func(url string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, url, nil))
		return w
	}

	w := get("/data/run-timings?dir=traffic")
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	var hist runtiming.History
	if err := json.Unmarshal(w.Body.Bytes(), &hist); err != nil {
		t.Fatalf("decode: %v (%s)", err, w.Body.String())
	}
	if hist.GeneratedAt == "" || hist.HistoryDir != "" {
		t.Errorf("generated_at %q, history_dir %q", hist.GeneratedAt, hist.HistoryDir)
	}
	if len(hist.Runs) != 2 || hist.Runs[0].RunID != "run-1" || hist.Runs[1].RunID != "run-2" {
		t.Fatalf("runs = %+v", hist.Runs)
	}
	if string(hist.Runs[0].Nodes[0].Timing) != `{"phases":{"write":4000}}` {
		t.Errorf("node timing = %s", hist.Runs[0].Nodes[0].Timing)
	}

	w = get("/data/run-timings?dir=traffic&limit=1&since=7d")
	_ = json.Unmarshal(w.Body.Bytes(), &hist)
	if w.Code != http.StatusOK || len(hist.Runs) != 1 || hist.Runs[0].RunID != "run-2" {
		t.Errorf("limit=1: %d %s", w.Code, w.Body.String())
	}

	for _, bad := range []string{
		"/data/run-timings",
		"/data/run-timings?dir=traffic&limit=-1",
		"/data/run-timings?dir=traffic&limit=x",
		"/data/run-timings?dir=traffic&since=soon",
	} {
		if w := get(bad); w.Code != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", bad, w.Code)
		}
	}
}
