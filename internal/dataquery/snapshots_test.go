package dataquery_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vesahyp/clavesa/internal/dataquery"
	"github.com/vesahyp/clavesa/internal/observability"
)

// ADR-018: the snapshot endpoint backend swapped from an Athena
// `<table>$snapshots` query to a direct Delta `_delta_log/` read over
// Glue + S3 (see internal/observability/cloud.go). The data-shape
// assertions that exercised the old Athena path now live in
// internal/observability/cloud_test.go where the Glue + S3 stubs sit;
// this file keeps the request-validation tests since those still run
// against the HTTP layer and don't care which backend serves.
//
// The dataquery handler's internal CloudProvider doesn't have Glue/S3
// wired (those flow through the resolver in production), so the
// Snapshots() call from this handler returns empty. We keep one test
// asserting that empty is the response shape, plus the validation
// tests that catch malformed identifiers / limits at the HTTP layer.

func decodeSnapshots(t *testing.T, body []byte) observability.SnapshotsResult {
	t.Helper()
	var r observability.SnapshotsResult
	if err := json.Unmarshal(body, &r); err != nil {
		t.Fatalf("decode SnapshotsResult: %v (body: %s)", err, body)
	}
	return r
}

// TestSnapshotsHandlerEmptyWhenNoBackend — the dataquery handler's
// internal CloudProvider is built without Glue/S3 (those flow through
// the resolver in production). Snapshots therefore returns an empty
// result rather than a 500 — the same fail-soft contract as
// `undeployed()`. Data-shape coverage lives in
// internal/observability/cloud_test.go.
func TestSnapshotsHandlerEmptyWhenNoBackend(t *testing.T) {
	h := dataquery.NewHandler(&mockS3Client{}, &mockAthenaClient{}, "out")

	req := httptest.NewRequest(http.MethodGet, "/data/tables/db/tbl/snapshots", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}
	r := decodeSnapshots(t, w.Body.Bytes())
	if len(r.Snapshots) != 0 {
		t.Errorf("expected empty snapshots (no Glue/S3 wired), got %d", len(r.Snapshots))
	}
	if r.Truncated {
		t.Error("expected truncated=false for empty result")
	}
}

func TestSnapshotsInvalidIdentifier(t *testing.T) {
	h := dataquery.NewHandler(&mockS3Client{}, &mockAthenaClient{}, "out")

	cases := []string{
		"/data/tables/db.with.dot/tbl/snapshots",
		"/data/tables/db/tbl-with-dash/snapshots",
		"/data/tables/db/1starts_with_digit/snapshots",
	}
	for _, u := range cases {
		req := httptest.NewRequest(http.MethodGet, u, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Errorf("url %s: expected 400, got %d", u, w.Code)
		}
	}
}

// (TestSnapshotsAthenaFailure removed in ADR-018 sub-slice — the
// Athena path it exercised no longer exists. Backend-failure surfacing
// lives in internal/observability/cloud_test.go alongside the new
// Glue/S3 path.)

func TestSnapshotsLimitValidation(t *testing.T) {
	h := dataquery.NewHandler(&mockS3Client{}, &mockAthenaClient{}, "out")
	cases := []string{
		"/data/tables/db/tbl/snapshots?limit=0",
		"/data/tables/db/tbl/snapshots?limit=-1",
		"/data/tables/db/tbl/snapshots?limit=201",
		"/data/tables/db/tbl/snapshots?limit=abc",
	}
	for _, u := range cases {
		req := httptest.NewRequest(http.MethodGet, u, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Errorf("url %s: expected 400, got %d", u, w.Code)
		}
	}
}
