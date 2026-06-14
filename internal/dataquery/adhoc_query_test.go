package dataquery_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"

	"github.com/vesahyp/clavesa/internal/dataquery"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/service"
)

// adhocAthenaMock returns a mock whose query result is one bigint column
// `n` with a single row "42".
func adhocAthenaMock() *mockAthenaClient {
	queryID := "adhoc-q-1"
	return &mockAthenaClient{
		startOutput: &athena.StartQueryExecutionOutput{QueryExecutionId: &queryID},
		getExecOutput: &athena.GetQueryExecutionOutput{
			QueryExecution: &athenatypes.QueryExecution{
				QueryExecutionId: &queryID,
				Status: &athenatypes.QueryExecutionStatus{
					State: athenatypes.QueryExecutionStateSucceeded,
				},
			},
		},
		getResultsOutput: &athena.GetQueryResultsOutput{
			ResultSet: &athenatypes.ResultSet{
				ResultSetMetadata: &athenatypes.ResultSetMetadata{
					ColumnInfo: []athenatypes.ColumnInfo{
						{Name: aws.String("n"), Type: aws.String("bigint")},
					},
				},
				Rows: []athenatypes.Row{
					runsRow("n"), // header — provider strips it
					runsRow("42"),
				},
			},
		},
	}
}

func postAdhocQuery(t *testing.T, h http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/data/query", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

// TestAdhocQueryServiceSeamShapeIdentical pins the ADR-014 contract for
// the WithQueryService refactor: the HTTP body served through the
// injected service seam is byte-identical to the body the legacy
// direct-provider path serves for the same provider result.
func TestAdhocQueryServiceSeamShapeIdentical(t *testing.T) {
	// Legacy path: no resolver, no QueryFunc → the built-in cloud
	// provider over the Athena mock.
	legacy := dataquery.NewHandler(&mockS3Client{}, adhocAthenaMock(), "out")
	wLegacy := postAdhocQuery(t, legacy, `{"sql":"SELECT count(*) AS n FROM t"}`)
	if wLegacy.Code != http.StatusOK {
		t.Fatalf("legacy path: expected 200, got %d (body: %s)", wLegacy.Code, wLegacy.Body.String())
	}

	// Service path: QueryFunc returns the provider-shape result the cloud
	// provider derives from that same Athena response — including the
	// Served stamp the cloud provider puts on everything it executes.
	var gotSQL, gotDir string
	var gotMaxRows int
	seam := dataquery.NewHandler(&mockS3Client{}, &mockAthenaClient{}, "out").(*dataquery.Handler).
		WithQueryService(func(_ context.Context, sql, dir string, maxRows int) (*observability.QueryResult, error) {
			gotSQL, gotDir, gotMaxRows = sql, dir, maxRows
			return &observability.QueryResult{
				Columns:  []observability.SampleTableColumn{{Name: "n", Type: "bigint"}},
				Rows:     [][]string{{"42"}},
				RowCount: 1,
				Served:   &observability.Served{Engine: "athena", Warehouse: "cloud"},
			}, nil
		})
	wSeam := postAdhocQuery(t, seam, `{"sql":"SELECT count(*) AS n FROM t"}`)
	if wSeam.Code != http.StatusOK {
		t.Fatalf("seam path: expected 200, got %d (body: %s)", wSeam.Code, wSeam.Body.String())
	}

	if !bytes.Equal(wLegacy.Body.Bytes(), wSeam.Body.Bytes()) {
		t.Errorf("response bodies differ between legacy and service-seam paths (ADR-014):\nlegacy: %s\nseam:   %s",
			wLegacy.Body.String(), wSeam.Body.String())
	}

	if gotSQL != "SELECT count(*) AS n FROM t" {
		t.Errorf("seam received sql %q", gotSQL)
	}
	if gotDir != "" {
		t.Errorf("seam received dir %q, want empty (no ?dir= on the request)", gotDir)
	}
	if gotMaxRows != 1000 {
		t.Errorf("seam received maxRows %d, want the route cap 1000", gotMaxRows)
	}

	// The legacy body itself is the long-standing wire shape plus the
	// ADR-024 `served` stamp — pin it so neither path can drift it
	// silently. The legacy fallback never transpiles, so `transpiled` is
	// honestly absent (omitempty); engine + warehouse come from the cloud
	// provider that executed the query.
	const want = `{"columns":[{"name":"n","type":"bigint","nullable":true}],"rows":[["42"]],"row_count":1,"truncated":false,"served":{"engine":"athena","warehouse":"cloud"}}`
	if got := strings.TrimSpace(wLegacy.Body.String()); got != want {
		t.Errorf("wire shape drifted:\ngot:  %s\nwant: %s", got, want)
	}
}

// TestAdhocQueryServedTranspiled pins the transpiled variant of the wire
// shape: when the service seam transpiled the SparkSQL for a cloud
// warehouse it sets Served.Transpiled, and the HTTP body carries it.
func TestAdhocQueryServedTranspiled(t *testing.T) {
	h := dataquery.NewHandler(&mockS3Client{}, &mockAthenaClient{}, "out").(*dataquery.Handler).
		WithQueryService(func(context.Context, string, string, int) (*observability.QueryResult, error) {
			return &observability.QueryResult{
				Columns:  []observability.SampleTableColumn{{Name: "n", Type: "bigint"}},
				Rows:     [][]string{{"42"}},
				RowCount: 1,
				Served:   &observability.Served{Engine: "athena", Warehouse: "cloud", Transpiled: true},
			}, nil
		})
	w := postAdhocQuery(t, h, `{"sql":"SELECT count(*) AS n FROM t"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}
	const want = `"served":{"engine":"athena","warehouse":"cloud","transpiled":true}`
	if !strings.Contains(w.Body.String(), want) {
		t.Errorf("body missing transpiled served stamp:\ngot:  %s\nwant substring: %s", w.Body.String(), want)
	}
}

// TestAdhocQueryDialectRejectionIs400 pins the error-status unification
// with the dashboards query route: a SparkSQL→Trino dialect rejection
// from the service seam is the author's problem (400), not a 500. The
// concrete error is *service.DialectError, matched in the handler via
// its DialectRejection marker (dataquery cannot import internal/service —
// import cycle).
func TestAdhocQueryDialectRejectionIs400(t *testing.T) {
	h := dataquery.NewHandler(&mockS3Client{}, &mockAthenaClient{}, "out").(*dataquery.Handler).
		WithQueryService(func(context.Context, string, string, int) (*observability.QueryResult, error) {
			return nil, fmt.Errorf("query: %w", &service.DialectError{Message: "cannot transpile FOO()", Line: 1, Col: 8})
		})

	w := postAdhocQuery(t, h, `{"sql":"SELECT FOO()"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("dialect rejection: expected 400, got %d (body: %s)", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "cannot transpile FOO()") {
		t.Errorf("body should carry the rejection message, got: %s", w.Body.String())
	}

	// Any other seam failure stays a 500 — only dialect rejections demote.
	h500 := dataquery.NewHandler(&mockS3Client{}, &mockAthenaClient{}, "out").(*dataquery.Handler).
		WithQueryService(func(context.Context, string, string, int) (*observability.QueryResult, error) {
			return nil, fmt.Errorf("athena: throttled")
		})
	if w := postAdhocQuery(t, h500, `{"sql":"SELECT 1"}`); w.Code != http.StatusInternalServerError {
		t.Errorf("non-dialect failure: expected 500, got %d", w.Code)
	}
}

// TestAdhocQueryValidation pins the 400 cases shared by both paths.
func TestAdhocQueryValidation(t *testing.T) {
	h := dataquery.NewHandler(&mockS3Client{}, &mockAthenaClient{}, "out").(*dataquery.Handler).
		WithQueryService(func(context.Context, string, string, int) (*observability.QueryResult, error) {
			t.Fatal("QueryFunc must not run for an invalid body")
			return nil, nil
		})

	if w := postAdhocQuery(t, h, `{`); w.Code != http.StatusBadRequest {
		t.Errorf("malformed body: expected 400, got %d", w.Code)
	}
	if w := postAdhocQuery(t, h, `{"sql":"   "}`); w.Code != http.StatusBadRequest {
		t.Errorf("blank sql: expected 400, got %d", w.Code)
	}
}
