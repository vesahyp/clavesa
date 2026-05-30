package dataquery_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"

	"github.com/vesahyp/clavesa/internal/dataquery"
	"github.com/vesahyp/clavesa/internal/observability"
)

// rightsizeRows builds n metric-bearing node_runs rows for one node, each
// allocated 2048 MB with peak RSS `peak` and no spill — the over-provisioned
// shape, so the recommendation lands below current.
func rightsizeRows(node string, n int, allocMB, peak string) []athenatypes.Row {
	rows := []athenatypes.Row{nodeRunsHeader()}
	for i := 0; i < n; i++ {
		rows = append(rows, nodeRunsRow(
			"r", "my_pipeline", node, "2026-05-06T10:00:00.000Z", "",
			"100", "ok", "lambda", allocMB, "false", "", "", "",
			"sha256:abc123", "v2.3.0", "", "exec-0",
			// peak_rss + 14 aggregates; memory/disk spill (cols 3+4) zero.
			peak, "", "0", "0", "", "", "", "", "", "", "", "", "", "", "",
		))
	}
	return rows
}

func TestRightsizeRecommendsDown(t *testing.T) {
	queryID := "rs-q-1"
	athc := &mockAthenaClient{
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
				Rows: rightsizeRows("filter", 12, "2048", "800"),
			},
		},
	}
	h := dataquery.NewHandler(&mockS3Client{}, athc, "out")

	req := httptest.NewRequest(http.MethodGet, "/data/rightsize?pipeline=my_pipeline", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}
	var body struct {
		Rows []observability.NodeRightsize `json:"rows"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v (body: %s)", err, w.Body.String())
	}
	if len(body.Rows) != 1 {
		t.Fatalf("expected 1 node, got %d", len(body.Rows))
	}
	r := body.Rows[0]
	if r.Node != "filter" {
		t.Errorf("node = %q, want filter", r.Node)
	}
	if r.Confidence != "high" {
		t.Errorf("confidence = %q, want high (12 samples)", r.Confidence)
	}
	if r.CurrentMB == nil || *r.CurrentMB != 2048 {
		t.Errorf("current = %v, want 2048", r.CurrentMB)
	}
	// 800 peak, no spill → 960 MB; below current 2048.
	if r.RecommendedMB == nil || *r.RecommendedMB != 960 {
		t.Errorf("recommended = %v, want 960", r.RecommendedMB)
	}

	// The handler must force the metrics-bearing scan (the same SQL the
	// node-runs route runs); without it the local fast path would omit
	// the metric columns. Cloud always runs SQL, but we assert the query
	// reached Athena targeting the right table.
	gotSQL := aws.ToString(athc.lastStartInput.QueryString)
	if !strings.Contains(gotSQL, "peak_rss_mb") {
		t.Errorf("SQL missing peak_rss_mb column: %s", gotSQL)
	}
}

func TestRightsizeValidation(t *testing.T) {
	h := dataquery.NewHandler(&mockS3Client{}, &mockAthenaClient{}, "out")
	cases := []string{
		"/data/rightsize",                      // missing pipeline
		"/data/rightsize?pipeline=",            // empty pipeline
		"/data/rightsize?pipeline=1bad",        // leading digit
		"/data/rightsize?pipeline=p&last=0",    // last=0
		"/data/rightsize?pipeline=p&last=501",  // last>max
		"/data/rightsize?pipeline=p&last=abc",  // last non-numeric
	}
	for _, url := range cases {
		req := httptest.NewRequest(http.MethodGet, url, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Errorf("url %s: expected 400, got %d", url, w.Code)
		}
	}
}
