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

func runsRow(values ...string) athenatypes.Row {
	d := func(s string) athenatypes.Datum {
		if s == "" {
			return athenatypes.Datum{}
		}
		return athenatypes.Datum{VarCharValue: aws.String(s)}
	}
	out := athenatypes.Row{}
	for _, v := range values {
		out.Data = append(out.Data, d(v))
	}
	return out
}

func runsHeader() athenatypes.Row {
	return runsRow(
		"run_id", "pipeline", "sf_execution_arn", "status", "trigger",
		"started_at", "ended_at", "duration_ms",
		"failed_step", "error_class", "error_msg",
	)
}

func decodeRuns(t *testing.T, body []byte) observability.RunsResult {
	t.Helper()
	var r observability.RunsResult
	if err := json.Unmarshal(body, &r); err != nil {
		t.Fatalf("decode RunsResult: %v (body: %s)", err, body)
	}
	return r
}

func TestRunsBasic(t *testing.T) {
	queryID := "runs-q-1"
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
				Rows: []athenatypes.Row{
					runsHeader(),
					runsRow(
						"exec-abc", "my_pipeline",
						"arn:aws:states:eu-north-1:1:execution:clavesa-my_pipeline:exec-abc",
						"SUCCEEDED", "",
						"2026-05-07T12:30:00.000Z", "2026-05-07T12:30:05.000Z",
						"5000", "", "", "",
					),
					runsRow(
						"exec-def", "my_pipeline",
						"arn:aws:states:eu-north-1:1:execution:clavesa-my_pipeline:exec-def",
						"FAILED", "",
						"2026-05-07T12:25:00.000Z", "2026-05-07T12:25:01.000Z",
						"1000", "", "States.TaskFailed", "Lambda function not found",
					),
				},
			},
		},
	}
	h := dataquery.NewHandler(&mockS3Client{}, athc, "out")

	req := httptest.NewRequest(http.MethodGet, "/data/runs?pipeline=my_pipeline", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}
	r := decodeRuns(t, w.Body.Bytes())

	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
	if r.Rows[0].RunID != "exec-abc" || r.Rows[0].Status != "SUCCEEDED" {
		t.Errorf("rows[0] unexpected: %+v", r.Rows[0])
	}
	if r.Rows[0].DurationMs == nil || *r.Rows[0].DurationMs != 5000 {
		t.Errorf("rows[0].DurationMs = %v, want 5000", r.Rows[0].DurationMs)
	}
	if r.Rows[1].Status != "FAILED" || r.Rows[1].ErrorClass != "States.TaskFailed" {
		t.Errorf("rows[1] unexpected: %+v", r.Rows[1])
	}
	if r.Truncated {
		t.Error("expected truncated=false")
	}

	gotSQL := aws.ToString(athc.lastStartInput.QueryString)
	if !strings.Contains(gotSQL, `"clavesa_my_pipeline"."runs"`) {
		t.Errorf("SQL did not target the right table: %s", gotSQL)
	}
}

func TestRunsValidation(t *testing.T) {
	h := dataquery.NewHandler(&mockS3Client{}, &mockAthenaClient{}, "out")
	cases := []struct{ url string }{
		{"/data/runs"},                              // missing pipeline
		{"/data/runs?pipeline="},                    // empty pipeline
		{"/data/runs?pipeline=1bad"},                // leading digit not allowed
		{"/data/runs?pipeline=bad+space"},           // non-ident charset
		{"/data/runs?pipeline=p&limit=0"},           // limit=0
		{"/data/runs?pipeline=p&limit=501"},         // limit>max
		{"/data/runs?pipeline=p&limit=abc"},         // limit non-numeric
	}
	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodGet, tc.url, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Errorf("url %s: expected 400, got %d", tc.url, w.Code)
		}
	}
}

func TestRunsTruncated(t *testing.T) {
	queryID := "runs-q-trunc"
	rows := []athenatypes.Row{runsHeader()}
	for i := 0; i < 6; i++ {
		rows = append(rows, runsRow(
			"e", "p", "arn", "SUCCEEDED", "",
			"2026-05-07T12:00:00.000Z", "2026-05-07T12:00:01.000Z", "1000", "", "", "",
		))
	}
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
			ResultSet: &athenatypes.ResultSet{Rows: rows},
		},
	}
	h := dataquery.NewHandler(&mockS3Client{}, athc, "out")

	req := httptest.NewRequest(http.MethodGet, "/data/runs?pipeline=p&limit=4", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	r := decodeRuns(t, w.Body.Bytes())
	if len(r.Rows) != 4 {
		t.Errorf("expected 4 rows, got %d", len(r.Rows))
	}
	if !r.Truncated {
		t.Error("expected truncated=true")
	}
}
