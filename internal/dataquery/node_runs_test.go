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

func nodeRunsRow(values ...string) athenatypes.Row {
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

func nodeRunsHeader() athenatypes.Row {
	return nodeRunsRow(
		"run_id", "pipeline", "node", "started_at", "ended_at",
		"duration_ms", "status", "compute_target", "memory_mb",
		"cold_start", "lambda_request_id", "error_class", "error_msg",
		"runner_image_digest", "module_version", "output_rows",
		"sf_execution_arn",
		// Spark-observability columns (appended after sf_execution_arn).
		"peak_rss_mb", "peak_execution_memory_mb", "memory_spilled_bytes",
		"disk_spilled_bytes", "shuffle_read_bytes", "shuffle_write_bytes",
		"input_bytes", "input_records", "num_stages", "num_tasks",
		"num_failed_tasks", "jvm_gc_time_ms", "executor_cpu_time_ms",
		"executor_run_time_ms", "max_task_duration_ms",
	)
}

func decodeNodeRuns(t *testing.T, body []byte) observability.NodeRunsResult {
	t.Helper()
	var r observability.NodeRunsResult
	if err := json.Unmarshal(body, &r); err != nil {
		t.Fatalf("decode NodeRunsResult: %v (body: %s)", err, body)
	}
	return r
}

func TestNodeRunsBasic(t *testing.T) {
	queryID := "nr-q-1"
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
					nodeRunsHeader(),
					nodeRunsRow(
						"run-abc", "my_pipeline", "filter_complete",
						"2026-05-06T10:30:00.000Z", "2026-05-06T10:30:02.500Z",
						"2500", "ok", "lambda", "3008",
						"true", "req-xyz", "", "",
						"sha256:abc123", "v0.13.0", "1234",
						"exec-1",
						// Spark metrics: peak_rss + 14 aggregates.
						"512", "2", "0", "0", "12577", "12577",
						"6133", "14", "9", "124", "0", "1083", "11605",
						"43346", "2462",
					),
					nodeRunsRow(
						"run-def", "my_pipeline", "filter_complete",
						"2026-05-06T10:25:00.000Z", "2026-05-06T10:25:01.000Z",
						"1000", "failed", "lambda", "3008",
						"false", "req-uvw", "AnalysisException", "Table not found",
						"sha256:abc123", "v0.13.0", "",
						"exec-0",
						// Older row without Spark metrics — all null.
						"", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
					),
				},
			},
		},
	}
	h := dataquery.NewHandler(&mockS3Client{}, athc, "out")

	req := httptest.NewRequest(http.MethodGet, "/data/node-runs?pipeline=my_pipeline", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}
	r := decodeNodeRuns(t, w.Body.Bytes())

	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
	if r.Rows[0].RunID != "run-abc" || r.Rows[0].Status != "ok" {
		t.Errorf("rows[0] unexpected: %+v", r.Rows[0])
	}
	if r.Rows[0].DurationMs == nil || *r.Rows[0].DurationMs != 2500 {
		t.Errorf("rows[0].DurationMs = %v, want 2500", r.Rows[0].DurationMs)
	}
	if r.Rows[0].ColdStart == nil || *r.Rows[0].ColdStart != true {
		t.Errorf("rows[0].ColdStart = %v, want true", r.Rows[0].ColdStart)
	}
	// Spark metrics flow through the appended columns.
	if r.Rows[0].PeakRSSMB == nil || *r.Rows[0].PeakRSSMB != 512 {
		t.Errorf("rows[0].PeakRSSMB = %v, want 512", r.Rows[0].PeakRSSMB)
	}
	if r.Rows[0].ShuffleReadBytes == nil || *r.Rows[0].ShuffleReadBytes != 12577 {
		t.Errorf("rows[0].ShuffleReadBytes = %v, want 12577", r.Rows[0].ShuffleReadBytes)
	}
	if r.Rows[0].NumTasks == nil || *r.Rows[0].NumTasks != 124 {
		t.Errorf("rows[0].NumTasks = %v, want 124", r.Rows[0].NumTasks)
	}
	// Null Spark columns on the older row stay nil.
	if r.Rows[1].PeakRSSMB != nil {
		t.Errorf("rows[1].PeakRSSMB = %v, want nil", r.Rows[1].PeakRSSMB)
	}
	if r.Rows[1].Status != "failed" || r.Rows[1].ErrorClass != "AnalysisException" {
		t.Errorf("rows[1] unexpected: %+v", r.Rows[1])
	}
	if r.Rows[1].ColdStart == nil || *r.Rows[1].ColdStart != false {
		t.Errorf("rows[1].ColdStart = %v, want false", r.Rows[1].ColdStart)
	}
	if r.Truncated {
		t.Error("expected truncated=false")
	}

	gotSQL := aws.ToString(athc.lastStartInput.QueryString)
	if !strings.Contains(gotSQL, `"clavesa_my_pipeline"."node_runs"`) {
		t.Errorf("SQL did not target the right table: %s", gotSQL)
	}
	if !strings.Contains(gotSQL, "ORDER BY started_at DESC") {
		t.Errorf("SQL missing ORDER BY: %s", gotSQL)
	}
}

func TestNodeRunsFilterByNode(t *testing.T) {
	queryID := "nr-q-node"
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
				Rows: []athenatypes.Row{nodeRunsHeader()},
			},
		},
	}
	h := dataquery.NewHandler(&mockS3Client{}, athc, "out")

	req := httptest.NewRequest(http.MethodGet, "/data/node-runs?pipeline=p&node=load_orders", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}

	gotSQL := aws.ToString(athc.lastStartInput.QueryString)
	// As of v0.20.0 the runs/node_runs/tables tables are workspace-wide
	// (ADR-016 "Workspace system catalog"), so every query carries a
	// `pipeline = '<name>'` filter alongside the caller's explicit conds.
	if !strings.Contains(gotSQL, "pipeline = 'p'") {
		t.Errorf("SQL missing pipeline filter: %s", gotSQL)
	}
	if !strings.Contains(gotSQL, "node = 'load_orders'") {
		t.Errorf("SQL missing node filter: %s", gotSQL)
	}
}

func TestNodeRunsTruncated(t *testing.T) {
	queryID := "nr-q-trunc"
	rows := []athenatypes.Row{nodeRunsHeader()}
	for i := 0; i < 6; i++ {
		rows = append(rows, nodeRunsRow(
			"r", "p", "n", "2026-05-06T10:00:00.000Z", "",
			"100", "ok", "lambda", "1024", "false", "", "", "",
			"sha256:abc123", "v0.13.0", "", "exec-trunc",
			"", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
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

	req := httptest.NewRequest(http.MethodGet, "/data/node-runs?pipeline=p&limit=4", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	r := decodeNodeRuns(t, w.Body.Bytes())
	if len(r.Rows) != 4 {
		t.Errorf("expected 4 rows, got %d", len(r.Rows))
	}
	if !r.Truncated {
		t.Error("expected truncated=true")
	}
}

func TestNodeRunsValidation(t *testing.T) {
	h := dataquery.NewHandler(&mockS3Client{}, &mockAthenaClient{}, "out")
	cases := []struct{ url string }{
		{"/data/node-runs"},                          // missing pipeline
		{"/data/node-runs?pipeline="},                // empty pipeline
		{"/data/node-runs?pipeline=1bad"},            // leading digit not allowed
		{"/data/node-runs?pipeline=bad+space"},       // non-ident charset
		{"/data/node-runs?pipeline=p&node=bad-dash"}, // node still strict (no dashes)
		{"/data/node-runs?pipeline=p&limit=0"},       // limit=0
		{"/data/node-runs?pipeline=p&limit=501"},     // limit>max
		{"/data/node-runs?pipeline=p&limit=abc"},     // limit non-numeric
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
