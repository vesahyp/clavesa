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

// snapshotsRow makes one Athena row with the nine columns querySnapshots
// expects: snapshot_id, parent_id, committed_at, operation, added_records,
// deleted_records, total_records, trigger, writer_run_id.
func snapshotsRow(snapshotID, parentID, committedAt, op, added, deleted, total, trigger, writerRunID string) athenatypes.Row {
	d := func(s string) athenatypes.Datum {
		if s == "" {
			return athenatypes.Datum{}
		}
		return athenatypes.Datum{VarCharValue: aws.String(s)}
	}
	return athenatypes.Row{Data: []athenatypes.Datum{
		d(snapshotID), d(parentID), d(committedAt), d(op),
		d(added), d(deleted), d(total), d(trigger), d(writerRunID),
	}}
}

func snapshotsHeaderRow() athenatypes.Row {
	return snapshotsRow(
		"snapshot_id", "parent_id", "committed_at", "operation",
		"added_records", "deleted_records", "total_records",
		"trigger", "writer_run_id",
	)
}

func decodeSnapshots(t *testing.T, body []byte) observability.SnapshotsResult {
	t.Helper()
	var r observability.SnapshotsResult
	if err := json.Unmarshal(body, &r); err != nil {
		t.Fatalf("decode SnapshotsResult: %v (body: %s)", err, body)
	}
	return r
}

func TestSnapshotsBasic(t *testing.T) {
	queryID := "snap-q-1"
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
					snapshotsHeaderRow(),
					snapshotsRow("987654", "123", "2026-05-05T10:30:00.000Z", "append", "100", "0", "5000", "backfill", "abc123"),
					snapshotsRow("123", "", "2026-05-05T09:00:00.000Z", "append", "4900", "0", "4900", "", ""),
				},
			},
		},
	}
	h := dataquery.NewHandler(&mockS3Client{}, athc, "out")

	req := httptest.NewRequest(http.MethodGet, "/data/tables/clavesa_p/orders__default/snapshots", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}
	r := decodeSnapshots(t, w.Body.Bytes())

	if len(r.Snapshots) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(r.Snapshots))
	}
	if r.Snapshots[0].SnapshotID != "987654" {
		t.Errorf("snapshots[0].SnapshotID = %q, want 987654", r.Snapshots[0].SnapshotID)
	}
	if r.Snapshots[0].ParentID != "123" {
		t.Errorf("snapshots[0].ParentID = %q, want 123", r.Snapshots[0].ParentID)
	}
	if r.Snapshots[0].Operation != "append" {
		t.Errorf("snapshots[0].Operation = %q, want append", r.Snapshots[0].Operation)
	}
	if r.Snapshots[0].AddedRecords == nil || *r.Snapshots[0].AddedRecords != 100 {
		t.Errorf("snapshots[0].AddedRecords = %v, want 100", r.Snapshots[0].AddedRecords)
	}
	if r.Snapshots[0].TotalRecords == nil || *r.Snapshots[0].TotalRecords != 5000 {
		t.Errorf("snapshots[0].TotalRecords = %v, want 5000", r.Snapshots[0].TotalRecords)
	}
	if r.LatestRecordCount == nil || *r.LatestRecordCount != 5000 {
		t.Errorf("LatestRecordCount = %v, want 5000", r.LatestRecordCount)
	}
	if r.Snapshots[0].Trigger != "backfill" {
		t.Errorf("snapshots[0].Trigger = %q, want backfill", r.Snapshots[0].Trigger)
	}
	if r.Snapshots[0].WriterRunID != "abc123" {
		t.Errorf("snapshots[0].WriterRunID = %q, want abc123", r.Snapshots[0].WriterRunID)
	}
	if r.Snapshots[1].Trigger != "" {
		t.Errorf("snapshots[1].Trigger = %q, want empty (external)", r.Snapshots[1].Trigger)
	}
	if r.Truncated {
		t.Error("expected truncated=false")
	}

	// Sanity check the issued SQL targets the $snapshots metadata table.
	gotSQL := aws.ToString(athc.lastStartInput.QueryString)
	if !strings.Contains(gotSQL, `"orders__default$snapshots"`) {
		t.Errorf("SQL did not target $snapshots table: %s", gotSQL)
	}
	if !strings.Contains(gotSQL, "ORDER BY committed_at DESC") {
		t.Errorf("SQL missing ORDER BY committed_at DESC: %s", gotSQL)
	}
}

func TestSnapshotsTruncated(t *testing.T) {
	queryID := "snap-q-trunc"
	rows := []athenatypes.Row{snapshotsHeaderRow()}
	for i := 0; i < 5; i++ {
		rows = append(rows, snapshotsRow("id", "", "2026-05-05T10:30:00.000Z", "append", "1", "0", "1", "scheduled", "run-x"))
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

	req := httptest.NewRequest(http.MethodGet, "/data/tables/db/tbl/snapshots?limit=3", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	r := decodeSnapshots(t, w.Body.Bytes())

	if len(r.Snapshots) != 3 {
		t.Errorf("expected 3 snapshots (limit=3), got %d", len(r.Snapshots))
	}
	if !r.Truncated {
		t.Error("expected truncated=true (5 rows received with limit+1=4 returned by SQL)")
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

func TestSnapshotsAthenaFailure(t *testing.T) {
	queryID := "snap-fail"
	reason := "TABLE_NOT_FOUND: orders$snapshots"
	athc := &mockAthenaClient{
		startOutput: &athena.StartQueryExecutionOutput{QueryExecutionId: &queryID},
		getExecOutput: &athena.GetQueryExecutionOutput{
			QueryExecution: &athenatypes.QueryExecution{
				QueryExecutionId: &queryID,
				Status: &athenatypes.QueryExecutionStatus{
					State:             athenatypes.QueryExecutionStateFailed,
					StateChangeReason: &reason,
				},
			},
		},
	}
	h := dataquery.NewHandler(&mockS3Client{}, athc, "out")

	req := httptest.NewRequest(http.MethodGet, "/data/tables/db/tbl/snapshots", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d (body: %s)", w.Code, w.Body.String())
	}
	if msg := decodeError(t, w.Body.Bytes()); !strings.Contains(msg, reason) {
		t.Errorf("error message missing reason: %q", msg)
	}
}

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
