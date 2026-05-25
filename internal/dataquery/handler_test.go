package dataquery_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/vesahyp/clavesa/internal/dataquery"
	"github.com/vesahyp/clavesa/internal/graph"
)

// ---------------------------------------------------------------------------
// Mock S3 client
// ---------------------------------------------------------------------------

type mockS3Client struct {
	listOutput *s3.ListObjectsV2Output
	listErr    error
	getOutput  *s3.GetObjectOutput
	getErr     error
}

func (m *mockS3Client) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return m.listOutput, m.listErr
}

func (m *mockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return m.getOutput, m.getErr
}

// ---------------------------------------------------------------------------
// Mock Athena client
// ---------------------------------------------------------------------------

type mockAthenaClient struct {
	startOutput      *athena.StartQueryExecutionOutput
	startErr         error
	getExecOutput    *athena.GetQueryExecutionOutput
	getExecErr       error
	getResultsOutput *athena.GetQueryResultsOutput
	getResultsErr    error
	// lastStartInput is set on each StartQueryExecution call so tests can
	// assert which SQL was issued.
	lastStartInput *athena.StartQueryExecutionInput
}

func (m *mockAthenaClient) StartQueryExecution(ctx context.Context, params *athena.StartQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error) {
	m.lastStartInput = params
	return m.startOutput, m.startErr
}

func (m *mockAthenaClient) GetQueryExecution(ctx context.Context, params *athena.GetQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.GetQueryExecutionOutput, error) {
	return m.getExecOutput, m.getExecErr
}

func (m *mockAthenaClient) GetQueryResults(ctx context.Context, params *athena.GetQueryResultsInput, optFns ...func(*athena.Options)) (*athena.GetQueryResultsOutput, error) {
	return m.getResultsOutput, m.getResultsErr
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func decodeResult(t *testing.T, body []byte) dataquery.QueryResult {
	t.Helper()
	var r dataquery.QueryResult
	if err := json.Unmarshal(body, &r); err != nil {
		t.Fatalf("decode QueryResult: %v (body: %s)", err, body)
	}
	return r
}

func decodeError(t *testing.T, body []byte) string {
	t.Helper()
	var e map[string]string
	if err := json.Unmarshal(body, &e); err != nil {
		t.Fatalf("decode error: %v (body: %s)", err, body)
	}
	return e["error"]
}

func makeCSVBody(content string) *s3.GetObjectOutput {
	return &s3.GetObjectOutput{
		Body: io.NopCloser(strings.NewReader(content)),
	}
}

func makeListOutput(keys ...string) *s3.ListObjectsV2Output {
	objs := make([]s3types.Object, len(keys))
	for i, k := range keys {
		k := k
		objs[i] = s3types.Object{Key: &k}
	}
	return &s3.ListObjectsV2Output{Contents: objs}
}

// ---------------------------------------------------------------------------
// /data/source — CSV
// ---------------------------------------------------------------------------

func TestSourceCSV(t *testing.T) {
	csvData := "id,name,value\n1,alice,100\n2,bob,200\n"

	s3c := &mockS3Client{
		listOutput: makeListOutput("prefix/data.csv"),
		getOutput:  makeCSVBody(csvData),
	}
	athc := &mockAthenaClient{}
	h := dataquery.NewHandler(s3c, athc, "output-bucket")

	req := httptest.NewRequest(http.MethodGet, "/data/source?bucket=my-bucket&prefix=prefix/&format=csv&limit=10", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}
	r := decodeResult(t, w.Body.Bytes())

	if len(r.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(r.Columns))
	}
	if r.Columns[0].Name != "id" || r.Columns[1].Name != "name" || r.Columns[2].Name != "value" {
		t.Errorf("unexpected columns: %+v", r.Columns)
	}
	// All column types should be "string" (inference fallback).
	for _, c := range r.Columns {
		if c.Type != "string" {
			t.Errorf("expected type string for column %s, got %s", c.Name, c.Type)
		}
	}
	if len(r.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(r.Rows))
	}
	if r.Rows[0][0] != "1" || r.Rows[0][1] != "alice" || r.Rows[0][2] != "100" {
		t.Errorf("unexpected row 0: %v", r.Rows[0])
	}
	if r.RowCount != 2 {
		t.Errorf("expected row_count 2, got %d", r.RowCount)
	}
	if r.Truncated {
		t.Error("expected truncated=false")
	}
}

// ---------------------------------------------------------------------------
// /data/source — NDJSON
// ---------------------------------------------------------------------------

func TestSourceNDJSON(t *testing.T) {
	ndjson := `{"id":"1","name":"alice"}` + "\n" + `{"id":"2","name":"bob"}` + "\n"

	s3c := &mockS3Client{
		listOutput: makeListOutput("data.json"),
		getOutput:  makeCSVBody(ndjson),
	}
	athc := &mockAthenaClient{}
	h := dataquery.NewHandler(s3c, athc, "output-bucket")

	req := httptest.NewRequest(http.MethodGet, "/data/source?bucket=my-bucket&prefix=&format=json&limit=10", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}
	r := decodeResult(t, w.Body.Bytes())

	if len(r.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d: %+v", len(r.Columns), r.Columns)
	}
	if len(r.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(r.Rows))
	}
	if r.RowCount != 2 {
		t.Errorf("expected row_count 2, got %d", r.RowCount)
	}
	if r.Truncated {
		t.Error("expected truncated=false")
	}
}

// ---------------------------------------------------------------------------
// /data/source — wrapped JSON with json_path
// ---------------------------------------------------------------------------

func TestSourceJSONWithPath(t *testing.T) {
	jsonData := `{"metadata":{"total":2},"cars":[{"id":"1","brand":"Volvo"},{"id":"2","brand":"Polestar"}]}`

	s3c := &mockS3Client{
		listOutput: makeListOutput("data.json"),
		getOutput:  makeCSVBody(jsonData),
	}
	athc := &mockAthenaClient{}
	h := dataquery.NewHandler(s3c, athc, "output-bucket")

	req := httptest.NewRequest(http.MethodGet, "/data/source?bucket=my-bucket&prefix=&format=json&json_path=cars&limit=10", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}
	r := decodeResult(t, w.Body.Bytes())

	if len(r.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d: %+v", len(r.Columns), r.Columns)
	}
	if len(r.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(r.Rows))
	}
	if r.RowCount != 2 {
		t.Errorf("expected row_count 2, got %d", r.RowCount)
	}
}

// ---------------------------------------------------------------------------
// /data/source — limit enforcement
// ---------------------------------------------------------------------------

func TestSourceLimitEnforced(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("col\n")
	for i := 0; i < 10; i++ {
		sb.WriteString("val\n")
	}

	s3c := &mockS3Client{
		listOutput: makeListOutput("data.csv"),
		getOutput:  makeCSVBody(sb.String()),
	}
	athc := &mockAthenaClient{}
	h := dataquery.NewHandler(s3c, athc, "output-bucket")

	req := httptest.NewRequest(http.MethodGet, "/data/source?bucket=b&prefix=&format=csv&limit=3", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	r := decodeResult(t, w.Body.Bytes())

	if len(r.Rows) != 3 {
		t.Errorf("expected 3 rows (limited), got %d", len(r.Rows))
	}
	if r.RowCount != 3 {
		t.Errorf("expected row_count 3, got %d", r.RowCount)
	}
	if !r.Truncated {
		t.Error("expected truncated=true")
	}
}

// ---------------------------------------------------------------------------
// /data/source — limit defaults to 100
// ---------------------------------------------------------------------------

func TestSourceDefaultLimit(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("col\n")
	for i := 0; i < 150; i++ {
		sb.WriteString("val\n")
	}

	s3c := &mockS3Client{
		listOutput: makeListOutput("data.csv"),
		getOutput:  makeCSVBody(sb.String()),
	}
	athc := &mockAthenaClient{}
	h := dataquery.NewHandler(s3c, athc, "output-bucket")

	req := httptest.NewRequest(http.MethodGet, "/data/source?bucket=b&prefix=&format=csv", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	r := decodeResult(t, w.Body.Bytes())

	if len(r.Rows) != 100 {
		t.Errorf("expected 100 rows (default limit), got %d", len(r.Rows))
	}
	if !r.Truncated {
		t.Error("expected truncated=true when rows exceed default limit of 100")
	}
}

// ---------------------------------------------------------------------------
// /data/source — S3 list returns no objects → 404
// ---------------------------------------------------------------------------

func TestSourceS3NotFound(t *testing.T) {
	s3c := &mockS3Client{
		listOutput: &s3.ListObjectsV2Output{Contents: nil}, // empty
	}
	athc := &mockAthenaClient{}
	h := dataquery.NewHandler(s3c, athc, "output-bucket")

	req := httptest.NewRequest(http.MethodGet, "/data/source?bucket=b&prefix=missing/&format=csv", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d (body: %s)", w.Code, w.Body.String())
	}
	if msg := decodeError(t, w.Body.Bytes()); msg == "" {
		t.Error("expected non-empty error message")
	}
}

// ---------------------------------------------------------------------------
// /data/source — missing required params
// ---------------------------------------------------------------------------

func TestSourceMissingParams(t *testing.T) {
	h := dataquery.NewHandler(&mockS3Client{}, &mockAthenaClient{}, "out")

	cases := []struct {
		url string
	}{
		{"/data/source"},
		{"/data/source?bucket=b"},
		{"/data/source?bucket=b&prefix=&format=bad"},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodGet, tc.url, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code == http.StatusOK {
			t.Errorf("url %s: expected non-200, got 200", tc.url)
		}
	}
}

// ---------------------------------------------------------------------------
// /data/table — Athena result mapping
// ---------------------------------------------------------------------------

func TestTableAthenaResult(t *testing.T) {
	queryID := "test-query-id-123"
	s3c := &mockS3Client{}
	athc := &mockAthenaClient{
		startOutput: &athena.StartQueryExecutionOutput{
			QueryExecutionId: &queryID,
		},
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
						{Name: aws.String("id"), Type: aws.String("varchar")},
						{Name: aws.String("amount"), Type: aws.String("double")},
					},
				},
				Rows: []athenatypes.Row{
					// First row is the header row returned by Athena
					{Data: []athenatypes.Datum{
						{VarCharValue: aws.String("id")},
						{VarCharValue: aws.String("amount")},
					}},
					// Data rows
					{Data: []athenatypes.Datum{
						{VarCharValue: aws.String("1")},
						{VarCharValue: aws.String("99.99")},
					}},
					{Data: []athenatypes.Datum{
						{VarCharValue: aws.String("2")},
						{VarCharValue: aws.String("50.00")},
					}},
				},
			},
		},
	}

	h := dataquery.NewHandler(s3c, athc, "output-bucket")
	req := httptest.NewRequest(http.MethodGet, "/data/table?catalog_db=clavesa_my_pipeline&catalog_table=s3_source__default&limit=10", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}
	r := decodeResult(t, w.Body.Bytes())

	if len(r.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(r.Columns))
	}
	wantCols := []graph.Column{
		{Name: "id", Type: "varchar", Nullable: true},
		{Name: "amount", Type: "double", Nullable: true},
	}
	for i, wc := range wantCols {
		if i >= len(r.Columns) {
			break
		}
		if r.Columns[i].Name != wc.Name {
			t.Errorf("col[%d].Name: got %s, want %s", i, r.Columns[i].Name, wc.Name)
		}
		if r.Columns[i].Type != wc.Type {
			t.Errorf("col[%d].Type: got %s, want %s", i, r.Columns[i].Type, wc.Type)
		}
	}
	// Athena returns first row as header; we should have 2 data rows.
	if len(r.Rows) != 2 {
		t.Errorf("expected 2 data rows, got %d", len(r.Rows))
	}
	if r.Rows[0][0] != "1" || r.Rows[0][1] != "99.99" {
		t.Errorf("unexpected row[0]: %v", r.Rows[0])
	}
	if r.RowCount != 2 {
		t.Errorf("expected row_count 2, got %d", r.RowCount)
	}
	if r.Truncated {
		t.Error("expected truncated=false")
	}
}

// ---------------------------------------------------------------------------
// /data/table — limit enforcement
// ---------------------------------------------------------------------------

func TestTableLimitEnforced(t *testing.T) {
	queryID := "qid-limit"
	rows := []athenatypes.Row{
		// header
		{Data: []athenatypes.Datum{{VarCharValue: aws.String("col")}}},
	}
	for i := 0; i < 10; i++ {
		rows = append(rows, athenatypes.Row{
			Data: []athenatypes.Datum{{VarCharValue: aws.String("val")}},
		})
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
			ResultSet: &athenatypes.ResultSet{
				ResultSetMetadata: &athenatypes.ResultSetMetadata{
					ColumnInfo: []athenatypes.ColumnInfo{
						{Name: aws.String("col"), Type: aws.String("varchar")},
					},
				},
				Rows: rows,
			},
		},
	}
	h := dataquery.NewHandler(&mockS3Client{}, athc, "out")
	req := httptest.NewRequest(http.MethodGet, "/data/table?catalog_db=db&catalog_table=tbl&limit=3", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	r := decodeResult(t, w.Body.Bytes())

	if len(r.Rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(r.Rows))
	}
	if !r.Truncated {
		t.Error("expected truncated=true")
	}
}

// ---------------------------------------------------------------------------
// /data/table — Athena query failure → 500
// ---------------------------------------------------------------------------

func TestTableAthenaFailure(t *testing.T) {
	queryID := "fail-query"
	reason := "SYNTAX_ERROR: line 1:1"
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
	req := httptest.NewRequest(http.MethodGet, "/data/table?catalog_db=db&catalog_table=tbl", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d (body: %s)", w.Code, w.Body.String())
	}
	if msg := decodeError(t, w.Body.Bytes()); !strings.Contains(msg, reason) {
		t.Errorf("expected error to contain %q, got %q", reason, msg)
	}
}

// ---------------------------------------------------------------------------
// /data/table — missing required params
// ---------------------------------------------------------------------------

func TestTableMissingParams(t *testing.T) {
	h := dataquery.NewHandler(&mockS3Client{}, &mockAthenaClient{}, "out")

	cases := []struct {
		url string
	}{
		{"/data/table"},
		{"/data/table?catalog_db=db"},
		{"/data/table?catalog_table=tbl"},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodGet, tc.url, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code == http.StatusOK {
			t.Errorf("url %s: expected non-200, got 200", tc.url)
		}
	}
}

// ---------------------------------------------------------------------------
// Parquet — smoke test (parse round-tripped parquet bytes)
// ---------------------------------------------------------------------------

func TestSourceParquet(t *testing.T) {
	// Build a minimal in-memory Parquet file using parquet-go and verify the
	// handler can parse it back.  We encode two rows with columns "id" and
	// "score".
	type Row struct {
		ID    string  `parquet:"id"`
		Score float64 `parquet:"score"`
	}

	var buf bytes.Buffer
	if err := dataquery.WriteParquetRows(&buf, []Row{
		{ID: "a", Score: 1.5},
		{ID: "b", Score: 2.5},
	}); err != nil {
		t.Fatalf("WriteParquetRows: %v", err)
	}

	s3c := &mockS3Client{
		listOutput: makeListOutput("data.parquet"),
		getOutput: &s3.GetObjectOutput{
			Body: io.NopCloser(bytes.NewReader(buf.Bytes())),
		},
	}
	athc := &mockAthenaClient{}
	h := dataquery.NewHandler(s3c, athc, "out")

	req := httptest.NewRequest(http.MethodGet, "/data/source?bucket=b&prefix=&format=parquet&limit=10", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}
	r := decodeResult(t, w.Body.Bytes())
	if len(r.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d: %+v", len(r.Columns), r.Columns)
	}
	if len(r.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(r.Rows))
	}
}
