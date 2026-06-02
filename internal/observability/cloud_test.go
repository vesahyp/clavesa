package observability

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
)

// mkEvent builds an SFN history event for tests. It picks a sensible detail
// struct based on the event type.
func mkEvent(t sfntypes.HistoryEventType, stateName string, ts time.Time) sfntypes.HistoryEvent {
	ev := sfntypes.HistoryEvent{Type: t, Timestamp: &ts}
	switch t {
	case sfntypes.HistoryEventTypeTaskStateEntered:
		ev.StateEnteredEventDetails = &sfntypes.StateEnteredEventDetails{Name: aws.String(stateName)}
	case sfntypes.HistoryEventTypeTaskStateExited:
		ev.StateExitedEventDetails = &sfntypes.StateExitedEventDetails{Name: aws.String(stateName)}
	case sfntypes.HistoryEventTypeTaskFailed:
		ev.TaskFailedEventDetails = &sfntypes.TaskFailedEventDetails{
			Error: aws.String("err"),
			Cause: aws.String("cause"),
		}
	}
	return ev
}

func TestStateStatusesEmpty(t *testing.T) {
	got := StateStatusesFromHistory(nil)
	if len(got) != 0 {
		t.Errorf("expected empty map, got %v", got)
	}
}

func TestStateStatusesAllSucceeded(t *testing.T) {
	now := time.Date(2026, 5, 5, 10, 0, 0, 0, time.UTC)
	events := []sfntypes.HistoryEvent{
		mkEvent(sfntypes.HistoryEventTypeTaskStateEntered, "load_orders", now),
		mkEvent(sfntypes.HistoryEventTypeTaskStateExited, "load_orders", now.Add(2*time.Second)),
		mkEvent(sfntypes.HistoryEventTypeTaskStateEntered, "filter_complete", now.Add(3*time.Second)),
		mkEvent(sfntypes.HistoryEventTypeTaskStateExited, "filter_complete", now.Add(5*time.Second)),
	}
	got := StateStatusesFromHistory(events)
	if got["load_orders"].Status != "SUCCEEDED" {
		t.Errorf("load_orders = %+v, want SUCCEEDED", got["load_orders"])
	}
	if got["filter_complete"].Status != "SUCCEEDED" {
		t.Errorf("filter_complete = %+v, want SUCCEEDED", got["filter_complete"])
	}
	if got["load_orders"].EnteredAt == "" {
		t.Error("load_orders.EnteredAt should be populated from the entered event")
	}
}

func TestStateStatusesInProgress(t *testing.T) {
	now := time.Date(2026, 5, 5, 10, 0, 0, 0, time.UTC)
	events := []sfntypes.HistoryEvent{
		mkEvent(sfntypes.HistoryEventTypeTaskStateEntered, "load_orders", now),
		mkEvent(sfntypes.HistoryEventTypeTaskStateExited, "load_orders", now.Add(2*time.Second)),
		mkEvent(sfntypes.HistoryEventTypeTaskStateEntered, "filter_complete", now.Add(3*time.Second)),
	}
	got := StateStatusesFromHistory(events)
	if got["load_orders"].Status != "SUCCEEDED" {
		t.Errorf("load_orders = %+v, want SUCCEEDED", got["load_orders"])
	}
	if got["filter_complete"].Status != "RUNNING" {
		t.Errorf("filter_complete = %+v, want RUNNING", got["filter_complete"])
	}
}

func TestStateStatusesFailure(t *testing.T) {
	now := time.Date(2026, 5, 5, 10, 0, 0, 0, time.UTC)
	events := []sfntypes.HistoryEvent{
		mkEvent(sfntypes.HistoryEventTypeTaskStateEntered, "load_orders", now),
		mkEvent(sfntypes.HistoryEventTypeTaskStateExited, "load_orders", now.Add(1*time.Second)),
		mkEvent(sfntypes.HistoryEventTypeTaskStateEntered, "filter_complete", now.Add(2*time.Second)),
		mkEvent(sfntypes.HistoryEventTypeTaskFailed, "", now.Add(3*time.Second)),
	}
	got := StateStatusesFromHistory(events)
	if got["filter_complete"].Status != "FAILED" {
		t.Errorf("filter_complete = %+v, want FAILED", got["filter_complete"])
	}
	if got["load_orders"].Status != "SUCCEEDED" {
		t.Errorf("load_orders should remain SUCCEEDED; got %+v", got["load_orders"])
	}
}

func TestStateStatusesRetrySucceeds(t *testing.T) {
	// A state fails on first attempt, retries, succeeds. Latest outcome wins.
	now := time.Date(2026, 5, 5, 10, 0, 0, 0, time.UTC)
	events := []sfntypes.HistoryEvent{
		mkEvent(sfntypes.HistoryEventTypeTaskStateEntered, "flaky", now),
		mkEvent(sfntypes.HistoryEventTypeTaskFailed, "", now.Add(1*time.Second)),
		mkEvent(sfntypes.HistoryEventTypeTaskStateEntered, "flaky", now.Add(5*time.Second)),
		mkEvent(sfntypes.HistoryEventTypeTaskStateExited, "flaky", now.Add(7*time.Second)),
	}
	got := StateStatusesFromHistory(events)
	if got["flaky"].Status != "SUCCEEDED" {
		t.Errorf("flaky after retry = %+v, want SUCCEEDED", got["flaky"])
	}
}

func TestStateStatusesParallelBranches(t *testing.T) {
	// Parallel state entries fire TaskStateEntered for both the Parallel
	// container and each branch state. We track them all uniformly.
	now := time.Date(2026, 5, 5, 10, 0, 0, 0, time.UTC)
	events := []sfntypes.HistoryEvent{
		mkEvent(sfntypes.HistoryEventTypeTaskStateEntered, "fanout_Branches", now),
		mkEvent(sfntypes.HistoryEventTypeTaskStateEntered, "branch_a", now.Add(1*time.Second)),
		mkEvent(sfntypes.HistoryEventTypeTaskStateExited, "branch_a", now.Add(3*time.Second)),
		mkEvent(sfntypes.HistoryEventTypeTaskStateEntered, "branch_b", now.Add(1*time.Second)),
		mkEvent(sfntypes.HistoryEventTypeTaskStateExited, "fanout_Branches", now.Add(4*time.Second)),
	}
	got := StateStatusesFromHistory(events)
	if got["branch_a"].Status != "SUCCEEDED" {
		t.Errorf("branch_a = %+v, want SUCCEEDED", got["branch_a"])
	}
	if got["branch_b"].Status != "RUNNING" {
		t.Errorf("branch_b = %+v, want RUNNING", got["branch_b"])
	}
	if got["fanout_Branches"].Status != "SUCCEEDED" {
		t.Errorf("fanout_Branches = %+v, want SUCCEEDED", got["fanout_Branches"])
	}
}

func TestStateStatusesIgnoresEmptyName(t *testing.T) {
	now := time.Date(2026, 5, 5, 10, 0, 0, 0, time.UTC)
	events := []sfntypes.HistoryEvent{
		{Type: sfntypes.HistoryEventTypeTaskStateEntered, Timestamp: &now},
		mkEvent(sfntypes.HistoryEventTypeTaskStateEntered, "real", now),
	}
	got := StateStatusesFromHistory(events)
	if got["real"].Status != "RUNNING" {
		t.Errorf("real = %+v, want RUNNING", got["real"])
	}
	if _, ok := got[""]; ok {
		t.Error("empty-name event should not produce a map entry")
	}
}

func TestStateMachineNameFromExecutionARN(t *testing.T) {
	cases := []struct {
		arn  string
		want string
	}{
		{
			arn:  "arn:aws:states:eu-north-1:123456789012:execution:clavesa-my-pipeline:abc-123",
			want: "clavesa-my-pipeline",
		},
		{
			arn:  "arn:aws:states:us-east-1:000000000000:execution:sm:exec",
			want: "sm",
		},
		{arn: "", want: ""},
		{arn: "not-an-arn", want: ""},
		{arn: "arn:aws:states:eu-north-1:123:stateMachine:foo", want: ""},
	}
	for _, tc := range cases {
		if got := StateMachineNameFromExecutionARN(tc.arn); got != tc.want {
			t.Errorf("StateMachineNameFromExecutionARN(%q) = %q, want %q", tc.arn, got, tc.want)
		}
	}
}

func TestPipelineNameFromStateMachineName(t *testing.T) {
	cases := []struct{ in, want string }{
		{"clavesa-my-pipeline", "my-pipeline"},
		{"clavesa-x", "x"},
		{"non-prefixed", "non-prefixed"},
		{"clavesa-", ""},
	}
	for _, tc := range cases {
		if got := PipelineNameFromStateMachineName(tc.in); got != tc.want {
			t.Errorf("PipelineNameFromStateMachineName(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestStepTimeWindowSucceededStep(t *testing.T) {
	now := time.Date(2026, 5, 5, 10, 0, 0, 0, time.UTC)
	events := []sfntypes.HistoryEvent{
		mkEvent(sfntypes.HistoryEventTypeTaskStateEntered, "load_orders", now),
		mkEvent(sfntypes.HistoryEventTypeTaskStateExited, "load_orders", now.Add(3*time.Second)),
		mkEvent(sfntypes.HistoryEventTypeTaskStateEntered, "filter_complete", now.Add(4*time.Second)),
	}
	start, end := StepTimeWindow(events, "load_orders")
	if !start.Equal(now) {
		t.Errorf("start = %v, want %v", start, now)
	}
	if !end.Equal(now.Add(3 * time.Second)) {
		t.Errorf("end = %v, want %v", end, now.Add(3*time.Second))
	}
}

func TestStepTimeWindowFailedStep(t *testing.T) {
	now := time.Date(2026, 5, 5, 10, 0, 0, 0, time.UTC)
	events := []sfntypes.HistoryEvent{
		mkEvent(sfntypes.HistoryEventTypeTaskStateEntered, "filter_complete", now),
		mkEvent(sfntypes.HistoryEventTypeTaskFailed, "", now.Add(2*time.Second)),
	}
	start, end := StepTimeWindow(events, "filter_complete")
	if !start.Equal(now) {
		t.Errorf("start = %v, want %v", start, now)
	}
	if !end.Equal(now.Add(2 * time.Second)) {
		t.Errorf("end = %v, want %v", end, now.Add(2*time.Second))
	}
}

func TestStepTimeWindowRunningStep(t *testing.T) {
	now := time.Date(2026, 5, 5, 10, 0, 0, 0, time.UTC)
	events := []sfntypes.HistoryEvent{
		mkEvent(sfntypes.HistoryEventTypeTaskStateEntered, "filter_complete", now),
	}
	start, end := StepTimeWindow(events, "filter_complete")
	if !start.Equal(now) {
		t.Errorf("start = %v, want %v", start, now)
	}
	if !end.IsZero() {
		t.Errorf("end = %v, want zero", end)
	}
}

func TestStepTimeWindowMissingStep(t *testing.T) {
	now := time.Date(2026, 5, 5, 10, 0, 0, 0, time.UTC)
	events := []sfntypes.HistoryEvent{
		mkEvent(sfntypes.HistoryEventTypeTaskStateEntered, "other", now),
	}
	start, end := StepTimeWindow(events, "filter_complete")
	if !start.IsZero() || !end.IsZero() {
		t.Errorf("expected zero times, got start=%v end=%v", start, end)
	}
}

// TestCloudUndeployedReturnsEmpty — an undeployed workspace (no
// pipeline_bucket → empty Athena results bucket) makes every
// Athena-backed read short-circuit to an empty result with nil error,
// not a 500. Switching such a workspace to cloud mode is a valid empty
// state. nil Athena client is fine: undeployed() short-circuits before
// the client is touched.
func TestCloudUndeployedReturnsEmpty(t *testing.T) {
	c := NewCloudProvider(nil, "", nil, nil)
	ctx := context.Background()

	if r, err := c.NodeRuns(ctx, NodeRunsQuery{PipelineName: "demo", Limit: 10}); err != nil || r == nil || len(r.Rows) != 0 {
		t.Errorf("NodeRuns = (%v, %v), want empty rows, nil err", r, err)
	}
	if r, err := c.Runs(ctx, RunsQuery{PipelineName: "demo", Limit: 10}); err != nil || r == nil || len(r.Rows) != 0 {
		t.Errorf("Runs = (%v, %v), want empty rows, nil err", r, err)
	}
	if r, err := c.Tables(ctx, TablesQuery{PipelineName: "demo", Limit: 10}); err != nil || r == nil || len(r.Rows) != 0 {
		t.Errorf("Tables = (%v, %v), want empty rows, nil err", r, err)
	}
	if r, err := c.Snapshots(ctx, SnapshotsQuery{Database: "db", Table: "t", Limit: 10}); err != nil || r == nil || len(r.Snapshots) != 0 {
		t.Errorf("Snapshots = (%v, %v), want empty snapshots, nil err", r, err)
	}
	if r, err := c.SampleTable(ctx, SampleTableQuery{Database: "db", Table: "t", Limit: 10}); err != nil || r == nil || len(r.Rows) != 0 {
		t.Errorf("SampleTable = (%v, %v), want empty rows, nil err", r, err)
	}
	if r, err := c.Query(ctx, QueryQuery{SQL: "SELECT 1"}); err != nil || r == nil || len(r.Rows) != 0 {
		t.Errorf("Query = (%v, %v), want empty rows, nil err", r, err)
	}
}

// TestCloudExecutionStatesNonARNRef — a ref that isn't a real SFN
// execution ARN (a bare `dir` from the dashboard's dir-mode poll on an
// undeployed workspace) returns empty states with nil error, not a 500.
func TestCloudExecutionStatesNonARNRef(t *testing.T) {
	c := NewCloudProvider(nil, "", nil, nil)
	r, err := c.ExecutionStates(context.Background(), ExecutionStatesQuery{ExecutionRef: "bronze"})
	if err != nil || r == nil || len(r.States) != 0 || r.Status != "" {
		t.Errorf("ExecutionStates(bare dir) = (%v, %v), want empty result, nil err", r, err)
	}
}

// ---------------------------------------------------------------------------
// Snapshots() via Delta `_delta_log` over Glue + S3 (ADR-018)
// ---------------------------------------------------------------------------

// stubGlue / stubS3 are local to this test file. They satisfy the
// GlueClient + s3fs.S3API interfaces with the smallest moves the
// Snapshots() path needs.
type stubGlue struct {
	location string
	err      error
}

func (g *stubGlue) GetTable(_ context.Context, _ *glue.GetTableInput, _ ...func(*glue.Options)) (*glue.GetTableOutput, error) {
	if g.err != nil {
		return nil, g.err
	}
	return &glue.GetTableOutput{
		Table: &gluetypes.Table{
			StorageDescriptor: &gluetypes.StorageDescriptor{
				Location: aws.String(g.location),
			},
		},
	}, nil
}

type stubS3Snap struct {
	objects map[string][]byte
}

func (s *stubS3Snap) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	bucket := aws.ToString(in.Bucket)
	prefix := aws.ToString(in.Prefix)
	var contents []s3types.Object
	for path, body := range s.objects {
		full := bucket + "/"
		if !strings.HasPrefix(path, full) {
			continue
		}
		key := path[len(full):]
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		size := int64(len(body))
		k := key
		mt := time.UnixMilli(1700000000000)
		contents = append(contents, s3types.Object{Key: &k, Size: &size, LastModified: &mt})
	}
	trunc := false
	return &s3.ListObjectsV2Output{Contents: contents, IsTruncated: &trunc}, nil
}

func (s *stubS3Snap) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	path := aws.ToString(in.Bucket) + "/" + aws.ToString(in.Key)
	body, ok := s.objects[path]
	if !ok {
		return nil, errors.New("NoSuchKey")
	}
	size := int64(len(body))
	mt := time.UnixMilli(1700000000000)
	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: &size,
		LastModified:  &mt,
	}, nil
}

// TestSnapshotsReadsDeltaLog runs the full Glue→S3→delta path end to
// end and asserts the same JSON shape the v1.x Athena `$snapshots`
// query produced. This is the canonical proof that the storage swap
// preserved the wire contract (sub-slice 7's UI assumes it).
func TestSnapshotsReadsDeltaLog(t *testing.T) {
	const tablePrefix = "demo/_warehouse/clavesa_demo__public.db/orders__default/"
	const tableURI = "s3://demo-bucket/" + tablePrefix
	schema := `{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}`
	jsonEsc := func(s string) string { // minimal JSON-string escape for the test fixture
		var b strings.Builder
		b.WriteByte('"')
		for _, r := range s {
			switch r {
			case '"', '\\':
				b.WriteByte('\\')
				b.WriteRune(r)
			default:
				b.WriteRune(r)
			}
		}
		b.WriteByte('"')
		return b.String()
	}
	c0 := `{"metaData":{"id":"x","format":{"provider":"parquet"},"schemaString":` + jsonEsc(schema) + `,"partitionColumns":[],"configuration":{}}}` + "\n" +
		`{"commitInfo":{"timestamp":1700000010000,"operation":"WRITE","userMetadata":"{\"clavesa.trigger\":\"manual\",\"clavesa.run-id\":\"r-1\"}","operationMetrics":{"numOutputRows":"42"}}}` + "\n"
	c1 := `{"commitInfo":{"timestamp":1700000060000,"operation":"MERGE","userMetadata":"{\"clavesa.trigger\":\"scheduled\",\"clavesa.run-id\":\"r-2\"}","operationMetrics":{"numTargetRowsInserted":"3","numTargetRowsUpdated":"1","numTargetRowsDeleted":"2"}}}` + "\n"

	s3stub := &stubS3Snap{
		objects: map[string][]byte{
			"demo-bucket/" + tablePrefix + "_delta_log/00000000000000000000.json": []byte(c0),
			"demo-bucket/" + tablePrefix + "_delta_log/00000000000000000001.json": []byte(c1),
		},
	}
	gstub := &stubGlue{location: tableURI}

	c := NewCloudProvider(nil, "demo-bucket", nil, nil).
		WithGlue(gstub).
		WithS3(s3stub)

	res, err := c.Snapshots(context.Background(), SnapshotsQuery{
		Database: "clavesa_demo__public",
		Table:    "orders__default",
		Limit:    20,
	})
	if err != nil {
		t.Fatalf("Snapshots: %v", err)
	}
	if len(res.Snapshots) != 2 {
		t.Fatalf("snapshots = %d, want 2", len(res.Snapshots))
	}
	// Newest first.
	if res.Snapshots[0].SnapshotID != "1" || res.Snapshots[0].Operation != "MERGE" {
		t.Errorf("[0] = %+v, want version=1 + MERGE", res.Snapshots[0])
	}
	if res.Snapshots[0].ParentID != "0" {
		t.Errorf("[0].ParentID = %q, want 0", res.Snapshots[0].ParentID)
	}
	if res.Snapshots[0].AddedRecords == nil || *res.Snapshots[0].AddedRecords != 4 { // 3+1
		t.Errorf("[0].AddedRecords = %v, want 4", res.Snapshots[0].AddedRecords)
	}
	if res.Snapshots[0].DeletedRecords == nil || *res.Snapshots[0].DeletedRecords != 2 {
		t.Errorf("[0].DeletedRecords = %v, want 2", res.Snapshots[0].DeletedRecords)
	}
	if res.Snapshots[0].Trigger != "scheduled" {
		t.Errorf("[0].Trigger = %q, want scheduled", res.Snapshots[0].Trigger)
	}
	if res.Snapshots[0].WriterRunID != "r-2" {
		t.Errorf("[0].WriterRunID = %q, want r-2", res.Snapshots[0].WriterRunID)
	}
	if res.Snapshots[1].SnapshotID != "0" || res.Snapshots[1].ParentID != "" {
		t.Errorf("[1] = %+v, want version=0 + empty parent", res.Snapshots[1])
	}
	if res.Snapshots[1].AddedRecords == nil || *res.Snapshots[1].AddedRecords != 42 {
		t.Errorf("[1].AddedRecords = %v, want 42", res.Snapshots[1].AddedRecords)
	}
	if res.Snapshots[1].Trigger != "manual" || res.Snapshots[1].WriterRunID != "r-1" {
		t.Errorf("[1] provenance = (%q, %q), want (manual, r-1)", res.Snapshots[1].Trigger, res.Snapshots[1].WriterRunID)
	}
}

// TestSnapshotsLimitTruncates — Limit=1 should keep only the newest
// commit and set Truncated=true.
func TestSnapshotsLimitTruncates(t *testing.T) {
	const tablePrefix = "p/_warehouse/db/orders__default/"
	const tableURI = "s3://b/" + tablePrefix
	schema := `{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}`
	jsonEsc := func(s string) string {
		var b strings.Builder
		b.WriteByte('"')
		for _, r := range s {
			switch r {
			case '"', '\\':
				b.WriteByte('\\')
				b.WriteRune(r)
			default:
				b.WriteRune(r)
			}
		}
		b.WriteByte('"')
		return b.String()
	}
	c0 := `{"metaData":{"id":"x","format":{"provider":"parquet"},"schemaString":` + jsonEsc(schema) + `,"partitionColumns":[],"configuration":{}}}` + "\n" +
		`{"commitInfo":{"timestamp":1700000010000,"operation":"WRITE"}}` + "\n"
	c1 := `{"commitInfo":{"timestamp":1700000020000,"operation":"WRITE"}}` + "\n"

	s3stub := &stubS3Snap{
		objects: map[string][]byte{
			"b/" + tablePrefix + "_delta_log/00000000000000000000.json": []byte(c0),
			"b/" + tablePrefix + "_delta_log/00000000000000000001.json": []byte(c1),
		},
	}
	gstub := &stubGlue{location: tableURI}
	c := NewCloudProvider(nil, "b", nil, nil).WithGlue(gstub).WithS3(s3stub)

	res, err := c.Snapshots(context.Background(), SnapshotsQuery{
		Database: "db", Table: "orders__default", Limit: 1,
	})
	if err != nil {
		t.Fatalf("Snapshots: %v", err)
	}
	if len(res.Snapshots) != 1 {
		t.Fatalf("snapshots = %d, want 1 (limit)", len(res.Snapshots))
	}
	if !res.Truncated {
		t.Error("expected Truncated=true")
	}
}

// TestSnapshotsMissingGlueOrS3IsEmpty — back-compat for callers that
// haven't wired Glue/S3 (the dataquery handler's internal CloudProvider
// is one). Empty result, not a 500.
func TestSnapshotsMissingGlueOrS3IsEmpty(t *testing.T) {
	c := NewCloudProvider(nil, "b", nil, nil) // no WithGlue / WithS3
	res, err := c.Snapshots(context.Background(), SnapshotsQuery{Database: "db", Table: "t", Limit: 10})
	if err != nil || res == nil || len(res.Snapshots) != 0 {
		t.Errorf("Snapshots(no glue/s3) = (%v, %v), want empty + nil err", res, err)
	}
}

// TestSnapshotsTableNotFoundIsEmpty — a Glue lookup failing with
// EntityNotFoundException is "no such table" not "the world is on
// fire"; empty result, not a 500.
func TestSnapshotsTableNotFoundIsEmpty(t *testing.T) {
	gstub := &stubGlue{err: errors.New("EntityNotFoundException: not here")}
	c := NewCloudProvider(nil, "b", nil, nil).WithGlue(gstub).WithS3(&stubS3Snap{})
	res, err := c.Snapshots(context.Background(), SnapshotsQuery{Database: "db", Table: "missing", Limit: 10})
	if err != nil || res == nil || len(res.Snapshots) != 0 {
		t.Errorf("Snapshots(missing table) = (%v, %v), want empty + nil err", res, err)
	}
}

// TestIsAthenaMissingTableErr exercises the Athena error-string classifier
// that turns "system Delta tables don't exist yet" into an empty result
// instead of a 500. Matches the contract documented at provider.go.
func TestIsAthenaMissingTableErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"table_not_found", errors.New("SYNTAX_ERROR: TABLE_NOT_FOUND: line 1:15"), true},
		{"table_not_found_text", errors.New("Athena: Table not found 'runs'"), true},
		{"does_not_exist", errors.New("Schema clavesa_demo does not exist"), true},
		{"database_does_not_exist", errors.New("database does not exist"), true},
		{"access_denied", errors.New("AccessDeniedException: user is not authorized"), false},
		{"unrelated", errors.New("ThrottlingException: rate exceeded"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isAthenaMissingTableErr(tc.err)
			if got != tc.want {
				t.Errorf("isAthenaMissingTableErr(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// progressJSON renders a `_progress/<node>.json` body with the given
// counters and updated_ms timestamp. Helper for the live-progress tests.
func progressJSON(updatedMs int64) []byte {
	return []byte(fmt.Sprintf(
		`{"status":"running","stages_total":3,"stages_completed":1,"tasks_total":40,"tasks_completed":18,"tasks_failed":2,"updated_ms":%d}`,
		updatedMs))
}

// terminalProgressJSON is the final marker the runner writes when a node
// finishes — it carries an explicit terminal status and no counters.
func terminalProgressJSON(status string, updatedMs int64) []byte {
	return []byte(fmt.Sprintf(
		`{"status":%q,"started_ms":1,"ended_ms":%d,"updated_ms":%d,"metrics":{}}`,
		status, updatedMs, updatedMs))
}

// TestLiveProgressStates asserts that LISTing the per-node `_progress`
// objects yields a RUNNING StateStatus per fresh node with its counters
// populated, and that a stale object (older than the freshness window) is
// dropped so the UI doesn't render a ghost progress bar.
func TestLiveProgressStates(t *testing.T) {
	const arn = "arn:aws:states:eu-west-1:111122223333:execution:clavesa-demo:run-1"
	const bucket = "demo-bucket"
	now := time.UnixMilli(1700000000000)
	s3stub := &stubS3Snap{
		objects: map[string][]byte{
			// Fresh: stamped right at now.
			bucket + "/_progress/" + arn + "/a.json": progressJSON(now.UnixMilli()),
			// Stale: far older than now - freshnessWindowMs (12s).
			bucket + "/_progress/" + arn + "/b.json": progressJSON(now.UnixMilli() - 60000),
		},
	}
	c := NewCloudProvider(nil, bucket, nil, nil).WithS3(s3stub)

	states := c.liveProgressStates(context.Background(), arn, now)

	a, ok := states["a"]
	if !ok {
		t.Fatalf("fresh node a missing from states: %v", states)
	}
	if a.Status != "RUNNING" {
		t.Errorf("a.Status = %q, want RUNNING", a.Status)
	}
	if a.TasksTotal == nil || *a.TasksTotal != 40 {
		t.Errorf("a.TasksTotal = %v, want 40", a.TasksTotal)
	}
	if a.StagesTotal == nil || *a.StagesTotal != 3 {
		t.Errorf("a.StagesTotal = %v, want 3", a.StagesTotal)
	}
	if a.TasksFailed == nil || *a.TasksFailed != 2 {
		t.Errorf("a.TasksFailed = %v, want 2", a.TasksFailed)
	}
	if _, ok := states["b"]; ok {
		t.Errorf("stale node b must be dropped, got %v", states["b"])
	}
}

// TestLiveProgressStatesNoS3 is a safety check: a provider with no bucket
// and no S3 client returns an empty, non-nil map without panicking.
func TestLiveProgressStatesNoS3(t *testing.T) {
	c := NewCloudProvider(nil, "", nil, nil) // no bucket, no WithS3
	states := c.liveProgressStates(context.Background(), "arn", time.Now())
	if states == nil {
		t.Fatal("liveProgressStates returned nil map, want empty non-nil")
	}
	if len(states) != 0 {
		t.Errorf("liveProgressStates(no s3) = %v, want empty", states)
	}
}

// stubSFN is a minimal SFNClient for the ExecutionStates branch test.
// DescribeExecution returns a fixed status + start date; GetExecutionHistory
// returns an empty history (the single-Task machine no longer drives
// per-node states from history).
type stubSFN struct {
	status sfntypes.ExecutionStatus
	arn    string
	start  time.Time
}

func (s *stubSFN) DescribeExecution(_ context.Context, in *sfn.DescribeExecutionInput, _ ...func(*sfn.Options)) (*sfn.DescribeExecutionOutput, error) {
	return &sfn.DescribeExecutionOutput{
		Status:       s.status,
		StartDate:    &s.start,
		ExecutionArn: aws.String(s.arn),
	}, nil
}

func (s *stubSFN) GetExecutionHistory(_ context.Context, _ *sfn.GetExecutionHistoryInput, _ ...func(*sfn.Options)) (*sfn.GetExecutionHistoryOutput, error) {
	return &sfn.GetExecutionHistoryOutput{}, nil
}

// TestExecutionStatesRunningVsTerminal proves per-node status is read from
// the `_progress` files' own status field: a RUNNING execution surfaces a
// fresh running node, and a terminal execution surfaces each node's terminal
// marker from S3 — authoritative even when the marker is stale.
func TestExecutionStatesRunningVsTerminal(t *testing.T) {
	const arn = "arn:aws:states:eu-west-1:111122223333:execution:clavesa-demo:run-1"
	const bucket = "demo-bucket"
	now := time.UnixMilli(1700000000000)
	s3stub := &stubS3Snap{
		objects: map[string][]byte{
			bucket + "/_progress/" + arn + "/a.json": progressJSON(now.UnixMilli()),
		},
	}

	t.Run("running surfaces fresh node", func(t *testing.T) {
		sfnStub := &stubSFN{status: sfntypes.ExecutionStatusRunning, arn: arn, start: now}
		c := NewCloudProvider(nil, bucket, sfnStub, nil).WithS3(s3stub)
		c.clock = func() time.Time { return now }

		res, err := c.ExecutionStates(context.Background(), ExecutionStatesQuery{ExecutionRef: arn})
		if err != nil {
			t.Fatalf("ExecutionStates: %v", err)
		}
		if res.Status != "RUNNING" {
			t.Errorf("Status = %q, want RUNNING", res.Status)
		}
		a, ok := res.States["a"]
		if !ok || a.Status != "RUNNING" {
			t.Fatalf("expected fresh node a RUNNING, got states=%v", res.States)
		}
		if a.TasksTotal == nil || *a.TasksTotal != 40 {
			t.Errorf("a.TasksTotal = %v, want 40", a.TasksTotal)
		}
	})

	t.Run("terminal surfaces per-node terminal markers from S3", func(t *testing.T) {
		// A finished node writes a terminal marker. Even a STALE marker
		// (older than the freshness window) must surface — terminal markers
		// are authoritative and never expire, unlike still-"running" files.
		staleMs := now.UnixMilli() - 60000
		s3term := &stubS3Snap{
			objects: map[string][]byte{
				bucket + "/_progress/" + arn + "/a.json": terminalProgressJSON("succeeded", staleMs),
				bucket + "/_progress/" + arn + "/b.json": terminalProgressJSON("failed", staleMs),
			},
		}
		sfnStub := &stubSFN{status: sfntypes.ExecutionStatusSucceeded, arn: arn, start: now}
		c := NewCloudProvider(nil, bucket, sfnStub, nil).WithS3(s3term)
		c.clock = func() time.Time { return now }

		res, err := c.ExecutionStates(context.Background(), ExecutionStatesQuery{ExecutionRef: arn})
		if err != nil {
			t.Fatalf("ExecutionStates: %v", err)
		}
		if res.Status != "SUCCEEDED" {
			t.Errorf("Status = %q, want SUCCEEDED", res.Status)
		}
		if got := res.States["a"].Status; got != "SUCCEEDED" {
			t.Errorf("node a Status = %q, want SUCCEEDED (terminal marker, authoritative even when stale)", got)
		}
		if got := res.States["b"].Status; got != "FAILED" {
			t.Errorf("node b Status = %q, want FAILED", got)
		}
	})
}
