package observability

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
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
