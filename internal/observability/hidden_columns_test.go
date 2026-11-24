package observability

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
)

// hidingAthena fails a query that names a hidden column the way Athena
// does for a column the table does not have, and records each query.
type hidingAthena struct {
	hidden  []string
	queries []string
}

func (h *hidingAthena) StartQueryExecution(_ context.Context, in *athena.StartQueryExecutionInput, _ ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error) {
	sql := aws.ToString(in.QueryString)
	h.queries = append(h.queries, sql)
	for _, c := range h.hidden {
		if strings.Contains(sql, "\n  "+c+",") || strings.Contains(sql, "\n  "+c+"\nFROM") {
			return nil, errors.New("COLUMN_NOT_FOUND: line 12:3: Column '" + c + "' cannot be resolved")
		}
	}
	return &athena.StartQueryExecutionOutput{QueryExecutionId: aws.String("q")}, nil
}

func (h *hidingAthena) GetQueryExecution(context.Context, *athena.GetQueryExecutionInput, ...func(*athena.Options)) (*athena.GetQueryExecutionOutput, error) {
	return &athena.GetQueryExecutionOutput{QueryExecution: &athenatypes.QueryExecution{
		Status: &athenatypes.QueryExecutionStatus{State: athenatypes.QueryExecutionStateSucceeded},
	}}, nil
}

func (h *hidingAthena) GetQueryResults(context.Context, *athena.GetQueryResultsInput, ...func(*athena.Options)) (*athena.GetQueryResultsOutput, error) {
	return &athena.GetQueryResultsOutput{ResultSet: &athenatypes.ResultSet{}}, nil
}

// TestRunQueriesWithoutHiddenColumns: Lake Formation column tags hide
// error_msg from the hosted reader, and the run history still loads,
// with error_msg read as NULL.
func TestRunQueriesWithoutHiddenColumns(t *testing.T) {
	ctx := context.Background()
	// The heap columns are missing on a node_runs table no runner v2.28+
	// has written to; they read as NULL the same way.
	h := &hidingAthena{hidden: []string{"error_msg", "timing", "heap_old_after_gc_mb", "heap_committed_mb", "heap_max_mb"}}
	c := NewCloudProvider(h, "bucket", nil, nil)
	if _, err := c.NodeRuns(ctx, NodeRunsQuery{PipelineName: "taxi", Database: "sys__pipelines", Limit: 10}); err != nil {
		t.Fatalf("node runs: %v", err)
	}
	last := h.queries[len(h.queries)-1]
	if !strings.Contains(last, "CAST(NULL AS varchar) AS error_msg,") || !strings.Contains(last, "CAST(NULL AS varchar) AS timing,") ||
		!strings.Contains(last, "CAST(NULL AS varchar) AS heap_old_after_gc_mb,") || !strings.Contains(last, "CAST(NULL AS varchar) AS heap_max_mb\nFROM") {
		t.Errorf("node runs: last query keeps a hidden column:\n%s", last)
	}
	h.queries = nil
	if _, err := c.Runs(ctx, RunsQuery{PipelineName: "taxi", Database: "sys__pipelines", Limit: 10}); err != nil {
		t.Fatalf("runs: %v", err)
	}
	if last := h.queries[len(h.queries)-1]; !strings.Contains(last, "CAST(NULL AS varchar) AS error_msg\nFROM") {
		t.Errorf("runs: last query keeps error_msg:\n%s", last)
	}
	// Another error is not retried.
	h.hidden = nil
	c2 := NewCloudProvider(&failAthena{}, "bucket", nil, nil)
	if _, err := c2.Runs(ctx, RunsQuery{PipelineName: "taxi", Database: "sys__pipelines", Limit: 10}); err == nil {
		t.Error("an access error must not pass as a hidden column")
	}
}

type failAthena struct{ hidingAthena }

func (f *failAthena) StartQueryExecution(context.Context, *athena.StartQueryExecutionInput, ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error) {
	return nil, errors.New("AccessDeniedException")
}
