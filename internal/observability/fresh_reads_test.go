package observability

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/athena"
)

type captureAthena struct{ in *athena.StartQueryExecutionInput }

func (c *captureAthena) StartQueryExecution(_ context.Context, in *athena.StartQueryExecutionInput, _ ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error) {
	c.in = in
	return nil, errors.New("stop")
}
func (c *captureAthena) GetQueryExecution(context.Context, *athena.GetQueryExecutionInput, ...func(*athena.Options)) (*athena.GetQueryExecutionOutput, error) {
	return nil, errors.New("unused")
}
func (c *captureAthena) GetQueryResults(context.Context, *athena.GetQueryResultsInput, ...func(*athena.Options)) (*athena.GetQueryResultsOutput, error) {
	return nil, errors.New("unused")
}

// TestWithFreshReads: result reuse is on by default and off under
// WithFreshReads (the hosted refresh must see a run that just ended).
func TestWithFreshReads(t *testing.T) {
	c := &captureAthena{}
	_, _ = runAthenaQuery(context.Background(), c, "b", "wg", "SELECT 1")
	if c.in.ResultReuseConfiguration == nil {
		t.Fatal("result reuse should be on by default")
	}
	_, _ = runAthenaQuery(WithFreshReads(context.Background()), c, "b", "wg", "SELECT 1")
	if c.in.ResultReuseConfiguration != nil {
		t.Error("WithFreshReads must turn result reuse off")
	}
	if c.in.WorkGroup == nil || *c.in.WorkGroup != "wg" {
		t.Error("workgroup lost")
	}
}
