package observability

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
)

func TestAthenaOutputLocation(t *testing.T) {
	t.Setenv(AthenaOutputEnv, "")
	if got, err := AthenaOutputLocation(DefaultAthenaOutput("ws-clavesa")); err != nil || got != "s3://ws-clavesa/athena-results/" {
		t.Errorf("unset: %q, %v", got, err)
	}
	for in, want := range map[string]string{
		"s3://ws-clavesa/athena-results/viewer/": "s3://ws-clavesa/athena-results/viewer/",
		"s3://ws-clavesa/athena-results/viewer":  "s3://ws-clavesa/athena-results/viewer/",
		" s3://other/x/ ":                        "s3://other/x/",
		"s3://bucket-only":                       "s3://bucket-only/",
	} {
		t.Setenv(AthenaOutputEnv, in)
		if got, err := AthenaOutputLocation(DefaultAthenaOutput("ws-clavesa")); err != nil || got != want {
			t.Errorf("%q: %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"ws-clavesa/athena-results/", "s3://", "s3:///x/", "https://bk/x/", "s3://bkt/x?y", "s3://My_Bucket/x/"} {
		t.Setenv(AthenaOutputEnv, bad)
		if _, err := AthenaOutputLocation(DefaultAthenaOutput("ws-clavesa")); err == nil || !strings.Contains(err.Error(), AthenaOutputEnv) {
			t.Errorf("%q: err %v, want a %s error", bad, err, AthenaOutputEnv)
		}
	}
}

// outputRecorder is an AthenaClient that records the StartQueryExecution
// input and answers one empty, finished query.
type outputRecorder struct {
	in *athena.StartQueryExecutionInput
}

func (r *outputRecorder) StartQueryExecution(_ context.Context, in *athena.StartQueryExecutionInput, _ ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error) {
	r.in = in
	return &athena.StartQueryExecutionOutput{QueryExecutionId: aws.String("q1")}, nil
}

func (r *outputRecorder) GetQueryExecution(context.Context, *athena.GetQueryExecutionInput, ...func(*athena.Options)) (*athena.GetQueryExecutionOutput, error) {
	return &athena.GetQueryExecutionOutput{QueryExecution: &athenatypes.QueryExecution{
		Status: &athenatypes.QueryExecutionStatus{State: athenatypes.QueryExecutionStateSucceeded},
	}}, nil
}

func (r *outputRecorder) GetQueryResults(context.Context, *athena.GetQueryResultsInput, ...func(*athena.Options)) (*athena.GetQueryResultsOutput, error) {
	return &athena.GetQueryResultsOutput{ResultSet: &athenatypes.ResultSet{}}, nil
}

// TestRunAthenaQuery_OutputLocation: every query the UI starts through
// the shared runAthenaQuery writes to the setting's prefix; with no
// setting, to <bucket>/athena-results/; with a hosted workgroup, to the
// workgroup's own location (no ResultConfiguration sent).
func TestRunAthenaQuery_OutputLocation(t *testing.T) {
	ctx := context.Background()
	run := func(workgroup string) *athena.StartQueryExecutionInput {
		t.Helper()
		rec := &outputRecorder{}
		if _, err := runAthenaQuery(ctx, rec, "ws-clavesa", workgroup, "SELECT 1"); err != nil {
			t.Fatal(err)
		}
		return rec.in
	}
	loc := func(in *athena.StartQueryExecutionInput) string {
		if in.ResultConfiguration == nil {
			return ""
		}
		return aws.ToString(in.ResultConfiguration.OutputLocation)
	}

	t.Setenv(AthenaOutputEnv, "")
	if got := loc(run("")); got != "s3://ws-clavesa/athena-results/" {
		t.Errorf("default: %q", got)
	}
	t.Setenv(AthenaOutputEnv, "s3://ws-clavesa/athena-results/viewer/")
	if got := loc(run("")); got != "s3://ws-clavesa/athena-results/viewer/" {
		t.Errorf("setting: %q", got)
	}
	// A hosted workgroup keeps its own location: with the setting set,
	// the query fails before it starts, so nobody believes the results
	// are in their prefix.
	rec := &outputRecorder{}
	if _, err := runAthenaQuery(ctx, rec, "ws-clavesa", "clavesa-ui-ws", "SELECT 1"); err == nil || !strings.Contains(err.Error(), "hosted workgroup") || rec.in != nil {
		t.Errorf("workgroup with the setting: err %v, started %v", err, rec.in != nil)
	}
	t.Setenv(AthenaOutputEnv, "")
	if in := run("clavesa-ui-ws"); in.ResultConfiguration != nil || aws.ToString(in.WorkGroup) != "clavesa-ui-ws" {
		t.Errorf("workgroup: %+v", in)
	}
	t.Setenv(AthenaOutputEnv, "not-a-uri")
	rec = &outputRecorder{}
	if _, err := runAthenaQuery(ctx, rec, "ws-clavesa", "", "SELECT 1"); err == nil || rec.in != nil {
		t.Errorf("a bad setting must fail before any query starts: err %v, started %v", err, rec.in != nil)
	}
}
