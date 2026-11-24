package pipelinestatus

import "testing"

// TestServedWithoutErrorText: the hosted reader of a workspace with
// column tags gets no Step Functions cause; the error names stay.
func TestServedWithoutErrorText(t *testing.T) {
	d := executionDetail{Status: "FAILED", Error: "States.TaskFailed", Cause: "row 203.0.113.7", StepError: "RuntimeError", StepCause: "row 203.0.113.7"}
	if got := NewHandler(t.TempDir()).served(d); got.Cause == "" || got.StepCause == "" {
		t.Errorf("default handler dropped the cause: %+v", got)
	}
	got := NewHandler(t.TempDir()).WithoutErrorText(true).served(d)
	if got.Cause != "" || got.StepCause != "" || got.Error == "" || got.StepError == "" {
		t.Errorf("hidden: %+v", got)
	}
}
