package cli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/vesahyp/clavesa/internal/service"
)

// fakeRunEndPublish swaps the run-end publish for the test.
func fakeRunEndPublish(t *testing.T, res *service.PublishResult, err error) *int {
	t.Helper()
	calls := 0
	prev := runEndPublish
	runEndPublish = func(context.Context, *cobra.Command, string) (*service.PublishResult, error) {
		calls++
		return res, err
	}
	t.Cleanup(func() { runEndPublish = prev })
	return &calls
}

// TestRunEndPublish: a publish failure is one stderr line and nothing
// else; success is one line; nothing to publish prints nothing; the flag
// and the env var turn it off.
func TestRunEndPublish(t *testing.T) {
	t.Setenv(noPublishEnv, "")
	var stderr bytes.Buffer

	calls := fakeRunEndPublish(t, nil, errors.New("assume role: AccessDenied\n  (more detail)"))
	reportRunEndPublish(nil, "/ws", false, &stderr)
	if *calls != 1 || strings.Count(stderr.String(), "\n") != 1 ||
		!strings.Contains(stderr.String(), "summary not published (the run result is unchanged): assume role: AccessDenied (more detail)") {
		t.Errorf("failure: calls=%d stderr=%q", *calls, stderr.String())
	}

	stderr.Reset()
	fakeRunEndPublish(t, &service.PublishResult{URI: "s3://b/summaries/w.json"}, nil)
	reportRunEndPublish(nil, "/ws", false, &stderr)
	if stderr.String() != "Hosted: published summary to s3://b/summaries/w.json\n" {
		t.Errorf("success: %q", stderr.String())
	}

	stderr.Reset()
	fakeRunEndPublish(t, nil, nil)
	reportRunEndPublish(nil, "/ws", false, &stderr)
	if stderr.Len() != 0 {
		t.Errorf("not hosted: %q", stderr.String())
	}

	calls = fakeRunEndPublish(t, nil, errors.New("x"))
	reportRunEndPublish(nil, "/ws", true, &stderr)
	t.Setenv(noPublishEnv, "1")
	reportRunEndPublish(nil, "/ws", false, &stderr)
	if *calls != 0 || stderr.Len() != 0 {
		t.Errorf("--no-publish / %s=1: calls=%d stderr=%q", noPublishEnv, *calls, stderr.String())
	}
	t.Setenv(noPublishEnv, "0")
	reportRunEndPublish(nil, "/ws", false, &stderr)
	if *calls != 1 {
		t.Errorf("%s=0 must keep the publish on", noPublishEnv)
	}
}
