package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/vesahyp/clavesa/internal/service"
)

// noPublishEnv turns the run-end publish off, like --no-publish.
const noPublishEnv = "CLAVESA_NO_PUBLISH"

// runEndPublish publishes the workspace summary at the end of a local run
// (ADR-032 section 2). (nil, nil) means there was nothing to publish.
// Tests swap it for a fake.
var runEndPublish = func(ctx context.Context, cmd *cobra.Command, ws string) (*service.PublishResult, error) {
	svc, _, err := newService(cmd)
	if err != nil {
		return nil, err
	}
	return svc.PublishAfterRun(ctx)
}

// publishOff reports whether --no-publish or CLAVESA_NO_PUBLISH turns the
// run-end publish off.
func publishOff(flag bool) bool {
	if flag {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(os.Getenv(noPublishEnv))) {
	case "", "0", "false", "no":
		return false
	}
	return true
}

// publishAfterLocalRun runs the run-end publish and reports it in one
// stderr line. It never returns an error: the run's own result is what
// the command exits with, and stdout (the --json document) is untouched.
func publishAfterLocalRun(cmd *cobra.Command, ws string, noPublish bool) {
	reportRunEndPublish(cmd, ws, noPublish, os.Stderr)
}

func reportRunEndPublish(cmd *cobra.Command, ws string, noPublish bool, stderr io.Writer) {
	if publishOff(noPublish) {
		return
	}
	ctx := context.Background()
	if cmd != nil && cmd.Context() != nil {
		ctx = cmd.Context()
	}
	res, err := runEndPublish(ctx, cmd, ws)
	switch {
	case err != nil:
		fmt.Fprintf(stderr, "Hosted: summary not published (the run result is unchanged): %s\n", oneLine(err.Error()))
	case res != nil:
		fmt.Fprintf(stderr, "Hosted: published summary to %s\n", res.URI)
	}
}
