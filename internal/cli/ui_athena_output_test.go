package cli

import (
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/observability"
)

// TestUICmd_BadAthenaOutputFailsAtStart: a bad CLAVESA_ATHENA_OUTPUT
// stops `clavesa ui` before the server starts.
func TestUICmd_BadAthenaOutputFailsAtStart(t *testing.T) {
	t.Setenv(observability.AthenaOutputEnv, "athena-results/viewer/")
	cmd := newUICmd()
	err := cmd.RunE(cmd, nil)
	if err == nil || !strings.Contains(err.Error(), observability.AthenaOutputEnv) {
		t.Fatalf("err = %v, want a %s error", err, observability.AthenaOutputEnv)
	}
}
