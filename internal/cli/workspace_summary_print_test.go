package cli

import (
	"bytes"
	"strings"
	"testing"

	tuiservice "github.com/vesahyp/clavesa/internal/service"
)

// TestPrintWorkspaceSummaryNotes: the notes (recovered failures, a paused
// schedule) print under the status, as the hosted UI shows them.
func TestPrintWorkspaceSummaryNotes(t *testing.T) {
	var b bytes.Buffer
	printWorkspaceSummary(&b, &tuiservice.WorkspaceSummary{
		Name: "ucguide", Kind: "cloud", Status: "ok",
		Notes: []string{"guide_events: schedule paused since 9.11 09:21"},
	})
	if !strings.Contains(b.String(), "ucguide (cloud warehouse): OK\n  note: guide_events: schedule paused since 9.11 09:21\n") {
		t.Errorf("output = %q", b.String())
	}
}
