package service

import (
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/observability"
)

// TestMarshalWithinCap: over the cap, widget results go first, then
// table snapshots, each with a note; the runs part stays.
func TestMarshalWithinCap(t *testing.T) {
	big := strings.Repeat("x", 4000)
	widgets := map[string]*observability.QueryResult{}
	for _, k := range []string{"a", "b", "c"} {
		widgets[k] = &observability.QueryResult{Columns: []observability.SampleTableColumn{{Name: "v"}}, Rows: [][]string{{big}}}
	}
	sum := &WorkspaceSummary{Name: "w", Drill: &SummaryDrill{
		Pipelines: map[string]PipelineDrill{"p": {Name: "p"}},
		Snapshot:  &DrillSnapshot{Widgets: widgets, Snapshots: map[string]*observability.SnapshotsResult{"d.t": {}}},
	}}
	body, err := marshalWithinCap(sum, 9000)
	if err != nil {
		t.Fatal(err)
	}
	if len(body) > 9000 {
		t.Fatalf("body %d bytes, over the cap", len(body))
	}
	s := string(body)
	if !strings.Contains(s, "left out") || !strings.Contains(s, `"p"`) || !strings.Contains(s, `"d.t"`) {
		t.Errorf("want a note, the runs part and the table snapshot kept: %s", s[:200])
	}
	if len(sum.Drill.Snapshot.Widgets) != 3 {
		t.Error("the caller's summary was changed")
	}
}
