package cli

import (
	"testing"

	"github.com/vesahyp/clavesa/internal/api"
	"github.com/vesahyp/clavesa/internal/observability"
)

func ti64(v int64) *int64 { return &v }

// TestMergeTableMetrics pins the join between catalog rows and the
// observability tables-state rows: match by table name, fall back to
// `<node>__<output_key>`, and leave rows with no metrics (unmatched, no
// dir, or a dir the fetch couldn't answer for) with nil fields.
func TestMergeTableMetrics(t *testing.T) {
	tables := []api.CatalogTable{
		{Database: "clavesa_ws__demo", Name: "trips", Dir: "demo", OwningPipeline: "demo", OwningNode: "trips", OutputKey: "default"},
		{Database: "clavesa_ws__demo", Name: "revenue", Dir: "demo", OwningPipeline: "demo", OwningNode: "revenue", OutputKey: "default"},
		{Database: "external", Name: "orphan", Dir: "", OwningPipeline: ""},
	}
	byDir := map[string][]observability.TableInfo{
		"demo": {
			{Node: "trips", OutputKey: "default", TableName: "trips", FileCount: ti64(4), TotalBytes: ti64(4096)},
			// revenue matched via node__output_key fallback (no TableName).
			{Node: "revenue", OutputKey: "default", FileCount: ti64(2), TotalBytes: ti64(1024)},
		},
	}

	out := mergeTableMetrics(tables, byDir)
	if len(out) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(out))
	}
	if out[0].FileCount == nil || *out[0].FileCount != 4 || out[0].TotalBytes == nil || *out[0].TotalBytes != 4096 {
		t.Errorf("trips metrics not attached: %+v", out[0])
	}
	if out[1].FileCount == nil || *out[1].FileCount != 2 {
		t.Errorf("revenue metrics not attached via node__key fallback: %+v", out[1])
	}
	if out[2].FileCount != nil || out[2].TotalBytes != nil {
		t.Errorf("orphan (no owning pipeline) should have nil metrics: %+v", out[2])
	}
}

func TestFormatBytes(t *testing.T) {
	cases := []struct {
		n    int64
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.0 GB"},
	}
	for _, c := range cases {
		if got := formatBytes(c.n); got != c.want {
			t.Errorf("formatBytes(%d) = %q, want %q", c.n, got, c.want)
		}
	}
}

func TestAvgSizeCell(t *testing.T) {
	if got := avgSizeCell(nil, ti64(100)); got != "—" {
		t.Errorf("nil file count = %q, want em-dash", got)
	}
	if got := avgSizeCell(ti64(0), ti64(100)); got != "—" {
		t.Errorf("zero file count = %q, want em-dash", got)
	}
	if got := avgSizeCell(ti64(4), ti64(4096)); got != "1.0 KB" {
		t.Errorf("avg of 4096/4 = %q, want 1.0 KB", got)
	}
}

func TestFileCountCell(t *testing.T) {
	if got := fileCountCell(nil); got != "—" {
		t.Errorf("nil = %q, want em-dash", got)
	}
	if got := fileCountCell(ti64(7)); got != "7" {
		t.Errorf("7 = %q, want 7", got)
	}
}
