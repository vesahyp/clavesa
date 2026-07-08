package sources

import (
	"os"
	"path/filepath"
	"testing"
)

// TestOnDiskFormatIsByteStable pins the exact bytes Add writes so existing
// workspaces keep working across the internal/registry consolidation: two-
// space indent, struct field order, trailing newline, omitempty on unset
// fields. If this test breaks, the on-disk contract changed — that is a
// migration, not a refactor.
func TestOnDiskFormatIsByteStable(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	st := New(ws)
	if err := st.Add(Spec{
		Name: "trips", Kind: "http",
		URL: "https://example.com/x.parquet", Format: "parquet",
	}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(ws, RelDir, "trips.json"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	want := `{
  "kind": "http",
  "url": "https://example.com/x.parquet",
  "format": "parquet"
}
`
	if string(got) != want {
		t.Errorf("on-disk bytes changed:\ngot:  %q\nwant: %q", got, want)
	}
}

// TestTSVReadOptionsBytesStable pins the on-disk contract for a format=tsv
// source carrying read_options, so the tsv format + the ReadOptions field
// keep their serialized shape (sorted map keys, two-space indent, omitempty
// on unset fields). tsv is accepted alongside parquet/csv/json.
func TestTSVReadOptionsBytesStable(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	st := New(ws)
	if err := st.Add(Spec{
		Name: "events", Kind: "s3",
		Bucket: "events-bucket", Prefix: "raw/", Format: "tsv",
		ReadOptions: map[string]string{
			"delimiter": "\t",
			"comment":   "#",
			"header":    "false",
			"columns":   "a,b,c",
		},
	}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(ws, RelDir, "events.json"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	want := `{
  "kind": "s3",
  "bucket": "events-bucket",
  "prefix": "raw/",
  "format": "tsv",
  "read_options": {
    "columns": "a,b,c",
    "comment": "#",
    "delimiter": "\t",
    "header": "false"
  }
}
`
	if string(got) != want {
		t.Errorf("on-disk bytes changed:\ngot:  %q\nwant: %q", got, want)
	}
}

// TestGetReadsPreRefactorFixture proves a file written by the pre-
// consolidation code path (captured here as a literal) still loads. The
// fixture is the byte-for-byte output of the old writeJSON for a
// partitioned s3 source.
func TestGetReadsPreRefactorFixture(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	st := New(ws)
	fixture := `{
  "kind": "s3",
  "bucket": "events-bucket",
  "prefix": "events/",
  "format": "parquet",
  "partitions": [
    "year",
    "month",
    "day"
  ],
  "start_from": "now"
}
`
	if err := os.MkdirAll(st.Dir(), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(st.Path("events"), []byte(fixture), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := st.Get("events")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != "events" || got.Kind != "s3" || got.Bucket != "events-bucket" ||
		got.Prefix != "events/" || got.Format != "parquet" ||
		len(got.Partitions) != 3 || got.Partitions[2] != "day" || got.StartFrom != "now" {
		t.Errorf("fixture round-trip mismatch: %#v", got)
	}
}
