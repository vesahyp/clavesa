package sources

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestAddListGetDelete(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	st := New(ws)

	if list, err := st.List(); err != nil || len(list) != 0 {
		t.Fatalf("List on empty workspace = %v / %v, want empty / nil", list, err)
	}

	spec := Spec{Name: "trips", Kind: "http", URL: "https://example.com/x.parquet", Format: "parquet"}
	if err := st.Add(spec); err != nil {
		t.Fatalf("Add: %v", err)
	}
	// Round-trip Get.
	got, err := st.Get("trips")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.URL != spec.URL || got.Format != spec.Format || got.Name != "trips" {
		t.Errorf("Get returned %#v, want %#v", got, spec)
	}

	// List shows it.
	list, err := st.List()
	if err != nil || len(list) != 1 || list[0].Name != "trips" {
		t.Fatalf("List = %#v / %v", list, err)
	}

	// Duplicate Add refuses.
	if err := st.Add(spec); err == nil {
		t.Fatal("Add of existing source should refuse")
	}

	// Delete works.
	if err := st.Delete("trips"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := st.Get("trips"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Get after Delete = %v, want os.ErrNotExist", err)
	}
}

func TestValidationRejectsBadInputs(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	st := New(ws)

	cases := []struct {
		name string
		spec Spec
		want string
	}{
		{"empty name", Spec{Kind: "http", URL: "https://x/y.parquet", Format: "parquet"}, "name is required"},
		{"capital name", Spec{Name: "Trips", Kind: "http", URL: "https://x/y.parquet", Format: "parquet"}, "lowercase letter"},
		{"leading digit", Spec{Name: "1trips", Kind: "http", URL: "https://x/y.parquet", Format: "parquet"}, "lowercase letter"},
		{"bad char", Spec{Name: "tr.ps", Kind: "http", URL: "https://x/y.parquet", Format: "parquet"}, "invalid char"},
		{"bad kind", Spec{Name: "trips", Kind: "smb", URL: "smb://x"}, "unsupported source kind"},
		{"missing url", Spec{Name: "trips", Kind: "http", Format: "parquet"}, "url is required"},
		{"bad scheme", Spec{Name: "trips", Kind: "http", URL: "ftp://x/y.parquet", Format: "parquet"}, "http:// or https://"},
		{"missing format", Spec{Name: "trips", Kind: "http", URL: "https://x/y"}, "format is required"},
		{"bad format", Spec{Name: "trips", Kind: "http", URL: "https://x/y", Format: "xml"}, "unsupported format"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := st.Add(c.spec)
			if err == nil || !contains(err.Error(), c.want) {
				t.Errorf("Add(%#v) err = %v, want substring %q", c.spec, err, c.want)
			}
		})
	}
}

func TestS3KindRoundTripAndPrefixNormalization(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	st := New(ws)
	// Prefix without trailing slash should be normalized on Add.
	if err := st.Add(Spec{
		Name: "logs", Kind: "s3",
		Bucket: "my-data-bucket", Prefix: "events", Format: "json",
	}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	got, err := st.Get("logs")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Prefix != "events/" {
		t.Errorf("prefix not normalized: got %q, want %q", got.Prefix, "events/")
	}
	if got.Bucket != "my-data-bucket" || got.Format != "json" {
		t.Errorf("round-trip mismatch: %#v", got)
	}
}

// TestS3PartitionedRoundTrip verifies Partitions + StartFrom persist intact
// through Add/Get so the orchestration emitter + local-run descriptor both
// see the same incremental-read parameters.
func TestS3PartitionedRoundTrip(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	st := New(ws)
	if err := st.Add(Spec{
		Name: "events", Kind: "s3",
		Bucket: "events-bucket", Prefix: "events/", Format: "parquet",
		Partitions: []string{"year", "month", "day"},
		StartFrom:  "now",
	}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	got, err := st.Get("events")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got.Partitions) != 3 || got.Partitions[0] != "year" || got.Partitions[2] != "day" {
		t.Errorf("partitions mismatch: %v", got.Partitions)
	}
	if got.StartFrom != "now" {
		t.Errorf("start_from: got %q want %q", got.StartFrom, "now")
	}
}

func TestS3ValidationRejectsBadInputs(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	st := New(ws)
	cases := []struct {
		name string
		spec Spec
		want string
	}{
		{"missing bucket", Spec{Name: "x", Kind: "s3", Format: "parquet"}, "bucket is required"},
		{"bad bucket char", Spec{Name: "x", Kind: "s3", Bucket: "MyBucket", Format: "parquet"}, "invalid char"},
		{"missing format", Spec{Name: "x", Kind: "s3", Bucket: "my-bucket"}, "format is required"},
		{"bad format", Spec{Name: "x", Kind: "s3", Bucket: "my-bucket", Format: "yaml"}, "unsupported format"},
		{
			"partitions on http rejected",
			Spec{Name: "x", Kind: "http", URL: "https://example.com/data.parquet", Format: "parquet", Partitions: []string{"year"}},
			"only valid for kind=s3",
		},
		{
			"start_from without partitions rejected",
			Spec{Name: "x", Kind: "s3", Bucket: "b", Format: "parquet", StartFrom: "now"},
			"start_from set without partitions",
		},
		{
			"partitions with csv rejected",
			Spec{Name: "x", Kind: "s3", Bucket: "b", Format: "csv", Partitions: []string{"year"}},
			"format=parquet",
		},
		{
			"empty partition name rejected",
			Spec{Name: "x", Kind: "s3", Bucket: "b", Format: "parquet", Partitions: []string{"year", ""}},
			"partitions[1] is empty",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := st.Add(c.spec)
			if err == nil || !contains(err.Error(), c.want) {
				t.Errorf("Add(%#v) err = %v, want substring %q", c.spec, err, c.want)
			}
		})
	}
}

func TestListSkipsMalformedFiles(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	st := New(ws)
	if err := os.MkdirAll(st.Dir(), 0o755); err != nil {
		t.Fatal(err)
	}
	// Valid source.
	if err := st.Add(Spec{Name: "good", Kind: "http", URL: "https://x/y.parquet", Format: "parquet"}); err != nil {
		t.Fatal(err)
	}
	// Junk JSON — List should skip without erroring.
	if err := os.WriteFile(filepath.Join(st.Dir(), "bad.json"), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Wrong-name file — also skipped.
	if err := os.WriteFile(filepath.Join(st.Dir(), "BadName.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	list, err := st.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 || list[0].Name != "good" {
		t.Errorf("List = %#v, want one entry [good]", list)
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (haystack == needle || stringIndex(haystack, needle) >= 0)
}

func stringIndex(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
