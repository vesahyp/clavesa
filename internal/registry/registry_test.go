package registry

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// item is a minimal spec-like type standing in for the real registries'
// Spec structs.
type item struct {
	Name  string `json:"-"`
	Value string `json:"value"`
}

func newTestStore(t *testing.T) (*Store[item], string) {
	t.Helper()
	ws := t.TempDir()
	st := New(ws, Config[item]{
		Kind:   "widget",
		RelDir: ".clavesa/widgets",
		Ext:    ".json",
		Marshal: func(v item) ([]byte, error) {
			return MarshalIndentJSON(v)
		},
		Unmarshal: func(name string, data []byte) (item, error) {
			var v item
			if err := json.Unmarshal(data, &v); err != nil {
				return item{}, fmt.Errorf("parse %s.json: %w", name, err)
			}
			v.Name = name
			return v, nil
		},
	})
	return st, ws
}

func TestValidName(t *testing.T) {
	t.Parallel()
	bad := []struct {
		name string
		want string
	}{
		{"", "name is required"},
		{strings.Repeat("a", 65), "<=64 chars"},
		{"1leading-digit", "lowercase letter"},
		{"-leading-dash", "lowercase letter"},
		{"_leading-underscore", "lowercase letter"},
		{"Capital", "lowercase letter"},
		{"has space", "invalid char"},
		{"has.dot", "invalid char"},
		{"../traversal", "lowercase letter"},
		{"a/../b", "invalid char"},
		{"a/b", "invalid char"},
	}
	for _, c := range bad {
		if err := ValidName(c.name); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("ValidName(%q) = %v, want substring %q", c.name, err, c.want)
		}
	}
	for _, ok := range []string{"a", "trips", "a-b_2", strings.Repeat("a", 64)} {
		if err := ValidName(ok); err != nil {
			t.Errorf("ValidName(%q) = %v, want nil", ok, err)
		}
	}
}

func TestWriteFileAtomic(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "out.json")

	if err := WriteFileAtomic(path, []byte("one\n")); err != nil {
		t.Fatalf("WriteFileAtomic: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "one\n" {
		t.Fatalf("read back = %q / %v, want %q", got, err, "one\n")
	}

	// Overwrite replaces content and leaves no temp file behind.
	if err := WriteFileAtomic(path, []byte("two\n")); err != nil {
		t.Fatalf("WriteFileAtomic overwrite: %v", err)
	}
	got, _ = os.ReadFile(path)
	if string(got) != "two\n" {
		t.Errorf("overwrite = %q, want %q", got, "two\n")
	}
	if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("temp file left behind: stat = %v, want not-exist", err)
	}

	// A missing parent directory fails (caller owns MkdirAll).
	if err := WriteFileAtomic(filepath.Join(dir, "nope", "x.json"), []byte("x")); err == nil {
		t.Error("WriteFileAtomic into missing dir should fail")
	}
}

// TestMarshalIndentJSONFormat pins the exact on-disk byte shape of the
// spec-backed registries (two-space indent + trailing newline). Sources and
// credentials files written before the internal/registry consolidation must
// stay byte-identical.
func TestMarshalIndentJSONFormat(t *testing.T) {
	t.Parallel()
	got, err := MarshalIndentJSON(item{Name: "ignored", Value: "v1"})
	if err != nil {
		t.Fatalf("MarshalIndentJSON: %v", err)
	}
	want := "{\n  \"value\": \"v1\"\n}\n"
	if string(got) != want {
		t.Errorf("MarshalIndentJSON = %q, want %q", got, want)
	}
}

func TestStoreCreateGetUpdatePutDelete(t *testing.T) {
	t.Parallel()
	st, ws := newTestStore(t)

	if got := st.Dir(); got != filepath.Join(ws, ".clavesa/widgets") {
		t.Errorf("Dir = %q", got)
	}
	if got := st.Path("a"); got != filepath.Join(ws, ".clavesa/widgets", "a.json") {
		t.Errorf("Path = %q", got)
	}

	// Update before create refuses with the kind-worded message.
	if err := st.Update("a", item{Value: "v"}); err == nil || !strings.Contains(err.Error(), `widget "a" does not exist`) {
		t.Errorf("Update missing = %v, want does-not-exist", err)
	}

	if err := st.Create("a", item{Value: "v1"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := st.Get("a")
	if err != nil || got.Value != "v1" || got.Name != "a" {
		t.Fatalf("Get = %#v / %v, want Name=a Value=v1", got, err)
	}

	// Duplicate create refuses with the kind-worded message.
	if err := st.Create("a", item{Value: "v2"}); err == nil || !strings.Contains(err.Error(), `widget "a" already exists`) {
		t.Errorf("Create duplicate = %v, want already-exists", err)
	}

	// Update overwrites.
	if err := st.Update("a", item{Value: "v2"}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if got, _ := st.Get("a"); got.Value != "v2" {
		t.Errorf("after Update, Value = %q, want v2", got.Value)
	}

	// Put upserts: overwrite existing and create fresh.
	if err := st.Put("a", item{Value: "v3"}); err != nil {
		t.Fatalf("Put overwrite: %v", err)
	}
	if err := st.Put("b", item{Value: "b1"}); err != nil {
		t.Fatalf("Put create: %v", err)
	}
	if got, _ := st.Get("a"); got.Value != "v3" {
		t.Errorf("after Put, Value = %q, want v3", got.Value)
	}

	// Delete removes; Get then reports os.ErrNotExist.
	if err := st.Delete("a"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := st.Get("a"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Get after Delete = %v, want os.ErrNotExist", err)
	}
	if err := st.Delete("a"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Delete of missing = %v, want os.ErrNotExist", err)
	}
}

func TestStoreValidatesNamesEverywhere(t *testing.T) {
	t.Parallel()
	st, _ := newTestStore(t)
	if err := st.Create("../evil", item{}); err == nil {
		t.Error("Create with traversal name should fail")
	}
	if err := st.Put("Bad", item{}); err == nil {
		t.Error("Put with invalid name should fail")
	}
	if err := st.Update("Bad", item{}); err == nil {
		t.Error("Update with invalid name should fail")
	}
	if _, err := st.Get("Bad"); err == nil {
		t.Error("Get with invalid name should fail")
	}
	if err := st.Delete("Bad"); err == nil {
		t.Error("Delete with invalid name should fail")
	}
}

func TestStoreCustomValidator(t *testing.T) {
	t.Parallel()
	// A digit-leading-permissive validator, like dashboards.ValidSlug.
	st := New(t.TempDir(), Config[item]{
		Kind:   "widget",
		RelDir: "w",
		Ext:    ".json",
		ValidName: func(s string) error {
			if s == "" {
				return fmt.Errorf("slug is required")
			}
			return nil
		},
		Marshal:   func(v item) ([]byte, error) { return MarshalIndentJSON(v) },
		Unmarshal: func(name string, data []byte) (item, error) { return item{Name: name}, nil },
	})
	if err := st.Create("2024-report", item{}); err != nil {
		t.Fatalf("custom validator should allow digit-leading name: %v", err)
	}
	if err := st.Put("", item{}); err == nil || !strings.Contains(err.Error(), "slug is required") {
		t.Errorf("custom validator wording lost: %v", err)
	}
	names, err := st.ListNames()
	if err != nil || len(names) != 1 || names[0] != "2024-report" {
		t.Errorf("ListNames = %v / %v", names, err)
	}
}

func TestListOrderingAndSkips(t *testing.T) {
	t.Parallel()
	st, _ := newTestStore(t)

	// Empty state: missing dir lists as empty non-nil slices.
	names, err := st.ListNames()
	if err != nil || names == nil || len(names) != 0 {
		t.Fatalf("ListNames on missing dir = %v / %v, want empty non-nil", names, err)
	}
	items, err := st.List()
	if err != nil || items == nil || len(items) != 0 {
		t.Fatalf("List on missing dir = %v / %v, want empty non-nil", items, err)
	}

	// Insert out of order, including the hyphen case where raw filename
	// order ("a-b.json" < "a.json") differs from name order ("a" < "a-b").
	for _, n := range []string{"zeta", "a-b", "a", "mid_2"} {
		if err := st.Create(n, item{Value: n}); err != nil {
			t.Fatalf("Create(%q): %v", n, err)
		}
	}
	// Files List must skip: subdirectory, wrong extension, invalid name,
	// malformed JSON.
	if err := os.Mkdir(filepath.Join(st.Dir(), "subdir.json"), 0o755); err != nil {
		t.Fatal(err)
	}
	for f, content := range map[string]string{
		"notes.txt":    "hi",
		"BadName.json": "{}",
		"broken.json":  "{not json",
	} {
		if err := os.WriteFile(filepath.Join(st.Dir(), f), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	names, err = st.ListNames()
	if err != nil {
		t.Fatalf("ListNames: %v", err)
	}
	// "broken" has a valid name so ListNames includes it; only List (which
	// parses) skips it.
	wantNames := []string{"a", "a-b", "broken", "mid_2", "zeta"}
	if len(names) != len(wantNames) {
		t.Fatalf("ListNames = %v, want %v", names, wantNames)
	}
	for i := range wantNames {
		if names[i] != wantNames[i] {
			t.Fatalf("ListNames = %v, want %v", names, wantNames)
		}
	}

	items, err = st.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	wantItems := []string{"a", "a-b", "mid_2", "zeta"}
	if len(items) != len(wantItems) {
		t.Fatalf("List = %#v, want names %v", items, wantItems)
	}
	for i := range wantItems {
		if items[i].Name != wantItems[i] {
			t.Fatalf("List order = %#v, want %v", items, wantItems)
		}
	}
}
