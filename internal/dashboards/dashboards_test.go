package dashboards

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSaveGetListDelete(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	st := New(ws)

	if list, err := st.List(); err != nil || len(list) != 0 {
		t.Fatalf("List on empty workspace = %v / %v, want empty / nil", list, err)
	}

	doc := []byte(`{"slug":"revenue","title":"Revenue","datasets":[],"widgets":[]}`)
	if err := st.Save("revenue", doc); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// File lands at the expected registry path with a trailing newline.
	want := filepath.Join(ws, RelDir, "revenue.json")
	got, err := os.ReadFile(want)
	if err != nil {
		t.Fatalf("read saved file: %v", err)
	}
	if got[len(got)-1] != '\n' {
		t.Errorf("saved file should end in a newline")
	}

	// Round-trip Get returns the bytes (sans the appended newline trim is
	// fine — JSON ignores trailing whitespace).
	rt, err := st.Get("revenue")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !strings.Contains(string(rt), `"slug":"revenue"`) {
		t.Errorf("Get returned %q, want the saved document", rt)
	}

	// List shows it.
	list, err := st.List()
	if err != nil || len(list) != 1 || list[0] != "revenue" {
		t.Fatalf("List = %#v / %v", list, err)
	}

	// Save again overwrites (slug is the key).
	if err := st.Save("revenue", []byte(`{"slug":"revenue","title":"Revenue v2"}`)); err != nil {
		t.Fatalf("Save (overwrite): %v", err)
	}
	rt, _ = st.Get("revenue")
	if !strings.Contains(string(rt), "Revenue v2") {
		t.Errorf("overwrite did not take: %q", rt)
	}

	// Delete works.
	if err := st.Delete("revenue"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := st.Get("revenue"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Get after Delete = %v, want os.ErrNotExist", err)
	}
}

func TestGetMissingIsNotExist(t *testing.T) {
	t.Parallel()
	st := New(t.TempDir())
	if _, err := st.Get("nope"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Get of missing slug = %v, want os.ErrNotExist", err)
	}
}

func TestSlugValidation(t *testing.T) {
	t.Parallel()
	st := New(t.TempDir())
	cases := []struct {
		name string
		slug string
		want string
	}{
		{"empty", "", "slug is required"},
		{"capital", "Revenue", "invalid char"},
		{"space", "my dash", "invalid char"},
		{"dot traversal", "../etc", "invalid char"},
		{"slash", "a/b", "invalid char"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := st.Save(c.slug, []byte("{}"))
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("Save(%q) err = %v, want substring %q", c.slug, err, c.want)
			}
		})
	}
	// Valid slugs pass.
	for _, ok := range []string{"revenue", "pipeline-runs-demo", "a_b_2"} {
		if err := ValidSlug(ok); err != nil {
			t.Errorf("ValidSlug(%q) = %v, want nil", ok, err)
		}
	}
}

func TestListSkipsNonRegistryFiles(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	st := New(ws)
	if err := st.Save("good", []byte(`{"slug":"good"}`)); err != nil {
		t.Fatal(err)
	}
	// Wrong-name file — skipped (could not have been a valid slug).
	if err := os.WriteFile(filepath.Join(st.Dir(), "BadName.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Non-JSON file — skipped.
	if err := os.WriteFile(filepath.Join(st.Dir(), "notes.txt"), []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}
	list, err := st.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 || list[0] != "good" {
		t.Errorf("List = %#v, want [good]", list)
	}
}
