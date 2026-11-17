package pathutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestContain(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "root")
	if err := os.MkdirAll(filepath.Join(root, "a"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(base, filepath.Join(root, "up")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "a"), filepath.Join(root, "in")); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		p  string
		ok bool
	}{
		{"a", true},
		{"a/new/file.py", true}, // need not exist yet
		{"", true},
		{".", true},
		{"in", true}, // a symlink that stays inside
		{filepath.Join(root, "a"), true},
		{"/proc/self", false},
		{"/etc", false},
		{"..", false},
		{"a/../..", false},
		{"../root2", false},
		{"up", false},
		{"up/root/a", true}, // through a symlink, but its real path is inside
		{"up/x/new", false},
	} {
		_, err := Contain(root, c.p)
		if (err == nil) != c.ok {
			t.Errorf("Contain(root, %q) err = %v, want ok = %v", c.p, err, c.ok)
		}
	}
	if _, err := Contain("", "a"); err == nil {
		t.Error("an empty root must contain nothing")
	}
}
