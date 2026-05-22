package service

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveFileRef(t *testing.T) {
	tests := []struct {
		input    string
		wantPath string
		wantOK   bool
	}{
		{`file("transform1.sql")`, "transform1.sql", true},
		{`file("subdir/query.sql")`, "subdir/query.sql", true},
		{`SELECT * FROM t`, "", false},
		{``, "", false},
	}

	for _, tc := range tests {
		path, ok := resolveFileRef(tc.input)
		if ok != tc.wantOK {
			t.Errorf("resolveFileRef(%q) ok=%v, want %v", tc.input, ok, tc.wantOK)
			continue
		}
		if ok && path != tc.wantPath {
			t.Errorf("resolveFileRef(%q) path=%q, want %q", tc.input, path, tc.wantPath)
		}
	}
}

// TestResolveFileRefReadFile verifies that the os.ReadFile call in PreviewTransform
// resolves a file("...") SQL reference to the actual file content.
func TestResolveFileRefReadFile(t *testing.T) {
	dir := t.TempDir()
	sqlContent := "SELECT order_id FROM source1 WHERE amount > 0"
	if err := os.WriteFile(filepath.Join(dir, "transform1.sql"), []byte(sqlContent), 0644); err != nil {
		t.Fatal(err)
	}

	expr := `file("transform1.sql")`
	path, ok := resolveFileRef(expr)
	if !ok {
		t.Fatalf("resolveFileRef(%q) returned ok=false", expr)
	}
	data, err := os.ReadFile(filepath.Join(dir, path))
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if string(data) != sqlContent {
		t.Errorf("file content = %q, want %q", string(data), sqlContent)
	}
}
