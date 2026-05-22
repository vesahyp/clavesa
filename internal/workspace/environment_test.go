package workspace

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadEnvironmentMode(t *testing.T) {
	tests := []struct {
		name    string
		content string // "" = don't write the file at all
		want    Mode
	}{
		{"absent file → local", "", ModeLocal},
		{"explicit local → local", `{"mode":"local"}`, ModeLocal},
		{"explicit cloud → cloud", `{"mode":"cloud"}`, ModeCloud},
		{"unknown value → local", `{"mode":"banana"}`, ModeLocal},
		{"malformed json → local", `{not json`, ModeLocal},
		{"empty object → local", `{}`, ModeLocal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			if tt.content != "" {
				if err := os.MkdirAll(filepath.Join(root, ".clavesa"), 0o755); err != nil {
					t.Fatalf("mkdir: %v", err)
				}
				if err := os.WriteFile(EnvironmentFilePath(root), []byte(tt.content), 0o644); err != nil {
					t.Fatalf("write: %v", err)
				}
			}
			if got := LoadEnvironmentMode(root); got != tt.want {
				t.Errorf("LoadEnvironmentMode = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseMode(t *testing.T) {
	tests := []struct {
		in     string
		want   Mode
		wantOK bool
	}{
		{"local", ModeLocal, true},
		{"cloud", ModeCloud, true},
		{"", ModeLocal, false},
		{"Local", ModeLocal, false},
		{"banana", ModeLocal, false},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, ok := ParseMode(tt.in)
			if got != tt.want || ok != tt.wantOK {
				t.Errorf("ParseMode(%q) = (%q, %v), want (%q, %v)", tt.in, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestWriteEnvironmentMode(t *testing.T) {
	root := t.TempDir() // .clavesa/ does not exist yet

	if err := WriteEnvironmentMode(root, ModeCloud); err != nil {
		t.Fatalf("WriteEnvironmentMode: %v", err)
	}
	if got := LoadEnvironmentMode(root); got != ModeCloud {
		t.Errorf("round-trip cloud: got %q", got)
	}

	if err := WriteEnvironmentMode(root, ModeLocal); err != nil {
		t.Fatalf("WriteEnvironmentMode: %v", err)
	}
	if got := LoadEnvironmentMode(root); got != ModeLocal {
		t.Errorf("round-trip local: got %q", got)
	}

	// An unrecognized mode is coerced to local.
	if err := WriteEnvironmentMode(root, Mode("banana")); err != nil {
		t.Fatalf("WriteEnvironmentMode: %v", err)
	}
	if got := LoadEnvironmentMode(root); got != ModeLocal {
		t.Errorf("coerce unknown: got %q", got)
	}
}
