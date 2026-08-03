package clidocs_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/vesahyp/clavesa/internal/cli"
	"github.com/vesahyp/clavesa/internal/clidocs"
)

// referenceDir is the committed CLI reference, relative to this test's package
// directory (internal/clidocs).
const referenceDir = "../../docs/reference/cli"

// TestReferenceInSync fails when docs/reference/cli/ drifts from the live CLI
// tree — a CLI change that doesn't regenerate the reference breaks the build.
// Fix: `make docs-cli`.
func TestReferenceInSync(t *testing.T) {
	want := clidocs.Files(cli.NewRootCmd())

	got := map[string]string{}
	entries, err := os.ReadDir(referenceDir)
	if err != nil {
		t.Fatalf("read reference dir: %v (run `make docs-cli`)", err)
	}
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".md" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(referenceDir, e.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		got[e.Name()] = string(b)
	}

	for name, content := range want {
		switch g, ok := got[name]; {
		case !ok:
			t.Errorf("missing generated file %s — run `make docs-cli`", name)
		case g != content:
			t.Errorf("%s is stale — run `make docs-cli`", name)
		}
	}
	for name := range got {
		if _, ok := want[name]; !ok {
			t.Errorf("orphan file %s (command removed?) — run `make docs-cli`", name)
		}
	}
}
