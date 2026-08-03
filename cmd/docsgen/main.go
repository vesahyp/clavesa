// Command docsgen regenerates the CLI reference under docs/reference/cli/ from
// the live Cobra command tree. It ships in no binary — it is a build-time tool
// invoked by `make docs-cli`. The matching drift test (internal/clidocs) runs
// under `make test-go`, so a CLI change that isn't reflected here fails CI.
//
//	go run ./cmd/docsgen <output-dir>   # default: docs/reference/cli
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/vesahyp/clavesa/internal/cli"
	"github.com/vesahyp/clavesa/internal/clidocs"
)

func main() {
	outDir := "docs/reference/cli"
	if len(os.Args) > 1 {
		outDir = os.Args[1]
	}
	if err := run(outDir); err != nil {
		fmt.Fprintf(os.Stderr, "docsgen: %v\n", err)
		os.Exit(1)
	}
}

func run(outDir string) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	files := clidocs.Files(cli.NewRootCmd())

	// Remove stale generated files (renamed/removed commands) so the tree
	// exactly mirrors the current CLI.
	existing, err := filepath.Glob(filepath.Join(outDir, "*.md"))
	if err != nil {
		return err
	}
	for _, path := range existing {
		if _, keep := files[filepath.Base(path)]; !keep {
			if err := os.Remove(path); err != nil {
				return err
			}
		}
	}

	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := os.WriteFile(filepath.Join(outDir, name), []byte(files[name]), 0o644); err != nil {
			return err
		}
	}
	fmt.Printf("docsgen: wrote %d files to %s\n", len(files), outDir)
	return nil
}
