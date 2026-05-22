// Package modules embeds the Clavesa Terraform module tree so the CLI
// can extract it into a workspace at init time. Once extracted, generated
// pipeline `.tf` files reference modules with relative-path `source =`
// values, making `terraform init` resolve locally with no network call.
package modules

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// FS holds every tracked file under modules/ from the dev tree.
// `all:` so dotfiles like `.terraform.lock.hcl` are included.
//
//go:embed all:files
var FS embed.FS

// MetadataDirName is the workspace-relative directory that holds extracted
// modules (and other Clavesa workspace metadata). v1.0.0 renames this
// to ".clavesa".
const MetadataDirName = ".clavesa"

// ExtractRoot returns the absolute directory under workspaceDir where
// modules for moduleVersion get extracted: <workspaceDir>/.clavesa/modules/<moduleVersion>/.
func ExtractRoot(workspaceDir, moduleVersion string) string {
	return filepath.Join(workspaceDir, MetadataDirName, "modules", moduleVersion)
}

// Extract materialises the embedded module tree into ExtractRoot(workspaceDir,
// moduleVersion). Idempotent: if the destination already exists, validates
// it matches the embedded SHA and skips re-writing. Returns nil if the
// tree was already present, an extraction error otherwise.
func Extract(workspaceDir, moduleVersion string) error {
	dest := ExtractRoot(workspaceDir, moduleVersion)
	if ok, err := destMatchesEmbedded(dest); err != nil {
		return fmt.Errorf("validate extracted modules: %w", err)
	} else if ok {
		return nil
	}

	// Re-extract from scratch. Old contents (if any) are removed so
	// edits to embedded modules between versions can never leave behind
	// stale files.
	if err := os.RemoveAll(dest); err != nil {
		return fmt.Errorf("clear stale modules: %w", err)
	}
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return fmt.Errorf("create modules dir: %w", err)
	}

	if err := fs.WalkDir(FS, "files", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel := strings.TrimPrefix(path, "files/")
		if rel == "files" || rel == "" {
			return nil
		}
		target := filepath.Join(dest, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := FS.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read embedded %s: %w", path, err)
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	}); err != nil {
		return err
	}

	// Stamp the SHA so future Extract calls can short-circuit.
	sha, err := EmbeddedSHA()
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dest, ".embedded_sha"), []byte(sha), 0o644); err != nil {
		return fmt.Errorf("write sha stamp: %w", err)
	}
	return nil
}

// RelativeSource returns the value for a Terraform `source = "..."` attribute
// in a pipeline .tf file located at pipelineDir, pointing at the module at
// moduleRel (e.g. "transform/aws") under the workspace's extracted modules
// for moduleVersion. The result is always forward-slash, Terraform-friendly.
//
// Example: pipelineDir=/ws/demo, workspaceDir=/ws, moduleVersion=v0.30.0,
// moduleRel=transform/aws → "../.clavesa/modules/v0.30.0/transform/aws".
func RelativeSource(pipelineDir, workspaceDir, moduleVersion, moduleRel string) (string, error) {
	absPipeline, err := filepath.Abs(pipelineDir)
	if err != nil {
		return "", err
	}
	absWorkspace, err := filepath.Abs(workspaceDir)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(absPipeline, ExtractRoot(absWorkspace, moduleVersion))
	if err != nil {
		return "", err
	}
	return filepath.ToSlash(filepath.Join(rel, moduleRel)), nil
}

var embeddedSHACache string

// EmbeddedSHA returns a deterministic hex digest of every embedded file,
// used to detect whether a previously-extracted modules directory was
// produced by the same binary build.
func EmbeddedSHA() (string, error) {
	if embeddedSHACache != "" {
		return embeddedSHACache, nil
	}
	var paths []string
	if err := fs.WalkDir(FS, "files", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			paths = append(paths, path)
		}
		return nil
	}); err != nil {
		return "", err
	}
	sort.Strings(paths)
	h := sha256.New()
	for _, p := range paths {
		data, err := FS.ReadFile(p)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(h, "%s\x00", p)
		h.Write(data)
		h.Write([]byte{0})
	}
	embeddedSHACache = hex.EncodeToString(h.Sum(nil))
	return embeddedSHACache, nil
}

// destMatchesEmbedded reports whether the .embedded_sha stamp in dest
// matches the current EmbeddedSHA() — i.e. dest was extracted by this
// binary build and doesn't need re-writing.
func destMatchesEmbedded(dest string) (bool, error) {
	stamp, err := os.ReadFile(filepath.Join(dest, ".embedded_sha"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	want, err := EmbeddedSHA()
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(string(stamp)) == want, nil
}
