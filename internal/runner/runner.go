// Package runner embeds the Clavesa transform runner source files so the
// CLI can extract them at workspace init time without requiring the user to
// have the source tree present.
package runner

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"sort"
)

// FS contains Dockerfile, runner.py, requirements.txt, and entrypoint.sh.
//
//go:embed files/*
var FS embed.FS

// SHALabel is the Docker label key used to stamp the embedded-runner-source
// digest onto each built workspace image. workspace.Init compares this label
// against EmbeddedSHA() to decide whether to rebuild — cache hits only when
// every embedded file is byte-identical to the build that produced the image.
// Without this, edits to runner.py (or entrypoint.sh, etc.) would silently
// continue serving the previously-built image until manually cleared.
const SHALabel = "clavesa.runner_sha"

var embeddedSHACache string

// EmbeddedSHA returns a deterministic hex digest of every file under files/
// in the embedded FS. Order-independent: walks paths sorted, hashes
// "<path>\x00<bytes>\x00" per file. Invalidates whenever any embedded
// runner-source byte changes — exactly the condition for needing a rebuild.
func EmbeddedSHA() (string, error) {
	if embeddedSHACache != "" {
		return embeddedSHACache, nil
	}
	var paths []string
	err := fs.WalkDir(FS, "files", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Strings(paths)
	h := sha256.New()
	for _, p := range paths {
		f, err := FS.Open(p)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(h, "%s\x00", p)
		if _, err := io.Copy(h, f); err != nil {
			f.Close()
			return "", err
		}
		f.Close()
		h.Write([]byte{0})
	}
	embeddedSHACache = hex.EncodeToString(h.Sum(nil))
	return embeddedSHACache, nil
}

// LocalImageName returns the local Docker image name for a workspace's
// transform runner, namespaced to avoid collisions between workspaces.
func LocalImageName(workspaceName string) string {
	return "clavesa/" + workspaceName + "/transform-runner"
}
