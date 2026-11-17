// Package pathutil provides shared path resolution and traversal checks.
package pathutil

import (
	"errors"
	"path/filepath"
	"strings"
)

// ResolveDir resolves a potentially-relative dir against root.
// If dir is already absolute, it is returned as-is.
func ResolveDir(root, dir string) string {
	if filepath.IsAbs(dir) {
		return dir
	}
	return filepath.Join(root, dir)
}

// IsWithin reports whether target is inside base (or equal to base).
// Both paths are cleaned and resolved to absolute form before comparison.
func IsWithin(base, target string) bool {
	return target == base || strings.HasPrefix(target, base+string(filepath.Separator))
}

// ErrOutsideRoot is Contain's answer for a path that leaves its root.
var ErrOutsideRoot = errors.New("path must be inside the workspace")

// Contain resolves p, a path taken from a request, against root and
// returns its absolute form only when it stays inside root:
//
//   - an absolute p counts only when it is inside root;
//   - ".." may not climb out of root (filepath.Clean first);
//   - a symlink may not lead out of root: the deepest existing part of
//     the path is resolved with EvalSymlinks and checked again.
//
// This is the check for every dir or path that comes from a request. The
// hosted UI serves the API from a Lambda whose /proc/self/environ holds
// AWS keys, so a dir such as "/proc/self" must never be read
// (2026-11-17 review). An empty root contains nothing.
func Contain(root, p string) (string, error) {
	if root == "" {
		return "", ErrOutsideRoot
	}
	base, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	base = filepath.Clean(base)
	abs := p
	if !filepath.IsAbs(abs) {
		abs = filepath.Join(base, abs)
	}
	abs = filepath.Clean(abs)
	if !IsWithin(base, abs) {
		return "", ErrOutsideRoot
	}
	realBase, err := filepath.EvalSymlinks(base)
	if err != nil {
		return "", err
	}
	// The deepest part of abs that exists, with its symlinks resolved.
	probe, rest := abs, ""
	for {
		real, err := filepath.EvalSymlinks(probe)
		if err == nil {
			if !IsWithin(realBase, filepath.Join(real, rest)) {
				return "", ErrOutsideRoot
			}
			return abs, nil
		}
		parent := filepath.Dir(probe)
		if parent == probe {
			return "", ErrOutsideRoot
		}
		rest = filepath.Join(filepath.Base(probe), rest)
		probe = parent
	}
}
