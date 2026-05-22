// Package pathutil provides shared path resolution and traversal checks.
package pathutil

import (
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
