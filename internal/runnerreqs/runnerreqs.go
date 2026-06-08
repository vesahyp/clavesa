// Package runnerreqs manages the workspace-level list of extra Python pip
// dependencies installed into the transform runner image so user UDFs can
// import third-party packages (e.g. pyasn, crawlerdetect).
//
// One plain pip-requirements file per workspace under
// `<workspace>/.clavesa/runner-requirements.txt`. It is staged into the
// runner Docker build context as `extra-requirements.txt` and installed by
// a cheap tail layer in the runner Dockerfile.
//
// Parsing is intentionally minimal — just enough to dedupe by package name
// and skip blanks/comments. We do NOT reimplement pip's requirement-spec
// parser; the raw file is handed to `pip install -r` verbatim at build time.
//
// The package is filesystem-naive (no locking) — same shape as the sibling
// credentials/sources packages; intended for the single-user CLI /
// single-process UI clavesa runs in today.
package runnerreqs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// RelPath is the workspace-relative path of the requirements file.
const RelPath = ".clavesa/runner-requirements.txt"

// Path returns the absolute path of the requirements file under root.
func Path(root string) string {
	return filepath.Join(root, RelPath)
}

// Read returns the raw file content. Returns ("", nil) when the file does
// not exist — an absent file is an empty requirement set, not an error.
func Read(root string) (string, error) {
	data, err := os.ReadFile(Path(root))
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", fmt.Errorf("read runner requirements: %w", err)
	}
	return string(data), nil
}

// Write persists content to the requirements file, creating the .clavesa
// directory as needed.
func Write(root, content string) error {
	if err := os.MkdirAll(filepath.Dir(Path(root)), 0o755); err != nil {
		return fmt.Errorf("create .clavesa dir: %w", err)
	}
	if err := os.WriteFile(Path(root), []byte(content), 0o644); err != nil {
		return fmt.Errorf("write runner requirements: %w", err)
	}
	return nil
}

// Lines returns the meaningful requirement lines from content — non-empty,
// non-comment (lines whose first non-space char is `#`), each trimmed.
func Lines(content string) []string {
	out := []string{}
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		out = append(out, trimmed)
	}
	return out
}

// pkgToken returns the package-name token of a requirement spec: the text
// before the first version operator, extra/marker delimiter, or whitespace.
// Compared case-insensitively (pip treats package names that way).
func pkgToken(spec string) string {
	s := strings.TrimSpace(spec)
	i := strings.IndexAny(s, "=<>~![ ;\t")
	if i >= 0 {
		s = s[:i]
	}
	return strings.ToLower(strings.TrimSpace(s))
}

// AddLine appends spec as a new line unless an existing meaningful line
// already declares the same package name. Existing lines, comments, and
// order are preserved; the result always ends with a trailing newline.
// Returns the new content and whether a line was added.
func AddLine(content, spec string) (string, bool) {
	token := pkgToken(spec)
	for _, line := range Lines(content) {
		if pkgToken(line) == token {
			return content, false
		}
	}
	out := content
	if out != "" && !strings.HasSuffix(out, "\n") {
		out += "\n"
	}
	out += strings.TrimSpace(spec) + "\n"
	return out, true
}

// RemoveLine drops any meaningful line whose package-name token matches
// spec's token. Comments and blank lines are preserved; the result is
// normalized to end with a trailing newline when non-empty. Returns the new
// content and whether anything was removed.
func RemoveLine(content, spec string) (string, bool) {
	token := pkgToken(spec)
	removed := false
	kept := []string{}
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(trimmed, "#") && pkgToken(trimmed) == token {
			removed = true
			continue
		}
		kept = append(kept, line)
	}
	if !removed {
		return content, false
	}
	out := strings.Join(kept, "\n")
	out = strings.TrimRight(out, "\n")
	if out != "" {
		out += "\n"
	}
	return out, true
}
