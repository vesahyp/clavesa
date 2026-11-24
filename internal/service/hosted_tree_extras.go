package service

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

)

// hostedTreeExtras are the tree-copy files of parity slice 4 (ADR-033):
//
//   - .clavesa/sources/*.json, the source registry: references only (a
//     credential is named, never held). A URL loses its query string and
//     fragment, where an API key could sit.
//   - notebooks/*.ipynb, with their saved outputs, so the hosted page shows
//     them as the Mac last ran them. The author decides what a saved
//     output holds, as for a dashboard (docs/ideas/column-tags.md).
//   - .clavesa/runner-requirements.txt, the runner's Python packages.
//
// Only regular files directly in those places; anything else stays out.
func hostedTreeExtras(root string) ([]HostedTreeFile, error) {
	var files []HostedTreeFile

	if entries, err := os.ReadDir(filepath.Join(root, ".clavesa", "sources")); err == nil {
		for _, e := range entries {
			name := e.Name()
			if !e.Type().IsRegular() || !strings.HasSuffix(name, ".json") || hostedTreeDenied(name) {
				continue
			}
			body, err := hostedSourceBody(filepath.Join(root, ".clavesa", "sources", name))
			if err != nil {
				return nil, fmt.Errorf("source %s: %w", name, err)
			}
			files = append(files, HostedTreeFile{Key: ".clavesa/sources/" + name, Body: body})
		}
	}

	if entries, err := os.ReadDir(filepath.Join(root, "notebooks")); err == nil {
		for _, e := range entries {
			name := e.Name()
			if !e.Type().IsRegular() || !strings.HasSuffix(name, ".ipynb") || hostedTreeDenied(name) {
				continue
			}
			files = append(files, HostedTreeFile{Key: "notebooks/" + name, Path: filepath.Join(root, "notebooks", name)})
		}
	}

	req := filepath.Join(root, ".clavesa", "runner-requirements.txt")
	if fi, err := os.Lstat(req); err == nil && fi.Mode().IsRegular() {
		files = append(files, HostedTreeFile{Key: ".clavesa/runner-requirements.txt", Path: req})
	}
	return files, nil
}

// hostedSourceBody is a source spec with its URL's query string and
// fragment removed.
func hostedSourceBody(path string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	if raw, ok := m["url"].(string); ok && raw != "" {
		if u, err := url.Parse(raw); err == nil {
			u.RawQuery, u.Fragment, u.User = "", "", nil
			m["url"] = u.String()
		} else {
			delete(m, "url")
		}
	}
	out, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(out, '\n'), nil
}

