package service

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/vesahyp/clavesa/internal/workspace"
)

// hostedTreeExtras are the tree-copy files of parity slice 4 (ADR-033):
//
//   - .clavesa/sources/*.json, the source registry: references only (a
//     credential is named, never held). A URL loses its query string and
//     fragment, where an API key could sit.
//   - notebooks/*.ipynb, with their saved outputs, so the hosted page shows
//     them as the Mac last ran them. In a workspace whose data holds
//     personal data (hosted.personal_data), outputs are removed: they can
//     hold rows.
//   - .clavesa/runner-requirements.txt, the runner's Python packages.
//
// Only regular files directly in those places; anything else stays out.
func hostedTreeExtras(root string) ([]HostedTreeFile, error) {
	var files []HostedTreeFile
	// Fail closed: a manifest that does not load could hide personal_data.
	m, err := workspace.Load(root)
	if err != nil {
		return nil, fmt.Errorf("hosted tree copy: %w", err)
	}
	personal := m != nil && m.Hosted.MarksPersonalData()

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
			path := filepath.Join(root, "notebooks", name)
			if !personal {
				files = append(files, HostedTreeFile{Key: "notebooks/" + name, Path: path})
				continue
			}
			body, err := notebookWithoutOutputs(path)
			if err != nil {
				return nil, fmt.Errorf("notebook %s: %w", name, err)
			}
			files = append(files, HostedTreeFile{Key: "notebooks/" + name, Body: body})
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

// notebookWithoutOutputs is an .ipynb with every cell's outputs and
// execution count removed.
func notebookWithoutOutputs(path string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return NotebookWithoutOutputs(b)
}

// NotebookWithoutOutputs removes every code cell's outputs and execution
// count from an .ipynb body. The hosted Lambda applies it too, for a
// workspace its registry marks personal_data (slice 4 review).
func NotebookWithoutOutputs(b []byte) ([]byte, error) {
	var nb map[string]any
	if err := json.Unmarshal(b, &nb); err != nil {
		return nil, err
	}
	if cells, ok := nb["cells"].([]any); ok {
		for _, c := range cells {
			if cell, ok := c.(map[string]any); ok && cell["cell_type"] == "code" {
				cell["outputs"] = []any{}
				cell["execution_count"] = nil
			}
		}
	}
	out, err := json.MarshalIndent(nb, "", " ")
	if err != nil {
		return nil, err
	}
	return append(out, '\n'), nil
}
