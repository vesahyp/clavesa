// Package bundle reads a Databricks Asset Bundle (Declarative Automation
// Bundle) the way the Databricks CLI resolves it, with no Databricks login,
// and plans how its jobs run on clavesa (ADR-031).
//
// The resolver covers the YAML part of a bundle: include globs, variables
// (default, target value, --var, BUNDLE_VAR_*), target overrides merged by
// task_key / job_cluster_key / environment_key, ${...} substitutions and
// presets. A conformance test compares its output with what
// `databricks bundle validate -o json` prints for the bundles in
// testdata/bundles/, so a CLI change to the merge rules fails a test before
// any deploy relies on it.
package bundle

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// ConfigFileNames are the bundle root file names the Databricks CLI accepts,
// in its order of preference.
var ConfigFileNames = []string{"databricks.yml", "databricks.yaml", "bundle.yml", "bundle.yaml"}

// FindRoot returns the bundle config file in dir.
func FindRoot(dir string) (string, error) {
	for _, n := range ConfigFileNames {
		p := filepath.Join(dir, n)
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}
	return "", fmt.Errorf("no databricks.yml in %s: --bundle must point at a bundle folder", dir)
}

// rawFile is one YAML file of the bundle, decoded into plain maps, with the
// file it came from (relative paths inside resources are relative to it).
type rawFile struct {
	Path string // absolute
	Rel  string // relative to the bundle root
	Tree map[string]any
}

// loadFiles reads the root file and every file its include globs match, in
// the CLI's order: the root first, then the includes sorted by path.
func loadFiles(root string) ([]rawFile, error) {
	cfg, err := FindRoot(root)
	if err != nil {
		return nil, err
	}
	first, err := readYAML(root, cfg)
	if err != nil {
		return nil, err
	}
	files := []rawFile{first}

	var patterns []string
	if inc, ok := first.Tree["include"].([]any); ok {
		for _, p := range inc {
			s, ok := p.(string)
			if !ok {
				return nil, fmt.Errorf("%s: include entries must be strings", first.Rel)
			}
			patterns = append(patterns, s)
		}
	}
	seen := map[string]bool{cfg: true}
	for _, pat := range patterns {
		matches, err := filepath.Glob(filepath.Join(root, pat))
		if err != nil {
			return nil, fmt.Errorf("include %q: %w", pat, err)
		}
		sort.Strings(matches)
		if len(matches) == 0 && !strings.ContainsAny(pat, "*?[") {
			return nil, fmt.Errorf("include %q: no such file", pat)
		}
		for _, m := range matches {
			if seen[m] {
				continue
			}
			seen[m] = true
			f, err := readYAML(root, m)
			if err != nil {
				return nil, err
			}
			files = append(files, f)
		}
	}
	return files, nil
}

func readYAML(root, path string) (rawFile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return rawFile{}, err
	}
	var tree map[string]any
	if err := yaml.Unmarshal(b, &tree); err != nil {
		return rawFile{}, fmt.Errorf("%s: %w", path, err)
	}
	if tree == nil {
		tree = map[string]any{}
	}
	rel, _ := filepath.Rel(root, path)
	return rawFile{Path: path, Rel: filepath.ToSlash(rel), Tree: normalize(tree).(map[string]any)}, nil
}

// normalize turns yaml.v3's decoded values into JSON-shaped values
// (map[string]any, []any, string, float64/int, bool, nil).
func normalize(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = normalize(val)
		}
		return out
	case map[any]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[fmt.Sprint(k)] = normalize(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = normalize(val)
		}
		return out
	default:
		return v
	}
}
