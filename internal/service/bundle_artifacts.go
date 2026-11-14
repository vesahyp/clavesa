package service

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/vesahyp/clavesa/internal/bundle"
)

// DefaultWheelBuild builds a Python wheel from a pyproject.toml or
// setup.py with no extra tools, when an artifact names no build command.
const DefaultWheelBuild = "python3 -m pip wheel --no-deps --wheel-dir dist ."

// buildArtifacts runs the build command of every whl artifact of the bundle
// in the copy of its files (root), the way `databricks bundle deploy` does
// on the developer's machine (ADR-031 section 5).
func buildArtifacts(ctx context.Context, b *bundle.Bundle, root string) error {
	for _, name := range sortedAnyKeys(b.Artifacts) {
		a, _ := b.Artifacts[name].(map[string]any)
		if t, _ := a["type"].(string); t != "" && t != "whl" {
			return fmt.Errorf("artifact %s: type %s is not supported (whl only)", name, t)
		}
		dir, _ := a["path"].(string)
		if dir == "" {
			dir = "."
		}
		build, _ := a["build"].(string)
		if build == "" {
			build = DefaultWheelBuild
		}
		cmd := exec.CommandContext(ctx, "sh", "-c", build)
		cmd.Dir = filepath.Join(root, dir)
		var out bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &out
		if err := cmd.Run(); err != nil {
			tail := strings.TrimSpace(out.String())
			if len(tail) > 1500 {
				tail = tail[len(tail)-1500:]
			}
			return fmt.Errorf("artifact %s: %q in %s failed: %v\n%s", name, build, dir, err, tail)
		}
	}
	return nil
}

// taskWheels resolves the wheel files a task installs: its whl libraries
// and the local wheel paths of its environment (globs, relative to the
// bundle root), in the built copy of the bundle.
func taskWheels(root string, jt *bundle.JobTask, envDeps []string) ([]string, error) {
	var patterns []string
	libs, _ := jt.Task["libraries"].([]any)
	for _, l := range libs {
		lm, _ := l.(map[string]any)
		if w, ok := lm["whl"].(string); ok {
			patterns = append(patterns, w)
		}
	}
	for _, d := range envDeps {
		if strings.HasSuffix(d, ".whl") && !strings.Contains(d, "://") && !strings.HasPrefix(d, "/") {
			patterns = append(patterns, d)
		}
	}
	var out []string
	for _, p := range patterns {
		matches, err := filepath.Glob(filepath.Join(root, p))
		if err != nil {
			return nil, err
		}
		if len(matches) == 0 {
			return nil, fmt.Errorf("task %s: no wheel matches %s (did the artifact build?)", jt.Key, p)
		}
		sort.Strings(matches)
		for _, m := range matches {
			rel, _ := filepath.Rel(root, m)
			out = append(out, path.Clean(filepath.ToSlash(rel)))
		}
	}
	return out, nil
}

func sortedAnyKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

var _ = os.Stat
